"""Validated official trade archives, raw Parquet and minute-level enrichment."""

import hashlib
import json
import logging
import shutil
import tempfile
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .config import normalize_symbol
from .storage import CandleStore

LOG=logging.getLogger(__name__)
COLUMNS=['timestamp','price','volume','type','order_type','misc','trade_id']


def digest(path):
    with path.open('rb') as source: return hashlib.file_digest(source,'sha256').hexdigest()


def members(archive,symbols):
    names=set(archive.namelist()); result={}
    for raw in symbols:
        symbol=normalize_symbol(raw);base,quote=symbol.split('/')
        aliases=[base]+{'BTC':['XBT'],'DOGE':['XDG']}.get(base,[])
        matches=[f'{alias}{quote}.csv' for alias in aliases if f'{alias}{quote}.csv' in names]
        if len(matches)>1: raise ValueError(f'Ambiguous trade archive pair: {symbol}')
        result[symbol]=matches[0] if matches else None
    return result


def stage_csv(store,path,symbol,start,end):
    db=store.connection
    db.execute('DROP TABLE IF EXISTS trade_raw')
    db.execute("""CREATE TEMP TABLE trade_raw AS SELECT * FROM read_csv(?,header=true,
        columns={'timestamp':'VARCHAR','price':'VARCHAR','volume':'VARCHAR','type':'VARCHAR',
                 'order_type':'VARCHAR','misc':'VARCHAR','trade_id':'VARCHAR'},
        strict_mode=true,null_padding=false)""",[str(path)])
    invalid=db.execute("""SELECT count(*) FROM trade_raw WHERE timestamp IS NULL
        OR NOT regexp_full_match(timestamp,'[0-9]+(\\.[0-9]{1,9})?')
        OR trade_id IS NULL OR NOT regexp_full_match(trade_id,'[0-9]+')
        OR type IS NULL OR type NOT IN ('b','s') OR order_type IS NULL OR order_type NOT IN ('l','m')
        OR price IS NULL OR volume IS NULL
        OR NOT regexp_full_match(price,'[0-9]+(\\.[0-9]{1,18})?')
        OR NOT regexp_full_match(volume,'[0-9]+(\\.[0-9]{1,18})?')""").fetchone()[0]
    if invalid: raise ValueError(f'{symbol}: {invalid} malformed trade records')
    db.execute('DROP TABLE IF EXISTS trade_stage')
    db.execute("""CREATE TEMP TABLE trade_stage AS SELECT ? AS symbol,
        timestamp::DECIMAL(24,9) AS epoch_seconds,
        price::DECIMAL(38,18) AS price,volume::DECIMAL(38,18) AS volume,
        type AS side,order_type,coalesce(misc,'') AS misc,trade_id::BIGINT AS trade_id
        FROM trade_raw""",[symbol])
    invalid=db.execute("""SELECT count(*) FROM trade_stage WHERE price<=0 OR volume<=0
        OR trade_id<0 OR epoch_seconds<? OR epoch_seconds>=?""",
        [int(start.timestamp()),int(end.timestamp())]).fetchone()[0]
    if invalid: raise ValueError(f'{symbol}: {invalid} invalid values or trades outside manifest coverage')
    conflicting=db.execute("""SELECT count(*) FROM (SELECT trade_id FROM trade_stage
        GROUP BY trade_id HAVING count(DISTINCT (epoch_seconds,price,volume,side,order_type,misc))>1)""").fetchone()[0]
    if conflicting: raise ValueError(f'{symbol}: conflicting repeated trade IDs')
    source_rows=db.execute('SELECT count(*) FROM trade_stage').fetchone()[0]
    db.execute('DROP TABLE IF EXISTS trade_unique')
    db.execute("""CREATE TEMP TABLE trade_unique AS SELECT *,
        make_timestamp((epoch_seconds*1000000)::BIGINT)::TIMESTAMPTZ AS timestamp
        FROM trade_stage QUALIFY row_number() OVER (PARTITION BY trade_id ORDER BY epoch_seconds)=1""")
    rows,first,last=db.execute('SELECT count(*),min(timestamp),max(timestamp) FROM trade_unique').fetchone()
    return {'source_rows':source_rows,'trades':rows,'duplicate_rows':source_rows-rows,
            'first_utc':first.isoformat() if first else None,'last_utc':last.isoformat() if last else None}


def write_features(store,path,symbol,archive_hash):
    db=store.connection
    db.execute('DROP TABLE IF EXISTS trade_minutes')
    db.execute("""CREATE TEMP TABLE trade_minutes AS SELECT symbol,
        to_timestamp((epoch_seconds-epoch_seconds%60)::BIGINT) AS timestamp,
        count(*) AS trades,sum(volume) AS volume,
        sum(price::DOUBLE*volume::DOUBLE) AS quote_volume,
        sum(price::DOUBLE*volume::DOUBLE)/sum(volume::DOUBLE) AS vwap,
        max(price::DOUBLE*volume::DOUBLE) AS largest_trade_quote,
        avg(price::DOUBLE*volume::DOUBLE) AS mean_trade_quote,
        sum(CASE WHEN side='b' THEN volume ELSE 0 END) AS buy_classified_volume,
        sum(CASE WHEN side='s' THEN volume ELSE 0 END) AS sell_classified_volume,
        sum(CASE WHEN side='b' THEN volume::DOUBLE ELSE -volume::DOUBLE END)/sum(volume::DOUBLE) AS signed_volume_imbalance,
        sum(CASE WHEN side='b' THEN price::DOUBLE*volume::DOUBLE ELSE -price::DOUBLE*volume::DOUBLE END)
            /sum(price::DOUBLE*volume::DOUBLE) AS signed_quote_imbalance
        FROM read_parquet(?) GROUP BY symbol,(epoch_seconds-epoch_seconds%60)""",[str(path)])
    conflicts=db.execute('''SELECT count(*) FROM trade_features t JOIN trade_minutes s
        USING(symbol,timestamp) WHERE t.archive_sha256<>?''',[archive_hash]).fetchone()[0]
    if conflicts:
        raise ValueError(f'{symbol}: overlapping minute features from a different archive')
    # Keep a separate offline DB: no live collector lock or candle overwrites.
    db.execute('BEGIN')
    try:
        db.execute('DELETE FROM trade_features WHERE symbol=? AND archive_sha256=?',[symbol,archive_hash])
        db.execute("""INSERT INTO trade_features SELECT *, ? AS archive_sha256 FROM trade_minutes
            ON CONFLICT DO NOTHING""",[archive_hash])
        db.execute('COMMIT')
    except BaseException:
        db.execute('ROLLBACK');raise
    return db.execute('SELECT count(*) FROM trade_minutes').fetchone()[0]


def import_trades(archive_path,symbols,output_dir,db_path):
    archive_hash=digest(archive_path)
    destination=output_dir/archive_path.stem
    destination.mkdir(parents=True,exist_ok=True)
    report_path=destination/'report.json'
    report={'complete':False,'archive':archive_path.name,'sha256':archive_hash,
        'assets':[],'precision':'Original epoch seconds retained exactly to 9 decimals. UTC timestamp is a rounded microsecond-resolution convenience; exact epoch seconds are authoritative. Raw prices/volumes DECIMAL(38,18); derived quote values and VWAP use double precision.',
        'meaning':'Buy/sell fields are archive trade classifications, not order-book depth, capital inflows or executable quotes.'}
    with zipfile.ZipFile(archive_path) as archive, CandleStore(db_path) as store:
        manifest=json.loads(archive.read('MANIFEST.json'))
        if manifest.get('product')!='trades' or manifest.get('has_header') is not True or manifest.get('columns')!=COLUMNS:
            raise ValueError('Expected Kraken seven-column, headered trade archive')
        start=datetime.fromisoformat(manifest['coverage']['start']).replace(tzinfo=UTC)
        end=datetime.fromisoformat(manifest['coverage']['end']).replace(tzinfo=UTC)+timedelta(days=1)
        if end>datetime.now(UTC): raise ValueError('Archive coverage extends into the future')
        report['manifest']=manifest
        store.connection.execute("""CREATE TABLE IF NOT EXISTS trade_features (
            symbol VARCHAR, timestamp TIMESTAMPTZ,trades BIGINT,volume DECIMAL(38,18),
            quote_volume DOUBLE,vwap DOUBLE,largest_trade_quote DOUBLE,mean_trade_quote DOUBLE,
            buy_classified_volume DECIMAL(38,18),sell_classified_volume DECIMAL(38,18),
            signed_volume_imbalance DOUBLE,signed_quote_imbalance DOUBLE,archive_sha256 VARCHAR,
            PRIMARY KEY(symbol,timestamp))""")
        for symbol,member in members(archive,symbols).items():
            if member is None:
                item={'symbol':symbol,'status':'not_in_archive','trades':0}
            else:
                parquet=destination/(symbol.replace('/','_')+'.parquet')
                metadata=parquet.with_suffix('.json')
                if parquet.exists():
                    saved=json.loads(metadata.read_text())
                    if saved['archive_sha256']!=archive_hash or saved['parquet_sha256']!=digest(parquet):
                        raise ValueError(f'{symbol}: existing Parquet provenance or contents differ; refusing overwrite')
                    item={**saved['summary'],'status':'already_imported'}
                else:
                    required=archive.getinfo(member).file_size+512*1024**2
                    if shutil.disk_usage(destination).free<required:
                        raise OSError(f'Insufficient free disk to process {symbol}')
                    with tempfile.TemporaryDirectory(prefix='kraken-trades-',dir=destination) as temporary:
                        csv=Path(temporary)/'trades.csv'
                        with archive.open(member) as source,csv.open('wb') as target:
                            shutil.copyfileobj(source,target,length=1024*1024)
                        # Verify the real header, rather than assigning column names to mismatched CSVs.
                        with csv.open() as header_file:
                            header=header_file.readline().strip().split(',')
                        if header!=COLUMNS:
                            raise ValueError(f'{symbol}: unexpected CSV header')
                        summary=stage_csv(store,csv,symbol,start,end)
                        part=Path(temporary)/'trades.parquet'
                        store.connection.sql('SELECT * FROM trade_unique ORDER BY epoch_seconds,trade_id').write_parquet(str(part),compression='zstd')
                        part.replace(parquet)
                        item={'symbol':symbol,'member':member,'status':'imported',**summary}
                        metadata.write_text(json.dumps({'archive_sha256':archive_hash,
                            'parquet_sha256':digest(parquet),'summary':item},indent=2)+'\n')
                item['feature_minutes']=write_features(store,parquet,symbol,archive_hash)
                item['parquet']=str(parquet)
            report['assets'].append(item)
            report_path.write_text(json.dumps(report,indent=2)+'\n')
            LOG.info('%s: %s trades (%s)',symbol,item['trades'],item['status'])
            for name in ('trade_raw','trade_stage','trade_unique','trade_minutes'):
                store.connection.execute(f'DROP TABLE IF EXISTS {name}')
    report['complete']=True
    report['total_trades']=sum(item['trades'] for item in report['assets'])
    report['feature_minutes']=sum(item.get('feature_minutes',0) for item in report['assets'])
    report['feature_db']=str(db_path)
    report_path.write_text(json.dumps(report,indent=2)+'\n')
    return report
