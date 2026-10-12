import json
import zipfile
from datetime import UTC,datetime,timedelta

import pytest

from kraken_bot.storage import CandleStore
from kraken_bot.trade_history import COLUMNS,import_trades,stage_csv,write_features

START=datetime(2026,4,1,tzinfo=UTC)
EPOCH=int(START.timestamp())


def csv_file(tmp_path, rows):
    p=tmp_path/'trades.csv';p.write_text(','.join(COLUMNS)+'\n'+'\n'.join(rows)+'\n');return p


def test_precision_deduplication_and_volume_balance(tmp_path):
    rows=[f'{EPOCH}.123456789,100,2,b,l,,1', f'{EPOCH+1},110,1,s,m,,2']
    path=csv_file(tmp_path,rows+[rows[0]])
    with CandleStore(':memory:') as store:
        summary=stage_csv(store,path,'BTC/USD',START,START+timedelta(days=1))
        assert summary['trades']==2
        assert summary['duplicate_rows']==1
        from decimal import Decimal
        stamp=store.connection.execute('SELECT epoch_seconds FROM trade_unique WHERE trade_id=1').fetchone()[0]
        assert stamp==Decimal(f'{EPOCH}.123456789')
        assert summary['first_utc'].endswith('+00:00')
        parquet=tmp_path/'trades.parquet'
        store.connection.sql('SELECT * FROM trade_unique').write_parquet(str(parquet))
        # Use real schema via archive importer separately; verify aggregation in isolation.
        store.connection.execute("""CREATE TABLE trade_features AS SELECT symbol,
            timestamp,0::BIGINT trades,0::DECIMAL(38,18) volume,
            0::DOUBLE quote_volume,0::DOUBLE vwap,0::DOUBLE largest_trade_quote,
            0::DOUBLE mean_trade_quote,0::DECIMAL(38,18) buy_classified_volume,
            0::DECIMAL(38,18) sell_classified_volume,0::DOUBLE signed_volume_imbalance,
            0::DOUBLE signed_quote_imbalance,''::VARCHAR archive_sha256
            FROM trade_unique WHERE FALSE""")
        store.connection.execute('ALTER TABLE trade_features ADD PRIMARY KEY(symbol,timestamp)')
        assert write_features(store,parquet,'BTC/USD','hash')==1
        values=store.connection.execute('SELECT trades,volume,vwap,signed_volume_imbalance,signed_quote_imbalance FROM trade_features').fetchone()
        assert values[0]==2 and values[1]==3
        assert values[2]==pytest.approx(310/3)
        assert values[3]==pytest.approx(1/3)
        assert values[4]==pytest.approx(90/310)


@pytest.mark.parametrize('bad',[
    f'{EPOCH},100,-1,b,l,,1',f'{EPOCH},100,1,x,l,,1',
    f'{EPOCH}.1234567891,100,1,b,l,,1',f'{EPOCH-1},100,1,b,l,,1',
    f'{EPOCH},0,1,b,l,,1'])
def test_invalid_trades_rejected(tmp_path,bad):
    with CandleStore(':memory:') as store:
        with pytest.raises(ValueError):
            stage_csv(store,csv_file(tmp_path,[bad]),'BTC/USD',START,START+timedelta(days=1))
        assert store.summary()==[]


def test_conflicting_trade_id_rejected(tmp_path):
    path=csv_file(tmp_path,[f'{EPOCH},100,1,b,l,,1',f'{EPOCH},101,1,b,l,,1'])
    with CandleStore(':memory:') as store:
        with pytest.raises(ValueError,match='conflicting'):
            stage_csv(store,path,'BTC/USD',START,START+timedelta(days=1))


def test_archive_restart_no_duplicates_and_crc_provenance(tmp_path):
    archive=tmp_path/'quarter.zip'
    manifest={'product':'trades','has_header':True,'columns':COLUMNS,
              'coverage':{'start':'2026-04-01','end':'2026-06-30'}}
    with zipfile.ZipFile(archive,'w') as z:
        z.writestr('MANIFEST.json',json.dumps(manifest))
        z.writestr('XBTUSD.csv',','.join(COLUMNS)+f'\n{EPOCH},100,2,b,l,,1\n')
    db=tmp_path/'features.duckdb'
    report=import_trades(archive,['BTC/USD','LIGHTER/USD'],tmp_path/'raw',db)
    assert report['complete'] and report['total_trades']==1
    assert report['assets'][1]['status']=='not_in_archive'
    repeated=import_trades(archive,['BTC/USD'],tmp_path/'raw',db)
    assert repeated['assets'][0]['status']=='already_imported'
    with CandleStore(db,read_only=True) as store:
        assert store.connection.execute('SELECT count(*) FROM trade_features').fetchone()[0]==1
    raw=tmp_path/'raw/quarter/BTC_USD.parquet'
    raw.write_bytes(b'corrupted')
    with pytest.raises(ValueError,match='contents differ'):
        import_trades(archive,['BTC/USD'],tmp_path/'raw',db)


def test_nanosecond_near_minute_boundary_stays_in_exact_bucket(tmp_path):
    path=csv_file(tmp_path,[f'{EPOCH+59}.999999999,100,1,b,l,,1'])
    with CandleStore(':memory:') as store:
        stage_csv(store,path,'BTC/USD',START,START+timedelta(days=1))
        store.connection.execute("""SELECT (epoch_seconds-epoch_seconds%60)::BIGINT
                                    FROM trade_unique""")
        assert store.connection.fetchone()[0]==EPOCH
