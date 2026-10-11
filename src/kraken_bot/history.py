"""Import official headerless Kraken OHLCVT archives with bounded disk usage."""

import hashlib
import json
import shutil
import tempfile
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .config import normalize_symbol
from .storage import CandleStore


def archive_members(archive: zipfile.ZipFile, symbols: list[str]) -> dict[str, str | None]:
    names = set(archive.namelist())
    result = {}
    for raw in symbols:
        symbol = normalize_symbol(raw)
        base, quote = symbol.split('/')
        bases = [base] + ({'BTC': ['XBT'], 'DOGE': ['XDG']}.get(base, []))
        matches = [f'{alias}{quote}_1.csv' for alias in bases
                   if f'{alias}{quote}_1.csv' in names]
        if len(matches) > 1:
            raise ValueError(f'Ambiguous archive members for {symbol}: {matches}')
        result[symbol] = matches[0] if matches else None
    return result


def import_csv(store: CandleStore, path: Path, symbol: str) -> dict:
    """Validate a whole member, then atomically insert without replacing live rows."""
    symbol = normalize_symbol(symbol)
    db = store.connection
    db.execute('DROP TABLE IF EXISTS historical_stage')
    malformed = db.execute("""SELECT count(*) FROM read_csv(?, header=false,
        columns={'epoch':'VARCHAR','open':'VARCHAR','high':'VARCHAR',
        'low':'VARCHAR','close':'VARCHAR','volume':'VARCHAR','trades':'VARCHAR'},
        strict_mode=true, null_padding=false)
        WHERE NOT regexp_full_match(epoch, '[0-9]+')
        OR NOT regexp_full_match(trades, '[0-9]+')""", [str(path)]).fetchone()[0]
    if malformed:
        raise ValueError(f'{symbol}: timestamps and trade counts must be integers')
    db.execute("""CREATE TEMP TABLE historical_stage AS SELECT
        epoch::BIGINT AS epoch, open::DECIMAL(38,18) AS open,
        high::DECIMAL(38,18) AS high, low::DECIMAL(38,18) AS low,
        close::DECIMAL(38,18) AS close, volume::DECIMAL(38,18) AS volume,
        trades::BIGINT AS trades FROM read_csv(?, header=false,
        columns={'epoch':'VARCHAR','open':'VARCHAR','high':'VARCHAR',
        'low':'VARCHAR','close':'VARCHAR','volume':'VARCHAR','trades':'VARCHAR'},
        strict_mode=true, null_padding=false)""", [str(path)])
    conflicting = db.execute('SELECT count(*) FROM (SELECT epoch FROM historical_stage GROUP BY epoch HAVING count(DISTINCT (open,high,low,close,volume,trades)) > 1)').fetchone()[0]
    if conflicting:
        raise ValueError(f'{symbol}: conflicting duplicate historical timestamps')
    invalid = db.execute("""SELECT count(*) FROM historical_stage WHERE
        epoch IS NULL OR epoch % 60 != 0 OR epoch < 0 OR epoch >= ?
        OR open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
        OR volume IS NULL OR trades IS NULL OR least(open,high,low,close,volume) < 0
        OR low > least(open,close) OR high < greatest(open,close) OR trades < 0
        """, [int(datetime.now(UTC).replace(second=0, microsecond=0).timestamp())]).fetchone()[0]
    if invalid:
        raise ValueError(f'{symbol}: {invalid} invalid or incomplete historical candles')
    rows, first, last = db.execute('SELECT count(*), min(epoch), max(epoch) FROM historical_stage').fetchone()
    before = db.execute('SELECT count(*) FROM candles WHERE symbol=?', [symbol]).fetchone()[0]
    db.execute('BEGIN')
    try:
        db.execute("""INSERT INTO candles SELECT ?, 1, to_timestamp(epoch),
            open,high,low,close,NULL,volume,trades FROM historical_stage
            QUALIFY row_number() OVER (PARTITION BY epoch ORDER BY epoch)=1
            ON CONFLICT DO NOTHING""", [symbol])
        after = db.execute('SELECT count(*) FROM candles WHERE symbol=?', [symbol]).fetchone()[0]
        db.execute('COMMIT')
    except BaseException:
        db.execute('ROLLBACK')
        raise
    finally:
        db.execute('DROP TABLE IF EXISTS historical_stage')
    return {'symbol': symbol, 'source_rows': rows, 'inserted_rows': after-before,
            'first_utc': datetime.fromtimestamp(first, UTC).isoformat() if first is not None else None,
            'last_utc': datetime.fromtimestamp(last, UTC).isoformat() if last is not None else None}


def import_archive(store: CandleStore, path: Path, symbols: list[str], report_path: Path) -> dict:
    with path.open('rb') as archive_file:
        digest = hashlib.file_digest(archive_file, 'sha256').hexdigest()
    report = {'complete': False, 'archive': path.name, 'sha256': digest, 'imported_at_utc': datetime.now(UTC).isoformat(),
              'vwap': 'unavailable in archive; stored as NULL',
              'omitted_minutes': 'no trades according to Kraken; not synthetic candles',
              'assets': []}
    with zipfile.ZipFile(path) as archive:
        if 'MANIFEST.json' in archive.namelist():
            report['manifest'] = json.loads(archive.read('MANIFEST.json'))
        coverage_end = report.get('manifest', {}).get('coverage', {}).get('end')
        cutoff = (datetime.fromisoformat(coverage_end).replace(tzinfo=UTC) + timedelta(days=1)
                  if coverage_end else None)
        members = archive_members(archive, symbols)
        for symbol, member in members.items():
            if member is None:
                item = {'symbol': symbol, 'status': 'not_in_archive', 'inserted_rows': 0}
            else:
                # Extract only one allowlisted member at a time; never trust ZIP paths.
                with tempfile.TemporaryDirectory(prefix='kraken-history-') as temporary:
                    csv_path = Path(temporary) / 'candles.csv'
                    with archive.open(member) as source, csv_path.open('wb') as destination:
                        shutil.copyfileobj(source, destination, length=1024*1024)
                    live_first = store.connection.execute(
                        'SELECT min(timestamp) FROM candles WHERE symbol=? AND timestamp >= ?',
                        [symbol, cutoff or datetime.min.replace(tzinfo=UTC)]).fetchone()[0]
                    item = {**import_csv(store, csv_path, symbol), 'member': member, 'status': 'imported'}
                    if live_first and item['last_utc']:
                        archive_last = (cutoff-timedelta(minutes=1) if cutoff else
                                        datetime.fromisoformat(item['last_utc']))
                        if (live_first-archive_last).total_seconds() > 60:
                            item['uncovered_between_archive_and_existing'] = {
                                'after_utc': archive_last.isoformat(), 'before_utc': live_first.isoformat(),
                                'minutes': int((live_first-archive_last).total_seconds()/60)-1,
                                'status': 'not_covered_by_this_archive_or_recent_OHLC_window'}
            report['assets'].append(item)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2, default=str)+'\n')
            print(json.dumps(item), flush=True)
    report['complete'] = True
    report_path.write_text(json.dumps(report, indent=2, default=str)+'\n')
    return report
