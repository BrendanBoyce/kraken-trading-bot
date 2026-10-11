import json
import zipfile
from decimal import Decimal

import pytest

from kraken_bot.history import import_archive, import_csv
from kraken_bot.storage import CandleStore
from test_storage import candle


def csv_file(tmp_path, rows):
    path = tmp_path / 'history.csv'
    path.write_text('\n'.join(rows)+'\n')
    return path


def test_import_deduplication_utc_and_unknown_vwap(tmp_path):
    stamp = int(candle(0).timestamp.timestamp())
    path = csv_file(tmp_path, [f'{stamp+i*60},10,12,9,11,2,3' for i in range(5)])
    with CandleStore(':memory:') as store:
        store.append([candle(0)])
        report = import_csv(store, path, 'BTC/USD')
        assert report['inserted_rows'] == 4
        assert import_csv(store, path, 'BTC/USD')['inserted_rows'] == 0
        rows = store.recent('BTC/USD', limit=5)
        assert rows[-1]['open'] == candle(0).open
        assert rows[0]['vwap'] is None
        assert rows[0]['timestamp'].utcoffset().total_seconds() == 0
        assert store.recent('BTC/USD', 5)[0]['vwap'] is None


@pytest.mark.parametrize('bad', ['1,10,12,9,11,2,3', '60,10,9,12,11,2,3',
                                '60,10,12,9,11,-1,3', '60,10,12,9,11,2,wrong',
                                '60,10,12,9,11,2,3,extra', '60.4,10,12,9,11,2,3',
                                '60,10,12,9,11,2,3.2'])
def test_invalid_import_atomic(tmp_path, bad):
    path = csv_file(tmp_path, ['0,10,12,9,11,2,3', bad])
    with CandleStore(':memory:') as store:
        with pytest.raises(Exception):
            import_csv(store, path, 'BTC/USD')
        assert store.summary() == []


def test_zip_allowlist_aliases_gaps_and_report(tmp_path):
    path = tmp_path / 'archive.zip'
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('XBTUSD_1.csv', '0,10,12,9,11,2,3\n120,10,12,9,11,2,3\n')
        archive.writestr('ETHUSD_5.csv', 'unselected')
        archive.writestr('../escape', 'unselected')
    report_path = tmp_path / 'report.json'
    with CandleStore(':memory:') as store:
        store.append([candle(0)])
        report = import_archive(store, path, ['BTC/USD','ETH/USD'], report_path)
        assert report['assets'][0]['inserted_rows'] == 2
        assert report['assets'][0]['uncovered_between_archive_and_existing']['minutes'] > 0
        assert report['assets'][1]['status'] == 'not_in_archive'
        assert store.connection.execute('SELECT count(*) FROM candles WHERE timestamp=to_timestamp(60)').fetchone()[0] == 0
        assert json.loads(report_path.read_text())['sha256'] == report['sha256']


def test_old_schema_migration(tmp_path):
    path = tmp_path/'old.duckdb'
    with CandleStore(path) as store:
        store.connection.execute('ALTER TABLE candles ALTER COLUMN vwap SET NOT NULL')
        store.append([candle(0)])
    with CandleStore(path) as store:
        from dataclasses import replace
        assert store.append([replace(candle(1), vwap=None)]) == 1
        assert store.recent('BTC/USD')[0]['vwap'] is None


def test_conflicting_duplicate_rejected(tmp_path):
    path = csv_file(tmp_path, ['0,10,12,9,11,2,3', '0,10,12,9,10,2,3'])
    with CandleStore(':memory:') as store:
        with pytest.raises(ValueError, match='conflicting duplicate'):
            import_csv(store, path, 'BTC/USD')
        assert store.summary() == []
