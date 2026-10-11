from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from kraken_bot.research import fit, predict, samples, research
from kraken_bot.storage import CandleStore
from test_storage import START, candle


def test_known_linear_relation_and_train_only_scaling():
    train = [(START,(float(i),0,0,0,0),float(i)*0.01,0) for i in range(100)]
    model = fit(train, penalty=0.001)
    assert model['means'][0] == 49.5
    assert predict(model,(150,0,0,0,0)) == pytest.approx(1.5,rel=0.001)
    assert model['means'][0] == 49.5


def test_complete_windows_targets_and_past_only_features():
    rows = [replace(candle(i),open=Decimal(100+i),close=Decimal(100+i),
                    low=Decimal(100+i),high=Decimal(100+i),volume=Decimal(1))
            for i in range(240)]
    with CandleStore(':memory:') as store:
        store.append(rows)
        result = samples(store,'BTC/USD',START,START+timedelta(minutes=240))
        assert len(result)==2
        stamp, features, target, high = result[0]
        assert stamp == START+timedelta(minutes=120)
        assert features[2] == pytest.approx(219/159-1)
        assert target == pytest.approx(279/219-1)
        store.connection.execute('UPDATE candles SET high=9999 WHERE timestamp=?', [START+timedelta(minutes=150)])
        changed = samples(store,'BTC/USD',START,START+timedelta(minutes=240))[0]
        assert changed[1] == features
        assert changed[3]>high
        store.connection.execute('DELETE FROM candles WHERE timestamp=?', [START+timedelta(minutes=130)])
        assert samples(store,'BTC/USD',START,START+timedelta(minutes=240))==[]


def test_chronological_purge_and_gap_exclusion(tmp_path):
    with CandleStore(':memory:') as store:
        # 200 hourly decisions, with a varying deterministic relationship.
        # Efficient fixture insert using DuckDB, avoiding huge Python VALUES batches.
        store.connection.execute("""INSERT INTO candles SELECT 'BTC/USD',1,
            ?::TIMESTAMPTZ+i*INTERVAL '1 minute',100+i%100,100+i%100,
            100+i%100,100+i%100,NULL,2,3 FROM range(12000) t(i)""", [START])
        report = research(store,['BTC/USD','ETH/USD'],tmp_path/'report.json',
                          START,START+timedelta(hours=200))
        assert report['train_observations']>100
        assert report['test']['observations']>30
        assert report['coverage'][1]['eligible_samples']==0
        from datetime import datetime
        cutoff = datetime.fromisoformat(report['test_start_utc'])
        all_rows=samples(store,'BTC/USD',START,START+timedelta(hours=200))
        expected=sum(row[0]+timedelta(hours=1)<=cutoff for row in all_rows)
        assert report['train_observations']==expected
        assert (tmp_path/'report.json').exists()
