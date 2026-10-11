import json
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from kraken_bot.research import samples
from kraken_bot.storage import CandleStore
from kraken_bot.walk_forward import prepare_archive_source, split_fold, walk_forward

START = datetime(2026,4,1,tzinfo=UTC)


def make_csv(path, minutes):
    lines=[]
    for minute in minutes:
        price=100+minute/100
        lines.append(f'{int((START+timedelta(minutes=minute)).timestamp())},{price},{price+1},{price-1},{price},2,1')
    path.write_text('\n'.join(lines)+'\n')
    return path


def test_archive_carry_is_bounded_past_only_and_temporary(tmp_path):
    path=make_csv(tmp_path/'candles.csv',sorted(set(range(5,240,10)) | {59,119,179,239}))
    with CandleStore(':memory:') as store:
        counts=prepare_archive_source(store,path,'BTC/USD',START,START+timedelta(minutes=300))
        assert counts['research_minutes']==235
        assert counts['carried_no_trade_minutes']>0
        marked=store.connection.execute('SELECT close,volume,observed FROM research_source WHERE timestamp=?',
                                        [START+timedelta(minutes=6)]).fetchone()
        assert marked==pytest.approx((100.05,0,False))
        assert store.summary()==[]
        assert samples(store,'BTC/USD',START,START+timedelta(minutes=300))==[]
        eligible=samples(store,'BTC/USD',START,START+timedelta(minutes=300),archive_mode=True)
        assert len(eligible)==2
        # Future data changes targets but cannot alter earlier features or carries.
        path.write_text(path.read_text().replace(',101.79,102.79,100.79,101.79,',',901.79,902.79,900.79,901.79,'))
        prepare_archive_source(store,path,'BTC/USD',START,START+timedelta(minutes=300))
        changed=samples(store,'BTC/USD',START,START+timedelta(minutes=300),archive_mode=True)
        assert changed[0][1]==eligible[0][1]
        assert changed[0][2]!=eligible[0][2]


def test_endpoints_require_observed_trade_minutes(tmp_path):
    path=make_csv(tmp_path/'candles.csv',range(0,241,10))
    with CandleStore(':memory:') as store:
        prepare_archive_source(store,path,'BTC/USD',START,START+timedelta(minutes=300))
        # Hourly endpoint minute 59/119/... has no recorded trade.
        assert samples(store,'BTC/USD',START,START+timedelta(minutes=300),archive_mode=True)==[]


def test_fold_purge_and_nonoverlap():
    boundary=START+timedelta(days=28)
    rows=[(boundary+timedelta(minutes=m),(0,)*5,0,0,'BTC/USD') for m in (-120,-60,-30,0,60,120)]
    train,test=split_fold(rows,boundary,boundary+timedelta(minutes=120),60)
    assert [r[0] for r in train]==[boundary-timedelta(minutes=120),boundary-timedelta(minutes=60)]
    assert [r[0] for r in test]==[boundary,boundary+timedelta(minutes=60)]
    _,next_test=split_fold(rows,boundary+timedelta(minutes=120),boundary+timedelta(minutes=240),60)
    assert not set(r[0] for r in test)&set(r[0] for r in next_test)


def synthetic_archive(tmp_path):
    csv=make_csv(tmp_path/'source.csv',sorted(set(range(40*1440)) - {m for m in range(40*1440) if m%10 and m%60!=59}))
    path=tmp_path/'archive.zip'
    manifest={'product':'ohlcvt','has_header':False,'coverage':{'start':'2026-04-01','end':'2026-05-10'}}
    with zipfile.ZipFile(path,'w',compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('MANIFEST.json',json.dumps(manifest))
        archive.write(csv,'XBTUSD_1.csv')
    return path


def test_walk_forward_reports_sparse_asset_and_locks_training_scaling(tmp_path):
    archive=synthetic_archive(tmp_path)
    with CandleStore(':memory:') as store:
        report=walk_forward(store,['BTC/USD','LIGHTER/USD'],archive,tmp_path/'report.json',
                            START,START+timedelta(days=40))
        assert report['aggregate']['evaluated_folds']==2
        assert report['aggregate']['assets_with_eligible_samples']==1
        assert report['coverage'][1]['status']=='not_in_archive'
        assert all(f['status']=='evaluated' for f in report['folds'])
        assert report['folds'][0]['train_observations']<report['folds'][1]['train_observations']
        assert report['folds'][0]['end_utc']==report['folds'][1]['start_utc']
        assert (tmp_path/'report.md').exists()
        assert store.summary()==[]
        # Change data occurring after the first fold's training cutoff. The first
        # fitted scaler and coefficients must stay identical; later retraining
        # may incorporate that data once its labels have become available.
        changed_archive=tmp_path/'changed.zip'
        with zipfile.ZipFile(archive) as source, zipfile.ZipFile(changed_archive,'w') as target:
            target.writestr('MANIFEST.json',source.read('MANIFEST.json'))
            lines=[]
            for line in source.read('XBTUSD_1.csv').decode().splitlines():
                fields=line.split(',')
                if int(fields[0])>=int((START+timedelta(days=30)).timestamp()):
                    fields[1:5]=[str(float(value)+100) for value in fields[1:5]]
                lines.append(','.join(fields))
            target.writestr('XBTUSD_1.csv','\n'.join(lines)+'\n')
        changed=walk_forward(store,['BTC/USD','LIGHTER/USD'],changed_archive,tmp_path/'changed.json',
                             START,START+timedelta(days=40))
        assert changed['folds'][0]['model']==report['folds'][0]['model']
        assert changed['folds'][1]['model']!=report['folds'][1]['model']
        with pytest.raises(ValueError,match='outside this archive'):
            walk_forward(store,['BTC/USD'],archive,tmp_path/'bad.json',START,START+timedelta(days=90))
