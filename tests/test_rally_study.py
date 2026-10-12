from datetime import UTC, datetime, timedelta

import pytest

from kraken_bot.rally_study import (choose_condition, condition_stats, episodes,
    evaluate_periods, event_samples, matched_description, net_close, prepare_features)
from kraken_bot.storage import CandleStore

START=datetime(2026,4,1,tzinfo=UTC)


def row(hour=0,rally=False,volume=1,symbol='BTC/USD',**changes):
    value={'symbol':symbol,'decision_utc':START+timedelta(hours=hour),
        'label_end_utc':START+timedelta(hours=hour+4),'rally':rally,
        'close_return':0,'high_return':0.12 if rally else 0,'low_return':-0.01,
        'volume_ratio':volume,'return_60m':0,'return_240m':0,'range_60m':0.03,
        'volatility_60m':0.01,'traded_fraction_60m':1,'breakout':False,
        'future_observed':True,'path_outcome':'target_before_downside' if rally else 'neither_level'}
    value.update(changes)
    return value


def test_first_crossings_and_intraminute_ambiguity():
    with CandleStore(':memory:') as store:
        store.connection.execute("""CREATE TEMP TABLE research_source AS
            SELECT 'BTC/USD' AS symbol, ?::TIMESTAMPTZ+i*INTERVAL '1 minute' AS timestamp,
                100.0 AS close,100.0 AS high,100.0 AS low,1.0 AS volume,TRUE AS observed
            FROM range(1300) t(i)""",[START])
        for minute,high,low in [(490,100,94),(500,111,100),(739,112,94),(1000,111,100),(1001,100,94)]:
            store.connection.execute('UPDATE research_source SET high=?,low=? WHERE timestamp=?',
                                     [high,low,START+timedelta(minutes=minute)])
        prepare_features(store)
        values=event_samples(store,'BTC/USD',240)
        assert [r['path_outcome'] for r in values]==[
            'downside_before_target','ambiguous_same_minute','target_before_downside']
        assert all(r['rally'] for r in values)
        first_features=tuple(values[0][f] for f in ('return_60m','range_60m','volume_ratio'))
        store.connection.execute('UPDATE research_source SET high=999 WHERE timestamp=?',
                                 [START+timedelta(minutes=501)])
        prepare_features(store)
        assert tuple(event_samples(store,'BTC/USD',240)[0][f] for f in ('return_60m','range_60m','volume_ratio'))==first_features
        # Future endpoint has no trade: label remains eligible and is marked.
        store.connection.execute('UPDATE research_source SET observed=FALSE WHERE timestamp=?',
                                 [START+timedelta(minutes=719)])
        prepare_features(store)
        assert event_samples(store,'BTC/USD',240)[0]['future_observed'] is False
        assert store.summary()==[]


def test_cost_proxy_does_not_turn_high_into_profit():
    case=row(rally=True,close_return=-0.02)
    assert net_close(case,0)==pytest.approx(-2)
    assert net_close(case,100)==pytest.approx(-2.98)


def test_same_asset_week_controls_without_reuse():
    values=[row(4,True,4),row(12,True,6),row(8,False,1),
            row(400,True,9),row(8,False,99,symbol='ETH/USD')]
    report=matched_description(values)
    assert report['matched_rally_windows']==1
    assert report['unmatched_rally_windows']==2
    assert report['feature_medians']['volume_ratio']['control']==1


def test_asset_matched_baseline_avoids_cross_asset_lift():
    # Condition merely selects the high-event-rate asset: adjustment removes lift.
    values=[row(i*4,i%2==0,4,'BTC/USD') for i in range(10)]
    values += [row(i*4,i%2==0,1,'BTC/USD') for i in range(10,20)]
    values += [row(i*4,False,1,'ETH/USD') for i in range(50)]
    result=condition_stats(values,'volume_surge',(0,100))
    assert result['asset_matched_control_rate']==0.5
    assert result['lift']==1
    assert result['false_positives']==5


def test_training_selection_is_purged_and_future_outcomes_cannot_choose():
    values=[row(i*4,i%10==0,4 if i%2==0 else 1) for i in range(230)]
    report,_=evaluate_periods(values,START,START+timedelta(days=40),240,(0,50))
    boundary=START+timedelta(days=28)
    first=report['folds'][0]
    assert first['training_observations']==sum(r['label_end_utc']<=boundary for r in values)
    changed=[{**r,'rally':not r['rally']} if r['decision_utc']>=boundary else r for r in values]
    modified,_=evaluate_periods(changed,START,START+timedelta(days=40),240,(0,50))
    assert first['training_candidates']==modified['folds'][0]['training_candidates']
    assert first['selected_hypothesis']==modified['folds'][0]['selected_hypothesis']


def test_unsupported_condition_and_episode_merging():
    assert choose_condition([row(i*4,True,4) for i in range(5)],(0,))[0] is None
    assert episodes([row(0,True),row(4,True),row(12,True)])==2


def test_end_to_end_report_missing_pair_and_archive_boundaries(tmp_path):
    import json
    import zipfile
    from kraken_bot.rally_study import rally_study
    archive=tmp_path/'archive.zip'
    lines=[]
    for minute in range(35*1440):
        stamp=int((START+timedelta(minutes=minute)).timestamp())
        high=112 if minute%1440==720 else 100
        lines.append(f'{stamp},100,{high},100,100,1,1')
    with zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr('MANIFEST.json',json.dumps({'product':'ohlcvt','has_header':False,
            'coverage':{'start':'2026-04-01','end':'2026-05-05'}}))
        z.writestr('XBTUSD_1.csv','\n'.join(lines)+'\n')
    with CandleStore(':memory:') as store:
        report=rally_study(store,['BTC/USD','LIGHTER/USD'],archive,tmp_path/'study.json',
                           START,START+timedelta(days=35),horizons=(240,),costs=(0,50))
        assert report['coverage'][1]['status']=='not_in_archive'
        experiment=report['experiments'][0]
        assert experiment['chronological']['test_baseline']['rallies']>0
        assert experiment['chronological']['selected_test']['observations']==0
        assert experiment['matched_test_description']['matched_rally_windows']>0
        assert (tmp_path/'study.md').exists()
        assert store.summary()==[]
        with pytest.raises(ValueError,match='outside archive'):
            rally_study(store,['BTC/USD'],archive,tmp_path/'bad.json',START,START+timedelta(days=90))
