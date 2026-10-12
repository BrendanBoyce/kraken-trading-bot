from datetime import UTC,datetime,timedelta

import pytest

from kraken_bot.pullback_study import analogues,previous_peak,recovery_outcome,summarize,wilson

START=datetime(2026,4,1,tzinfo=UTC)


def bars(n=120):
    return [{'timestamp':START+timedelta(hours=i),'high':100,'low':98,'close':99} for i in range(n)]


def test_reference_is_past_only_and_requires_contiguous_hours():
    series=bars()
    reference=previous_peak(series,30)
    for b in series[31:]:b['high']=999
    assert previous_peak(series,30)==reference
    assert previous_peak(series,10) is None
    shortened=series[:20]+series[21:]
    assert previous_peak(shortened,30) is None


def test_fall_before_recovery_same_bar_and_no_recovery():
    series=bars();series[24]['close']=95
    for b in series[25:73]:b.update(high=98,low=94,close=96)
    series[26]['low']=91
    series[30]['high']=100
    r=recovery_outcome(series,24,100)
    assert r['recovered_24h'] and r['recovered_48h']
    assert r['further_3pct_before_recovery'] and not r['recovered_before_further_3pct']
    series[26]['high']=100
    assert recovery_outcome(series,24,100)['ambiguous_same_hour']
    for b in series[25:73]:b['high']=98
    assert not recovery_outcome(series,24,100)['recovered_48h']


def test_recovery_first_and_future_outside_horizon_cannot_change_label():
    series=bars();series[24]['close']=95
    for b in series[25:73]:b.update(high=98,low=94,close=96)
    series[25]['high']=101
    series[40]['low']=90
    before=recovery_outcome(series,24,100)
    assert before['recovered_before_further_3pct']
    series[73]['low']=1
    assert recovery_outcome(series,24,100)==before
    assert recovery_outcome(series[:60],24,100) is None


def test_analogues_have_fixed_spacing_and_match_market_and_drop():
    series=bars(240)
    for b in series:b.update(close=95,low=94,high=100)
    market={b['timestamp']:-.01 for b in series}
    selected=analogues(series,.05,market,True)
    assert len(selected)>1
    assert all(b['decision_utc']-a['decision_utc']>=timedelta(hours=48) for a,b in zip(selected,selected[1:]))
    assert analogues(series,.05,market,False)==[]
    assert analogues(series,.20,market,True)==[]


def test_empty_samples_and_intervals_are_not_guaranteed_probability():
    assert summarize([])['cases']==0
    assert summarize([])['recovered_48h']['rate'] is None
    interval=wilson(20,20)
    assert 0<interval[0]<1 and interval[1]==pytest.approx(1)
    assert wilson(0,0) is None


def test_archive_newline_conventions_are_independent_and_duplicates_fail(tmp_path):
    import json,zipfile
    from kraken_bot.pullback_study import historical_hours
    archives=[]
    for i,newline in enumerate(('\r\n','\n')):
        path=tmp_path/f'{i}.zip';archives.append(path)
        epoch=int(START.timestamp())+i*3600
        with zipfile.ZipFile(path,'w') as z:
            z.writestr('MANIFEST.json',json.dumps({'product':'ohlcvt','has_header':False}))
            z.writestr('XBTUSD_1.csv',f'{epoch},2,3,1,2,1,1{newline}{epoch+60},2,4,1,3,1,1{newline}')
    result=historical_hours(['BTC/USD'],archives)['BTC/USD']
    assert len(result)==2 and result[0]['high']==4 and result[0]['close']==3
    with pytest.raises(ValueError,match='duplicate'):
        historical_hours(['BTC/USD'],[archives[0],archives[0]])
