from copy import deepcopy
from datetime import UTC,datetime,timedelta
import json
from pathlib import Path

import pytest

from kraken_bot.precision_trades import dollars,evaluate_policy,precision_gate,validate_plan

PLAN=json.loads(Path('config/precision-trade-plan.json').read_text())
START=datetime(2026,7,1,tzinfo=UTC)


def prediction(gross=.03,start=START,stale=False):
    return {'symbol':'BTC/USD','decision_utc':start.isoformat(),'score':.9,'positive_net':gross>0 and not stale,
        'outcome':{'status':'marked','gross_return':gross,'exit_bar_utc':start.isoformat(),
                   'exit_reason':'target','hold_minutes':240,'stale_timeout_mark':stale}}


def test_hundred_dollar_returns_include_winners_losers_and_costs():
    r=dollars([prediction(),prediction(-.02)],100,[8,20,50])
    assert r['costs']['8']['sum_pnl_usd']==pytest.approx(2.9176-2.0784)
    assert r['costs']['8']['mean_pnl_usd']==pytest.approx((2.9176-2.0784)/2)
    assert r['costs']['8']['wins']==1 and r['costs']['8']['losses']==1
    assert r['notional_per_trade_usd']==100
    assert r['peak_nominal_capital_usd']==200


def test_zero_selection_is_undefined_and_unfilled_and_stale_are_explicit():
    empty=dollars([],100,[8])
    assert empty['costs']['8']['win_rate'] is None
    assert empty['costs']['8']['mean_pnl_usd'] is None
    nofill=prediction();nofill['positive_net']=False;nofill['outcome']={'status':'unfilled_no_entry_minute'}
    r=dollars([nofill,prediction(stale=True)],100,[8])
    assert r['unfilled']==1 and r['stale_timeout_marks']==1
    assert r['net_positive_labels']==0
    assert r['costs']['8']['known_marks_sum_pnl_usd']==0
    assert not r['ready_for_trading']


def test_insufficient_validation_winners_abstain_including_ties():
    rows=[{'score':.9,'actual_rally':True} for _ in range(19)]
    assert precision_gate(rows,1.0,20,.8)['threshold'] is None
    rows.append({'score':.9,'actual_rally':False})
    assert precision_gate(rows,1.0,20,.8)['threshold'] is None
    assert precision_gate(rows,.95,20,.8)['threshold']==.9


def test_perfect_validation_does_not_guarantee_later_wins_or_change_gate():
    rows=[{'score':.9,'actual_rally':True} for _ in range(20)]
    policy={'kind':'empirical_precision','value':1.0}
    r=evaluate_policy(rows,[prediction(-.02)],policy,PLAN,.8)
    changed=evaluate_policy(rows,[prediction(.03)],policy,PLAN,.8)
    assert r['validation_gate']==changed['validation_gate']
    assert r['later']['costs']['8']['losses']==1
    assert not r['ready_for_trading']


def test_threshold_can_only_reduce_old_scope_and_wrong_size_rejected():
    rows=[{'score':i/100,'actual_rally':i%2==0} for i in range(100)]
    later=[{**prediction(),'score':s} for s in (.96,.97,.99)]
    r=evaluate_policy(rows,later,{'kind':'quantile','value':.99},PLAN,.95)
    assert r['later']['signals']<len(later)
    assert r['status']=='fixed_diagnostic_not_approved'
    validate_plan(PLAN)
    with pytest.raises(ValueError,match='100'):
        validate_plan({**PLAN,'notional_usd':1000})


def test_exit_before_new_entry_does_not_double_count_capital_and_timeouts_hold():
    first=prediction();second=prediction(start=START+timedelta(minutes=1))
    r=dollars([first,second],100,[8])
    assert r['peak_simultaneous_positions']==1
    first['outcome']['exit_reason']='timeout'
    assert dollars([first,second],100,[8])['peak_simultaneous_positions']==2
