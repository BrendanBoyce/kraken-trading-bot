import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
pytest.importorskip('sklearn')

from kraken_bot.net_model import (NET_FEATURES, choose_candidate, evaluate_later,
    evaluate_training, label_samples, outcome_summary, validate_plan)

PLAN = json.loads(Path('config/net-model-plan.json').read_text())
START = datetime(2026, 4, 1, tzinfo=UTC)


def row(i):
    win = i % 3 == 0
    outcome = {'status':'marked','gross_return':.03 if win else -.02,
        'positive_net':win,'exit_reason':'target' if win else 'stop',
        'ambiguous_stop_first':False,'stop_gap':False,'stale_timeout_mark':False}
    r={'symbol':'BTC/USD','decision_utc':START+timedelta(hours=4*i),
        'label_end_utc':START+timedelta(hours=4*i+4),
        'outcomes':{str(t):dict(outcome) for t in (3,4,5)}}
    r.update({feature: .8 if win else -.8 for feature in NET_FEATURES})
    return r


def test_purging_and_later_labels_cannot_fit_or_select_models():
    plan={**PLAN,'fit_end_utc':(START+timedelta(days=28)).isoformat(),
          'validation_end_utc':(START+timedelta(days=35)).isoformat(),
          'validation_quantiles':[.95],'learners':['logistic']}
    rows=[{**row(i),'symbol':symbol} for symbol in ('BTC/USD','ETH/USD') for i in range(42*6)]
    candidates,models,chosen=evaluate_training(rows,plan)
    modified=deepcopy(rows)
    for r in modified:
        if r['decision_utc']>=START+timedelta(days=35):
            for feature in NET_FEATURES:r[feature]=999
            for outcome in r['outcomes'].values():outcome['positive_net']=not outcome['positive_net']
    later=[r for r in modified if r['decision_utc']>=START+timedelta(days=35)]
    evaluate_later(later,candidates,models,plan)
    changed,_,second=evaluate_training(modified,plan)
    assert candidates==changed
    assert chosen==second
    assert candidates[0]['training_samples']==sum(r['label_end_utc']<=START+timedelta(days=28) for r in rows)


def test_support_stale_and_positive_net_gate():
    c={'id':'a','target_pct':3,'learner':'logistic','validation_quantile':.95,
       'validation':{'signals':20,'trades':20,'stale_timeout_marks':0,
        'net_by_cost_bps':{'8':{'wins':10,'mean_per_signal_pct':.2}}}}
    assert choose_candidate([c],PLAN)=='a'
    for field,value in [('signals',19),('trades',19),('stale_timeout_marks',1)]:
        changed=deepcopy(c);changed['validation'][field]=value
        assert choose_candidate([changed],PLAN) is None
    changed=deepcopy(c);changed['validation']['net_by_cost_bps']['8']['mean_per_signal_pct']=-.1
    assert choose_candidate([changed],PLAN) is None


def test_unfilled_signal_is_retained_as_zero_return_not_a_filtered_winner():
    r=row(0)
    nofill=row(1);nofill['outcomes']['3']={'status':'unfilled_no_entry_minute','positive_net':False}
    result=outcome_summary([r,nofill],3,[8])
    assert result['signals']==2 and result['trades']==1 and result['unfilled_entries']==1
    assert result['positive_label_precision']==.5
    assert result['net_by_cost_bps']['8']['mean_per_signal_pct']==pytest.approx(result['net_by_cost_bps']['8']['mean_pct']/2)


def test_new_labels_follow_exit_profit_not_old_ten_percent_touch(monkeypatch):
    r={'symbol':'BTC/USD','decision_utc':START,'rally':False,
       'mean_BTC_ETH_return_60m':None,'return_60m_minus_mean_BTC_ETH':None}
    bar={'timestamp':START,'open':100,'high':104,'low':99,'close':103}
    monkeypatch.setattr('kraken_bot.net_model.load_paths',lambda *args:{('BTC/USD',START):[bar]})
    label_samples([r],None,PLAN)
    assert r['rally'] is False
    assert r['outcomes']['3']['positive_net'] is True
    assert r['context_missing']==1 and r['market_return_60m']==0
    assert r['outcomes']['5']['stale_timeout_mark'] is True
    assert r['outcomes']['5']['positive_net'] is False


def test_invalid_chronology_and_cost_plan_rejected():
    validate_plan(PLAN)
    with pytest.raises(ValueError,match='boundaries'):
        validate_plan({**PLAN,'fit_end_utc':PLAN['later_test_start_utc']})
    with pytest.raises(ValueError,match='cost'):
        validate_plan({**PLAN,'selection_cost_bps':50})
