from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

from kraken_bot.exit_study import (exit_grid, load_paths, select_settings,
                                  simulate_exit, summarize_exits, validate_plan)

START = datetime(2026, 7, 1, tzinfo=UTC)


def bar(minute, opening=100, high=101, low=99.5, close=100):
    return {'timestamp': START + timedelta(minutes=minute), 'open': opening,
            'high': high, 'low': low, 'close': close}


def test_target_and_stop_order_and_ambiguous_bar():
    target_first = [bar(0, high=104), bar(1, low=96)]
    assert simulate_exit(START, target_first, 3, 2, 60)['gross_return'] == pytest.approx(.03)
    stop_first = [bar(0, low=97), bar(1, high=104)]
    assert simulate_exit(START, stop_first, 3, 2, 60)['gross_return'] == pytest.approx(-.02)
    ambiguous = simulate_exit(START, [bar(0, high=104, low=97)], 3, 2, 60)
    assert ambiguous['exit_reason'] == 'stop'
    assert ambiguous['ambiguous_stop_first']


def test_known_open_orders_target_before_later_low_and_worse_stop_gap():
    target = simulate_exit(START, [bar(0), bar(1, opening=105, high=106, low=96, close=100)], 3, 2, 60)
    assert target['exit_reason'] == 'target'
    assert not target['ambiguous_stop_first']
    gap = simulate_exit(START, [bar(0), bar(1, opening=90, high=95, low=89, close=92)], 3, 2, 60)
    assert gap['gross_return'] == pytest.approx(-.1)
    assert gap['stop_gap']


def test_entry_is_next_minute_open_and_future_after_deadline_cannot_affect_exit():
    bars = [bar(0, opening=102, high=103, low=101, close=102),
            bar(1, opening=102, high=103, low=101, close=103), bar(2, high=120)]
    result = simulate_exit(START, bars, 5, 2, 2)
    assert result['entry_price'] == 102
    assert result['exit_reason'] == 'timeout'
    assert result['exit_price'] == 103
    assert not result['stale_timeout_mark']
    assert simulate_exit(START, bars[:2], 5, 2, 2) == result


def test_missing_entry_does_not_become_delayed_fill_and_stale_marks_flagged():
    assert simulate_exit(START, [bar(1)], 3, 2, 60)['status'] == 'unfilled_no_entry_minute'
    stale = simulate_exit(START, [bar(0)], 3, 2, 60)
    assert stale['stale_timeout_mark']


def test_costs_and_losses_determine_expectancy_not_target_hits_alone():
    target = simulate_exit(START, [bar(0, high=104)], 3, 2, 60)
    stop = simulate_exit(START, [bar(0, low=97)], 3, 2, 60)
    summary = summarize_exits([target, stop, stop], [0,50,100])
    assert summary['net_by_cost_bps']['50']['mean_pct'] < 0
    assert summary['net_by_cost_bps']['50']['wins'] == 1
    assert summary['net_by_cost_bps']['50']['losses'] == 2
    assert summary['net_by_cost_bps']['50']['win_rate'] == pytest.approx(1/3)
    assert summary['net_by_cost_bps']['50']['worst_pct'] == pytest.approx(-2.49)
    assert summarize_exits([], [50])['net_by_cost_bps']['50']['mean_pct'] is None


def test_settings_use_only_validation_and_require_support_and_positive_net():
    plan = {'targets_pct':[3,4,5], 'stops_pct':[2], 'hold_minutes':[60],
            'combined_cost_bps':[50], 'selection_cost_bps':50, 'minimum_validation_trades':20}
    entries = [{'symbol':'A/USD','decision_utc':START}]
    path = {('A/USD',START):[bar(0, high=106)]}
    validation = exit_grid(entries,path,plan)
    assert select_settings(validation,plan) is None
    plan['minimum_validation_trades'] = 1
    assert select_settings(validation,plan) == {'target_pct':5,'stop_pct':2,'hold_minutes':60}
    original = select_settings(validation,plan)
    exit_grid(entries,{('A/USD',START):[bar(0, low=95)]},plan)
    assert select_settings(validation,plan) == original
    for candidate in validation:
        candidate['summary']['net_by_cost_bps']['50']['mean_pct'] = -.1
    assert select_settings(validation,plan) is None


def test_stale_timeout_cannot_support_settings_and_invalid_plan_fails():
    plan=json.loads(Path('config/exit-study-plan.json').read_text())
    validate_plan(plan)
    plan['stops_pct']=[-1]
    with pytest.raises(ValueError): validate_plan(plan)
    plan=json.loads(Path('config/exit-study-plan.json').read_text())
    candidate={'target_pct':3,'stop_pct':2,'hold_minutes':60,
        'summary': {'trades':30,'stale_timeout_marks':1,'net_by_cost_bps':{'50':{'mean_pct':1}}}}
    assert select_settings([candidate],plan) is None


def test_exit_paths_read_observed_bars_and_respect_archive_end(tmp_path):
    import zipfile
    archive=tmp_path/'test.zip'
    manifest={'product':'ohlcvt','has_header':False,'coverage':{'start':'2026-07-01','end':'2026-09-30'}}
    with zipfile.ZipFile(archive,'w') as z:
        z.writestr('MANIFEST.json',json.dumps(manifest))
        z.writestr('XBTUSD_1.csv',f'{int(START.timestamp())},100,101,99,100,1,1\n'
            f'{int((START+timedelta(minutes=1)).timestamp())},100,100,100,100,0,0\n'
            f'{int((START+timedelta(minutes=2)).timestamp())},100,104,99,103,2,2\n')
    entries=[{'symbol':'BTC/USD','decision_utc':START}]
    paths=load_paths(entries,archive,3)
    assert len(paths[('BTC/USD',START)])==2
    assert simulate_exit(START,paths[('BTC/USD',START)],3,2,3)['exit_reason']=='target'
    with pytest.raises(ValueError,match='coverage'):
        load_paths([{'symbol':'BTC/USD','decision_utc':datetime(2026,9,30,23,59,tzinfo=UTC)}],archive,3)
