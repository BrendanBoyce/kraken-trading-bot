from datetime import UTC, datetime, timedelta
import json
import math

import pytest

from kraken_bot.filter_study import add_context, choose_filter, digest, evaluate_filters, load_model, passes

PLAN = {
    'filters': {'none': {}, 'liquid': {'trade_count_60m_min': 100},
                'relative': {'return_60m_minus_mean_BTC_ETH_min': .01}},
    'combinations': {'both': ['liquid', 'relative']},
    'selection_policy': {'minimum_cumulative_validation_selections': 10,
                         'minimum_empirical_precision': .5, 'requires_better_than_baseline': True}}


def test_context_uses_closed_minute_and_missing_reference_abstains():
    decision = datetime(2026, 7, 1, 4, tzinfo=UTC)
    stamp = decision - timedelta(minutes=1)
    row = {'decision_utc': decision, 'return_60m': .04,
           'log_trade_count_60m': math.log1p(100), 'log_mean_trade_quote': math.log1p(500)}
    references = {symbol: {stamp: .01, decision: 999} for symbol in ('BTC/USD', 'ETH/USD')}
    result = add_context(row, references)
    assert result['mean_BTC_ETH_return_60m'] == .01
    assert result['return_60m_minus_mean_BTC_ETH'] == pytest.approx(.03)
    assert result['quote_volume_60m'] == pytest.approx(50000)
    references['ETH/USD'].pop(stamp)
    assert not passes(add_context(row, references), 'relative', PLAN)


def test_filter_rejects_nonfinite_missing_and_combinations():
    assert passes({'trade_count_60m': 100, 'return_60m_minus_mean_BTC_ETH': .02}, 'both', PLAN)
    assert not passes({'trade_count_60m': 99, 'return_60m_minus_mean_BTC_ETH': .02}, 'both', PLAN)
    assert not passes({'trade_count_60m': float('nan')}, 'liquid', PLAN)
    assert not passes({}, 'liquid', PLAN)


def test_insufficient_validation_support_abstains_even_with_zero_errors():
    result = {'none': {'selections': 50, 'precision': .1},
              'liquid': {'selections': 9, 'precision': 1.0}}
    assert choose_filter(result, PLAN) is None
    result['liquid']['selections'] = 10
    assert choose_filter(result, PLAN) == 'liquid'
    result['liquid']['precision'] = .49
    assert choose_filter(result, PLAN) is None


def test_holdout_outcomes_do_not_enter_validation_selection():
    stamp = datetime(2026, 7, 1, tzinfo=UTC)
    rows = [{'symbol': 'A/USD', 'decision_utc': stamp, 'actual_rally': i < 6,
             'selected_by_score': True, 'trade_count_60m': 100 if i < 10 else 1,
             'return_60m_minus_mean_BTC_ETH': 0, 'close_return': 0, 'worst_downside': -.1}
            for i in range(20)]
    validation = evaluate_filters(rows, PLAN)
    chosen = choose_filter(validation, PLAN)
    assert chosen == 'liquid'
    changed = [{**r, 'actual_rally': not r['actual_rally']} for r in rows]
    evaluate_filters(changed, PLAN)
    assert choose_filter(validation, PLAN) == chosen
    empty = evaluate_filters([{**r, 'selected_by_score': False} for r in rows], PLAN)['none']
    assert empty['precision'] is None and empty['ready_for_trading'] is False


def test_checksum_rejects_model_before_deserialization(tmp_path):
    pytest.importorskip('joblib')
    path = tmp_path / 'untrusted.joblib'
    path.write_text('not a model')
    with pytest.raises(ValueError, match='checksum'):
        load_model({'path': str(path), 'sha256': 'wrong'})
