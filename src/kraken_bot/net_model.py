"""Profit-outcome classifiers with fixed exits and chronological model selection."""
import json
import logging
import math
import statistics
from datetime import datetime
from pathlib import Path

from .enriched_model import FEATURES, TRADE_FEATURES, fit_classifier, probabilities, model_details
from .exit_study import load_paths, simulate_exit, summarize_exits
from .filter_study import digest, load_model, load_rows, passes

LOG = logging.getLogger(__name__)
CONTEXT_FEATURES = ('market_return_60m', 'relative_return_60m', 'context_missing')
NET_FEATURES = FEATURES + TRADE_FEATURES + CONTEXT_FEATURES


def label_samples(rows, archive, plan):
    """Keep unfilled/unknown-end signals rather than filtering on future liquidity."""
    by_symbol = {}
    for row in rows:
        by_symbol.setdefault(row['symbol'], []).append(row)
    for symbol, samples in by_symbol.items():
        paths = load_paths(samples, archive, plan['hold_minutes'])
        for row in samples:
            row['market_return_60m'] = row['mean_BTC_ETH_return_60m'] or 0.0
            row['relative_return_60m'] = row['return_60m_minus_mean_BTC_ETH'] or 0.0
            row['context_missing'] = int(row['mean_BTC_ETH_return_60m'] is None)
            row['outcomes'] = {}
            for target in plan['targets_pct']:
                result = simulate_exit(row['decision_utc'], paths[(symbol, row['decision_utc'])],
                                       target, plan['stop_pct'], plan['hold_minutes'])
                if result['status'] == 'marked':
                    net = (1 + result['gross_return']) * (1 - plan['selection_cost_bps'] / 10000) - 1
                    result['positive_net'] = bool(net > 0 and not result['stale_timeout_mark'])
                else:
                    result['positive_net'] = False
                row['outcomes'][str(target)] = result
        LOG.info('%s: %s profit-outcome samples', symbol, len(samples))
    return rows


def target_rows(rows, target):
    return [{**r, 'rally': r['outcomes'][str(target)]['positive_net']} for r in rows]


def outcome_summary(rows, target, costs):
    outcomes = [r['outcomes'][str(target)] for r in rows]
    result = summarize_exits(outcomes, costs)
    positives = sum(o['positive_net'] for o in outcomes)
    result.update(positive_net_labels=positives, nonpositive_or_unresolved_labels=len(rows)-positives,
                  positive_label_precision=positives/len(rows) if rows else None,
                  distinct_assets=len({r['symbol'] for r in rows}))
    # Unfilled signals produce no trade return; include their zeros in selection.
    # Stale marks remain flagged in evaluation, but cannot support model selection.
    for cost in costs:
        values = [(1+o['gross_return'])*(1-cost/10000)-1 if o['status']=='marked' else 0 for o in outcomes]
        result['net_by_cost_bps'][str(cost)]['mean_per_signal_pct'] = statistics.mean(values)*100 if rows else None
    return result


def choose_candidate(candidates, plan):
    cost = str(plan['selection_cost_bps'])
    supported = [c for c in candidates if c['validation']['signals'] >= plan['minimum_validation_signals']
        and c['validation']['trades'] >= plan['minimum_validation_marked_trades']
        and c['validation']['net_by_cost_bps'][cost]['wins'] >= plan['minimum_validation_positive_returns']
        and c['validation']['net_by_cost_bps'][cost]['mean_per_signal_pct'] > 0
        and c['validation']['stale_timeout_marks'] == 0]
    if not supported:
        return None
    return min(supported, key=lambda c: (-c['validation']['net_by_cost_bps'][cost]['mean_per_signal_pct'],
        -c['validation']['signals'], c['target_pct'], c['learner'] != 'logistic', c['validation_quantile']))['id']


def evaluate_training(rows, plan, model_dir=None):
    """This function sees no later-test rows, labels or returns."""
    import numpy as np
    from sklearn.metrics import average_precision_score
    fit_end = datetime.fromisoformat(plan['fit_end_utc'])
    validation_end = datetime.fromisoformat(plan['validation_end_utc'])
    training = [r for r in rows if r['label_end_utc'] <= fit_end]
    validation = [r for r in rows if r['decision_utc'] >= fit_end and r['label_end_utc'] <= validation_end]
    if not training or len(validation) < 50:
        raise ValueError('Insufficient chronological training/validation')
    candidates, pipelines = [], {}
    for target in plan['targets_pct']:
        for learner in plan['learners']:
            model = fit_classifier(target_rows(training, target), NET_FEATURES, learner)
            model.update(fit_end_utc=fit_end.isoformat(), validation_end_utc=validation_end.isoformat(),
                         horizon_minutes=plan['hold_minutes'], target_pct=target, stop_pct=plan['stop_pct'])
            scores = probabilities(model, validation)
            labels = [r['outcomes'][str(target)]['positive_net'] for r in validation]
            artifact = None
            if model_dir is not None:
                import joblib
                model_dir.mkdir(parents=True, exist_ok=True)
                path = model_dir / f'{target}pct-{learner}.joblib'
                joblib.dump(model, path)
                artifact = {'path': str(path), 'sha256': digest(path)}
            for quantile in plan['validation_quantiles']:
                cutoff = float(np.quantile(scores, quantile))
                selected = [r for r, p in zip(validation, scores) if p >= cutoff]
                name = f'{target}pct-{learner}-q{quantile:g}'
                candidates.append({'id': name, 'target_pct': target, 'learner': learner,
                    'validation_quantile': quantile, 'cutoff': cutoff,
                    'training_samples': len(training), 'training_positive_labels': sum(r['rally'] for r in target_rows(training,target)),
                    'validation_samples': len(validation), 'validation': outcome_summary(selected,target,plan['cost_sensitivities_bps']),
                    'validation_average_precision': float(average_precision_score(labels,scores)) if any(labels) else None,
                    'artifact': artifact, 'model': model_details(model)})
                pipelines[name] = model
    return candidates, pipelines, choose_candidate(candidates, plan)


def evaluate_later(rows, candidates, pipelines, plan):
    from sklearn.metrics import average_precision_score
    results = {}
    for candidate in candidates:
        target = candidate['target_pct']; model = pipelines[candidate['id']]
        scores = probabilities(model, rows)
        selected = [r for r, p in zip(rows, scores) if p >= candidate['cutoff']]
        labels = [r['outcomes'][str(target)]['positive_net'] for r in rows]
        result = outcome_summary(selected,target,plan['cost_sensitivities_bps'])
        positives = sum(labels)
        result.update(total_observations=len(rows), total_positive_labels=positives,
            positive_label_recall=result['positive_net_labels']/positives if positives else None,
            average_precision=float(average_precision_score(labels,scores)) if positives else None,
            selected_predictions=[{'symbol': r['symbol'], 'decision_utc': r['decision_utc'].isoformat(),
                'score': float(p), 'positive_net': r['outcomes'][str(target)]['positive_net'],
                'outcome': r['outcomes'][str(target)]} for r,p in zip(rows,scores) if p >= candidate['cutoff']])
        results[candidate['id']] = result
    return results


def validate_plan(plan):
    times = [datetime.fromisoformat(plan[k]) for k in ('fit_end_utc','validation_end_utc','later_test_start_utc','later_test_end_utc')]
    if any(t.tzinfo is None for t in times) or not times[0] < times[1] <= times[2] < times[3]:
        raise ValueError('Chronological aware boundaries required')
    if plan['hold_minutes'] != 240 or plan['stop_pct'] != 2 or plan['targets_pct'] != [3,4,5]:
        raise ValueError('Use fixed 3/4/5% targets, 2% stop and four-hour horizon')
    if plan['learners'] != ['logistic','nonlinear']:
        raise ValueError('Use predetermined learner candidates')
    if not plan['validation_quantiles'] or any(not 0 < q < 1 for q in plan['validation_quantiles']):
        raise ValueError('Validation quantiles must be in (0,1)')
    if not {0,8,20,50}.issubset(plan['cost_sensitivities_bps']) or plan['selection_cost_bps'] != 8:
        raise ValueError('Use documented base-spread proxy and fixed cost sensitivities')
    if any(not math.isfinite(c) or not 0 <= c < 10000 for c in plan['cost_sensitivities_bps']):
        raise ValueError('Invalid cost sensitivity')
    if any(type(plan[k]) is not int or plan[k] < 1 for k in ('minimum_validation_signals','minimum_validation_marked_trades','minimum_validation_positive_returns')):
        raise ValueError('Positive support requirements required')


def run_net_model(symbols, plan_path, filter_plan_path, input_report, filter_report,
                  training_archive, training_db, test_archive, test_db, output):
    from threadpoolctl import threadpool_limits
    if output.suffix != '.json' or output.resolve() in {p.resolve() for p in (plan_path,filter_plan_path,input_report,filter_report,training_archive,training_db,test_archive,test_db)}:
        raise ValueError('JSON output must not overwrite inputs')
    plan = json.loads(plan_path.read_text()); validate_plan(plan)
    source = json.loads(input_report.read_text())
    filter_plan = json.loads(filter_plan_path.read_text())
    previous = json.loads(filter_report.read_text())
    if digest(input_report) != filter_plan['training_input_sha256'] or digest(training_archive) != source['archive_sha256']:
        raise ValueError('Training source checksum mismatch')
    if digest(filter_plan_path) != previous['selection']['plan_sha256'] or digest(test_archive) != previous['holdout_candle_sha256']:
        raise ValueError('Earlier evaluation source checksum mismatch')
    if previous['selection']['model'] != filter_plan['frozen_model']:
        raise ValueError('Baseline model differs from earlier evaluation')
    end = datetime.fromisoformat(source['end_exclusive_utc'])
    if end != datetime.fromisoformat(plan['validation_end_utc']):
        raise ValueError('Training archive/report boundary differs from plan')
    earlier, coverage, hashes = load_rows(symbols,training_archive,training_db,
        datetime.fromisoformat(source['start_utc']),end,plan['hold_minutes'])
    if sorted(hashes) != sorted(source['trade_source_hashes']):
        raise ValueError('Training trade source mismatch')
    label_samples(earlier,training_archive,plan)
    with threadpool_limits(limits=2):
        candidates, pipelines, selected_id = evaluate_training(earlier,plan,output.parent/'models')
    selection = {'selected_candidate': selected_id, 'plan_sha256': digest(plan_path),
                 'candidates': [{k:c[k] for k in ('id','target_pct','learner','validation_quantile','cutoff','artifact','validation')} for c in candidates],
                 'basis': 'Q2 training and validation only; written before Q3 model evaluation',
                 'ready_for_trading': False}
    output.parent.mkdir(parents=True,exist_ok=True)
    output.with_suffix('.selection.json').write_text(json.dumps(selection,indent=2)+'\n')
    start, end = [datetime.fromisoformat(plan[k]) for k in ('later_test_start_utc','later_test_end_utc')]
    later, later_coverage, hashes = load_rows(symbols,test_archive,test_db,start,end,plan['hold_minutes'])
    if sorted(hashes) != sorted(previous['holdout_trade_source_hashes']):
        raise ValueError('Later trade source mismatch')
    label_samples(later,test_archive,plan)
    with threadpool_limits(limits=2):
        results = evaluate_later(later,candidates,pipelines,plan)
        baseline_model = load_model(filter_plan['frozen_model'])
        baseline_scores = probabilities(baseline_model,later)
    baseline_selected = [r for r,p in zip(later,baseline_scores) if p >= previous['selection']['score_threshold']]
    old_keys = {(r['symbol'],datetime.fromisoformat(r['decision_utc'])):r for r in previous['holdout_baseline_selections']}
    new_keys = {(r['symbol'],r['decision_utc']):float(p) for r,p in zip(later,baseline_scores)
                if p >= previous['selection']['score_threshold']}
    if old_keys.keys() != new_keys.keys() or any(not math.isclose(old_keys[k]['score'],new_keys[k],rel_tol=1e-10,abs_tol=1e-12) for k in old_keys):
        raise ValueError('Original baseline selection did not reproduce')
    baseline_buyer = [r for r in baseline_selected if passes(r,'buy_confirmation',filter_plan)]
    baseline = {str(target): {'original_score_cutoff': outcome_summary(baseline_selected,target,plan['cost_sensitivities_bps']),
        'original_buy_confirmation': outcome_summary(baseline_buyer,target,plan['cost_sensitivities_bps']),
        'all_eligible_signals': outcome_summary(later,target,plan['cost_sensitivities_bps'])} for target in plan['targets_pct']}
    selected_result = results.get(selected_id) if selected_id else None
    report = {'plan':plan, 'selection':selection, 'candidates':candidates, 'later_results':results,
        'validation_selected_later_result':selected_result, 'baselines':baseline,
        'training_coverage':coverage,'later_coverage':later_coverage, 'features':NET_FEATURES,
        'source_hashes': {'training_candles':digest(training_archive),'later_candles':digest(test_archive),
                         'earlier_model_report':digest(input_report),'earlier_filter_report':digest(filter_report)},
        'ready_for_trading':False,
        'limitations': ['Both quarters and the asset universe were previously inspected; this is retrospective experimentation, not untouched confirmation.',
            'Classifier predicts a positive net price-path outcome for a fixed target/stop/time limit, not a +10% high touch. Timeout gains can be positive labels without touching the target.',
            'Train-only clipping/scaling. Label-end purging separates fit, validation and later test. All features use completed candles/trades before the decision.',
            'Unfilled signals remain as zero-return nonpositive labels; future fill availability does not filter test eligibility. Stale timeouts are reported and cannot support validation model selection.',
            'Eight basis points approximates documented Funded spreads. Spot archives are not Funded quotes; price differences and additional costs remain unknown.',
            'Targets and stops are assumed price-path fills; conservative stop-first candles and worse gap opens do not reproduce network delays, order-book liquidity or actual trading.',
            'Funded has no advanced order types and its bot access is unconfirmed. These exits and classifiers are offline research only.',
            'Different asset positions can overlap. Per-trade averages are not portfolio returns or probabilities of passing the +12% / -3% challenge.',
            'Comparing twelve candidate pipelines can overfit the small validation week. A later-table winner cannot be chosen or promoted using that table.',
            'No live signals or order execution were added.']}
    output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    write_report(report,output.with_suffix('.md'))
    return report


def write_report(report, output):
    lines = ['# Profit-outcome model experiment','',report['plan']['evaluation_status'],'',
             'Predict positive net exits using 3/4/5% targets, a fixed 2% stop and four-hour limit. Base cost proxy: 0.08%; stress costs: 0.20% and 0.50%.', '',
             f"Candidate selected using June validation: {report['selection']['selected_candidate'] or 'none supported — abstain'}.", '',
             '| Candidate | June validation signals | June mean / signal, 8 bps | Later signals / marked trades | Later net-positive labels | Later mean / trade, 8 bps | Later mean / trade, 20 bps | Later mean / trade, 50 bps |',
             '|---|---:|---:|---:|---:|---:|---:|---:|']
    def fmt(v): return 'undefined' if v is None else f'{v:.3f}%'
    for candidate in report['candidates']:
        v=candidate['validation'];r=report['later_results'][candidate['id']]
        lines.append(f"| {candidate['id']} | {v['signals']} | {fmt(v['net_by_cost_bps']['8']['mean_per_signal_pct'])} | {r['signals']} / {r['trades']} | {r['positive_net_labels']} | {fmt(r['net_by_cost_bps']['8']['mean_pct'])} | {fmt(r['net_by_cost_bps']['20']['mean_pct'])} | {fmt(r['net_by_cost_bps']['50']['mean_pct'])} |")
    lines.extend(['','## Paired older-model baselines','',
        '| Exit target | Entry baseline | Signals / marked trades | Positive net labels | Mean / trade, 8 bps |',
        '|---|---|---:|---:|---:|'])
    for target, baselines in report['baselines'].items():
        for name,r in baselines.items():
            lines.append(f"| {target}% | {name} | {r['signals']} / {r['trades']} | {r['positive_net_labels']} | {fmt(r['net_by_cost_bps']['8']['mean_pct'])} |")
    lines.extend(['','Positive net labels include profitable timeouts, not just target exits. See JSON for target/stop/timeout counts, precision/recall, gap/stale/unfilled counts, average precision, model artifacts and individual predictions.', '',
        '## Limits','',*('- '+v for v in report['limitations'])])
    output.write_text('\n'.join(lines)+'\n')
