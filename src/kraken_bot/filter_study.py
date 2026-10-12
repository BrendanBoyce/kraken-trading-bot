"""Frozen past-only filters and an unchanged classifier on a later archive quarter."""
import hashlib
import json
import logging
import math
import shutil
import statistics
import tempfile
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .enriched_model import enrich_rows, prepare_trade_windows, probabilities
from .history import archive_members
from .rally_study import event_samples, prepare_features
from .storage import CandleStore
from .threshold_study import summarize
from .walk_forward import prepare_archive_source

LOG = logging.getLogger(__name__)


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def load_model(artifact):
    import joblib
    if digest(artifact['path']) != artifact['sha256']:
        raise ValueError('Frozen model checksum mismatch')
    return joblib.load(artifact['path'])


def passes(row, name, plan):
    if name in plan['combinations']:
        return all(passes(row, part, plan) for part in plan['combinations'][name])
    for bound, threshold in plan['filters'][name].items():
        field, direction = bound.rsplit('_', 1)
        value = row.get(field)
        if value is None or not math.isfinite(value):
            return False
        if direction == 'min' and value < threshold:
            return False
        if direction == 'max' and value > threshold:
            return False
        if direction not in ('min', 'max'):
            raise ValueError('Unknown filter bound')
    return True


def add_context(row, references):
    """Reference minute is the last closed bar, never the decision's next bar."""
    stamp = row['decision_utc'] - timedelta(minutes=1)
    values = [references.get(symbol, {}).get(stamp) for symbol in ('BTC/USD', 'ETH/USD')]
    market = statistics.mean(values) if all(v is not None for v in values) else None
    count = math.expm1(row['log_trade_count_60m'])
    return {**row, 'trade_count_60m': count,
            'quote_volume_60m': count * math.expm1(row['log_mean_trade_quote']),
            'mean_BTC_ETH_return_60m': market,
            'return_60m_minus_mean_BTC_ETH': row['return_60m'] - market if market is not None else None}


def load_rows(symbols, archive_path, feature_db, start, end, horizon):
    rows, coverage, references = [], [], {}
    with CandleStore(':memory:') as store, zipfile.ZipFile(archive_path) as archive:
        manifest = json.loads(archive.read('MANIFEST.json'))
        if manifest.get('product') != 'ohlcvt' or manifest.get('has_header') is not False:
            raise ValueError('Expected headerless OHLCVT archive')
        lower = datetime.fromisoformat(manifest['coverage']['start']).replace(tzinfo=UTC)
        upper = datetime.fromisoformat(manifest['coverage']['end']).replace(tzinfo=UTC) + timedelta(days=1)
        if start < lower or end > upper:
            raise ValueError('Study outside archive coverage')
        escaped = str(feature_db.resolve()).replace("'", "''")
        store.connection.execute(f"ATTACH '{escaped}' AS trade_db (READ_ONLY)")
        store.connection.execute('CREATE TEMP VIEW trade_input AS SELECT * FROM trade_db.trade_features')
        source_hashes = [r[0] for r in store.connection.execute('SELECT DISTINCT archive_sha256 FROM trade_input').fetchall()]
        members = archive_members(archive, list(dict.fromkeys(['BTC/USD', 'ETH/USD', *symbols])))
        for symbol, member in members.items():
            if member is None:
                coverage.append({'symbol': symbol, 'status': 'absent', 'samples': 0})
                continue
            with tempfile.TemporaryDirectory(prefix='kraken-filters-') as temporary:
                path = Path(temporary) / 'candles.csv'
                with archive.open(member) as src, path.open('wb') as dest:
                    shutil.copyfileobj(src, dest)
                prepare_archive_source(store, path, symbol, start, end)
                mismatches = store.connection.execute('''SELECT count(*) FROM archive_raw c
                    LEFT JOIN trade_input t ON t.symbol=? AND c.timestamp=t.timestamp
                    WHERE c.timestamp>=? AND c.timestamp<? AND c.trades>0
                    AND (t.timestamp IS NULL OR t.trades!=c.trades
                      OR abs(t.volume::DOUBLE-c.volume)>greatest(1e-12,abs(c.volume)*1e-9))''',
                    [symbol, start, end]).fetchone()[0]
                if symbol in symbols and mismatches:
                    raise ValueError(f'{symbol}: {mismatches} candle/trade count or volume mismatches')
                prepare_features(store)
                if symbol in ('BTC/USD', 'ETH/USD'):
                    references[symbol] = dict(store.connection.execute('SELECT timestamp,return_60m FROM rally_features').fetchall())
                if symbol in symbols:
                    prepare_trade_windows(store, symbol)
                    paired = enrich_rows(store, event_samples(store, symbol, horizon))
                    rows.extend(paired)
                    coverage.append({'symbol': symbol, 'status': 'paired', 'samples': len(paired)})
                    LOG.info('%s: %s filter-study samples', symbol, len(paired))
    return [add_context(row, references) for row in rows], coverage, source_hashes


def prediction(row, score):
    return {**row, 'score': float(score), 'actual_rally': row['rally'], 'worst_downside': row['low_return']}


def evaluate_filters(rows, plan):
    total = sum(r['actual_rally'] for r in rows)
    results = {}
    for name in [*plan['filters'], *plan['combinations']]:
        selected = [r for r in rows if r['selected_by_score'] and passes(r, name, plan)]
        results[name] = summarize(selected, total)
        results[name]['distinct_assets'] = len({r['symbol'] for r in selected})
        results[name]['distinct_UTC_days'] = len({r['decision_utc'].date() for r in selected})
    return results


def choose_filter(validation_results, plan):
    rule = plan['selection_policy']
    baseline = validation_results['none']['precision']
    candidates = [name for name, result in validation_results.items()
        if name != 'none' and result['selections'] >= rule['minimum_cumulative_validation_selections']
        and result['precision'] >= rule['minimum_empirical_precision']
        and (not rule['requires_better_than_baseline'] or result['precision'] > baseline)]
    # Deterministic choice and tie-breaking based exclusively on earlier validation.
    return min(candidates, key=lambda n: (-validation_results[n]['precision'], -validation_results[n]['selections'], n)) if candidates else None


def run_study(symbols, plan_path, training_report, training_archive, training_db,
              test_archive, test_db, output):
    import numpy as np
    from threadpoolctl import threadpool_limits
    if output.suffix != '.json':
        raise ValueError('Output must use .json')
    if output.resolve() in {p.resolve() for p in (plan_path, training_report, training_archive,
                                                 training_db, test_archive, test_db)}:
        raise ValueError('Output cannot overwrite a study input')
    if any(not symbol.endswith('/USD') for symbol in symbols):
        raise ValueError('Dollar liquidity thresholds require USD pairs')
    plan = json.loads(plan_path.read_text())
    if digest(training_report) != plan['training_input_sha256']:
        raise ValueError('Training report checksum mismatch')
    source = json.loads(training_report.read_text())
    if digest(training_archive) != source['archive_sha256']:
        raise ValueError('Training candle archive checksum mismatch')
    horizon = plan['horizon_minutes']
    if horizon != source['horizon_minutes']:
        raise ValueError('Horizon differs from original experiment')
    start = datetime.fromisoformat(source['start_utc'])
    end = datetime.fromisoformat(source['end_exclusive_utc'])
    test_start, test_end = [datetime.fromisoformat(t).replace(tzinfo=UTC) for t in plan['test_period']]
    if test_start < end or datetime.fromisoformat(plan['validation_end_utc']) > test_start:
        raise ValueError('Holdout overlaps training or validation')
    rows, coverage, hashes = load_rows(symbols, training_archive, training_db, start, end, horizon)
    if sorted(hashes) != sorted(source['trade_source_hashes']):
        raise ValueError('Training trade source differs from original experiment')
    folds = [f for f in source['evaluation']['folds'] if f['status'] == 'evaluated']
    validation_predictions, test_predictions = [], []
    frozen_threshold = None
    with threadpool_limits(limits=2):
        for fold in folds:
            artifact = fold['models']['trade_enriched']['artifact']
            model = load_model(artifact)
            cutoff = datetime.fromisoformat(fold['start_utc'])
            finish = datetime.fromisoformat(fold['end_utc'])
            validation_start = cutoff - timedelta(days=7)
            if datetime.fromisoformat(model['fit_end_utc']) != validation_start:
                raise ValueError('Unexpected model fitting boundary')
            validation = [r for r in rows if r['decision_utc'] >= validation_start and r['label_end_utc'] <= cutoff]
            test = [r for r in rows if r['decision_utc'] >= cutoff and r['label_end_utc'] <= finish]
            scores = probabilities(model, validation)
            original = fold['models']['trade_enriched']['validation_predictions']
            if len(original) != len(validation) or not np.allclose(scores, [r['score'] for r in original], rtol=1e-10, atol=1e-12):
                raise ValueError('Rebuilt validation predictions differ from original study')
            threshold = float(np.quantile(scores, plan['score_cutoff_validation_quantile']))
            validation_predictions.extend({**prediction(r, p), 'selected_by_score': bool(p >= threshold)} for r, p in zip(validation, scores))
            test_predictions.extend({**prediction(r, p), 'selected_by_score': bool(p >= threshold)} for r, p in zip(test, probabilities(model, test)))
            if artifact == plan['frozen_model']:
                frozen_threshold = threshold
        original = {(r['symbol'], r['decision_utc']): r for r in source['evaluation']['predictions']['trade_enriched']}
        rebuilt = {(r['symbol'], r['decision_utc'].isoformat()): r for r in test_predictions}
        if original.keys() != rebuilt.keys() or any(not math.isclose(original[k]['score'], rebuilt[k]['score'], rel_tol=1e-10, abs_tol=1e-12) or original[k]['actual_rally'] != rebuilt[k]['actual_rally'] for k in original):
            raise ValueError('Rebuilt Q2 tests differ from original study')
        if frozen_threshold is None:
            raise ValueError('Frozen model is not in original folds')
        validation_results = evaluate_filters(validation_predictions, plan)
        chosen = choose_filter(validation_results, plan)
        decision = {'chosen_filter': chosen, 'score_threshold': frozen_threshold,
                    'plan_sha256': digest(plan_path), 'model': plan['frozen_model'],
                    'selection_basis': 'Q2 validation labels only; frozen before Q3 feature/outcome processing',
                    'validation_results': validation_results}
        # Write the selection first: Q3 cannot influence the policy choice.
        output.parent.mkdir(parents=True, exist_ok=True)
        output.with_suffix('.selection.json').write_text(json.dumps(decision, indent=2) + '\n')
        model = load_model(plan['frozen_model'])
        if model['fit_end_utc'] != plan['fit_end_utc'] or model['validation_end_utc'] != plan['validation_end_utc']:
            raise ValueError('Frozen model chronology differs from plan')
        later, later_coverage, later_hashes = load_rows(symbols, test_archive, test_db, test_start, test_end, horizon)
        scores = probabilities(model, later)
        held_out = [{**prediction(r, p), 'selected_by_score': bool(p >= frozen_threshold)} for r, p in zip(later, scores)]
    old_selected = [r for r in test_predictions if r['selected_by_score']]
    fields = ['return_60m','return_240m','trade_count_60m','quote_volume_60m','traded_fraction_60m',
              'quote_imbalance_60m','largest_trade_fraction','mean_BTC_ETH_return_60m','return_60m_minus_mean_BTC_ETH']
    diagnostic = {label: {field: statistics.median([r[field] for r in old_selected if r['actual_rally'] == hit and r[field] is not None]) for field in fields}
                  for label, hit in [('hits', True), ('false_positives', False)]}
    holdout_results = evaluate_filters(held_out, plan)
    selected_policy = holdout_results[chosen] if chosen else summarize([], sum(r['actual_rally'] for r in held_out))
    report = {'plan': plan, 'selection': decision, 'training_coverage': coverage,
              'holdout_coverage': later_coverage, 'holdout_candle_sha256': digest(test_archive),
              'holdout_trade_source_hashes': later_hashes, 'Q2_results': evaluate_filters(test_predictions, plan),
              'Q2_descriptive_medians': diagnostic, 'holdout_observations': len(held_out),
              'holdout_rallies': sum(r['actual_rally'] for r in held_out), 'holdout_results': holdout_results,
              'holdout_assets_without_Q2_samples': sorted({r['symbol'] for r in later} - {r['symbol'] for r in rows}),
              'holdout_baseline_selections': [{k: r[k] for k in ('symbol', 'decision_utc', 'score', 'actual_rally', 'close_return', 'worst_downside')}
                                             for r in held_out if r['selected_by_score']],
              'validation_selected_policy_holdout': selected_policy, 'ready_for_trading': False,
              'limitations': ['Q2 is repeatedly inspected exploratory data; its comparisons do not constitute confirmation.',
                'Q3 outcomes were not used to fit the model, choose filters or set the cutoff. Q3 is now inspected and cannot be reused as a fresh holdout.',
                'Asset universe was chosen in October, after both archive quarters: retrospective selection bias remains.',
                'Fixed candidate Q3 comparisons are diagnostic; choosing the best Q3 result would overfit this holdout.',
                'Trade turnover is a liquidity proxy, not measured spreads, order-book depth or execution feasibility.',
                'The target is a +10% high touch within four hours, not a realized exit or profitable trade.',
                'Cost-adjusted four-hour closing marks are proxies, not portfolio returns. Correlated asset windows are not independent.',
                'An abstaining policy has undefined precision and provides no evidence of trading readiness.']}
    output.write_text(json.dumps(report, indent=2, default=str) + '\n')
    lines = ['# Frozen false-positive filter study', '',
             f"Q3: {len(held_out):,} windows, {report['holdout_rallies']} rally labels. Unchanged June model; preceding June validation cutoff {frozen_threshold:.8f}.", '',
             f"Validation-selected filter: {chosen or 'none supported — abstain'}. No live model signals or orders added.", '',
             '| Fixed filter | Q2 validation selected / hits | Q3 selected | Q3 hits | Q3 false positives | Q3 precision | Q3 missed rallies | Net closing proxy, 50 bps |',
             '|---|---:|---:|---:|---:|---:|---:|---:|']
    for name, result in holdout_results.items():
        val = validation_results[name]
        precision = f"{result['precision']:.1%}" if result['precision'] is not None else 'undefined'
        net = result['mean_net_close_proxy_pct']['50']
        net_text = f'{net:.3f}%' if net is not None else 'undefined'
        lines.append(f"| {name} | {val['selections']} / {val['rallies']} | {result['selections']} | {result['rallies']} | {result['false_positives']} | {precision} | {result['missed_rallies']} | {net_text} |")
    lines.extend(['', '## Earlier Q2 hits versus false positives (descriptive only)', '',
                  '| Past feature | Median: hits | Median: false positives |', '|---|---:|---:|'])
    for field in fields:
        lines.append(f"| {field} | {diagnostic['hits'][field]:.6g} | {diagnostic['false_positives'][field]:.6g} |")
    lines.extend(['', '## Limits', '', *('- ' + limit for limit in report['limitations'])])
    output.with_suffix('.md').write_text('\n'.join(lines) + '\n')
    return report
