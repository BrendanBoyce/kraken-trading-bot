"""Exploratory long-only candle exit proxies for frozen, past-only model entries."""
import itertools
import json
import logging
import math
import shutil
import statistics
import tempfile
import zipfile
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .enriched_model import probabilities
from .filter_study import digest, load_model, load_rows, passes
from .history import archive_members
from .storage import CandleStore

LOG = logging.getLogger(__name__)


def validate_plan(plan):
    for name in ('targets_pct', 'stops_pct'):
        if not plan[name] or any(not math.isfinite(v) or not 0 < v < 100 for v in plan[name]):
            raise ValueError('Targets and stops must be positive percentages below 100')
    if any(type(v) is not int or not 1 <= v <= 240 for v in plan['hold_minutes']):
        raise ValueError('Holding periods must be whole minutes up to 240')
    if not plan['hold_minutes']:
        raise ValueError('Holding periods cannot be empty')
    if any(not math.isfinite(v) or not 0 <= v < 10000 for v in plan['combined_cost_bps']):
        raise ValueError('Costs must be finite basis points below 10000')
    if plan['selection_cost_bps'] not in plan['combined_cost_bps']:
        raise ValueError('Selection cost must be reported')
    if type(plan['minimum_validation_trades']) is not int or plan['minimum_validation_trades'] < 1:
        raise ValueError('Positive minimum validation support required')
    if plan['same_minute_order'] != 'stop_first' or plan['entry_streams'] != ['none', 'buy_confirmation']:
        raise ValueError('Only frozen baseline/buy-confirmation entries and conservative ordering supported')


def simulate_exit(entry_utc, bars, target_pct, stop_pct, hold_minutes):
    """Actual next-minute open; gaps at worse opens; both-level bars stop first.

    These are price references, not demonstrated fills. Missing entry minutes are
    not filled; absent timeout minutes use a flagged stale closing mark.
    """
    deadline = entry_utc + timedelta(minutes=hold_minutes)
    path = [b for b in bars if entry_utc <= b['timestamp'] < deadline]
    if not path or path[0]['timestamp'] != entry_utc:
        return {'status': 'unfilled_no_entry_minute'}
    if len({b['timestamp'] for b in path}) != len(path) or any(a['timestamp'] >= b['timestamp'] for a, b in zip(path, path[1:])):
        raise ValueError('Exit path must be unique and chronological')
    if any(any(not math.isfinite(b[k]) or b[k] <= 0 for k in ('open','high','low','close'))
           or b['low'] > min(b['open'], b['close']) or b['high'] < max(b['open'], b['close']) for b in path):
        raise ValueError('Invalid OHLC exit path')
    entry_price = path[0]['open']
    target = entry_price * (1 + target_pct / 100)
    stop = entry_price * (1 - stop_pct / 100)
    reason = 'timeout'; ambiguous = gap_stop = False
    price = path[-1]['close']; exit_bar = path[-1]
    for bar in path:
        exit_bar = bar
        if bar['open'] <= stop:
            price = bar['open']; reason = 'stop'; gap_stop = True
            break
        if bar['open'] >= target:
            price = target; reason = 'target'
            break
        stop_hit, target_hit = bar['low'] <= stop, bar['high'] >= target
        if stop_hit:
            price = stop; reason = 'stop'; ambiguous = target_hit
            break
        if target_hit:
            price = target; reason = 'target'
            break
    stale = reason == 'timeout' and exit_bar['timestamp'] + timedelta(minutes=1) != deadline
    return {'status': 'marked', 'entry_utc': entry_utc, 'entry_price': entry_price,
            'exit_price': price, 'exit_bar_utc': exit_bar['timestamp'], 'exit_reason': reason,
            'gross_return': price / entry_price - 1, 'ambiguous_stop_first': ambiguous,
            'stop_gap': gap_stop, 'stale_timeout_mark': stale,
            'hold_minutes': hold_minutes, 'target_pct': target_pct, 'stop_pct': stop_pct}


def summarize_exits(trades, costs):
    marked = [t for t in trades if t['status'] == 'marked']
    result = {'signals': len(trades), 'trades': len(marked),
              'unfilled_entries': len(trades) - len(marked),
              'exit_reasons': dict(Counter(t['exit_reason'] for t in marked)),
              'ambiguous_stop_first': sum(t['ambiguous_stop_first'] for t in marked),
              'stop_gaps': sum(t['stop_gap'] for t in marked),
              'stale_timeout_marks': sum(t['stale_timeout_mark'] for t in marked),
              'net_by_cost_bps': {}, 'ready_for_trading': False}
    for cost in costs:
        returns = [(1 + t['gross_return']) * (1 - cost / 10000) - 1 for t in marked]
        wins = [v for v in returns if v > 0]; losses = [v for v in returns if v < 0]
        result['net_by_cost_bps'][str(cost)] = {
            'mean_pct': statistics.mean(returns) * 100 if returns else None,
            'median_pct': statistics.median(returns) * 100 if returns else None,
            'wins': len(wins), 'losses': len(losses), 'flat': len(returns) - len(wins) - len(losses),
            'win_rate': len(wins) / len(returns) if returns else None,
            'profit_factor': sum(wins) / -sum(losses) if losses else None,
            'worst_pct': min(returns) * 100 if returns else None}
    return result


def exit_grid(entries, paths, plan):
    results = []
    for target, stop, hold in itertools.product(plan['targets_pct'], plan['stops_pct'], plan['hold_minutes']):
        trades = [{**simulate_exit(r['decision_utc'], paths[(r['symbol'], r['decision_utc'])], target, stop, hold),
                   'symbol': r['symbol']} for r in entries]
        results.append({'target_pct': target, 'stop_pct': stop, 'hold_minutes': hold,
                        'summary': summarize_exits(trades, plan['combined_cost_bps'])})
    return results


def select_settings(validation, plan):
    cost = str(plan['selection_cost_bps'])
    supported = [r for r in validation if r['summary']['trades'] >= plan['minimum_validation_trades']
                 and r['summary']['net_by_cost_bps'][cost]['mean_pct'] > 0
                 and r['summary']['stale_timeout_marks'] == 0]
    if not supported:
        return None
    best = min(supported, key=lambda r: (-r['summary']['net_by_cost_bps'][cost]['mean_pct'],
                                        r['target_pct'], r['stop_pct'], r['hold_minutes']))
    return {k: best[k] for k in ('target_pct', 'stop_pct', 'hold_minutes')}


def load_paths(entries, archive_path, max_hold):
    """Read observed bars only. Do not silently manufacture executable no-trade bars."""
    paths = {(r['symbol'], r['decision_utc']): [] for r in entries}
    by_symbol = {}
    for key in paths:
        by_symbol.setdefault(key[0], []).append(key[1])
    with CandleStore(':memory:') as store, zipfile.ZipFile(archive_path) as archive:
        manifest = json.loads(archive.read('MANIFEST.json'))
        if manifest.get('product') != 'ohlcvt' or manifest.get('has_header') is not False:
            raise ValueError('Expected OHLCVT archive')
        lower = datetime.fromisoformat(manifest['coverage']['start']).replace(tzinfo=UTC)
        upper = datetime.fromisoformat(manifest['coverage']['end']).replace(tzinfo=UTC) + timedelta(days=1)
        for symbol, member in archive_members(archive, by_symbol).items():
            if member is None:
                raise ValueError(f'{symbol}: entry missing from archive')
            starts = by_symbol[symbol]
            if any(stamp < lower or stamp + timedelta(minutes=max_hold) > upper for stamp in starts):
                raise ValueError('Exit path crosses archive coverage')
            db = store.connection
            db.execute('DROP TABLE IF EXISTS exit_entries')
            db.execute('CREATE TEMP TABLE exit_entries (decision TIMESTAMPTZ)')
            db.executemany('INSERT INTO exit_entries VALUES (?)', [(t,) for t in starts])
            with tempfile.TemporaryDirectory(prefix='kraken-exits-') as temporary:
                csv = Path(temporary) / 'candles.csv'
                with archive.open(member) as src, csv.open('wb') as dest:
                    shutil.copyfileobj(src, dest)
                fetched = db.execute('''WITH candles AS (
                    SELECT to_timestamp(epoch) AS timestamp,open,high,low,close,trades
                    FROM read_csv(?,header=false,columns={'epoch':'BIGINT','open':'DOUBLE',
                    'high':'DOUBLE','low':'DOUBLE','close':'DOUBLE','volume':'DOUBLE','trades':'BIGINT'},
                    strict_mode=true,null_padding=false))
                    SELECT e.decision,c.timestamp,c.open,c.high,c.low,c.close FROM exit_entries e
                    JOIN candles c ON c.timestamp>=e.decision AND c.timestamp<e.decision+?*INTERVAL '1 minute'
                    WHERE c.trades>0 ORDER BY e.decision,c.timestamp''', [str(csv), max_hold]).fetchall()
            for decision, stamp, opening, high, low, closing in fetched:
                paths[(symbol, decision)].append({'timestamp': stamp, 'open': opening,
                                                  'high': high, 'low': low, 'close': closing})
    return paths


def run_exit_study(symbols, plan_path, filter_plan_path, input_report, filter_report,
                   training_archive, training_db, test_archive, test_db, output):
    import numpy as np
    from threadpoolctl import threadpool_limits
    inputs = (plan_path, filter_plan_path, input_report, filter_report, training_archive, training_db, test_archive, test_db)
    if output.suffix != '.json' or output.resolve() in {p.resolve() for p in inputs}:
        raise ValueError('JSON output must not overwrite an input')
    plan = json.loads(plan_path.read_text()); validate_plan(plan)
    filter_plan = json.loads(filter_plan_path.read_text())
    original = json.loads(input_report.read_text())
    prior = json.loads(filter_report.read_text())
    if digest(input_report) != filter_plan['training_input_sha256'] or digest(training_archive) != original['archive_sha256']:
        raise ValueError('Original Q2 source checksum mismatch')
    if digest(filter_plan_path) != prior['selection']['plan_sha256'] or digest(test_archive) != prior['holdout_candle_sha256']:
        raise ValueError('Prior Q3 source or filter plan checksum mismatch')
    if prior['selection']['model'] != filter_plan['frozen_model']:
        raise ValueError('Frozen entry model differs from prior study')
    horizon = filter_plan['horizon_minutes']
    if horizon != 240 or max(plan['hold_minutes']) > horizon:
        raise ValueError('Use original four-hour entries with no longer exit horizon')
    rows, _, hashes = load_rows(symbols, training_archive, training_db,
        datetime.fromisoformat(original['start_utc']), datetime.fromisoformat(original['end_exclusive_utc']), horizon)
    if sorted(hashes) != sorted(original['trade_source_hashes']):
        raise ValueError('Q2 trade source mismatch')
    validation_entries = []
    frozen_cutoff = None
    with threadpool_limits(limits=2):
        for fold in original['evaluation']['folds']:
            if fold['status'] != 'evaluated':
                continue
            artifact = fold['models']['trade_enriched']['artifact']
            model = load_model(artifact)
            boundary = datetime.fromisoformat(fold['start_utc'])
            fit_end = datetime.fromisoformat(model['fit_end_utc'])
            if fit_end != boundary - timedelta(days=7):
                raise ValueError('Unexpected entry-model fitting boundary')
            validation = [r for r in rows if r['decision_utc'] >= fit_end and r['label_end_utc'] <= boundary]
            scores = probabilities(model, validation)
            old = fold['models']['trade_enriched']['validation_predictions']
            if len(old) != len(scores) or not np.allclose(scores, [r['score'] for r in old], rtol=1e-10, atol=1e-12):
                raise ValueError('Original validation scores did not reproduce')
            cutoff = float(np.quantile(scores, filter_plan['score_cutoff_validation_quantile']))
            validation_entries.extend({**r, 'score': float(p)} for r, p in zip(validation, scores) if p >= cutoff)
            if artifact == filter_plan['frozen_model']:
                frozen_cutoff = cutoff
        if frozen_cutoff is None or not math.isclose(frozen_cutoff, prior['selection']['score_threshold'], rel_tol=1e-10):
            raise ValueError('Frozen entry cutoff differs from prior study')
        if len({(r['symbol'], r['decision_utc']) for r in validation_entries}) != len(validation_entries):
            raise ValueError('Duplicated validation entry windows')
        validation_paths = load_paths(validation_entries, training_archive, max(plan['hold_minutes']))
        validation_results = {}
        chosen = {}
        for stream in plan['entry_streams']:
            entries = [r for r in validation_entries if passes(r, stream, filter_plan)]
            validation_results[stream] = exit_grid(entries, validation_paths, plan)
            chosen[stream] = select_settings(validation_results[stream], plan)
        selection = {'settings_by_stream': chosen, 'entry_model': filter_plan['frozen_model'],
                     'entry_cutoff': frozen_cutoff, 'exit_plan_sha256': digest(plan_path),
                     'basis': 'Only earlier Q2 validation paths; saved before processing Q3 exit outcomes',
                     'evaluation_status': plan['evaluation_status'], 'ready_for_trading': False}
        output.parent.mkdir(parents=True, exist_ok=True)
        output.with_suffix('.selection.json').write_text(json.dumps(selection, indent=2) + '\n')
        model = load_model(filter_plan['frozen_model'])
        if model['fit_end_utc'] != filter_plan['fit_end_utc'] or model['validation_end_utc'] != filter_plan['validation_end_utc']:
            raise ValueError('Frozen entry model chronology differs from plan')
        start, end = [datetime.fromisoformat(t).replace(tzinfo=UTC) for t in filter_plan['test_period']]
        later, coverage, hashes = load_rows(symbols, test_archive, test_db, start, end, horizon)
        if sorted(hashes) != sorted(prior['holdout_trade_source_hashes']):
            raise ValueError('Q3 trade source mismatch')
        scores = probabilities(model, later)
        later_entries = [{**r, 'score': float(p)} for r, p in zip(later, scores) if p >= frozen_cutoff]
    previous = {(r['symbol'], datetime.fromisoformat(r['decision_utc'])): r for r in prior['holdout_baseline_selections']}
    rebuilt = {(r['symbol'], r['decision_utc']): r for r in later_entries}
    if previous.keys() != rebuilt.keys() or any(not math.isclose(previous[k]['score'], rebuilt[k]['score'], rel_tol=1e-10, abs_tol=1e-12) for k in previous):
        raise ValueError('Q3 entries differ from frozen prior predictions')
    later_paths = load_paths(later_entries, test_archive, max(plan['hold_minutes']))
    results = {}; records = {}; selected_results = {}
    days = (end - start).total_seconds() / 86400
    for stream in plan['entry_streams']:
        entries = [r for r in later_entries if passes(r, stream, filter_plan)]
        results[stream] = exit_grid(entries, later_paths, plan)
        chosen_setting = chosen[stream]
        selected_results[stream] = next((r for r in results[stream] if all(r[k] == v for k, v in chosen_setting.items())), None) if chosen_setting else None
        records[stream] = {'signals': len(entries), 'mean_signals_per_calendar_day': len(entries) / days,
                           'max_signals_per_UTC_day': max(Counter(r['decision_utc'].date() for r in entries).values(), default=0),
                           'distinct_assets': len({r['symbol'] for r in entries})}
    report = {'plan': plan, 'selection': selection, 'validation_results': validation_results,
              'Q3_results': results, 'Q3_validation_selected_results': selected_results,
              'Q3_entry_activity': records, 'coverage': coverage,
              'source_hashes': {'Q2_candles': digest(training_archive), 'Q3_candles': digest(test_archive),
                                'Q2_report': digest(input_report), 'prior_filter_report': digest(filter_report)},
              'ready_for_trading': False,
              'limitations': ['Both archive quarters were previously inspected. This is exploratory, not a new untouched holdout.',
                'Exit settings use Q2 validation only; Q3 candidate tables must not select or tune settings.',
                'The original +10% entry classifier is unchanged and has not been trained to predict smaller profitable moves.',
                'Entries use the next-minute opening trade price, not the previous closing mark. Missing entry-minute trades leave a signal unfilled.',
                'Stops use worse opening prices across gaps. If both levels are touched after the opening price in one minute, the stop is assumed first.',
                'Targets assume fills at the target price; stops assume fills at the stop or worse gap open. Order-book depth, queue position and partial fills are unavailable.',
                'Timeout marks can be stale; those counts are explicit. Stale validation timeouts cannot support settings selection.',
                '0/50/100 bps are hypothetical combined fee/spread/slippage haircuts, not verified exchange rates or actual fills.',
                'Same-pair signal windows do not overlap (four-hour sampling, maximum four-hour hold); different assets can overlap. No capital allocation or portfolio equity simulation.',
                'Current October asset selection, correlated windows, small validation samples and comparison of many exit settings limit inference.',
                'No live entry policy was approved by the earlier filter study. These are diagnostic simulations, not active trades.']}
    output.write_text(json.dumps(report, indent=2, default=str) + '\n')
    write_markdown(report, output.with_suffix('.md'))
    return report


def write_markdown(report, output):
    candidates = [r for results in report['Q3_results'].values() for r in results]
    positive = sum(r['summary']['net_by_cost_bps']['50']['mean_pct'] is not None
                   and r['summary']['net_by_cost_bps']['50']['mean_pct'] > 0 for r in candidates)
    lines = ['# Smaller-target exit study', '', report['plan']['evaluation_status'], '',
             'Frozen entries; next-minute opening prices; gap-aware stops; stop-first handling of ambiguous candles. All returns below are price-path proxies.', '',
             f"Of {len(candidates)} fixed entry/exit configurations, {positive} had positive average net returns at the hypothetical 50 bps combined cost. This cost assumption is not a verified fee rate.", '',
             '## Fixed comparison: 2% stop and four-hour limit', '',
             '| Entries | Target | Marked trades | Target exits | Stop exits | Timeouts | Mean gross | Mean net, 50 bps |',
             '|---|---:|---:|---:|---:|---:|---:|---:|']
    for stream, results in report['Q3_results'].items():
        for r in results:
            if r['target_pct'] not in (3,4,5) or r['stop_pct'] != 2 or r['hold_minutes'] != 240:
                continue
            summary = r['summary']; reasons = summary['exit_reasons']
            gross = summary['net_by_cost_bps']['0']['mean_pct']
            net = summary['net_by_cost_bps']['50']['mean_pct']
            means = f'{gross:.3f}% | {net:.3f}%' if gross is not None else 'undefined | undefined'
            lines.append(f"| {stream} | {r['target_pct']}% | {summary['trades']} | {reasons.get('target',0)} | {reasons.get('stop',0)} | {reasons.get('timeout',0)} | {means} |")
    lines.extend(['',
             '## Settings selected using Q2 validation', '', json.dumps(report['selection']['settings_by_stream']), '',
             '## July–September fixed-candidate comparison', '',
             '| Entries | Target | Stop | Timeout | Signals / marked trades | Target exits | Stop exits | Timeouts | Mean net, 50 bps | Net win rate, 50 bps | Ambiguous candles |',
             '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|'])
    for stream, results in report['Q3_results'].items():
        for r in results:
            summary = r['summary']; net = summary['net_by_cost_bps']['50']
            mean = f"{net['mean_pct']:.3f}%" if net['mean_pct'] is not None else 'undefined'
            wins = f"{net['win_rate']:.1%}" if net['win_rate'] is not None else 'undefined'
            reasons = summary['exit_reasons']
            lines.append(f"| {stream} | {r['target_pct']}% | {r['stop_pct']}% | {r['hold_minutes']}m | {summary['signals']} / {summary['trades']} | {reasons.get('target',0)} | {reasons.get('stop',0)} | {reasons.get('timeout',0)} | {mean} | {wins} | {summary['ambiguous_stop_first']} |")
    lines.extend(['', '## Signal frequency', '', json.dumps(report['Q3_entry_activity'], indent=2), '',
                  'Full JSON contains validation results and cost sensitivities at 0, 50 and 100 bps. Comparing the best Q3 rows is not independent confirmation.', '',
                  '## Limits', '', *('- ' + v for v in report['limitations'])])
    output.write_text('\n'.join(lines) + '\n')
