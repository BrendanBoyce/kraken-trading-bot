"""Stricter validation-only selection and fixed-$100 offline trade accounting."""
import json
import math
import statistics
from datetime import datetime, timedelta
from pathlib import Path

from .enriched_model import probabilities
from .filter_study import digest, load_model, load_rows
from .net_model import label_samples
from .threshold_study import gate


def dollars(predictions, notional, costs):
    """Independent fixed notional per position, without compounding or account equity."""
    marked = [p for p in predictions if p['outcome']['status'] == 'marked']
    stale = [p for p in marked if p['outcome']['stale_timeout_mark']]
    result = {'signals': len(predictions), 'marked_trades': len(marked),
        'unfilled': len(predictions)-len(marked), 'stale_timeout_marks': len(stale),
        'net_positive_labels': sum(p['positive_net'] for p in predictions),
        'notional_per_trade_usd': notional, 'ready_for_trading': False, 'costs': {}}
    for cost in costs:
        returns = [(1+p['outcome']['gross_return'])*(1-cost/10000)-1 for p in marked]
        pnl = [notional*r for r in returns]
        known = [notional*((1+p['outcome']['gross_return'])*(1-cost/10000)-1)
                 for p in marked if not p['outcome']['stale_timeout_mark']]
        result['costs'][str(cost)] = {
            'wins': sum(r>0 for r in returns), 'losses': sum(r<0 for r in returns),
            'win_rate': sum(r>0 for r in returns)/len(returns) if returns else None,
            'sum_pnl_usd': sum(pnl) if pnl else 0.0,
            'mean_pnl_usd': statistics.mean(pnl) if pnl else None,
            'worst_trade_pnl_usd': min(pnl) if pnl else None,
            'known_marks_sum_pnl_usd': sum(known),
            'known_marks_mean_pnl_usd': statistics.mean(known) if known else None}
    # Exit timestamps are candle bounds; using the candle end is conservative for
    # overlap estimates. Timeout positions remain open until their full deadline.
    events = []
    for p in marked:
        outcome = p['outcome']; start = datetime.fromisoformat(p['decision_utc'])
        if outcome['exit_reason'] == 'timeout':
            finish = start + timedelta(minutes=outcome['hold_minutes'])
        else:
            finish = datetime.fromisoformat(str(outcome['exit_bar_utc'])) + timedelta(minutes=1)
        events.extend([(start,1),(finish,-1)])
    active = peak = 0
    for _, change in sorted(events, key=lambda v:(v[0],v[1])):
        active += change; peak=max(peak,active)
    result['peak_simultaneous_positions'] = peak
    result['peak_nominal_capital_usd'] = peak*notional
    return result


def precision_gate(validation, target, minimum, floor):
    eligible = [r for r in validation if r['score'] >= floor]
    return gate(eligible,target=target,minimum=minimum) if eligible else {
        'threshold':None,'status':'no_validation_data','selected':0,'hits':0}


def evaluate_policy(validation, later_predictions, policy, plan, floor):
    if policy['kind']=='quantile':
        selected_gate = gate(validation,quantile=policy['value'])
        status = 'fixed_diagnostic_not_approved'
    else:
        selected_gate = precision_gate(validation,policy['value'],plan['minimum_validation_selections'],floor)
        status = selected_gate['status']
    cutoff = selected_gate['threshold']
    if cutoff is not None and cutoff < floor-1e-12:
        raise ValueError('Stricter selection cannot widen the original entry set')
    selected = [p for p in later_predictions if cutoff is not None and p['score'] >= cutoff]
    return {'policy': policy, 'validation_gate': selected_gate, 'status': status,
        'later': dollars(selected,plan['notional_usd'],plan['cost_sensitivities_bps']),
        'later_predictions': selected, 'ready_for_trading':False}


def validate_plan(plan):
    if plan['notional_usd'] != 100 or plan['selection_cost_bps'] != 8:
        raise ValueError('Use the requested $100 notional and fixed cost proxy')
    if type(plan['minimum_validation_selections']) is not int or plan['minimum_validation_selections'] < 1:
        raise ValueError('Positive validation support required')
    if plan['diagnostic_validation_quantiles'] != [.99,.995] or plan['empirical_precision_targets'] != [.95,1.0]:
        raise ValueError('Use fixed stricter policies')
    if plan['cost_sensitivities_bps'] != [8,20,50]:
        raise ValueError('Use fixed cost sensitivities')


def run_precision_study(symbols, plan_path, net_report, training_archive, training_db, output):
    import numpy as np
    from threadpoolctl import threadpool_limits
    if output.suffix != '.json' or output.resolve() in {p.resolve() for p in (plan_path,net_report,training_archive,training_db)}:
        raise ValueError('JSON output must not overwrite inputs')
    plan=json.loads(plan_path.read_text());validate_plan(plan)
    source=json.loads(net_report.read_text())
    if digest(training_archive)!=source['source_hashes']['training_candles']:
        raise ValueError('Original training archive checksum mismatch')
    model_plan=source['plan'];fit_end=datetime.fromisoformat(model_plan['fit_end_utc'])
    validation_end=datetime.fromisoformat(model_plan['validation_end_utc'])
    # One day supplies four-hour past windows, without rereading prior-quarter
    # features into Python. Verify counts and original cutoffs before using rows.
    rows,_,_=load_rows(symbols,training_archive,training_db,fit_end-timedelta(days=1),validation_end,model_plan['hold_minutes'])
    validation=[r for r in rows if r['decision_utc']>=fit_end and r['label_end_utc']<=validation_end]
    label_samples(validation,training_archive,model_plan)
    bases=[c for c in source['candidates'] if c['validation_quantile']==.95]
    if len(bases)!=6 or len({(c['target_pct'],c['learner']) for c in bases})!=6:
        raise ValueError('Expected six fixed prior fitted models')
    policies=[{'kind':'quantile','value':q} for q in plan['diagnostic_validation_quantiles']]+[
        {'kind':'empirical_precision','value':p} for p in plan['empirical_precision_targets']]
    results=[];selections=[]
    output.parent.mkdir(parents=True,exist_ok=True)
    with threadpool_limits(limits=2):
        for base in bases:
            model=load_model(base['artifact'])
            if model['fit_end_utc']!=model_plan['fit_end_utc'] or model['validation_end_utc']!=model_plan['validation_end_utc']:
                raise ValueError('Saved model chronology differs from original plan')
            scores=probabilities(model,validation)
            floor=float(np.quantile(scores,.95))
            if len(validation)!=base['validation_samples'] or not math.isclose(floor,base['cutoff'],rel_tol=1e-10,abs_tol=1e-12):
                raise ValueError('Original validation count or threshold did not reproduce')
            target=base['target_pct']
            records=[{'score':float(p),'actual_rally':r['outcomes'][str(target)]['positive_net']} for r,p in zip(validation,scores)]
            # Later records are complete only above the old floor. Every new gate
            # stays above that floor, so no missing below-floor scores are needed.
            later=source['later_results'][base['id']]['selected_predictions']
            if any(p['score']<floor-1e-12 for p in later):
                raise ValueError('Prior later records violate their original cutoff')
            for policy in policies:
                result=evaluate_policy(records,later,policy,plan,floor)
                result.update(model=base['id'],target_pct=target,artifact=base['artifact'])
                results.append(result)
                selections.append({k:result[k] for k in ('model','policy','validation_gate','status')})
    # These are fixed policy comparisons, not selection of a winner using later P&L.
    report={'plan':plan,'results':results,'validation_decisions':selections,
        'source_hashes':{'net_model_report':digest(net_report),'training_archive':digest(training_archive),'plan':digest(plan_path)},
        'approved_live_policies':0,'ready_for_trading':False,
        'limitations':['A 100% empirical validation win rate is not a guarantee of future wins; classifier scores and score percentiles are not guaranteed probabilities.',
            'High-precision gates use earlier validation only, require at least twenty cases and include whole score-tie groups. Unsupported gates abstain.',
            '99th/99.5th percentile gates are diagnostic comparisons, not approved entry policies or 99%/99.5% win probabilities.',
            'Both quarters were already inspected. Repeated experimentation, small calibration samples and current asset selection limit inference.',
            'Every simulated position has a fixed $100 notional. P&L sums do not compound or establish an account return; peak capital is an estimate, not a funded-balance simulation.',
            'Costs are hypothetical spread/execution sensitivities. Stale timeout marks and missing entry prices remain explicit; actual Funded bot access and software stops are unconfirmed.',
            'No live signals, order placement or guarantee of profitable trading were added.']}
    output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    output.with_suffix('.selection.json').write_text(json.dumps({'decisions':selections,'basis':'Only earlier validation scores and net-positive labels; no later P&L selects a policy','ready_for_trading':False},indent=2)+'\n')
    lines=['# Stricter selections with $100 per simulated trade','',plan['evaluation_status'],'',
        'No live policies activated. Zero selections have undefined performance, not a demonstrated zero-loss strategy. Values below assume an 8 bps combined cost.', '',
        '| Model | Policy | Validation selected / wins | Later trades | Later wins / losses | Total P&L | Average P&L | Stale marks | Peak nominal capital |',
        '|---|---|---:|---:|---:|---:|---:|---:|---:|']
    for r in results:
        v=r['validation_gate'];l=r['later'];cost=l['costs']['8'];p=r['policy']
        name=f"{p['kind']} {p['value']:.1%}"
        mean='undefined' if cost['mean_pnl_usd'] is None else f"${cost['mean_pnl_usd']:.3f}"
        lines.append(f"| {r['model']} | {name} | {v['selected']} / {v['hits']} | {l['marked_trades']} | {cost['wins']} / {cost['losses']} | ${cost['sum_pnl_usd']:.2f} | {mean} | {l['stale_timeout_marks']} | ${l['peak_nominal_capital_usd']:.0f} |")
    lines.extend(['','JSON includes 8/20/50 bps sensitivities, individual selections, known versus stale-mark P&L, unfilled entries and simultaneous-position estimates. Later winners cannot be retroactively chosen.', '',
        '## Limits','',*('- '+v for v in report['limitations'])])
    output.with_suffix('.md').write_text('\n'.join(lines)+'\n')
    return report
