"""Fixed stricter policies from past validation scores, never test outcomes."""

import hashlib
import json
import math
import statistics
from pathlib import Path
from datetime import UTC, datetime, timedelta

POLICIES=(('validation_q95',.95,None),('validation_q97_5',.975,None),
          ('validation_q99',.99,None),('validation_q99_5',.995,None),
          ('validation_precision80',None,.8),('validation_precision90',None,.9),
          ('validation_zero_false_positives',None,1.0))
MODELS=('candle_only','trade_enriched')


def gate(validation,quantile=None,target=None,minimum=10):
    import numpy as np
    if not validation: return {'threshold':None,'status':'no_validation_data','selected':0,'hits':0}
    if minimum<1 or (quantile is None)==(target is None):raise ValueError('Specify one policy with positive minimum support')
    if any(not math.isfinite(r['score']) or not 0<=r['score']<=1 for r in validation):
        raise ValueError('Scores must be finite in [0,1]')
    if quantile is not None:
        if not 0<quantile<1:raise ValueError('Quantile must be between 0 and 1')
        threshold=float(np.quantile([r['score'] for r in validation],quantile))
    else:
        if not 0<target<=1:raise ValueError('Precision target must be in (0,1]')
        ordered=sorted(validation,key=lambda r:r['score'],reverse=True)
        count=hits=0;threshold=None
        for i,row in enumerate(ordered):
            count+=1;hits+=int(row['actual_rally'])
            # Whole tie groups belong together; no outcome-dependent tie breaking.
            if i+1<len(ordered) and ordered[i+1]['score']==row['score']:continue
            if count>=minimum and hits/count>=target:
                threshold=row['score']
        if threshold is None:
            return {'threshold':None,'status':'unsupported_precision_target','selected':0,'hits':0,
                    'minimum_validation_selections':minimum,'target_precision':target}
    selected=[r for r in validation if r['score']>=threshold]
    hits=sum(r['actual_rally'] for r in selected)
    return {'threshold':threshold,'status':'supported','selected':len(selected),'hits':hits,
        'false_positives':len(selected)-hits,'precision':hits/len(selected) if selected else None,
        'minimum_validation_selections':minimum if target is not None else None}


def summarize(selected,total_rallies):
    n=len(selected);hits=sum(r['actual_rally'] for r in selected)
    status=('no_selections_no_performance_estimate' if not n else
            'zero_observed_false_positives_not_a_guarantee' if hits==n else 'false_positives_observed')
    return {'selections':n,'rallies':hits,'false_positives':n-hits,
        'precision':hits/n if n else None,'recall':hits/total_rallies if total_rallies else None,
        'missed_rallies':total_rallies-hits,'evidence_status':status,'ready_for_trading':False,
        'worst_downside_pct':min(min(0,r['worst_downside'])*100 for r in selected) if n else None,
        'mean_close_return_pct':statistics.mean(r['close_return']*100 for r in selected) if n else None,
        'mean_net_close_proxy_pct':{str(c):statistics.mean(((1+r['close_return'])*(1-c/10000)-1)*100 for r in selected)
                                   if n else None for c in (0,50,100)}}


def threshold_study(input_path,output,minimum=10):
    if output.suffix!='.json':raise ValueError('Output must use .json')
    if input_path.resolve()==output.resolve():raise ValueError('Output cannot overwrite the input experiment')
    if type(minimum) is not int or minimum<1:raise ValueError('Minimum support must be a positive integer')
    source=json.loads(input_path.read_text())
    paired=[{(r['symbol'],r['decision_utc']):r['actual_rally'] for r in source['evaluation']['predictions'][name]} for name in MODELS]
    if paired[0]!=paired[1]:raise ValueError('Models must have paired test keys and labels')
    def instant(value):
        stamp=datetime.fromisoformat(value)
        if stamp.tzinfo is None:raise ValueError('Report times must be timezone-aware')
        return stamp.astimezone(UTC)
    evaluated=[f for f in source['evaluation']['folds'] if f['status']=='evaluated']
    results=[]
    for name in MODELS:
        predictions=source['evaluation']['predictions'][name]
        if len({(r['symbol'],r['decision_utc']) for r in predictions})!=len(predictions):
            raise ValueError('Duplicate test prediction keys')
        if any(not math.isfinite(r['score']) or not 0<=r['score']<=1 for r in predictions):
            raise ValueError('Invalid test scores')
        total_rallies=sum(r['actual_rally'] for r in predictions)
        for policy,quantile,target in POLICIES:
            selected=[];periods=[];covered=[]
            for fold in evaluated:
                model=fold['models'][name]
                validation=model.get('validation_predictions')
                if validation is None:
                    raise ValueError('Input lacks validation scores; rerun research-enriched with the updated command')
                if any(instant(r['label_end_utc'])>instant(fold['start_utc']) for r in validation):
                    raise ValueError('Validation target crosses the test-period boundary')
                threshold=gate(validation,quantile,target,minimum)
                test=[r for r in predictions if instant(fold['start_utc'])<=instant(r['decision_utc'])<instant(fold['end_utc'])]
                if any(instant(r['decision_utc'])+timedelta(minutes=source['horizon_minutes'])>instant(fold['end_utc']) for r in test):
                    raise ValueError('Test target crosses its period boundary')
                covered.extend((r['symbol'],r['decision_utc']) for r in test)
                chosen=[r for r in test if threshold['threshold'] is not None and r['score']>=threshold['threshold']]
                selected.extend(chosen)
                periods.append({'start_utc':fold['start_utc'],'end_utc':fold['end_utc'],
                    'validation_gate':threshold,'test':summarize(chosen,sum(r['actual_rally'] for r in test))})
            if len(covered)!=len(predictions) or len(set(covered))!=len(predictions):
                raise ValueError('Test periods do not uniquely cover all predictions')
            results.append({'model':name,'policy':policy,'summary':summarize(selected,total_rallies),
                'periods':periods,'unsupported_periods':sum(p['validation_gate']['threshold'] is None for p in periods),
                'test_predictions':len(predictions),'test_rallies':total_rallies})
    report={'input':input_path.name,'input_sha256':hashlib.sha256(input_path.read_bytes()).hexdigest(),
        'horizon_minutes':source['horizon_minutes'],'minimum_validation_selections':minimum,
        'policies':results,'ready_for_trading':False,
        'method':'Frozen fitted primary logistic models and identical test predictions. Fixed validation score quantiles and supported validation precision targets. Thresholds use only each preceding validation week; test labels only measure outcomes. No selection of a winning policy from this table.',
        'limitations':['The Q2 quarter was previously examined; these later fit-held-out periods are not fresh untouched study data.',
            'Quantile thresholds do not promise a fixed test selection rate, especially with ties or distribution shifts.',
            'Minimum validation support is a guardrail, not statistical proof; precision targets are empirical validation rates, not calibrated probabilities.',
            'No selections have undefined precision and provide no evidence of successful prediction.',
            'Zero observed false positives cannot guarantee zero future false positives; assets and episodes are correlated.',
            'High-touch labels do not establish executable profit. Close marks and assumed combined cost haircuts remain research proxies.',
            'A stricter gate may miss rallies, and comparing multiple policies is exploratory. No live scanner changes or orders.']}
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report,indent=2)+'\n')
    lines=['# Stricter threshold study','',report['method'],'',
        'Precision = rally hits / selected windows. Empty selections are not successes. No policy is marked ready for trading.',
        '', '| Model | Policy | Selections | Rallies | False positives | Precision | Missed rallies | Net close proxy, 50 bps |',
        '|---|---|---:|---:|---:|---:|---:|---:|']
    for item in results:
        s=item['summary'];p='undefined' if s['precision'] is None else f"{s['precision']:.1%}"
        net=s['mean_net_close_proxy_pct']['50'];net='undefined' if net is None else f'{net:.3f}%'
        lines.append(f"| {item['model']} | {item['policy']} | {s['selections']} | {s['rallies']} | {s['false_positives']} | {p} | {s['missed_rallies']} | {net} |")
    lines.extend(['','## Limits','',*('- '+v for v in report['limitations'])])
    output.with_suffix('.md').write_text('\n'.join(lines)+'\n')
    return report
