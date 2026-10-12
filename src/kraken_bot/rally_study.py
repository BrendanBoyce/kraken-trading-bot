"""Exploratory rally hypotheses, matched controls and chronological evaluation."""

import hashlib
import json
import logging
import math
import shutil
import statistics
import tempfile
import zipfile
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .history import archive_members
from .walk_forward import prepare_archive_source

LOG = logging.getLogger(__name__)
HYPOTHESES = {
    'volume_surge': 'Past-hour volume >= 3x preceding three-hour hourly average',
    'momentum_volume': 'Past-hour close return >= 2%, volume ratio >= 2',
    'breakout_volume': 'Close above preceding four-hour high, volume ratio >= 2',
    'compression_volume': 'Past-hour range <= 2%, volume ratio >= 2, traded-minute fraction >= 50%',
}
FEATURES = ('return_60m', 'return_240m', 'range_60m', 'volume_ratio', 'volatility_60m', 'traded_fraction_60m')


def prepare_features(store):
    store.connection.execute('DROP TABLE IF EXISTS rally_features')
    store.connection.execute("""CREATE TEMP TABLE rally_features AS
      WITH returns AS (
        SELECT *, ln(close/lag(close) OVER (ORDER BY timestamp)) AS minute_return
        FROM research_source
      ) SELECT *,
        close/lag(close,60) OVER w-1 AS return_60m,
        close/lag(close,240) OVER w-1 AS return_240m,
        lag(timestamp,240) OVER w AS first_stamp,
        (max(high) OVER past60-min(low) OVER past60)/close AS range_60m,
        sum(volume) OVER past60 AS v60, sum(volume) OVER past240 AS v240,
        stddev_samp(minute_return) OVER past60 AS volatility_60m,
        avg(observed::INTEGER) OVER past60 AS traded_fraction_60m,
        max(high) OVER preceding240 AS previous_high
      FROM returns
      WINDOW w AS (ORDER BY timestamp),
        past60 AS (ORDER BY timestamp ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
        past240 AS (ORDER BY timestamp ROWS BETWEEN 239 PRECEDING AND CURRENT ROW),
        preceding240 AS (ORDER BY timestamp ROWS BETWEEN 240 PRECEDING AND 1 PRECEDING)""")


def event_samples(store, symbol, horizon):
    """Eligibility uses past/current data; future trade presence does not filter rows."""
    if type(horizon) is not int or not 60<=horizon<=1440:
        raise ValueError('Rally horizon must be 60–1440 whole minutes')
    db=store.connection
    db.execute('DROP TABLE IF EXISTS rally_decisions')
    db.execute(f"""CREATE TEMP TABLE rally_decisions AS WITH labels AS (
        SELECT *, lead(close,{horizon}) OVER w AS future_close,
          lead(observed,{horizon}) OVER w AS future_observed,
          lead(timestamp,{horizon}) OVER w AS last_stamp,
          max(high) OVER future AS future_high, min(low) OVER future AS future_low
        FROM rally_features
        WINDOW w AS (ORDER BY timestamp),
          future AS (ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING)
      ) SELECT timestamp,close AS price,return_60m,return_240m,range_60m,
          v60/(v240-v60)*3 AS volume_ratio,volatility_60m,traded_fraction_60m,
          close>previous_high AS breakout,
          future_close/close-1 AS close_return,future_high/close-1 AS high_return,
          future_low/close-1 AS low_return,future_observed
        FROM labels WHERE observed
          AND timestamp-first_stamp=INTERVAL '240 minutes'
          AND last_stamp-timestamp=INTERVAL '{horizon} minutes'
          AND epoch(timestamp+INTERVAL '1 minute')%({horizon}*60)=0
          AND traded_fraction_60m>=0.25 AND v240>v60 AND close>0""")
    cursor=db.execute(f"""SELECT d.*,
        min(s.timestamp) FILTER (WHERE s.observed AND s.high>=d.price*1.10) AS first_target,
        min(s.timestamp) FILTER (WHERE s.observed AND s.low<=d.price*0.95) AS first_downside
      FROM rally_decisions d LEFT JOIN research_source s
        ON s.timestamp>d.timestamp AND s.timestamp<=d.timestamp+INTERVAL '{horizon} minutes'
      GROUP BY ALL ORDER BY d.timestamp""")
    names=[v[0] for v in cursor.description]
    result=[]
    for values in cursor.fetchall():
        row=dict(zip(names,values))
        if any(row[f] is None or not math.isfinite(row[f]) for f in FEATURES):
            continue
        row['symbol']=symbol
        row['decision_utc']=row.pop('timestamp')+timedelta(minutes=1)
        row['label_end_utc']=row['decision_utc']+timedelta(minutes=horizon)
        target,downside=row['first_target'],row['first_downside']
        row['rally']=target is not None
        row['path_outcome']=('ambiguous_same_minute' if target is not None and target==downside else
            'downside_before_target' if downside is not None and (target is None or downside<target) else
            'target_before_downside' if target is not None else 'neither_level')
        result.append(row)
    return result


def hypothesis(row,name):
    if name=='volume_surge': return row['volume_ratio']>=3
    if name=='momentum_volume': return row['return_60m']>=0.02 and row['volume_ratio']>=2
    if name=='breakout_volume': return row['breakout'] and row['volume_ratio']>=2
    if name=='compression_volume':
        return row['range_60m']<=0.02 and row['volume_ratio']>=2 and row['traded_fraction_60m']>=0.5
    raise ValueError('Unknown research hypothesis')


def episodes(rows):
    """Merge adjacent positive windows per asset; still not independent market events."""
    last={}; count=0
    for row in sorted((r for r in rows if r['rally']),key=lambda r:(r['symbol'],r['decision_utc'])):
        if row['symbol'] not in last or row['decision_utc']>last[row['symbol']]: count+=1
        last[row['symbol']]=max(last.get(row['symbol'],row['label_end_utc']),row['label_end_utc'])
    return count


def net_close(row,cost_bps):
    # Illustrative total cost haircut, not a quote or actual Kraken fee schedule.
    return ((1+row['close_return'])*(1-cost_bps/10000)-1)*100


def summarize(rows,costs):
    if not rows:
        return {'observations':0,'rallies':0,'rally_rate':None,'merged_positive_episodes':0}
    n=len(rows)
    return {'observations':n,'rallies':sum(r['rally'] for r in rows),
        'rally_rate':sum(r['rally'] for r in rows)/n,'merged_positive_episodes':episodes(rows),
        'mean_forward_close_return_pct':statistics.mean(r['close_return']*100 for r in rows),
        'median_worst_downside_pct':statistics.median(min(0,r['low_return'])*100 for r in rows),
        'worst_downside_pct':min(min(0,r['low_return'])*100 for r in rows),
        'downside_5pct_touches':sum(r['low_return']<=-0.05+1e-12 for r in rows),
        'path_outcomes':dict(Counter(r['path_outcome'] for r in rows)),
        'target_endpoint_without_recorded_trade':sum(not r['future_observed'] for r in rows),
        'mean_net_close_proxy_pct_by_cost_bps':{str(c):statistics.mean(net_close(r,c) for r in rows) for c in costs}}


def condition_stats(rows,name,costs):
    selected=[r for r in rows if hypothesis(r,name)]
    controls=[r for r in rows if not hypothesis(r,name)]
    result=summarize(selected,costs)
    by_asset=defaultdict(list)
    for row in controls: by_asset[row['symbol']].append(row)
    global_rate=sum(r['rally'] for r in controls)/len(controls) if controls else None
    expected=0; fallback=0
    for row in selected:
        peers=by_asset[row['symbol']]
        rate=sum(r['rally'] for r in peers)/len(peers) if peers else global_rate
        if rate is None:
            expected=None;break
        if not peers: fallback+=1
        expected+=rate
    baseline=expected/len(selected) if selected and expected is not None else None
    result.update(hypothesis=name,asset_matched_control_rate=baseline,global_fallback_rows=fallback,
        rally_rate_difference=(result['rally_rate']-baseline if baseline is not None else None),
        lift=(result['rally_rate']/baseline if baseline and selected else None),
        false_positives=len(selected)-result['rallies'],
        recall=(result['rallies']/sum(r['rally'] for r in rows) if any(r['rally'] for r in rows) else None))
    return result


def choose_condition(training,costs):
    candidates=[condition_stats(training,name,costs) for name in HYPOTHESES]
    supported=[c for c in candidates if c['observations']>=30 and c['rallies']>=3
               and c['merged_positive_episodes']>=3 and c['rally_rate_difference'] is not None
               and c['rally_rate_difference']>0]
    # Fixed thresholds; choose only by earlier asset-adjusted rate difference.
    winner=max(supported,key=lambda c:(c['rally_rate_difference'],c['rallies'],c['hypothesis'])) if supported else None
    return (winner['hypothesis'] if winner else None),candidates


def matched_description(rows):
    """Descriptive label-based matches, never inputs to chronological selection."""
    week=lambda r: (r['symbol'],r['decision_utc'].isocalendar()[:2])
    negatives=defaultdict(list)
    for row in rows:
        if not row['rally']: negatives[week(row)].append(row)
    cases=[];controls=[];unmatched=0
    for case in sorted((r for r in rows if r['rally']),key=lambda r:r['decision_utc']):
        peers=negatives[week(case)]
        if not peers: unmatched+=1;continue
        control=min(peers,key=lambda r:abs((r['decision_utc']-case['decision_utc']).total_seconds()))
        peers.remove(control)
        cases.append(case);controls.append(control)
    return {'matched_rally_windows':len(cases),'unmatched_rally_windows':unmatched,
        'policy':'One nearest non-rally window from the same asset and ISO calendar week, without control reuse; descriptive only.',
        'feature_medians':{f:{'rally':statistics.median(r[f] for r in cases) if cases else None,
                               'control':statistics.median(r[f] for r in controls) if controls else None}
                           for f in FEATURES}}


def evaluate_periods(rows,start,end,horizon,costs,train_days=28,test_days=7):
    cutoff=start+timedelta(days=train_days)
    folds=[]; tested=[];selected_test=[]
    while cutoff<end:
        fold_end=min(cutoff+timedelta(days=test_days),end)
        training=[r for r in rows if r['label_end_utc']<=cutoff]
        test=[r for r in rows if r['decision_utc']>=cutoff and r['label_end_utc']<=fold_end]
        winner,candidates=choose_condition(training,costs)
        chosen=[r for r in test if winner and hypothesis(r,winner)]
        fold={'start_utc':cutoff.isoformat(),'end_utc':fold_end.isoformat(),
            'training_observations':len(training),'selected_hypothesis':winner,
            'training_candidates':candidates,'test_baseline':summarize(test,costs),
            'selected_test':condition_stats(test,winner,costs) if winner else summarize([],costs)}
        folds.append(fold);tested.extend(test);selected_test.extend(chosen)
        cutoff=fold_end
    return {'folds':folds,'test_baseline':summarize(tested,costs),
        'selected_test':summarize(selected_test,costs),
        'folds_with_selected_condition':sum(f['selected_hypothesis'] is not None for f in folds),
        'folds_with_test_exposure':sum(f['selected_test']['observations']>0 for f in folds),
        'folds_with_positive_asset_adjusted_difference':sum(
            f['selected_test'].get('rally_rate_difference') is not None and
            f['selected_test']['rally_rate_difference']>0 for f in folds)},tested


def rally_study(store,symbols,archive_path,output,start,end,horizons=(240,720,1440),costs=(0,50,100)):
    if output.suffix!='.json': raise ValueError('Output must use a .json extension')
    if not horizons or any(type(h) is not int or not 60<=h<=1440 for h in horizons) or len(set(horizons))!=len(horizons):
        raise ValueError('Distinct whole-minute horizons from 60 to 1440 are required')
    if not costs or any(not math.isfinite(c) or c<0 or c>=10000 for c in costs):
        raise ValueError('Cost scenarios must be finite basis points from 0 to less than 10000')
    if start.tzinfo is None or end.tzinfo is None or end-start<=timedelta(days=28):
        raise ValueError('An increasing aware date range longer than 28 days is required')
    all_rows={h:[] for h in horizons};coverage=[]
    with archive_path.open('rb') as source: digest=hashlib.file_digest(source,'sha256').hexdigest()
    with zipfile.ZipFile(archive_path) as archive:
        manifest=json.loads(archive.read('MANIFEST.json'))
        if manifest.get('product')!='ohlcvt' or manifest.get('has_header') is not False:
            raise ValueError('Expected an official-format Kraken OHLCVT archive')
        begin=datetime.fromisoformat(manifest['coverage']['start']).replace(tzinfo=UTC)
        finish=datetime.fromisoformat(manifest['coverage']['end']).replace(tzinfo=UTC)+timedelta(days=1)
        if start<begin or end>finish: raise ValueError('Requested dates extend outside archive coverage')
        for symbol,member in archive_members(archive,symbols).items():
            if member is None:
                coverage.append({'symbol':symbol,'status':'not_in_archive'});continue
            with tempfile.TemporaryDirectory(prefix='kraken-rallies-') as temporary:
                path=Path(temporary)/'candles.csv'
                with archive.open(member) as source,path.open('wb') as destination:
                    shutil.copyfileobj(source,destination,length=1024*1024)
                counts=prepare_archive_source(store,path,symbol,start,end)
                prepare_features(store)
                eligible={}
                for horizon in horizons:
                    rows=event_samples(store,symbol,horizon)
                    all_rows[horizon].extend(rows);eligible[str(horizon)]=len(rows)
            coverage.append({'symbol':symbol,'status':'read_from_archive',**counts,'eligible_samples':eligible})
            LOG.info('%s rally samples: %s',symbol,eligible)
    experiments=[]
    for horizon,rows in all_rows.items():
        chronological,tested=evaluate_periods(rows,start,end,horizon,costs)
        events=[{key:r[key] for key in ('symbol','decision_utc','label_end_utc','price','high_return',
                'low_return','close_return','path_outcome','first_target','first_downside',
                'future_observed',*FEATURES)} for r in tested if r['rally']]
        experiments.append({'horizon_minutes':horizon,'role':'primary' if horizon==horizons[0] else 'exploratory',
            'all_sample_summary':summarize(rows,costs),'chronological':chronological,
            'matched_test_description':matched_description(tested),'test_rally_windows':events,
            'test_rally_asset_days':len(set((r['symbol'],r['decision_utc'].date()) for r in tested if r['rally']))})
    report={'archive':archive_path.name,'archive_sha256':digest,'start_utc':start.isoformat(),
        'end_exclusive_utc':end.isoformat(),'rally_threshold_pct':10,'downside_threshold_pct':-5,
        'cost_scenarios_bps':costs,'hypotheses':HYPOTHESES,'coverage':coverage,'experiments':experiments,
        'selection_policy':'Fixed hypotheses and thresholds. Expanding 28-day initial training, 7-day test folds. Select highest earlier asset-adjusted rally-rate difference with >=30 exposures, >=3 rallies and >=3 merged positive episodes. No choice if unsupported. Label ends purged at fold boundaries.',
        'eligibility':'Recorded trade at decision minute; >=25% past-hour traded minutes, four-hour lookback and prior volume. Regular UTC-aligned, non-overlapping horizon windows. Future trade presence is reported, never used to filter eligibility. Archive-only carry marks do not change stored candles.',
        'cost_policy':'Illustrative combined fees/spread/slippage haircut: net close proxy = (1+gross close return)*(1-cost_bps/10000)-1. These are assumed costs, not current exchange fees. Future highs are never counted as realized returns.',
        'limitations':['Exploratory reuse of Q2 data already examined; a new untouched period is required for validation.',
            'A +10% high touch does not establish an executable sale or 10% profit. Neither closes nor carry marks are executable quotes.',
            'Downside-first comparisons use first crossing minute; same-minute target/downside order is ambiguous and is never assumed favorable.',
            'Non-overlapping windows and merged episodes do not remove cross-asset correlation or shared market shocks.',
            'Calendar matching is descriptive and uses outcome labels, never training inputs or trading instructions.',
            'Current asset selection has survivorship bias and is not a verified historical low-cap universe.',
            'Comparing multiple hypotheses and horizons creates multiple-testing risk. Small event counts cannot prove reliability.',
            'Regular UTC sampling misses rallies outside sampled reference windows; archive omissions are treated as no trades only within member bounds.',
            'No trading signals, order execution, optimal exits or portfolio-profit claims.']}
    report['conclusion']='This is an exploratory event study, not a validated predictive edge. Review false positives, downside and period consistency before interpreting any uplift.'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    lines=['# Rally-event research','',report['conclusion'],'',
        'Target: +10% future high touch; downside threshold: -5%. Four hours is the default primary horizon.',
        '',report['selection_policy'],'',report['cost_policy'],'',
        '## Chronological results','','| Horizon | Role | Test windows | Rally windows | Selected windows | Selected rallies | False positives |',
        '|---|---|---:|---:|---:|---:|---:|']
    for experiment in experiments:
        result=experiment['chronological'];base=result['test_baseline'];chosen=result['selected_test']
        lines.append(f"| {experiment['horizon_minutes']//60}h | {experiment['role']} | {base['observations']} | {base['rallies']} | {chosen['observations']} | {chosen['rallies']} | {chosen['observations']-chosen['rallies']} |")
    for experiment in experiments:
        result=experiment['chronological'];base=result['test_baseline'];chosen=result['selected_test']
        lines.extend(['',f"### {experiment['horizon_minutes']//60}h: downside and costs",'',
            f"Rally windows' first-crossing order: {dict(Counter(r['path_outcome'] for r in experiment['test_rally_windows']))}.",
            f"All-window mean net close proxies by assumed cost (bps): {base.get('mean_net_close_proxy_pct_by_cost_bps',{})}.",
            f"Selected-window mean net close proxies: {chosen.get('mean_net_close_proxy_pct_by_cost_bps',{})}.",
            f"Selected exposures' worst observed downside: {chosen.get('worst_downside_pct')}%.",
            f"Periods with positive asset-adjusted difference: {result['folds_with_positive_asset_adjusted_difference']} of {result['folds_with_test_exposure']} exposed periods."])
    for experiment in experiments:
        matched=experiment['matched_test_description']
        lines.extend(['',f"## {experiment['horizon_minutes']//60}h matched rally/control comparison",'',
            f"Matched windows: {matched['matched_rally_windows']}; unmatched rallies: {matched['unmatched_rally_windows']}.",
            matched['policy'],'','| Past feature | Rally median | Control median |', '|---|---:|---:|'])
        for feature,values in matched['feature_medians'].items():
            multiplier=1 if feature=='volume_ratio' else 100
            formatted=lambda value: 'unavailable' if value is None else f'{value*multiplier:.3f}'
            unit='x' if feature=='volume_ratio' else '%'
            lines.append(f"| {feature} ({unit}) | {formatted(values['rally'])} | {formatted(values['control'])} |")
    lines.extend(['','## Limits','',*('- '+v for v in report['limitations'])])
    output.with_suffix('.md').write_text('\n'.join(lines)+'\n')
    return report
