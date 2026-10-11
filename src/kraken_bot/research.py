"""Offline chronological regression baseline; no signals or execution."""

import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

FEATURES = ('return_1m', 'return_15m', 'return_60m', 'range_60m', 'volume_ratio')


def samples(store, symbol, start, end, horizon=60, *, archive_mode=False):
    # Unique minute keys plus exact timestamp spans prove contiguous windows.
    source = 'research_source' if archive_mode else 'candles'
    observed = 'observed' if archive_mode else 'TRUE'
    observed_filter = 'AND observed AND future_observed' if archive_mode else ''
    query = f"""WITH history AS (
      SELECT timestamp, {observed} AS observed,
        lead({observed},{horizon}) OVER w AS future_observed,
        CAST(close AS DOUBLE) AS price,
        lag(CAST(close AS DOUBLE),1) OVER w AS p1,
        lag(CAST(close AS DOUBLE),15) OVER w AS p15,
        lag(CAST(close AS DOUBLE),60) OVER w AS p60,
        lag(timestamp,60) OVER w AS first_stamp,
        max(CAST(high AS DOUBLE)) OVER past AS hi,
        min(CAST(low AS DOUBLE)) OVER past AS lo,
        sum(CAST(volume AS DOUBLE)) OVER past AS v60,
        sum(CAST(volume AS DOUBLE)) OVER last15 AS v15,
        lead(CAST(close AS DOUBLE),{horizon}) OVER w AS future_price,
        lead(timestamp,{horizon}) OVER w AS future_stamp,
        max(CAST(high AS DOUBLE)) OVER future AS future_high
      FROM {source} WHERE symbol=? AND timestamp>=? AND timestamp<?
      WINDOW w AS (ORDER BY timestamp),
        past AS (ORDER BY timestamp ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
        last15 AS (ORDER BY timestamp ROWS BETWEEN 14 PRECEDING AND CURRENT ROW),
        future AS (ORDER BY timestamp ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING)
    ) SELECT timestamp, price/p1-1, price/p15-1, price/p60-1,
        (hi-lo)/price, v15/(v60-v15)*3, future_price/price-1,
        future_high/price-1 FROM history
      WHERE timestamp-first_stamp=INTERVAL '60 minutes'
        AND future_stamp-timestamp=INTERVAL '{horizon} minutes'
        AND epoch(timestamp+INTERVAL '1 minute') % ({horizon}*60)=0
        AND price>0 AND p1>0 AND p15>0 AND p60>0 AND v60>v15
        {observed_filter}
      ORDER BY timestamp"""
    return [(stamp+timedelta(minutes=1), tuple(values[:5]), values[5], values[6])
            for stamp, *values in store.connection.execute(query, [symbol,start,end]).fetchall()
            if all(math.isfinite(v) for v in values)]


def solve(matrix, vector):
    rows = [list(row)+[value] for row,value in zip(matrix,vector)]
    for col in range(len(vector)):
        pivot = max(range(col,len(vector)), key=lambda i: abs(rows[i][col]))
        rows[col], rows[pivot] = rows[pivot], rows[col]
        if abs(rows[col][col]) < 1e-15:
            raise ValueError('Singular regression matrix')
        divisor = rows[col][col]
        rows[col] = [v/divisor for v in rows[col]]
        for i in range(len(vector)):
            if i != col:
                factor = rows[i][col]
                rows[i] = [a-factor*b for a,b in zip(rows[i],rows[col])]
    return [row[-1] for row in rows]


def fit(train, penalty=10.0):
    if len(train)<30:
        raise ValueError('At least 30 training observations are required')
    means = [sum(row[1][j] for row in train)/len(train) for j in range(5)]
    scales = [math.sqrt(sum((row[1][j]-means[j])**2 for row in train)/len(train)) or 1 for j in range(5)]
    matrix = [[0.0]*6 for _ in range(6)]
    vector = [0.0]*6
    for _, features, target, *_ in train:
        x = [1]+[(v-m)/s for v,m,s in zip(features,means,scales)]
        for i in range(6):
            vector[i] += x[i]*target
            for j in range(6):
                matrix[i][j] += x[i]*x[j]
    for i in range(1,6):
        matrix[i][i] += penalty
    coefficients = solve(matrix,vector)
    return {'means': means, 'scales': scales, 'coefficients': coefficients, 'ridge_penalty': penalty}


def predict(model, features):
    x = [1]+[(v-m)/s for v,m,s in zip(features,model['means'],model['scales'])]
    return sum(a*b for a,b in zip(x,model['coefficients']))


def evaluate(model, train, test):
    baseline = sum(row[2] for row in train)/len(train)
    actual = [row[2] for row in test]
    predicted = [predict(model,row[1]) for row in test]
    mse = sum((p-y)**2 for p,y in zip(predicted,actual))/len(test)
    baseline_mse = sum((baseline-y)**2 for y in actual)/len(test)
    mean = sum(actual)/len(actual)
    variance = sum((y-mean)**2 for y in actual)/len(actual)
    selected = sorted(zip(predicted,test),key=lambda item:item[0],reverse=True)[:max(1,len(test)//10)]
    return {'observations':len(test), 'rmse_pct':math.sqrt(mse)*100,
            'training_mean_baseline_rmse_pct': math.sqrt(baseline_mse)*100,
            'r_squared': 1-mse/variance if variance else None,
            'beats_training_mean_baseline':mse<baseline_mse,
            'future_10pct_high_events':sum(row[3]>=0.10 for row in test),
            'future_10pct_high_rate':sum(row[3]>=0.10 for row in test)/len(test),
            'top_decile':{'observations':len(selected),
                'mean_forward_close_return_pct':sum(row[2] for _,row in selected)/len(selected)*100,
                'future_10pct_high_events':sum(row[3]>=0.10 for _,row in selected)}}


def research(store, symbols, output: Path, start: datetime, end: datetime, horizon=60):
    if start.tzinfo is None or end.tzinfo is None or start>=end:
        raise ValueError('Research requires an increasing UTC date range')
    if horizon < 15 or horizon > 1440:
        raise ValueError('Horizon must be 15–1440 minutes')
    cutoff = start+(end-start)*0.7
    train, test, coverage, events = [], [], [], []
    for symbol in symbols:
        rows = samples(store,symbol,start,end,horizon)
        training = [row for row in rows if row[0]+timedelta(minutes=horizon)<=cutoff]
        testing = [row for row in rows if row[0]>=cutoff]
        train.extend(training);test.extend(testing)
        events.extend({'symbol':symbol,'decision_utc':row[0].isoformat(),
                       'future_high_return_pct':row[3]*100,'future_close_return_pct':row[2]*100}
                      for row in testing if row[3]>=0.10)
        coverage.append({'symbol':symbol,'eligible_samples':len(rows),'train':len(training),'test':len(testing)})
    if len(test)<30:
        raise ValueError('At least 30 held-out observations are required; increase the date range')
    model = fit(train)
    report = {'start_utc':start.isoformat(),'end_exclusive_utc':end.isoformat(),
              'test_start_utc':cutoff.isoformat(),'horizon_minutes':horizon,
              'train_observations':len(train),'features':FEATURES,'model':model,
              'test':evaluate(model,train,test),'coverage':coverage,'held_out_10pct_events':events,
              'method':'Pooled ridge linear regression; chronological 70/30 time split; train-only scaling; purged training labels; non-overlapping horizon samples per pair; complete minute windows only.',
              'limitations':['Single holdout, not walk-forward validation or a trading backtest.',
                  'Current asset selection introduces survivorship/selection bias; market caps and historic eligibility are unverified.',
                  'Sparse assets are excluded disproportionately by complete-window requirements.',
                  'Samples across assets remain correlated; counts are not independent events.',
                  'Future high touching 10% is descriptive, not an executable 10% profit or predicted probability.',
                  'Returns exclude fees, spread, slippage and order fills. No trading orders or signals.']}
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report,indent=2)+'\n')
    metrics = report['test']
    lines = ['# Historical regression baseline', '',
        f"Period: {start.isoformat()} through {end.isoformat()} (exclusive).",
        f"Training: {len(train):,} observations. Held out: {len(test):,} observations.",
        f"Target: close return over the next {horizon} minutes.", '',
        f"Model RMSE: {metrics['rmse_pct']:.3f} percentage points.",
        f"Training-mean baseline RMSE: {metrics['training_mean_baseline_rmse_pct']:.3f} percentage points.",
        f"Beats baseline: {metrics['beats_training_mean_baseline']}.",
        f"Held-out windows touching +10% at a future high: {metrics['future_10pct_high_events']}.", '',
        report['method'], '', '## Limits', '']
    lines.extend('- '+limit for limit in report['limitations'])
    lines.extend(['', '## Eligible complete windows by asset', '',
                  '| Asset | Training | Held out |', '|---|---:|---:|'])
    lines.extend(f"| {item['symbol']} | {item['train']} | {item['test']} |" for item in coverage)
    output.with_suffix('.md').write_text('\n'.join(lines)+'\n')
    return report
