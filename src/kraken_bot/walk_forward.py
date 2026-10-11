"""Archive-specific no-trade marking and expanding chronological evaluation."""

import hashlib
import json
import logging
import math
import shutil
import tempfile
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .history import archive_members
from .research import FEATURES, evaluate, fit, predict, samples

LOG = logging.getLogger(__name__)


def prepare_archive_source(store, path, symbol, start, end):
    """Carry past prices only inside an authoritative archive member's bounds.

    These temporary research marks are not inserted into the candle database.
    An absent archive minute means no trades, unlike a collector disconnect.
    Both entry-reference and future-close minutes must contain actual trades.
    """
    db = store.connection
    db.execute('DROP TABLE IF EXISTS archive_raw')
    db.execute("""CREATE TEMP TABLE archive_raw AS SELECT to_timestamp(epoch) AS timestamp,
        high,low,close,volume,trades FROM read_csv(?,header=false,
        columns={'epoch':'BIGINT','open':'DOUBLE','high':'DOUBLE','low':'DOUBLE',
          'close':'DOUBLE','volume':'DOUBLE','trades':'BIGINT'},
        strict_mode=true,null_padding=false)
        WHERE to_timestamp(epoch)>=? AND to_timestamp(epoch)<?""", [str(path),start,end])
    invalid = db.execute("""SELECT count(*) FROM archive_raw WHERE
        timestamp IS NULL OR epoch(timestamp)%60!=0 OR
        close IS NULL OR high IS NULL OR low IS NULL OR volume IS NULL OR trades IS NULL
        OR NOT isfinite(close) OR NOT isfinite(high) OR NOT isfinite(low) OR NOT isfinite(volume)
        OR close<=0 OR low<=0 OR high<close OR low>close OR volume<0 OR trades<0""").fetchone()[0]
    duplicate = db.execute('SELECT count(*)-count(DISTINCT timestamp) FROM archive_raw').fetchone()[0]
    if invalid or duplicate:
        raise ValueError(f'{symbol}: invalid or duplicate archive rows')
    db.execute('DROP TABLE IF EXISTS research_source')
    db.execute("""CREATE TEMP TABLE research_source AS
        WITH grid AS (
            SELECT tick AS timestamp FROM generate_series(
                (SELECT min(timestamp) FROM archive_raw),
                (SELECT max(timestamp) FROM archive_raw), INTERVAL '1 minute') AS ticks(tick)
        ), marks AS (
            SELECT grid.timestamp, archive_raw.high, archive_raw.low,
                coalesce(archive_raw.volume,0) AS volume,
                coalesce(archive_raw.trades>0,FALSE) AS observed,
                last_value(archive_raw.close IGNORE NULLS) OVER (
                    ORDER BY grid.timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS close
            FROM grid LEFT JOIN archive_raw USING(timestamp)
        ) SELECT ? AS symbol,timestamp,close,coalesce(high,close) AS high,
            coalesce(low,close) AS low,volume,observed FROM marks""", [symbol])
    grid, traded = db.execute('SELECT count(*), count(*) FILTER (WHERE observed) FROM research_source').fetchone()
    source_rows = db.execute('SELECT count(*) FROM archive_raw').fetchone()[0]
    return {'archive_rows':source_rows,'research_minutes':grid,
            'carried_no_trade_minutes':grid-source_rows,'traded_minutes':traded}


def split_fold(rows, start, end, horizon):
    duration = timedelta(minutes=horizon)
    training = [row for row in rows if row[0]+duration<=start]
    testing = [row for row in rows if row[0]>=start and row[0]+duration<=end]
    return training, testing


def asset_baseline_mse(train, test):
    totals = {}
    for row in train:
        total,count = totals.get(row[4],(0,0))
        totals[row[4]] = (total+row[2],count+1)
    global_mean = sum(row[2] for row in train)/len(train)
    return sum((row[2]-(totals[row[4]][0]/totals[row[4]][1]
                        if row[4] in totals else global_mean))**2 for row in test)/len(test)


def walk_forward(store, symbols, archive_path, output, start, end, horizon=60,
                 train_days=28, test_days=7):
    if type(horizon) is not int or not 15<=horizon<=1440:
        raise ValueError('Horizon must be an integer from 15 to 1440 minutes')
    if start.tzinfo is None or end.tzinfo is None or start>=end:
        raise ValueError('Increasing timezone-aware dates are required')
    if type(train_days) is not int or type(test_days) is not int or min(train_days,test_days)<1:
        raise ValueError('Training and test days must be positive integers')
    if output.suffix!='.json':
        raise ValueError('Report output must use a .json extension')
    cutoff = start+timedelta(days=train_days)
    if cutoff>=end:
        raise ValueError('Date range must extend beyond the initial training period')
    rows, coverage = [], []
    with archive_path.open('rb') as source:
        digest = hashlib.file_digest(source,'sha256').hexdigest()
    with zipfile.ZipFile(archive_path) as archive:
        manifest = json.loads(archive.read('MANIFEST.json'))
        if manifest.get('product')!='ohlcvt' or manifest.get('has_header') is not False:
            raise ValueError('Expected a Kraken headerless OHLCVT archive manifest')
        archive_start = datetime.fromisoformat(manifest['coverage']['start']).replace(tzinfo=UTC)
        archive_end = datetime.fromisoformat(manifest['coverage']['end']).replace(tzinfo=UTC)+timedelta(days=1)
        if start<archive_start or end>archive_end:
            raise ValueError('Requested dates extend outside this archive; coverage gaps cannot be filled')
        for symbol, member in archive_members(archive,symbols).items():
            if member is None:
                coverage.append({'symbol':symbol,'status':'not_in_archive','eligible_samples':0})
                continue
            with tempfile.TemporaryDirectory(prefix='kraken-research-') as temporary:
                path = Path(temporary)/'candles.csv'
                with archive.open(member) as source,path.open('wb') as destination:
                    shutil.copyfileobj(source,destination,length=1024*1024)
                counts = prepare_archive_source(store,path,symbol,start,end)
                eligible = samples(store,symbol,start,end,horizon,archive_mode=True)
            rows.extend((*row,symbol) for row in eligible)
            coverage.append({'symbol':symbol,'status':'read_from_archive',
                             'eligible_samples':len(eligible),**counts})
            LOG.info('%s: %s eligible research windows',symbol,len(eligible))
    folds, predictions, actual, events = [], [], [], []
    test_counts = {}
    while cutoff<end:
        fold_end = min(cutoff+timedelta(days=test_days),end)
        train,test = split_fold(rows,cutoff,fold_end,horizon)
        fold = {'start_utc':cutoff.isoformat(),'end_utc':fold_end.isoformat(),
                'train_observations':len(train),'test_observations':len(test)}
        if len(train)<30 or len(test)<30:
            fold['status']='insufficient_observations'
        else:
            model = fit(train)
            metrics = evaluate(model,train,test)
            metrics['per_asset_training_mean_rmse_pct']=math.sqrt(asset_baseline_mse(train,test))*100
            metrics['zero_return_rmse_pct']=math.sqrt(sum(row[2]**2 for row in test)/len(test))*100
            fold.update(status='evaluated',metrics=metrics,model=model)
            predicted = [predict(model,row[1]) for row in test]
            predictions.extend(predicted);actual.extend(row[2] for row in test)
            for row in test:
                test_counts[row[4]] = test_counts.get(row[4],0)+1
            events.extend({'symbol':row[4],'decision_utc':row[0].isoformat(),
                           'future_high_return_pct':row[3]*100,'future_close_return_pct':row[2]*100,
                           'predicted_close_return_pct':prediction*100}
                          for prediction,row in zip(predicted,test) if row[3]>=0.10)
        folds.append(fold)
        cutoff=fold_end
    for item in coverage:
        item['test_observations'] = test_counts.get(item['symbol'],0)
    valid = [fold for fold in folds if fold['status']=='evaluated']
    if not valid:
        raise ValueError('No folds had sufficient observations; no model results generated')
    total = len(actual)
    weighted_rmse = lambda key: math.sqrt(sum(f['metrics'][key]**2*f['test_observations'] for f in valid)/total)
    mse = sum((p-y)**2 for p,y in zip(predictions,actual))/total
    mean = sum(actual)/total
    variance = sum((y-mean)**2 for y in actual)/total
    report = {'archive':archive_path.name,'archive_sha256':digest,'manifest':manifest,
        'start_utc':start.isoformat(),'end_exclusive_utc':end.isoformat(),
        'horizon_minutes':horizon,'initial_train_days':train_days,'test_days':test_days,
        'features':FEATURES,'folds':folds,'coverage':coverage,'held_out_10pct_events':events,
        'aggregate':{'test_observations':total,'evaluated_folds':len(valid),
            'assets_with_eligible_samples':sum(c['eligible_samples']>0 for c in coverage),
            'assets_with_test_samples':len(test_counts),
            'model_rmse_pct':math.sqrt(mse)*100,'r_squared':1-mse/variance if variance else None,
            'training_mean_baseline_rmse_pct':weighted_rmse('training_mean_baseline_rmse_pct'),
            'per_asset_training_mean_rmse_pct':weighted_rmse('per_asset_training_mean_rmse_pct'),
            'zero_return_rmse_pct':weighted_rmse('zero_return_rmse_pct'),
            'folds_beating_training_mean':sum(f['metrics']['beats_training_mean_baseline'] for f in valid),
            'folds_beating_per_asset_mean':sum(f['metrics']['rmse_pct']<f['metrics']['per_asset_training_mean_rmse_pct'] for f in valid),
            'future_10pct_high_events':len(events)},
        'method':'Fixed ridge baseline, expanding training window, fresh train-only scaling per fold, purged label ends, disjoint chronological test periods. No hyperparameter search.',
        'no_trade_policy':'Past-price carry and zero volume only inside an actual archive member; bounds clipped to requested dates and first/last recorded trade. Entry and exit reference minutes require recorded trades. Temporary research marks never enter the stored candle table.',
        'limitations':['Exploratory reuse of previously examined Q2 data; these are not fresh untouched validation periods.',
            'Archive omissions are interpreted as no trades under Kraken documentation, not as collector gaps.',
            'Recorded trade closes do not guarantee executable fills; spread, depth, fees and slippage are unavailable.',
            'Requiring a future recorded-trade endpoint conditions evaluation on future label availability; that eligibility is not known at prediction time.',
            'Assets are correlated and the current allowlist introduces selection and survivorship bias; market caps are unverified.',
            'Carried marks do not reveal intra-minute prices or prove liquidity; missing assets remain excluded.',
            '10% future highs are descriptive observations, not independent events, predicted probabilities or trade profits.',
            'No trade signals, strategy deployment or orders. Results need a genuinely new period before any claim of reliability.']}
    metrics=report['aggregate']
    report['conclusion']='No consistent predictive improvement demonstrated across these periods.'
    if (metrics['model_rmse_pct'] < min(metrics['zero_return_rmse_pct'],
            metrics['training_mean_baseline_rmse_pct'],metrics['per_asset_training_mean_rmse_pct'])
            and metrics['folds_beating_training_mean']>=0.7*len(valid)
            and metrics['folds_beating_per_asset_mean']>=0.7*len(valid)):
        report['conclusion']='Lower aggregate error than all baselines in this run; requires fresh validation before any reliability claim.'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report,indent=2)+'\n')
    summary=report['aggregate']
    lines=['# Walk-forward archive research','', report['conclusion'], '',
        f"Evaluated {len(valid)} chronological periods with {total:,} test observations.",
        f"Assets with eligible samples: {summary['assets_with_eligible_samples']} of {len(coverage)}; {summary['assets_with_test_samples']} contribute test samples.",
        f"Model RMSE: {summary['model_rmse_pct']:.3f} percentage points.",
        f"Per-asset mean baseline RMSE: {summary['per_asset_training_mean_rmse_pct']:.3f} percentage points.",
        f"Zero-return baseline RMSE: {summary['zero_return_rmse_pct']:.3f} percentage points.",
        f"Periods beating the global training mean: {summary['folds_beating_training_mean']} of {len(valid)}.",
        f"Periods beating per-asset mean: {summary['folds_beating_per_asset_mean']} of {len(valid)}.",
        f"Future-high +10% observations: {len(events)}.",'',report['method'],'',report['no_trade_policy'],'',
        '## Periods','','| Test begins UTC | Train | Test | Model RMSE % | Asset baseline RMSE % |',
        '|---|---:|---:|---:|---:|']
    for fold in folds:
        metrics=fold.get('metrics',{})
        lines.append(f"| {fold['start_utc']} | {fold['train_observations']} | {fold['test_observations']} | {metrics.get('rmse_pct','skipped')} | {metrics.get('per_asset_training_mean_rmse_pct','skipped')} |")
    lines.extend(['','## Limits','',*('- '+v for v in report['limitations']),
                  '','## Archive coverage','','| Asset | Recorded minutes | Carried minutes | Eligible samples | Test samples |',
                  '|---|---:|---:|---:|---:|'])
    lines.extend(f"| {c['symbol']} | {c.get('archive_rows',0)} | {c.get('carried_no_trade_minutes',0)} | {c['eligible_samples']} | {c['test_observations']} |" for c in coverage)
    output.with_suffix('.md').write_text('\n'.join(lines)+'\n')
    return report
