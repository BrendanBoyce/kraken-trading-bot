"""Paired candle/trade-feature classifiers with nested chronological thresholds."""

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
from .rally_study import FEATURES, event_samples, prepare_features, summarize
from .walk_forward import prepare_archive_source

LOG=logging.getLogger(__name__)
TRADE_FEATURES=('quote_imbalance_15m','quote_imbalance_60m','imbalance_change',
    'log_trade_count_60m','trade_count_acceleration','log_mean_trade_quote',
    'largest_trade_fraction','price_to_vwap','momentum_imbalance_interaction')


def prepare_trade_windows(store,symbol):
    db=store.connection
    missing=db.execute('''SELECT count(*) FROM research_source s LEFT JOIN trade_input t
        ON t.symbol=? AND s.timestamp=t.timestamp
        WHERE s.observed AND (t.timestamp IS NULL OR t.trades<=0)''',[symbol]).fetchone()[0]
    if missing: raise ValueError(f'{symbol}: {missing} observed candle minutes lack trade enrichment')
    db.execute('DROP TABLE IF EXISTS enriched_windows')
    db.execute('''CREATE TEMP TABLE enriched_windows AS WITH aligned AS (
        SELECT s.timestamp,s.close,
            coalesce(t.trades,0) AS n,coalesce(t.quote_volume,0) AS q,
            coalesce(t.signed_quote_imbalance*t.quote_volume,0) AS signed_q,
            coalesce(t.volume::DOUBLE,0) AS v,coalesce(t.largest_trade_quote,0) AS largest
        FROM research_source s LEFT JOIN trade_input t ON t.symbol=? AND t.timestamp=s.timestamp
      ), sums AS (
        SELECT *,sum(q) OVER last15 AS q15,sum(signed_q) OVER last15 AS signed15,
            sum(q) OVER last60 AS q60,sum(signed_q) OVER last60 AS signed60,
            sum(q) OVER last240 AS q240,sum(signed_q) OVER last240 AS signed240,
            sum(n) OVER last60 AS n60,sum(n) OVER last240 AS n240,
            sum(v) OVER last60 AS v60,max(largest) OVER last60 AS largest60
        FROM aligned WINDOW
            last15 AS (ORDER BY timestamp ROWS BETWEEN 14 PRECEDING AND CURRENT ROW),
            last60 AS (ORDER BY timestamp ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
            last240 AS (ORDER BY timestamp ROWS BETWEEN 239 PRECEDING AND CURRENT ROW)
      ) SELECT timestamp,signed15/nullif(q15,0) AS quote_imbalance_15m,
        signed60/nullif(q60,0) AS quote_imbalance_60m,
        signed60/nullif(q60,0)-(signed240-signed60)/nullif(q240-q60,0) AS imbalance_change,
        ln(1+n60) AS log_trade_count_60m,n60*3.0/nullif(n240-n60,0) AS trade_count_acceleration,
        ln(1+q60/nullif(n60,0)) AS log_mean_trade_quote,
        largest60/nullif(q60,0) AS largest_trade_fraction,
        close/nullif(q60/nullif(v60,0),0)-1 AS price_to_vwap FROM sums''',[symbol])


def enrich_rows(store,rows):
    cursor=store.connection.execute('SELECT * FROM enriched_windows ORDER BY timestamp')
    names=[f[0] for f in cursor.description]
    lookup={row[0]:dict(zip(names[1:],row[1:])) for row in cursor.fetchall()}
    result=[]
    for row in rows:
        trade=lookup[row['decision_utc']-timedelta(minutes=1)]
        if any(value is None or not math.isfinite(value) for value in trade.values()):
            raise ValueError(f"{row['symbol']}: nonfinite enrichment at {row['decision_utc']}")
        trade['momentum_imbalance_interaction']=row['return_60m']*trade['quote_imbalance_60m']
        result.append({**row,**trade})
    return result


def matrix(rows,features):
    import numpy as np
    return np.array([[row[name] for name in features] for row in rows],dtype=float)


def fit_classifier(rows,features,kind="logistic"):
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.exceptions import ConvergenceWarning
    import warnings
    if len(rows)<50 or sum(r['rally'] for r in rows)<3 or all(r['rally'] for r in rows):
        raise ValueError('Insufficient class support for classifier')
    x=matrix(rows,features)
    lower=np.quantile(x,0.01,axis=0);upper=np.quantile(x,0.99,axis=0)
    scaler=StandardScaler().fit(np.clip(x,lower,upper))
    if kind=='logistic':
        estimator=LogisticRegression(C=1.0,solver='lbfgs',max_iter=1000,random_state=0)
    elif kind=='nonlinear':
        estimator=HistGradientBoostingClassifier(max_iter=100,learning_rate=0.05,
            max_leaf_nodes=7,max_depth=3,min_samples_leaf=50,l2_regularization=10,
            early_stopping=False,random_state=0)
    else:
        raise ValueError('Unknown classifier kind')
    with warnings.catch_warnings():
        warnings.simplefilter('error',ConvergenceWarning)
        estimator.fit(scaler.transform(np.clip(x,lower,upper)),[int(r['rally']) for r in rows])
    return {'kind':kind,'features':features,'lower':lower,'upper':upper,'scaler':scaler,'estimator':estimator}


def probabilities(model,rows):
    import numpy as np
    x=np.clip(matrix(rows,model['features']),model['lower'],model['upper'])
    return model['estimator'].predict_proba(model['scaler'].transform(x))[:,1]


def model_details(model):
    return {'kind':model['kind'],'features':model['features'],'clip_lower':model['lower'].tolist(),
        'clip_upper':model['upper'].tolist(),'training_means':model['scaler'].mean_.tolist(),
        'training_scales':model['scaler'].scale_.tolist(),
        'coefficients':model['estimator'].coef_[0].tolist() if model['kind']=='logistic' else None,
        'intercept':float(model['estimator'].intercept_[0]) if model['kind']=='logistic' else None,
        'parameters':model['estimator'].get_params()}


def classification_metrics(rows,scores,threshold=None,costs=(0,50,100)):
    import numpy as np
    from sklearn.metrics import average_precision_score,brier_score_loss,log_loss
    y=np.array([int(r['rally']) for r in rows])
    selected=[r for r,p in zip(rows,scores) if threshold is not None and p>=threshold]
    positives=int(y.sum());summary=summarize(selected,costs)
    summary.update(false_positives=summary['observations']-summary['rallies'],
        precision=summary['rallies']/summary['observations'] if selected else None,
        recall=summary['rallies']/positives if positives else None)
    return {'observations':len(rows),'rallies':positives,'prevalence':float(y.mean()),
        'average_precision':float(average_precision_score(y,scores)) if positives else None,
        'brier_score':float(brier_score_loss(y,scores)),
        'log_loss':float(log_loss(y,scores,labels=[0,1])),
        'operating_threshold':threshold,'selected':summary}


def evaluate_models(rows,start,end,horizon=240,model_dir=None):
    import numpy as np
    names=('candle_only','trade_enriched','candle_selected_family','trade_selected_family')
    folds=[];records={name:[] for name in names};operating={name:[] for name in names}
    cutoff=start+timedelta(days=28)
    while cutoff<end:
        finish=min(cutoff+timedelta(days=7),end)
        validation_start=cutoff-timedelta(days=7)
        earlier=[r for r in rows if r['label_end_utc']<=validation_start]
        validation=[r for r in rows if r['decision_utc']>=validation_start and r['label_end_utc']<=cutoff]
        training=[r for r in rows if r['label_end_utc']<=cutoff]
        test=[r for r in rows if r['decision_utc']>=cutoff and r['label_end_utc']<=finish]
        fold={'start_utc':cutoff.isoformat(),'end_utc':finish.isoformat(),
              'training_rows':len(earlier),'available_history_rows':len(training),'validation_rows':len(validation),'test_rows':len(test),'models':{}}
        if len(test)<30 or len(validation)<50 or sum(r['rally'] for r in earlier)<3 or all(r['rally'] for r in earlier):
            fold['status']='insufficient_support'
        else:
            fold['status']='evaluated'
            for name,features in (('candle_only',FEATURES),('trade_enriched',FEATURES+TRADE_FEATURES)):
                from sklearn.metrics import average_precision_score
                candidates={kind:fit_classifier(earlier,features,kind) for kind in ('logistic','nonlinear')}
                validation_y=[int(r['rally']) for r in validation]
                validation_scores={kind:probabilities(model,validation) for kind,model in candidates.items()}
                ap={kind:float(average_precision_score(validation_y,p)) if any(validation_y) else None
                    for kind,p in validation_scores.items()}
                kind='nonlinear' if ap['nonlinear'] is not None and ap['nonlinear']>ap['logistic'] else 'logistic'
                selected_kind=kind
                variants=((name,'logistic'),
                    ('candle_selected_family' if name=='candle_only' else 'trade_selected_family',selected_kind))
                for variant,kind in variants:
                    model=candidates[kind]
                    threshold=float(np.quantile(validation_scores[kind],0.95))
                    # The same pre-validation model scores validation and the next test week.
                    model['threshold']=threshold
                    model['fit_end_utc']=validation_start.isoformat()
                    model['validation_end_utc']=cutoff.isoformat()
                    model['horizon_minutes']=horizon
                    predicted=probabilities(model,test)
                    metrics=classification_metrics(test,predicted,threshold)
                    fold['models'][variant]={'metrics':metrics,'model':model_details(model),
                        'validation_candidate_average_precision':ap,
                        'threshold_policy':'95th percentile of preceding validation scores, never test percentiles.'}
                    if model_dir is not None:
                        import joblib
                        model_dir.mkdir(parents=True,exist_ok=True)
                        path=model_dir/f'{cutoff.date()}-{variant}.joblib'
                        joblib.dump(model,path)
                        with path.open('rb') as artifact:
                            model_hash=hashlib.file_digest(artifact,'sha256').hexdigest()
                        fold['models'][variant]['artifact']={'path':str(path),'sha256':model_hash}
                    records[variant].extend((r,float(p),threshold) for r,p in zip(test,predicted))
                    operating[variant].extend(r for r,p in zip(test,predicted) if p>=threshold)
        folds.append(fold);cutoff=finish
    aggregate={}
    for name,values in records.items():
        if not values: raise ValueError('No chronological periods have sufficient class support')
        tested=[r for r,_,_ in values];scores=[p for _,p,_ in values]
        metrics=classification_metrics(tested,scores)
        selected=summarize(operating[name],(0,50,100))
        selected.update(false_positives=selected['observations']-selected['rallies'],
            precision=selected['rallies']/selected['observations'] if selected['observations'] else None,
            recall=selected['rallies']/metrics['rallies'] if metrics['rallies'] else None)
        metrics['selected']=selected;aggregate[name]=metrics
    valid=[fold for fold in folds if fold['status']=='evaluated']
    ap_wins=sum(f['models']['trade_enriched']['metrics']['average_precision'] is not None and
        f['models']['trade_enriched']['metrics']['average_precision']>f['models']['candle_only']['metrics']['average_precision'] for f in valid)
    return {'folds':folds,'aggregate':aggregate,'evaluated_periods':len(valid),
        'periods_trade_enrichment_improves_average_precision':ap_wins,
        'predictions':{name:[{'symbol':r['symbol'],'decision_utc':r['decision_utc'].isoformat(),
            'actual_rally':r['rally'],'score':p,'threshold':t,'close_return':r['close_return'],
            'worst_downside':r['low_return']} for r,p,t in values] for name,values in records.items()}}


def compare_enrichment(store,symbols,archive_path,feature_db,output,start,end,horizon=240):
    try:
        from threadpoolctl import threadpool_limits
        import sklearn
    except ImportError as exc:
        raise ValueError("Install research dependencies with python -m pip install '.[research]'") from exc
    if output.suffix!='.json':raise ValueError('Output must use .json')
    if type(horizon) is not int or not 60<=horizon<=1440:raise ValueError('Horizon must be 60–1440 minutes')
    if start.tzinfo is None or end.tzinfo is None or end-start<=timedelta(days=28):raise ValueError('Need an aware period longer than 28 days')
    if not feature_db.is_file():raise FileNotFoundError(feature_db)
    escaped=str(feature_db.resolve()).replace("'","''")
    store.connection.execute(f"ATTACH '{escaped}' AS trade_db (READ_ONLY)")
    store.connection.execute('CREATE TEMP VIEW trade_input AS SELECT * FROM trade_db.trade_features')
    rows=[];coverage=[]
    with archive_path.open('rb') as source:archive_hash=hashlib.file_digest(source,'sha256').hexdigest()
    with zipfile.ZipFile(archive_path) as archive:
        manifest=json.loads(archive.read('MANIFEST.json'))
        if manifest.get('product')!='ohlcvt' or manifest.get('has_header') is not False:raise ValueError('Expected Kraken OHLCVT archive')
        lower=datetime.fromisoformat(manifest['coverage']['start']).replace(tzinfo=UTC)
        upper=datetime.fromisoformat(manifest['coverage']['end']).replace(tzinfo=UTC)+timedelta(days=1)
        if start<lower or end>upper:raise ValueError('Requested dates outside archive')
        for symbol,member in archive_members(archive,symbols).items():
            if member is None:
                coverage.append({'symbol':symbol,'status':'not_in_archive','samples':0});continue
            with tempfile.TemporaryDirectory(prefix='kraken-enriched-') as temporary:
                path=Path(temporary)/'candles.csv'
                with archive.open(member) as source,path.open('wb') as target:shutil.copyfileobj(source,target)
                prepare_archive_source(store,path,symbol,start,end)
                prepare_features(store);prepare_trade_windows(store,symbol)
                candles=event_samples(store,symbol,horizon)
                enriched=enrich_rows(store,candles)
            rows.extend(enriched);coverage.append({'symbol':symbol,'status':'paired','samples':len(enriched)})
            LOG.info('%s: %s paired enriched samples',symbol,len(enriched))
    with threadpool_limits(limits=2):evaluation=evaluate_models(rows,start,end,horizon,output.parent/(output.stem+"-models"))
    report={'start_utc':start.isoformat(),'end_exclusive_utc':end.isoformat(),
        'horizon_minutes':horizon,'archive_sha256':archive_hash,'sklearn_version':sklearn.__version__,
        'trade_source_hashes':[r[0] for r in store.connection.execute('SELECT DISTINCT archive_sha256 FROM trade_input ORDER BY archive_sha256').fetchall()],
        'coverage':coverage,'candle_features':FEATURES,'additional_trade_features':TRADE_FEATURES,
        'method':'Primary comparison holds logistic regression fixed for both feature sets. Separately reported selected-family variants compare the same fixed learner candidates: L2 logistic (C=1) and shallow histogram gradient boosting (100 iterations, depth 3, 7 leaves, leaf minimum 50, L2=10). For selected-family variants only, family selected by preceding validation-week average precision, logistic wins ties. No class weighting or parameter search. Expanding 28-day initial history, seven-day test periods. The last earlier week sets the threshold for an unchanged pre-validation model; target-end purging at both boundaries. Train-only 1/99 percentile clipping and standardization. Identical samples and labels for both models.',
        'evaluation':evaluation,
        'limitations':['The nonlinear candidate was added after observing the logistic comparison; this is an additional exploratory experiment, not preregistered confirmation.',
            'Exploratory reuse of the previously inspected Q2 quarter, not fresh untouched validation.',
            'Scores are classifier estimates, not demonstrated calibrated probabilities.',
            'A preceding week is reserved for the threshold; it is not reused for fitting the model tested immediately afterward.',
            'Rare events, correlated assets and current asset selection limit inference; no verified low-cap universe.',
            'Trade classifications do not establish capital inflows, order-book depth or executable liquidity.',
            'Forward closing marks and 0/50/100 bps combined cost haircuts are research proxies, not portfolio returns or actual fills.',
            'No live model signals, orders or execution were added. Existing live scanner remains unchanged.']}
    base=evaluation['aggregate']['candle_only'];enhanced=evaluation['aggregate']['trade_enriched']
    report['observed_change']={'average_precision_delta':(enhanced['average_precision']-base['average_precision'] if enhanced['average_precision'] is not None and base['average_precision'] is not None else None),
        'brier_score_delta':enhanced['brier_score']-base['brier_score'],
        'precision_delta':(enhanced['selected']['precision']-base['selected']['precision']
                           if enhanced['selected']['precision'] is not None and base['selected']['precision'] is not None else None)}
    report['conclusion']='Added trade inputs and evaluated real fitted classifiers. Operating precision and overall ranking can move in opposite directions; these exploratory results do not establish a reliable or profitable trading edge.'
    report['experiment_roles']={'candle_only':'primary fixed logistic baseline',
        'trade_enriched':'primary fixed logistic with trade inputs',
        'candle_selected_family':'secondary exploratory validation-selected learner',
        'trade_selected_family':'secondary exploratory validation-selected learner with trade inputs'}
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    lines=['# Trade-enriched model comparison','',report['conclusion'],'',report['method'],'',
        f"Primary target: +10% high touch within {horizon//60} hours. Paired test observations: {base['observations']:,}; rally labels: {base['rallies']}.",
        '', '| Model | Average precision | Brier error | Selected windows | Rallies | False positives | Precision |',
        '|---|---:|---:|---:|---:|---:|---:|']
    for name,metrics in evaluation['aggregate'].items():
        selected=metrics['selected'];precision=selected['precision']
        ap='unavailable' if metrics['average_precision'] is None else f"{metrics['average_precision']:.4f}"
        lines.append(f"| {name} | {ap} | {metrics['brier_score']:.5f} | {selected['observations']} | {selected['rallies']} | {selected['false_positives']} | {precision:.2%} |" if precision is not None else f"| {name} | {ap} | {metrics['brier_score']:.5f} | 0 | 0 | 0 | unavailable |")
    lines.extend(['', 'Average precision measures ranking quality; higher is better. Brier error measures squared score error; lower is better. Operating precision is target hits divided by selected windows, not general accuracy.',
        f"Trade enrichment improved ranking average precision in {evaluation['periods_trade_enrichment_improves_average_precision']} of {evaluation['evaluated_periods']} chronological periods.",
        '', '## Cost and downside proxies',''])
    for name,metrics in evaluation['aggregate'].items():
        selected=metrics['selected']
        lines.extend([f"{name}: mean net close-return proxy by assumed cost (bps): {selected.get('mean_net_close_proxy_pct_by_cost_bps',{})}; worst observed downside: {selected.get('worst_downside_pct')}%."])
    lines.extend(['','## Limits','',*('- '+item for item in report['limitations'])])
    output.with_suffix('.md').write_text('\n'.join(lines)+'\n')
    return report
