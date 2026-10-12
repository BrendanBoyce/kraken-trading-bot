from datetime import UTC,datetime,timedelta

import pytest
pytest.importorskip('sklearn')

from kraken_bot.enriched_model import (FEATURES,TRADE_FEATURES,classification_metrics,
    enrich_rows,evaluate_models,fit_classifier,model_details,prepare_trade_windows,probabilities)
from kraken_bot.storage import CandleStore

START=datetime(2026,4,1,tzinfo=UTC)


def fixture_row(i):
    r={'symbol':'BTC/USD','decision_utc':START+timedelta(hours=4*i),
        'label_end_utc':START+timedelta(hours=4*i+4),'rally':i%17==0,
        'close_return':0.01 if i%17==0 else -0.001,'low_return':-0.02,
        'high_return':0.12 if i%17==0 else 0.01,'future_observed':True,
        'path_outcome':'target_before_downside' if i%17==0 else 'neither_level'}
    r.update({name:(i%11)/100 for name in FEATURES})
    r.update({name:0.9 if i%17==0 else -0.9 for name in TRADE_FEATURES})
    return r


def test_known_new_feature_signal_and_training_only_scaler():
    train=[fixture_row(i) for i in range(150)]
    test=[fixture_row(i) for i in range(150,200)]
    model=fit_classifier(train,FEATURES+TRADE_FEATURES)
    metadata=model_details(model)
    scores=probabilities(model,test)
    assert min(p for r,p in zip(test,scores) if r['rally'])>max(p for r,p in zip(test,scores) if not r['rally'])
    assert model_details(model)==metadata
    baseline=classification_metrics(test,[0.5]*len(test))
    enhanced=classification_metrics(test,scores,0.5)
    assert enhanced['average_precision']>baseline['average_precision']


def test_scores_not_misreported_as_accuracy():
    rows=[fixture_row(1),fixture_row(17),fixture_row(2)]
    result=classification_metrics(rows,[0.8,0.9,0.1],0.5)
    assert result['selected']['observations']==2
    assert result['selected']['rallies']==1
    assert result['selected']['false_positives']==1
    assert result['selected']['precision']==0.5
    assert result['selected']['recall']==1


def test_paired_time_purging_and_future_outcomes_do_not_fit_earlier_model():
    rows=[{**fixture_row(i),'symbol':symbol} for symbol in ('BTC/USD','ETH/USD') for i in range(40*6)]
    report=evaluate_models(rows,START,START+timedelta(days=40))
    assert report['evaluated_periods']==2
    first=report['folds'][0]
    fit_end=START+timedelta(days=21)
    assert first['training_rows']==sum(r['label_end_utc']<=fit_end for r in rows)
    boundary=START+timedelta(days=28)
    changed=[{**r,'rally':not r['rally'],**{f:99 for f in TRADE_FEATURES}} if r['decision_utc']>=boundary else r for r in rows]
    modified=evaluate_models(changed,START,START+timedelta(days=40))
    for name in ('candle_only','trade_enriched'):
        assert first['models'][name]['model']==modified['folds'][0]['models'][name]['model']
        assert first['models'][name]['metrics']['operating_threshold']==modified['folds'][0]['models'][name]['metrics']['operating_threshold']
    assert report['aggregate']['candle_only']['observations']==report['aggregate']['trade_enriched']['observations']


def test_trade_features_are_past_only_and_missing_observed_minutes_fail():
    with CandleStore(':memory:') as store:
        db=store.connection
        db.execute("""CREATE TEMP TABLE research_source AS SELECT ?::TIMESTAMPTZ+i*INTERVAL '1 minute' AS timestamp,100.0 AS close,TRUE AS observed FROM range(300) r(i)""",[START])
        db.execute("""CREATE TEMP TABLE trade_input AS SELECT 'BTC/USD' AS symbol,timestamp,1 AS trades,100.0 AS quote_volume,0.2 AS signed_quote_imbalance,1.0 AS volume,100.0 AS largest_trade_quote FROM research_source""")
        prepare_trade_windows(store,'BTC/USD')
        before=db.execute('SELECT * FROM enriched_windows WHERE timestamp=?',[START+timedelta(minutes=240)]).fetchone()
        db.execute('UPDATE trade_input SET signed_quote_imbalance=-1,quote_volume=999 WHERE timestamp>?',[START+timedelta(minutes=240)])
        prepare_trade_windows(store,'BTC/USD')
        assert db.execute('SELECT * FROM enriched_windows WHERE timestamp=?',[START+timedelta(minutes=240)]).fetchone()==before
        assert before[1]==pytest.approx(0.2)
        db.execute('DELETE FROM trade_input WHERE timestamp=?',[START+timedelta(minutes=200)])
        with pytest.raises(ValueError,match='lack trade enrichment'):
            prepare_trade_windows(store,'BTC/USD')


def test_saved_classifier_roundtrip(tmp_path):
    import joblib
    rows=[fixture_row(i) for i in range(150)]
    model=fit_classifier(rows,FEATURES+TRADE_FEATURES)
    path=tmp_path/'model.joblib'
    joblib.dump(model,path)
    restored=joblib.load(path)
    assert probabilities(restored,rows[:10])==pytest.approx(probabilities(model,rows[:10]))
