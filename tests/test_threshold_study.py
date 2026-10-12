import json
from pathlib import Path

import pytest
pytest.importorskip('numpy')
from kraken_bot.threshold_study import gate,summarize,threshold_study


def validation():
    return [{'score':i/20,'actual_rally':i>=10,'decision_utc':'2026-04-25T00:00:00+00:00',
             'label_end_utc':'2026-04-25T04:00:00+00:00'} for i in range(20)]


def test_stricter_quantiles_and_supported_zero_validation_errors():
    data=validation()
    assert gate(data,.99)['threshold']>gate(data,.95)['threshold']
    result=gate(data,target=1,minimum=10)
    assert result['selected']==10 and result['false_positives']==0
    assert gate(data,target=1,minimum=11)['threshold'] is None


def test_ties_cannot_select_only_successful_members():
    data=[{'score':.9,'actual_rally':True}]*10+[{'score':.9,'actual_rally':False}]
    assert gate(data,target=1,minimum=10)['threshold'] is None
    assert gate(data,.99)['selected']==11


def test_empty_selections_are_not_perfect_precision():
    empty=summarize([],48)
    assert empty['precision'] is None and empty['missed_rallies']==48
    assert empty['evidence_status']=='no_selections_no_performance_estimate'
    assert empty['ready_for_trading'] is False
    one=summarize([{'actual_rally':True,'close_return':-.02,'worst_downside':-.1}],48)
    assert one['precision']==1 and one['ready_for_trading'] is False
    assert one['mean_net_close_proxy_pct']['100']==pytest.approx(-2.98)


def input_report():
    prediction={'symbol':'BTC/USD','decision_utc':'2026-04-29T00:00:00+00:00',
                'actual_rally':False,'score':.99,'close_return':-.01,'worst_downside':-.05}
    return {'horizon_minutes':240,'evaluation':{'folds':[{'status':'evaluated',
        'start_utc':'2026-04-29T00:00:00+00:00','end_utc':'2026-05-06T00:00:00+00:00',
        'models':{name:{'validation_predictions':validation()} for name in ('candle_only','trade_enriched')}}],
        'predictions':{name:[dict(prediction)] for name in ('candle_only','trade_enriched')}}}


def test_test_outcomes_cannot_change_validation_threshold(tmp_path):
    source=input_report();path=tmp_path/'input.json';path.write_text(json.dumps(source))
    before=threshold_study(path,tmp_path/'result.json')
    for values in source['evaluation']['predictions'].values():values[0]['actual_rally']=True
    path.write_text(json.dumps(source))
    after=threshold_study(path,tmp_path/'changed.json')
    assert [r['periods'][0]['validation_gate'] for r in before['policies']]==[r['periods'][0]['validation_gate'] for r in after['policies']]
    assert before['policies'][0]['summary']['false_positives']==1
    assert after['policies'][0]['summary']['false_positives']==0
    assert (tmp_path/'result.md').exists()


def test_validation_boundary_and_unpaired_labels_rejected(tmp_path):
    source=input_report();path=tmp_path/'input.json'
    source['evaluation']['folds'][0]['models']['trade_enriched']['validation_predictions'][0]['label_end_utc']='2026-04-30T00:00:00+00:00'
    path.write_text(json.dumps(source))
    with pytest.raises(ValueError,match='boundary'):threshold_study(path,tmp_path/'result.json')
    source=input_report();source['evaluation']['predictions']['trade_enriched'][0]['actual_rally']=True
    path.write_text(json.dumps(source))
    with pytest.raises(ValueError,match='paired'):threshold_study(path,tmp_path/'result.json')


def test_test_label_horizon_cannot_cross_period_end(tmp_path):
    source=input_report()
    for values in source['evaluation']['predictions'].values():
        values[0]['decision_utc']='2026-05-05T23:00:00+00:00'
    path=tmp_path/'input.json';path.write_text(json.dumps(source))
    with pytest.raises(ValueError,match='Test target crosses'):
        threshold_study(path,tmp_path/'result.json')
