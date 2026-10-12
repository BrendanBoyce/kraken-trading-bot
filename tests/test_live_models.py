import asyncio
from datetime import UTC,datetime,timedelta
from decimal import Decimal

import pytest

from kraken_bot.live_models import TradeWindow,features_from_minutes,minute_inputs,refresh_trades
from kraken_bot.models import Candle
from kraken_bot.storage import CandleStore
from kraken_bot.enriched_model import enrich_rows,prepare_trade_windows
from kraken_bot.rally_study import FEATURES,prepare_features

START=datetime(2026,10,12,12,tzinfo=UTC)
END=START+timedelta(minutes=241)


def raw_trade(i,price='100',qty='1',side='b'):
    return [price,qty,Decimal(int(START.timestamp()))+i*60+Decimal('0.123456789'),side,'m','',i]


def inputs():
    w=TradeWindow();w.coverage_start=START;candles=[]
    for i in range(241):
        price=Decimal(100)+Decimal(i)/1000
        w.add(raw_trade(i,str(price),side='b' if i%2 else 's'))
        candles.append(Candle('BTC/USD',START+timedelta(minutes=i),1,price,price+1,price-1,price,price,Decimal(1),1,True))
    return w,candles


def test_nanosecond_buckets_ids_and_conflicts():
    w=TradeWindow();r=raw_trade(0)
    r[2]=Decimal(int(START.timestamp()))+Decimal('59.999999999')
    assert w.add(r)
    assert START in w.minutes
    assert not w.add(r)
    assert w.minutes[START]['trades']==1
    changed=list(r);changed[1]='2'
    with pytest.raises(ValueError,match='Conflicting'):w.add(changed)
    with pytest.raises(ValueError):w.add([1,2,3])


def test_completed_source_parity_and_future_cannot_change_features():
    w,candles=inputs();sources,trades=minute_inputs(candles,w,START,END)
    with CandleStore(':memory:') as store:
        before=features_from_minutes(store,'BTC/USD',sources,trades,END)
        # Same shared training SQL must reproduce each candle/trade feature.
        db=store.connection
        prepare_features(store);prepare_trade_windows(store,'BTC/USD')
        cursor=db.execute('SELECT *,v60/(v240-v60)*3 AS volume_ratio FROM rally_features ORDER BY timestamp DESC LIMIT 1')
        last=dict(zip([d[0] for d in cursor.description],cursor.fetchone()))
        expected=enrich_rows(store,[{'symbol':'BTC/USD','decision_utc':END,**{f:last[f] for f in FEATURES}}])[0]
        assert all(before[k]==pytest.approx(v) for k,v in expected.items() if isinstance(v,float))
        w.add(raw_trade(241,'999'))
        candles.append(Candle('BTC/USD',END,1,Decimal(999),Decimal(999),Decimal(999),Decimal(999),Decimal(999),Decimal(1),1,False))
        after=features_from_minutes(store,'BTC/USD',*minute_inputs(candles,w,START,END),END)
        assert before==after


def test_missing_trade_coverage_or_mismatched_counts_block():
    w,candles=inputs();w.coverage_start=None
    with pytest.raises(ValueError,match='fully covered'):minute_inputs(candles,w,START,END)
    w.coverage_start=START;w.minutes[START]['trades']=2
    with pytest.raises(ValueError,match='disagree'):minute_inputs(candles,w,START,END)
    w,candles=inputs()
    with pytest.raises(ValueError,match='missing candle'):
        minute_inputs([c for c in candles if c.timestamp!=START+timedelta(minutes=200)],w,START,END)


def test_no_trade_minutes_are_known_zero_not_fabricated_trades():
    w,candles=inputs();stamp=START+timedelta(minutes=200)
    w.minutes.pop(stamp)
    candles=[c for c in candles if c.timestamp!=stamp]
    sources,trades=minute_inputs(candles,w,START,END)
    assert sources[200][-1] is False and sources[200][-2]==0
    assert all(t[0]!=stamp for t in trades)


def test_pagination_uses_cursor_and_page_limit_never_marks_partial_history():
    class Client:
        def __init__(self):self.calls=[]
        async def recent_trades(self,symbol,cursor):
            self.calls.append(cursor)
            if len(self.calls)==1:
                return [raw_trade(i) for i in range(1000)],'9999999999999999999'
            return [raw_trade(1000)],'99999999999999999999'
    c=Client();w=TradeWindow();finish=START+timedelta(minutes=1001)
    asyncio.run(refresh_trades(c,'BTC/USD',w,START,finish))
    assert c.calls[1]=='9999999999999999999'
    assert w.coverage_start==START and len(w.ids)==1001
    c=Client();w=TradeWindow()
    with pytest.raises(ValueError,match='exceeds'):
        asyncio.run(refresh_trades(c,'BTC/USD',w,START,finish,max_pages=1))
    assert w.coverage_start is None and w.cursor is None


def test_paper_scoring_is_experimental_and_missing_context_or_stale_data_blocks(monkeypatch):
    from kraken_bot.live_models import PaperModels
    from kraken_bot.net_model import NET_FEATURES
    paper=object.__new__(PaperModels)
    paper.models=[(3,{},.7,{'sha256':'test'})]
    paper.known_assets={'BTC/USD','ETH/USD'};paper.report_sha='test-report'
    rows=[{'symbol':symbol,'decision_utc':END,'price':100,**{f:.1 for f in NET_FEATURES}}
          for symbol in ('BTC/USD','ETH/USD','DOT/USD')]
    monkeypatch.setattr('kraken_bot.live_models.probabilities',lambda model,rows:[.6])
    scored=paper.score(rows,END,END+timedelta(minutes=1),{})
    dot=next(r for r in scored if r['symbol']=='DOT/USD')
    assert dot['models'][0]['class_prediction']=='positive_net'
    assert not dot['models'][0]['above_strict_watch_cutoff']
    assert 'asset_not_in_training_universe' in dot['flags']
    assert dot['paper_notional_usd']==100
    assert not dot['orders_enabled'] and not dot['validated_buy_signal']
    assert all(not r['models'] for r in paper.score(rows,END,END+timedelta(minutes=6),{}))
    assert all(not r['models'] for r in paper.score(rows[1:],END,END+timedelta(minutes=1),{}))


def test_exchange_clock_outage_emits_only_blocked_rows(monkeypatch,tmp_path):
    from kraken_bot.live_models import run_paper_scanner
    from kraken_bot.scanner import ScannerConfig
    recorded=[]
    class Models:
        def __init__(self,*args):pass
        def score(self,rows,decision,now,errors):
            assert rows==[]
            return [{'symbol':s,'reason':e,'models':[]} for s,e in errors.items()]
        def report(self,rows):recorded.extend(rows)
    class Client:
        async def server_time(self):raise RuntimeError('network unavailable')
    monkeypatch.setattr('kraken_bot.live_models.PaperModels',Models)
    asyncio.run(run_paper_scanner(Client(),None,['BTC/USD','ETH/USD'],ScannerConfig(),
        tmp_path/'model.json',tmp_path/'precision.json',tmp_path/'predictions.jsonl',once=True))
    assert len(recorded)==2
    assert all(r['reason']=='exchange_clock_unavailable' and not r['models'] for r in recorded)


def test_public_trade_adapter_resolves_pair_and_rejects_private_endpoints(monkeypatch):
    from kraken_bot.exchanges.kraken import KrakenClient
    from kraken_bot.models import Pair
    client=KrakenClient();client._pairs={'BTC/USD':Pair('BTC/USD','XXBTZUSD','XBTUSD','online')}
    calls=[]
    async def request(endpoint,params=None):
        calls.append((endpoint,params))
        return {'XXBTZUSD':[raw_trade(0)],'last':'123456789'} if endpoint=='Trades' else {'unixtime':int(START.timestamp())}
    monkeypatch.setattr(client,'_request',request)
    rows,cursor=asyncio.run(client.recent_trades('BTC/USD','123'))
    assert cursor=='123456789' and len(rows)==1
    assert calls[0]==('Trades',{'pair':'XBTUSD','since':'123','count':1000})
    assert asyncio.run(client.server_time())==START
    with pytest.raises(ValueError):asyncio.run(client.recent_trades('BTC/USD','not-a-cursor'))
    private=KrakenClient()
    with pytest.raises(ValueError,match='Unsupported public'):asyncio.run(private._request('AddOrder'))
