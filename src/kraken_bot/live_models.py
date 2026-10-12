"""Live feature parity and experimental paper predictions. Public data only."""
import asyncio
import json
import logging
import math
import os
import time
from datetime import UTC,datetime,timedelta
from decimal import Decimal
from pathlib import Path

from .enriched_model import enrich_rows,prepare_trade_windows,probabilities
from .filter_study import add_context,digest,load_model
from .net_model import NET_FEATURES
from .rally_study import FEATURES,prepare_features
from .scanner import ScanReporter,scan
from .storage import CandleStore

LOG=logging.getLogger(__name__)


class TradeWindow:
    """Transient deduplicated trades and rolling minute aggregates; never tick files."""
    def __init__(self):
        self.ids={};self.minutes={};self.cursor=None;self.coverage_start=None

    def add(self,row):
        if len(row)!=7:raise ValueError('Expected seven-field Kraken public trade')
        price,qty,epoch,side,_,_,trade_id=row
        price=Decimal(str(price));qty=Decimal(str(qty));epoch=Decimal(str(epoch))
        if not all(v.is_finite() for v in (price,qty,epoch)) or price<=0 or qty<=0 or epoch<0 or side not in ('b','s'):
            raise ValueError('Invalid public trade')
        if type(trade_id) is not int or trade_id<0:raise ValueError('Invalid trade ID')
        stamp=datetime.fromtimestamp(int(epoch//60)*60,UTC)
        signature=(stamp,price,qty,side)
        if trade_id in self.ids:
            if self.ids[trade_id]!=signature:raise ValueError('Conflicting duplicate public trade')
            return False
        self.ids[trade_id]=signature
        quote=float(price)*float(qty)
        if not math.isfinite(quote):raise ValueError('Nonfinite trade quote value')
        m=self.minutes.setdefault(stamp,{'trades':0,'volume':Decimal(0),'quote_volume':0.0,'signed_quote':0.0,'largest_trade_quote':0.0})
        m['trades']+=1;m['volume']+=qty;m['quote_volume']+=quote
        m['signed_quote']+=quote if side=='b' else -quote
        m['largest_trade_quote']=max(m['largest_trade_quote'],quote)
        return True

    def prune(self,start):
        self.ids={k:v for k,v in self.ids.items() if v[0]>=start}
        self.minutes={k:v for k,v in self.minutes.items() if k>=start}
        if self.coverage_start is not None:self.coverage_start=max(self.coverage_start,start)


async def refresh_trades(client,symbol,window,start,end,max_pages=100):
    # Reset only when the retained data cannot cover the requested feature window.
    if window.cursor is None or window.coverage_start is None or window.coverage_start>start:
        window.ids.clear();window.minutes.clear()
        window.cursor=str(int(start.timestamp())*10**9-1);window.coverage_start=None
    cursor=window.cursor
    for page in range(max_pages):
        rows,next_cursor=await client.recent_trades(symbol,cursor)
        if not next_cursor.isdigit():raise ValueError('Invalid next trade cursor')
        for row in rows:window.add(row)
        if len(rows)<1000 or (rows and Decimal(str(rows[-1][2]))>=Decimal(int(end.timestamp()))):
            window.cursor=next_cursor;window.coverage_start=start;window.prune(start)
            return page+1
        if int(next_cursor)<=int(cursor):raise ValueError('Trade cursor did not advance')
        cursor=next_cursor
    # Do not mark partial history covered. Next pass retries from a known boundary.
    window.coverage_start=None;window.cursor=None
    raise ValueError(f'Trade history exceeds {max_pages} pages')


def minute_inputs(candles,window,start,end):
    if window.coverage_start is None or window.coverage_start>start:raise ValueError('Trade history not fully covered')
    lookup={c.timestamp:c for c in candles if c.complete and c.timestamp<end}
    if len(lookup)!=len([c for c in candles if c.complete and c.timestamp<end]):raise ValueError('Duplicate live candles')
    previous=[c for c in lookup.values() if c.timestamp<start]
    if start not in lookup and not previous:raise ValueError('Missing prior price for feature window')
    price=lookup[start].close if start in lookup else max(previous,key=lambda c:c.timestamp).close
    sources=[];trades=[];stamp=start
    while stamp<end:
        c=lookup.get(stamp);t=window.minutes.get(stamp)
        n=t['trades'] if t else 0;volume=t['volume'] if t else Decimal(0)
        if c is None and n:raise ValueError(f'Traded minute missing candle: {stamp}')
        if c is not None:
            if c.interval_minutes!=1 or not c.complete:raise ValueError('Need completed 1m candles')
            if c.trades!=n or abs(float(c.volume)-float(volume))>max(1e-12,abs(float(c.volume))*1e-9):
                raise ValueError(f'Candle/trade totals disagree at {stamp}: candle={c.trades}, trades={n}')
            price=c.close;high=c.high;low=c.low
        else:high=low=price
        if not all(math.isfinite(float(v)) and float(v)>0 for v in (price,high,low)):
            raise ValueError('Invalid live candle prices')
        sources.append((stamp,float(price),float(high),float(low),float(volume),n>0))
        if t:
            q=t['quote_volume']
            trades.append((stamp,n,float(volume),q,t['signed_quote']/q,t['largest_trade_quote']))
        stamp+=timedelta(minutes=1)
    return sources,trades


def features_from_minutes(store,symbol,sources,trades,decision):
    db=store.connection
    db.execute('DROP TABLE IF EXISTS research_source')
    db.execute('CREATE TEMP TABLE research_source (timestamp TIMESTAMPTZ,close DOUBLE,high DOUBLE,low DOUBLE,volume DOUBLE,observed BOOLEAN)')
    db.executemany('INSERT INTO research_source VALUES (?,?,?,?,?,?)',sources)
    db.execute('DROP TABLE IF EXISTS trade_input')
    db.execute('CREATE TEMP TABLE trade_input (symbol VARCHAR,timestamp TIMESTAMPTZ,trades BIGINT,volume DOUBLE,quote_volume DOUBLE,signed_quote_imbalance DOUBLE,largest_trade_quote DOUBLE)')
    if trades:db.executemany('INSERT INTO trade_input VALUES (?,?,?,?,?,?,?)',[(symbol,*r) for r in trades])
    prepare_features(store)
    cursor=db.execute('SELECT *,v60/nullif(v240-v60,0)*3 AS volume_ratio FROM rally_features ORDER BY timestamp DESC LIMIT 1')
    raw=dict(zip([d[0] for d in cursor.description],cursor.fetchone()))
    if raw['timestamp']!=decision-timedelta(minutes=1) or raw['timestamp']-raw['first_stamp']!=timedelta(minutes=240):
        raise ValueError('Need 241 contiguous minutes for trained features')
    if not raw['observed']:raise ValueError('Latest completed minute has no observed trades')
    if raw['traded_fraction_60m']<.25 or raw['v240']<=raw['v60']:raise ValueError('Outside training liquidity eligibility')
    if any(raw[f] is None or not math.isfinite(raw[f]) for f in FEATURES):raise ValueError('Nonfinite candle feature')
    prepare_trade_windows(store,symbol)
    row={'symbol':symbol,'decision_utc':decision,'price':raw['close'],**{f:raw[f] for f in FEATURES}}
    return enrich_rows(store,[row])[0]


class PaperModels:
    def __init__(self,report_path,precision_path,log_path):
        report=json.loads(report_path.read_text());precision=json.loads(precision_path.read_text())
        if precision['source_hashes']['net_model_report']!=digest(report_path):raise ValueError('Paper threshold/model report mismatch')
        self.models=[];self.known_assets={r['symbol'] for r in report['training_coverage'] if r['samples']>0}
        self.log_path=log_path;self.log_path.parent.mkdir(parents=True,exist_ok=True)
        self.previous=None
        for target in (3,4,5):
            name=f'{target}pct-logistic-q0.95'
            candidate=next(c for c in report['candidates'] if c['id']==name)
            model=load_model(candidate['artifact'])
            if tuple(model['features'])!=NET_FEATURES:raise ValueError('Unexpected paper model feature schema')
            threshold=next(r['validation_gate']['threshold'] for r in precision['results']
                if r['model']==name and r['policy']=={'kind':'quantile','value':.995})
            self.models.append((target,model,threshold,candidate['artifact']))
        self.report_sha=digest(report_path)

    def score(self,rows,decision,scored_at,errors):
        reference={s:{} for s in ('BTC/USD','ETH/USD')}
        for r in rows:
            if r['symbol'] in reference:reference[r['symbol']][decision-timedelta(minutes=1)]=r['return_60m']
        output=[]
        for row in rows:
            try:
                enriched=add_context(row,reference)
                if enriched['mean_BTC_ETH_return_60m'] is None:raise ValueError('BTC/ETH context unavailable')
                enriched.update(market_return_60m=enriched['mean_BTC_ETH_return_60m'],
                    relative_return_60m=enriched['return_60m_minus_mean_BTC_ETH'],context_missing=0)
                if scored_at-decision>timedelta(minutes=5):raise ValueError('Prediction inputs are stale by more than five minutes')
                models=[]
                for target,model,threshold,artifact in self.models:
                    score=float(probabilities(model,[enriched])[0])
                    models.append({'target_pct':target,'score':score,'class_prediction':'positive_net' if score>=.5 else 'nonpositive_net',
                        'strict_watch_cutoff':threshold,'above_strict_watch_cutoff':score>=threshold,'model_sha256':artifact['sha256']})
                flags=['unvalidated_models','experimental_paper_only']
                if decision.hour%4 or decision.minute:flags.append('outside_original_4h_sampling_grid')
                if row['symbol'] not in self.known_assets:flags.append('asset_not_in_training_universe')
                output.append({'symbol':row['symbol'],'decision_utc':decision.isoformat(),'scored_at_utc':scored_at.isoformat(),
                    'last_completed_close':row['price'],'paper_notional_usd':100,'models':models,'flags':flags,
                    'validated_buy_signal':False,'orders_enabled':False,'features':{f:enriched[f] for f in NET_FEATURES},
                    'source_report_sha256':self.report_sha})
            except ValueError as exc:errors[row['symbol']]=str(exc)
        for symbol,error in errors.items():output.append({'symbol':symbol,'decision_utc':decision.isoformat(),
            'scored_at_utc':scored_at.isoformat(),'models':[],'flags':['quality_blocked'],'reason':error,
            'validated_buy_signal':False,'orders_enabled':False})
        return sorted(output,key=lambda r:r['symbol'])

    def report(self,rows):
        signature=tuple((r['symbol'],r['decision_utc'],r.get('reason')) for r in rows)
        if signature==self.previous:return
        self.previous=signature
        with self.log_path.open('a') as stream:
            for row in rows:stream.write(json.dumps(row,allow_nan=False)+'\n')
        latest=self.log_path.parent/'latest.json';latest.write_text(json.dumps(rows,indent=2)+'\n')
        print('\nEXPERIMENTAL PAPER MODEL SCORES — $100 notional; no orders; not calibrated probabilities',flush=True)
        print('PAIR | DECISION UTC | CLOSE | 3% SCORE | 4% SCORE | 5% SCORE | STRICT WATCH | STATUS',flush=True)
        lines=['# Live experimental paper predictions','',
            'Public Spot data; no orders. Scores are not calibrated winning probabilities. No policy passed historical validation. $100 is paper notional only.', '',
            '| Pair | Decision UTC | Close | 3% score | 4% score | 5% score | Strict watch | Status |',
            '|---|---|---:|---:|---:|---:|---|---|']
        for row in rows:
            models=row['models'];scores=[f"{m['score']:.4f}" for m in models] if models else ['—']*3
            watch=','.join(str(m['target_pct'])+'%' for m in models if m['above_strict_watch_cutoff']) or 'none'
            close=f"{row['last_completed_close']:.6g}" if models else '—'
            status=row.get('reason') or ','.join(row['flags'])
            fields=[row['symbol'],row['decision_utc'],close,*scores,watch,status]
            print(' | '.join(fields),flush=True);lines.append('| '+' | '.join(fields)+' |')
        (self.log_path.parent/'latest.md').write_text('\n'.join(lines)+'\n')


async def run_paper_scanner(client,store,symbols,settings,report_path,precision_path,log_path,*,once=False,poll_seconds=60,max_pages=100):
    if poll_seconds<60 or type(max_pages) is not int or max_pages<1:
        raise ValueError('Paper refresh must be at least 60 seconds with positive page limit')
    if not {'BTC/USD','ETH/USD'}.issubset(symbols):raise ValueError('BTC/USD and ETH/USD reference assets required')
    if any(not s.endswith('/USD') for s in symbols):raise ValueError('Trained paper models require USD pairs')
    reporter=ScanReporter(settings);models=PaperModels(report_path,precision_path,log_path)
    windows={s:TradeWindow() for s in symbols}
    (log_path.parent/'runtime.json').write_text(json.dumps({'pid':os.getpid(),'started_utc':datetime.now(UTC).isoformat(),
        'mode':'paper_predictions_only','symbols':list(symbols),'orders_enabled':False,'database':str(store.path) if hasattr(store,'path') else 'separate paper scanner database'})+'\n')
    while True:
        began=time.monotonic()
        try:
            decision=((await client.server_time())-timedelta(seconds=15)).replace(second=0,microsecond=0)
        except (RuntimeError,ValueError) as exc:
            LOG.warning('Paper feed clock unavailable; predictions blocked until recovery: %s',exc)
            now=datetime.now(UTC)
            models.report(models.score([],now,now,{s:'exchange_clock_unavailable' for s in symbols}))
            if once:return
            await asyncio.sleep(poll_seconds)
            continue
        start=decision-timedelta(minutes=241);features=[];errors={}
        for symbol in symbols:
            try:
                pages=await refresh_trades(client,symbol,windows[symbol],start,decision,max_pages)
                batch=await client.candles(symbol,1)
                store.reconcile(symbol,batch.candles,now=await client.server_time())
                source,trade=minute_inputs(batch.candles,windows[symbol],start,decision)
                with CandleStore(':memory:') as feature_store:
                    features.append(features_from_minutes(feature_store,symbol,source,trade,decision))
                LOG.info('%s live trade features ready (%d REST pages, %d buffered trades)',symbol,pages,len(windows[symbol].ids))
            except (ValueError,RuntimeError) as exc:
                errors[symbol]=str(exc);LOG.warning('%s paper prediction blocked: %s',symbol,exc)
        try:
            now=await client.server_time()
        except (RuntimeError,ValueError) as exc:
            LOG.warning('Paper feed clock unavailable after refresh; no scores emitted: %s',exc)
            now=datetime.now(UTC)
            models.report(models.score([],decision,now,{s:'exchange_clock_unavailable' for s in symbols}))
            if once:return
            await asyncio.sleep(poll_seconds)
            continue
        reporter.report(scan(store,symbols,settings,now=now))
        from threadpoolctl import threadpool_limits
        with threadpool_limits(limits=2):models.report(models.score(features,decision,now,errors))
        if once:return
        elapsed=time.monotonic()-began
        if elapsed>poll_seconds:LOG.warning('Paper feature refresh took %.1fs; next pass uses incremental trade cursors',elapsed)
        await asyncio.sleep(max(1,poll_seconds-elapsed))
