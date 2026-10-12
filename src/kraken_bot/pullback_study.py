"""Current pullbacks and historical recovery/downside analogues; no trade signals."""
import asyncio
import json
import logging
import math
import shutil
import tempfile
import zipfile
from datetime import UTC,datetime,timedelta
from pathlib import Path

import aiohttp

from .filter_study import digest
from .history import archive_members
from .storage import CandleStore

LOG=logging.getLogger(__name__)


def wilson(hits,total):
    if not total:return None
    z=1.959963984540054;p=hits/total;den=1+z*z/total
    centre=(p+z*z/(2*total))/den
    half=z*math.sqrt(p*(1-p)/total+z*z/(4*total*total))/den
    return [max(0,centre-half),min(1,centre+half)]


def previous_peak(bars,index,hours=24):
    stamp=bars[index]['timestamp'];start=stamp-timedelta(hours=hours-1)
    window=[b for b in bars[max(0,index-hours+1):index+1] if b['timestamp']>=start]
    if len(window)!=hours or window[-1]['timestamp']-window[0]['timestamp']!=timedelta(hours=hours-1):return None
    return max(b['high'] for b in window)


def recovery_outcome(bars,index,peak,horizon=48):
    current=bars[index];future=bars[index+1:index+1+horizon]
    if len(future)!=horizon or any(b['timestamp']!=current['timestamp']+timedelta(hours=i+1) for i,b in enumerate(future)):
        return None
    price=current['close'];down=price*.97
    recovery=next((i+1 for i,b in enumerate(future) if b['high']>=peak),None)
    further=next((i+1 for i,b in enumerate(future) if b['low']<=down),None)
    ambiguous=recovery is not None and recovery==further
    return {'recovered_24h':recovery is not None and recovery<=24,
            'recovered_48h':recovery is not None,'recovery_hour':recovery,
            'further_3pct_before_recovery':further is not None and (recovery is None or further<=recovery),
            'recovered_before_further_3pct':recovery is not None and (further is None or recovery<further),
            'ambiguous_same_hour':ambiguous,
            'worst_further_drawdown_pct':(min(b['low'] for b in future)/price-1)*100,
            'return_48h_pct':(future[-1]['close']/price-1)*100}


def analogues(bars,reference_drop,btc_returns,current_btc_negative,band=.02):
    events=[];available_after=None
    for i in range(24,len(bars)-48):
        stamp=bars[i]['timestamp']
        if available_after is not None and stamp<available_after:continue
        # Four-hour decision cadence uses only the completed hour, before entry.
        if (stamp+timedelta(hours=1)).hour%4:continue
        peak=previous_peak(bars,i)
        if peak is None:continue
        drop=1-bars[i]['close']/peak
        if abs(drop-reference_drop)>band or drop<.03:continue
        market=btc_returns.get(stamp)
        if market is None or (market<0)!=current_btc_negative:continue
        outcome=recovery_outcome(bars,i,peak)
        if outcome is None:continue
        events.append({'decision_utc':stamp+timedelta(hours=1),'drawdown_pct':drop*100,**outcome})
        # No same-asset overlapping outcome windows or repeated observations of
        # the same two-day path. Outcome values cannot decide entry eligibility.
        available_after=stamp+timedelta(hours=48)
    return events


def summarize(events):
    n=len(events);result={'cases':n,'sufficient_asset_support':n>=20}
    for name in ('recovered_24h','recovered_48h','further_3pct_before_recovery','recovered_before_further_3pct','ambiguous_same_hour'):
        hits=sum(e[name] for e in events)
        result[name]={'count':hits,'rate':hits/n if n else None,'wilson95':wilson(hits,n)}
    result['median_further_drawdown_pct']=__import__('statistics').median(e['worst_further_drawdown_pct'] for e in events) if events else None
    result['interpretation']='historical frequencies, not calibrated forecasts or recommendations'
    return result


async def snapshot(symbols):
    pair_for=lambda s:s.replace('BTC/','XBT/').replace('/','')
    records=[]
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        for symbol in symbols:
            pair=pair_for(symbol)
            for attempt in range(3):
                try:
                    async with session.get('https://api.kraken.com/0/public/Ticker',params={'pair':pair}) as response:
                        response.raise_for_status();ticker=await response.json()
                    async with session.get('https://api.kraken.com/0/public/OHLC',params={'pair':pair,'interval':60}) as response:
                        response.raise_for_status();candles=await response.json()
                    if ticker['error'] or candles['error']:raise ValueError(str(ticker['error']+candles['error']))
                    break
                except (aiohttp.ClientError,asyncio.TimeoutError):
                    if attempt==2:raise
                    await asyncio.sleep(2**attempt)
            received=datetime.now(UTC);t=next(iter(ticker['result'].values()))
            raw=next(v for k,v in candles['result'].items() if k!='last')
            # Kraken always includes an uncommitted final candle. Exclude it
            # explicitly even if the host clock is ahead of the exchange clock.
            complete=[r for r in raw[:-1] if datetime.fromtimestamp(r[0],UTC)+timedelta(hours=1)<=received]
            price=float(t['c'][0]);peak=float(t['h'][1]);bid=float(t['b'][0]);ask=float(t['a'][0])
            if len(complete)<25 or min(price,peak,bid,ask)<=0 or peak<price:raise ValueError(f'{symbol}: invalid snapshot')
            last=complete[-1];ago=next((r for r in complete if r[0]==last[0]-24*3600),None)
            if ago is None:raise ValueError('Missing 24-hour completed-price reference')
            records.append({'symbol':symbol,'snapshot_utc':received.isoformat(),'last_price':price,
                'previous_24h_high':peak,'drawdown_from_24h_high_pct':(price/peak-1)*100,
                'required_recovery_pct':(peak/price-1)*100,
                'completed_close_24h_change_pct':(float(last[4])/float(ago[4])-1)*100,
                'latest_completed_hour_utc':datetime.fromtimestamp(last[0],UTC).isoformat(),
                'last_completed_close':float(last[4]),'spread_bps':(ask-bid)/((ask+bid)/2)*10000,
                'return_last_3_completed_hours_pct':(float(last[4])/float(complete[-4][4])-1)*100,
                'spot_volume_24h':float(t['v'][1]),'spot_trade_count_24h':int(t['t'][1]),
                'source':f'https://api.kraken.com/0/public/Ticker?pair={pair}',
                'source_hourly':f'https://api.kraken.com/0/public/OHLC?pair={pair}&interval=60'})
            LOG.info('%s: %.2f%% below 24-hour high',symbol,records[-1]['drawdown_from_24h_high_pct'])
            await asyncio.sleep(1)
    return records


def historical_hours(symbols,archives):
    result={symbol:[] for symbol in symbols}
    with CandleStore(':memory:') as store:
        db=store.connection
        for symbol in symbols:
            paths=[]
            with tempfile.TemporaryDirectory(prefix='kraken-pullbacks-') as temporary:
                for i,archive_path in enumerate(archives):
                    with zipfile.ZipFile(archive_path) as archive:
                        manifest=json.loads(archive.read('MANIFEST.json'))
                        if manifest.get('product')!='ohlcvt' or manifest.get('has_header') is not False:raise ValueError('Expected OHLCVT archive')
                        member=archive_members(archive,[symbol])[symbol]
                        if member is None:continue
                        path=Path(temporary)/f'{i}.csv'
                        with archive.open(member) as src,path.open('wb') as dest:shutil.copyfileobj(src,dest)
                        paths.append(str(path))
                if not paths:continue
                db.execute('DROP TABLE IF EXISTS pullback_raw')
                # The quarters can use different newline conventions. Sniff each
                # CSV independently, without relaxing strict parsing or padding.
                for i,path in enumerate(paths):
                    prefix='CREATE TEMP TABLE pullback_raw AS' if i==0 else 'INSERT INTO pullback_raw'
                    db.execute(prefix+''' SELECT * FROM read_csv(?,header=false,
                        columns={'epoch':'BIGINT','open':'DOUBLE','high':'DOUBLE','low':'DOUBLE',
                        'close':'DOUBLE','volume':'DOUBLE','trades':'BIGINT'},strict_mode=true,null_padding=false)''',[path])
                invalid=db.execute('''SELECT count(*) FROM pullback_raw WHERE epoch IS NULL OR epoch%60!=0
                    OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL OR trades IS NULL
                    OR NOT isfinite(high) OR NOT isfinite(low) OR NOT isfinite(close) OR high<close OR low>close OR low<=0
                    OR volume<0 OR trades<0''').fetchone()[0]
                duplicates=db.execute('SELECT count(*)-count(DISTINCT epoch) FROM pullback_raw').fetchone()[0]
                if invalid or duplicates:raise ValueError('Invalid or duplicate archive minutes')
                values=db.execute('''SELECT to_timestamp(floor(epoch/3600)*3600) AS timestamp,
                    max(high),min(low),arg_max(close,epoch),sum(volume),sum(trades)
                    FROM pullback_raw GROUP BY 1 ORDER BY 1''').fetchall()
            result[symbol]=[dict(zip(('timestamp','high','low','close','volume','trades'),r)) for r in values]
            LOG.info('%s: %s historical hourly candles',symbol,len(values))
    return result


async def run_pullback(symbols,archives,output,min_drop=5):
    if output.suffix!='.json' or not 0<min_drop<100:raise ValueError('Use JSON output and positive drop threshold')
    live=await snapshot(symbols)
    output.parent.mkdir(parents=True,exist_ok=True)
    output.with_suffix('.snapshot.json').write_text(json.dumps(live,indent=2)+'\n')
    qualified=[r for r in live if -r['drawdown_from_24h_high_pct']>=min_drop]
    if 'BTC/USD' not in symbols:raise ValueError('BTC/USD reference required')
    history=historical_hours(tuple(dict.fromkeys(['BTC/USD',*[r['symbol'] for r in qualified]])),archives)
    btc=history['BTC/USD'];btc_returns={}
    for i,b in enumerate(btc):
        if i>=24 and btc[i-24]['timestamp']==b['timestamp']-timedelta(hours=24):
            btc_returns[b['timestamp']]=b['close']/btc[i-24]['close']-1
    current_btc=next(r for r in live if r['symbol']=='BTC/USD')['completed_close_24h_change_pct']
    for row in qualified:
        events=analogues(history[row['symbol']],-row['drawdown_from_24h_high_pct']/100,btc_returns,current_btc<0)
        row['historical_analogues']=summarize(events);row['historical_cases']=events
        evidence=row['historical_analogues'];interval=evidence['recovered_before_further_3pct']['wilson95']
        row['evidence_status']=('insufficient_asset_cases' if evidence['cases']<20 else
            'recovery_first_majority_supported_in_sample' if interval[0]>.5 else 'no_clear_recovery_first_edge')
    report={'snapshot_start_utc':live[0]['snapshot_utc'],'snapshot_end_utc':live[-1]['snapshot_utc'],
        'minimum_drop_from_24h_high_pct':min_drop,'watchlist':live,'qualified':qualified,
        'BTC_completed_24h_change_pct':current_btc,
        'archives':[{'path':str(p),'sha256':digest(p)} for p in archives],
        'method':'Current public ticker price versus its trailing 24-hour high. Historical completed hourly closes with prior 24-hour highs, drawdowns within two percentage points of current drawdown (minimum 3%), matching sign of BTC preceding 24-hour return. Decisions every four hours; same-asset outcome windows spaced at least 48 hours. Count recovery to the prior high within 24/48 hours and a further 3% decline before recovery. Same-hour touches count downside first. Skip incomplete/noncontiguous hourly outcome windows. No fitted or calibrated recovery predictor.',
        'ready_for_trading':False,'model_signal_status':'No approved model: older net-profit models do not predict recovery to arbitrary prior highs. Current trade-enriched scores are not fabricated from missing live trade inputs.',
        'limitations':['Historical frequencies are not probabilities calibrated for today; the reference high can itself be an exceptional spike.',
            'The manually chosen established-project list does not prove reputation, safety, market cap or Funded eligibility. DOT was not in the earlier classifier universe.',
            'Current snapshot spans multiple sequential requests and uses live ticker prices; historical cases use completed hourly closes.',
            '48-hour spacing reduces same-asset overlap, but events across coins and market regimes remain correlated; Wilson intervals assume independence and can understate uncertainty.',
            'October has an archive-to-live gap and a different macro backdrop. April–September analogues cannot determine today\'s future or exact bottom.',
            'Touching a price is not an executable exit. This analysis does not include costs, slippage, account capital or stop implementation.',
            'No buy/sell order, live trading rule or guarantee of a recovery is produced.']}
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    lines=['# Current established-coin pullback research','',f"Snapshot UTC: {report['snapshot_start_utc']} to {report['snapshot_end_utc']}.", '',
        'Reference: trailing 24-hour high, not a prediction or an assertion that the coin is 5% lower than yesterday. Historical rates below are descriptive and not calibrated forecasts.', '',
        '| Coin | Current price | Drop from 24h high | Recovery target | Required rise | Past matching cases | Recovered in 24h | Recovered in 48h | Further -3% before recovery | Recovery before -3% | Evidence |',
        '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|']
    def fmt(v):return 'unavailable' if v is None else f'{v:.0%}'
    for r in qualified:
        s=r['historical_analogues']
        lines.append(f"| {r['symbol']} | ${r['last_price']:.6g} | {r['drawdown_from_24h_high_pct']:.2f}% | ${r['previous_24h_high']:.6g} | {r['required_recovery_pct']:.2f}% | {s['cases']} | {fmt(s['recovered_24h']['rate'])} | {fmt(s['recovered_48h']['rate'])} | {fmt(s['further_3pct_before_recovery']['rate'])} | {fmt(s['recovered_before_further_3pct']['rate'])} | {r['evidence_status']} |")
    lines.extend(['','## Method','',report['method'],'',report['model_signal_status'],'','## Limits','',*('- '+v for v in report['limitations'])])
    output.with_suffix('.md').write_text('\n'.join(lines)+'\n')
    return report
