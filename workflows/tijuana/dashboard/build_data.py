"""Build the Tijuana River dashboard's data files from the public Resilient bucket.

Usage: python3 -I build_data.py <download_dir> <out_dir>

Standard library only, so a scheduled job can run it without installing anything.
Writes one JSON file per dashboard dataset into <out_dir> (row objects), plus
forecast_log.json, the running record of each day's published H2S forecast that
the "recent nights" check compares with what the monitors recorded.
"""
import csv
import datetime as dt
import json
import math
import os
import sys
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo

PUBLIC = 'https://oss.resilientservice.mooo.com/resilentpublic/'
DASHBOARD_PREFIX = 'latest/tijuana/dashboard/'
SITES = ('SAN YSIDRO', 'NESTOR - BES', 'IB CIVIC CTR')
PST = dt.timezone(dt.timedelta(hours=-8))
LA = ZoneInfo('America/Los_Angeles')
MGD_PER_CMS = 22.824465
NIGHTS_SHOWN = 45
WARN_CUTOFF = 0.5  # chance of >30 ppb at which a night counts as warned, as in the backtest
REVIEW_CUTOFFS = (0.3, 0.4, 0.5)  # warning cutoffs compared in the prediction review

FILES = {
    'boundary_cms.csv': 'latest/tijuana/streamflow/boundary_cms/boundary_cms.csv',
    'streamflow_forecast.csv': 'latest/tijuana/streamflow_forecast/latest.csv',
    'plant_balance.csv': 'latest/tijuana/plant_balance/daily/plant_balance.csv',
    'hs2_current.csv': 'tijuana/sd_apcd_air/output/hs2_current.csv',
    'hs2_lastday.csv': 'tijuana/sd_apcd_air/output/hs2_lastday.csv',
    'wind_grid.csv': 'latest/tijuana/weather_grid/wind_grid.csv',
    'forecast_15min.csv': 'latest/tijuana/weather_15min/forecast_15min.csv',
    'spills.csv': 'tijuana/ibwc/output/spills.csv',
    'daily_station_forecasts.csv': 'latest/tijuana/forecast_data/daily_station_forecasts.csv',
    'daily_summary.json': 'latest/tijuana/forecast_data/daily_summary.json',
    'apcd_h2s_latest.csv': 'latest/tijuana/sd_apcd_air/h2s/apcd_h2s_latest.csv',
    'forecast_performance.json': 'latest/tijuana/forecast_data/forecast_performance_latest.json',
    'forecast_skill_report.json': 'latest/tijuana/forecast_data/forecast_skill_report.json',
    'backtest_30ppb_metrics.json': 'latest/tijuana/forecast_data/backtest/h2s_backtest_30ppb_metrics.json',
    'backtest_30ppb_predictions.csv': 'latest/tijuana/forecast_data/backtest/h2s_backtest_30ppb_predictions.csv',
}
OPTIONAL = {'forecast_performance.json', 'forecast_skill_report.json',
            'backtest_30ppb_metrics.json', 'backtest_30ppb_predictions.csv'}
BACKTEST_PAGE = PUBLIC + 'latest/tijuana/forecast_data/backtest/h2s_backtest_30ppb.html'


def fetch(key, path):
    urllib.request.urlretrieve(PUBLIC + key, path)


def num(x):
    try:
        v = float(x)
        return None if math.isnan(v) else round(v, 3)
    except (TypeError, ValueError):
        return None


def sun_times(date, lat=32.7157, lon=-117.1611):
    """Sunrise and sunset (UTC) for a calendar date, NOAA sunrise equation; within about a minute of astral."""
    J = date.toordinal() - dt.date(2000, 1, 1).toordinal() - lon / 360.0
    M = math.radians((357.5291 + 0.98560028 * J) % 360)
    C = 1.9148 * math.sin(M) + 0.0200 * math.sin(2 * M) + 0.0003 * math.sin(3 * M)
    L = math.radians((math.degrees(M) + C + 180 + 102.9372) % 360)
    transit = J + 0.0053 * math.sin(M) - 0.0069 * math.sin(2 * L)
    dec = math.asin(math.sin(L) * math.sin(math.radians(23.4397)))
    w = math.degrees(math.acos((math.sin(math.radians(-0.833)) - math.sin(math.radians(lat)) * math.sin(dec))
                               / (math.cos(math.radians(lat)) * math.cos(dec))))
    base = dt.datetime(2000, 1, 1, 12, tzinfo=dt.timezone.utc)
    return base + dt.timedelta(days=transit - w / 360), base + dt.timedelta(days=transit + w / 360)


def night_window(day):
    """Sunset on `day` to sunrise the next morning, the pipeline's night."""
    return (sun_times(day)[1].replace(microsecond=0),
            sun_times(day + dt.timedelta(days=1))[0].replace(microsecond=0))


def main(D, O):
    os.makedirs(D, exist_ok=True)
    os.makedirs(O, exist_ok=True)
    for name, key in FILES.items():
        try:
            fetch(key, f'{D}/{name}')
        except urllib.error.HTTPError:
            if name not in OPTIONAL:
                raise
            print('optional file missing:', key)
    now = dt.datetime.now(dt.timezone.utc)
    have = lambda f: os.path.exists(f'{D}/{f}')
    rd = lambda f: list(csv.DictReader(open(f'{D}/{f}')))

    def dump(n, rows):
        json.dump(rows, open(f'{O}/{n}.json', 'w'))
        print(n, len(rows), len(json.dumps(rows)))

    # Border flow, last 30 days, hourly; timestamps are UTC-08:00
    b = []
    for r in rd('boundary_cms.csv'):
        v = num(r['Average (m^3/s)'])
        t = dt.datetime.fromisoformat(r['End of Interval (UTC-08:00)']).replace(tzinfo=PST)
        if v is None or t < now - dt.timedelta(days=30) or t > now:
            continue
        b.append({'time': t.astimezone(dt.timezone.utc).isoformat().replace('+00:00', 'Z'),
                  'flow_cms': v, 'flow_mgd': round(v * MGD_PER_CMS, 1)})
    dump('border_flow', b)
    dump('border_flow_forecast', [
        {'time': dt.datetime.fromisoformat(r['time']).replace(tzinfo=PST).astimezone(dt.timezone.utc)
            .isoformat().replace('+00:00', 'Z'), 'flow_cms': num(r['Average (m^3/s)'])}
        for r in rd('streamflow_forecast.csv')])

    cutoff = (now - dt.timedelta(days=90)).strftime('%Y-%m-%d')
    dump('plant_daily', [
        {'date': r['date'], 'influent_mgd': num(r['influent_mgd']), 'effluent_mgd': num(r['effluent_mgd']),
         'capacity_mgd': num(r['capacity_mgd']), 'border_flow_mgd': num(r['border_flow_mgd'])}
        for r in rd('plant_balance.csv') if r['date'] >= cutoff])

    dump('h2s_current', [
        {'site': r['SiteName'], 'location': r['LongName'], 'lat': float(r['Latitude']), 'lon': float(r['Longitude']),
         'time': r['Date with time'], 'h2s_ppb': num(r['Result']), 'level': r['level']}
        for r in rd('hs2_current.csv')])
    h = [{'site': r['SiteName'], 'time': r['Date with time'], 'h2s_ppb': num(r['Result'])}
         for r in rd('hs2_lastday.csv') if num(r['Result']) is not None]
    dump('h2s_recent', h)

    # Wind grid: the frame nearest now
    w = rd('wind_grid.csv')
    tsel = min({r['time'] for r in w}, key=lambda t: abs(dt.datetime.fromisoformat(t.replace('Z', '+00:00')) - now))
    dump('wind_now', [
        {'time': r['time'], 'lat': float(r['lat']), 'lon': float(r['lon']), 'speed_ms': num(r['wind_speed_10m']),
         'direction_deg': num(r['wind_direction_10m']), 'gust_ms': num(r['wind_gusts_10m'])}
        for r in w if r['time'] == tsel])
    dump('weather_15min', [
        {'time': r['time'], 'site': r['site_name'], 'temp_c': num(r['temperature_2m']),
         'humidity_pct': num(r['relative_humidity_2m']), 'wind_ms': num(r['wind_speed_10m']),
         'wind_dir_deg': num(r['wind_direction_10m'])}
        for r in rd('forecast_15min.csv')])

    sp = [{'start': r['Start Time'][:10], 'end': r['End Time'][:10], 'location': r['Discharge Location'],
           'event_type': r['Event Type'], 'volume': r['Approximate Discharge Volume'],
           'volume_gal': num(r['Approximate Discharge Volume Value']), 'status': r['Status'], 'notes': r['Notes']}
          for r in rd('spills.csv')]
    sp.sort(key=lambda x: x['start'], reverse=True)
    dump('spills', sp)

    dump('h2s_forecast_hourly', [
        {'time': r['time'].replace(' ', 'T').replace('+00:00', 'Z'), 'station': r['station'],
         'lead_hour': int(r['lead_hour']), 'h2s_pred_ppb': num(r['h2s_pred']), 'prob_5': num(r['prob_5']),
         'prob_10': num(r['prob_10']), 'prob_30': num(r['prob_30']), 'risk': r['risk']}
        for r in rd('daily_station_forecasts.csv')])
    s = json.load(open(f'{D}/daily_summary.json'))
    dump('h2s_forecast_24h', [
        {'station': s['stations'][k]['name'], 'generated_at': s['generated_at'], 'max_prob_30': v['max_prob_30'],
         'max_prob_10': v['max_prob_10'], 'max_prob_5': v['max_prob_5'], 'max_h2s_ppb': v['max_h2s'],
         'hours_orange': v['hours_orange'], 'hours_possible_orange': v['hours_possible_orange']}
        for k, v in s['forecast_24h'].items()])

    # Last night's H2S peak, sunset to sunrise
    hr = [(dt.datetime.fromisoformat(r['time']), r['site'], r['h2s_ppb']) for r in h]
    T = max(o[0] for o in hr)
    day = T.date()
    if T < sun_times(day)[1]:
        day -= dt.timedelta(days=1)
    n_start, n_end = night_window(day)
    nrows = []
    for site in SITES:
        pts = [o for o in hr if o[1] == site and n_start <= o[0] < n_end]
        if pts:
            pk = max(pts, key=lambda o: o[2])
            nrows.append({'site': site, 'night_start': n_start.isoformat(), 'night_end': n_end.isoformat(),
                          'night_complete': T >= n_end, 'peak_ppb': pk[2], 'peak_time': pk[0].isoformat(),
                          'hours': len(pts)})
    dump('h2s_night_peak', nrows)

    # H2S hourly history: previous year + current year + last two days
    yr = now.year
    hist = {}
    sources = []
    try:
        fetch(f'latest/tijuana/sd_apcd_air/h2s/apcd_h2s_{yr - 1}.csv', f'{D}/apcd_h2s_prev.csv')
        sources.append('apcd_h2s_prev.csv')
    except urllib.error.HTTPError:
        pass
    sources.append('apcd_h2s_latest.csv')
    for src in sources:
        for r in rd(src):
            v = num(r['Result'])
            if v is not None and r['SiteName'] in SITES:
                hist[(r['SiteName'], r['Date with time'])] = v
    for r in h:
        hist[(r['site'], r['time'])] = r['h2s_ppb']
    obs = sorted(((dt.datetime.fromisoformat(t), st, v) for (st, t), v in hist.items()), key=lambda x: x[0])
    as_of = obs[-1][0]

    # Exceedance episodes (>30 ppb, and >70 ppb, the ATSDR acute MRL used as the review's upper tier)
    ex = []
    for th in (30, 70):
        for scope in ('All sites',) + SITES:
            hits = [o for o in obs if o[2] > th and (scope == 'All sites' or o[1] == scope)]
            row = {'scope': scope, 'threshold_ppb': th, 'as_of': as_of.isoformat(), 'last_time': None,
                   'days_since': None, 'episode_start': None, 'episode_end': None, 'episode_peak_ppb': None,
                   'episode_peak_site': None, 'episode_hours': None,
                   'hours_this_year': sum(1 for o in hits if o[0].year == yr)}
            if hits:
                # last episode: run of exceedances with gaps of at most 3 hours
                ep = [hits[-1]]
                for o in reversed(hits[:-1]):
                    if ep[-1][0] - o[0] <= dt.timedelta(hours=3):
                        ep.append(o)
                    else:
                        break
                pk = max(ep, key=lambda o: o[2])
                row.update({'last_time': hits[-1][0].isoformat(), 'days_since': (as_of - hits[-1][0]).days,
                            'episode_start': ep[-1][0].isoformat(), 'episode_end': ep[0][0].isoformat(),
                            'episode_peak_ppb': pk[2], 'episode_peak_site': pk[1],
                            'episode_hours': len({o[0] for o in ep})})
            ex.append(row)
    dump('h2s_exceedances', ex)

    build_forecast_checks(D, O, dump, have, rd, s, obs, as_of, now)

    # Static map outlines (land, border, river), kept beside this script
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'map_outlines.json')) as f:
        dump('map_outlines', json.load(f))

    # When this refresh ran, for the "Data updated" pill
    dump('refresh_info', [{'refreshed_at': now.replace(microsecond=0).isoformat(),
                           'latest_h2s_reading': T.isoformat()}])


def build_forecast_checks(D, O, dump, have, rd, summary, obs, as_of, now):
    """Forecast skill from the backtest and the live performance report, and a night-by-night check."""
    # Backtest of the 30 ppb classifier: event nights warned, missed and false alarms at the backtest cutoff
    skill = []
    if have('backtest_30ppb_metrics.json'):
        m = json.load(open(f'{D}/backtest_30ppb_metrics.json'))
        cut = m['config'].get('cutoff', WARN_CUTOFF)

        def at_cutoff(rows):
            return next((c for c in rows if abs(c['cutoff'] - cut) < 1e-9), None)

        def row(scope, block, nightly):
            c = block.get('at_cutoff') or at_cutoff(m['cutoffs']['deployed'])
            return {'scope': scope, 'nights': nightly['n'], 'event_nights': nightly['positives'],
                    'warned': c['hits'], 'missed': c['misses'], 'false_alarms': c['false_alarms'],
                    'pod': c['pod'], 'far': c['far'], 'auc': nightly['auc'], 'cutoff': cut,
                    'first_issue': m['record']['first_issue'], 'last_issue': m['record']['last_issue'],
                    'page': BACKTEST_PAGE}

        skill.append(row('All sites', {}, m['nightly']['deployed']))
        for st in SITES:
            if st in m['by_station']:
                d = m['by_station'][st]['deployed']
                skill.append(row(st, d, d))
    dump('forecast_skill', skill)

    # The same backtest at the cutoffs the review compares (0.3, 0.4, 0.5), for the current
    # forecast, the proposed night-target model and persistence, the line any product must clear
    cutoffs = []
    if have('backtest_30ppb_metrics.json'):
        for variant in ('deployed', 'night', 'persistence'):
            for c in m['cutoffs'].get(variant, []):
                if any(abs(c['cutoff'] - k) < 1e-9 for k in REVIEW_CUTOFFS):
                    cutoffs.append({'variant': variant, 'cutoff': round(c['cutoff'], 2), 'warned': c['hits'],
                                    'missed': c['misses'], 'false_alarms': c['false_alarms'],
                                    'event_nights': c['hits'] + c['misses'], 'pod': c['pod'], 'far': c['far'],
                                    'auc': m['nightly'].get(variant, {}).get('auc')})
    dump('forecast_cutoffs', cutoffs)

    # Live performance of the published forecast, matched hour by hour with what was measured
    live = []
    if have('forecast_performance.json'):
        p = json.load(open(f'{D}/forecast_performance.json'))
        sm = p['summary']
        vc = sm['verdict_counts']
        period = {}
        if have('forecast_skill_report.json'):
            sk = json.load(open(f'{D}/forecast_skill_report.json'))
            period = {'period_start': sk.get('time_min'), 'period_end': sk.get('time_max'),
                      'generated_at': sk.get('generated_at')}
        live.append({'variant': sm.get('variant'), 'forecast_hours': sm['n'],
                     'acceptable_share': sm['tolerant_accuracy'], 'dangerous_misses': vc.get('dangerous_miss', 0),
                     'smell_misses': vc.get('smell_miss', 0), 'false_alarms': vc.get('false_alarm', 0),
                     'early_warnings': vc.get('early_warning_ok', 0), 'soft_misses': vc.get('soft_miss', 0),
                     **period})
    dump('forecast_live', live)

    # Running log of the forecast published each day: its highest chance of >30 ppb for the coming night
    log = []
    try:
        with urllib.request.urlopen(PUBLIC + DASHBOARD_PREFIX + 'data/forecast_log.json') as f:
            log = json.load(f)
    except (urllib.error.HTTPError, ValueError):
        pass
    issued = dt.datetime.fromisoformat(summary['generated_at'])
    night_of = issued.astimezone(PST).date().isoformat()  # issued late morning; covers that evening's night
    by_name = {v['name']: k for k, v in summary['stations'].items()}
    for st in SITES:
        code = by_name.get(st)
        fc = summary['forecast_24h'].get(code) if code else None
        if fc is None:
            continue
        entry = {'night_of': night_of, 'station': st, 'max_prob_30': fc['max_prob_30'] / 100.0,
                 'issued_at': summary['generated_at'], 'source': 'published'}
        log = [e for e in log if not (e['night_of'] == night_of and e['station'] == st)] + [entry]
    log.sort(key=lambda e: (e['night_of'], e['station']))
    json.dump(log, open(f'{O}/forecast_log.json', 'w'))

    # Night-by-night check. For each station-night: the night-target model's chance (backtest), the
    # current forecast's chance (backtest, then the published forecast once logged), persistence
    # ("the 24 h before the 15:00 issue reached 30 ppb") and the measured sunset-to-sunrise peak.
    peaks = {}
    by_site = {st: [] for st in SITES}
    for t, st, v in obs:
        by_site[st].append((t, v))
        local = t.astimezone(PST)
        day = local.date() if local.hour >= 12 else local.date() - dt.timedelta(days=1)
        n_start, n_end = night_window(day)
        if n_start <= t < n_end:
            key = (day.isoformat(), st)
            peaks[key] = max(peaks.get(key, 0), v)
    night_p, current = {}, {}
    if have('backtest_30ppb_predictions.csv'):
        for r in rd('backtest_30ppb_predictions.csv'):
            t = dt.datetime.fromisoformat(r['target_time'].replace('Z', '+00:00'))
            issue = dt.datetime.fromisoformat(r['issue_time'].replace('Z', '+00:00'))
            day = issue.astimezone(PST).date()
            n_start, n_end = night_window(day)
            if not (n_start <= t < n_end):
                continue
            key = (day.isoformat(), r['site_name'])
            night_p[key] = max(num(r['p_night']) or 0.0, night_p.get(key, 0.0))
            current[key] = {'source': 'backtest', 'chance': max(num(r['p_deployed']) or 0.0,
                                                                current.get(key, {}).get('chance', 0.0))}
    for e in log:
        current[(e['night_of'], e['station'])] = {'source': 'published', 'chance': e['max_prob_30']}

    def persistence(st, issue):
        """Highest reading in the 24 h before `issue`, and whether it reached 30 ppb."""
        vals = [v for t, v in by_site[st] if issue - dt.timedelta(hours=24) <= t < issue]
        return (max(vals) if vals else None), bool(vals) and max(vals) >= 30

    def outcome(warned, peak):
        if warned is None or peak is None:
            return None
        return ('warned' if warned else 'missed') if peak > 30 else ('false alarm' if warned else 'quiet')

    last_day = as_of.astimezone(PST).date()
    if as_of < night_window(last_day)[1]:
        last_day -= dt.timedelta(days=1)  # tonight's night has not finished
    days = [(last_day - dt.timedelta(days=i)).isoformat() for i in range(NIGHTS_SHOWN)][::-1]
    rows = []
    for d in days:
        issue = dt.datetime.combine(dt.date.fromisoformat(d), dt.time(15), tzinfo=LA)  # the backtest's issue time
        for st in SITES:
            peak = peaks.get((d, st))
            pn = night_p.get((d, st))
            cur = current.get((d, st))
            p24, pw = persistence(st, issue)
            if peak is None and pn is None and cur is None:
                continue
            rows.append({'night_of': d, 'station': st, 'peak_ppb': peak,
                         'chance_night': None if pn is None else round(pn, 4),
                         'chance_current': None if cur is None else round(cur['chance'], 4),
                         'current_source': None if cur is None else cur['source'],
                         'persistence_peak_24h': p24, 'persistence_warned': pw if p24 is not None else None,
                         'outcome_night': outcome(None if pn is None else pn >= WARN_CUTOFF, peak),
                         'outcome_persistence': outcome(pw if p24 is not None else None, peak)})
    dump('forecast_nights', rows)

    # Persistence for tonight, live: did the last 24 h of readings at each monitor reach 30 ppb?
    tonight = []
    for st in SITES:
        p24, pw = persistence(st, as_of + dt.timedelta(seconds=1))
        tonight.append({'station': st, 'peak_24h_ppb': p24, 'warn': pw if p24 is not None else None,
                        'as_of': as_of.isoformat()})
    dump('persistence_tonight', tonight)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
