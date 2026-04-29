#!/usr/bin/env python3
import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT
PUBLISH_DIR = ROOT

SYDNEY_TZ = ZoneInfo('Australia/Sydney')

REFRESH_SCHEDULES = [
    {
        'name': 'ASX open refresh',
        'timezone': 'Australia/Sydney',
        'hour': 10,
        'minute': 2,
        'weekdays': [0, 1, 2, 3, 4],
        'description': 'Mon–Fri 10:02 AM Sydney',
    },
    {
        'name': 'US open refresh',
        'timezone': 'America/New_York',
        'hour': 9,
        'minute': 32,
        'weekdays': [0, 1, 2, 3, 4],
        'description': 'Mon–Fri 9:32 AM New York (auto-converted to Sydney time)',
    },
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

SP500_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
ASX200_URL = 'https://en.wikipedia.org/wiki/S%26P/ASX_200'

SHORT_BUCKETS = [30, 20, 10, 5, 2]
LONG_BUCKETS = [40, 25, 10]

METRIC_ORDER = [
    ('open_vs_prev_close_pct', '1. Open vs previous close', 'Overnight panic / news gap'),
    ('prev_close_vs_prev_open_pct', '2. Previous close vs previous open', 'Previous session same-day damage'),
    ('today_open_vs_prev_open_pct', '3. Today open vs previous open', 'Two-opening-days comparison'),
    ('week_change_pct', '4. 1 week', 'Current price vs ~1 week ago'),
    ('month_change_pct', '5. 1 month', 'Current price vs ~1 month ago'),
    ('drawdown_52w_pct', '6. 52-week high drawdown', 'Current price vs 52-week high'),
]

SECTION_THRESHOLD = {
    'open_vs_prev_close_pct': -2.0,
    'prev_close_vs_prev_open_pct': -2.0,
    'today_open_vs_prev_open_pct': -2.0,
    'week_change_pct': -2.0,
    'month_change_pct': -2.0,
    'drawdown_52w_pct': -10.0,
}


class WikiTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = []
        self._in_table = False
        self._table_depth = 0
        self._current_table = []
        self._in_row = False
        self._current_row = []
        self._in_cell = False
        self._cell_text = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        cls = attrs.get('class', '')
        if tag == 'table' and ('wikitable' in cls or 'table table-striped data' in cls or cls == 'table table-striped data'):
            if not self._in_table:
                self._in_table = True
                self._table_depth = 1
                self._current_table = []
                return
        if not self._in_table:
            return
        if tag == 'table':
            self._table_depth += 1
        elif tag == 'tr':
            self._in_row = True
            self._current_row = []
        elif tag in ('td', 'th'):
            self._in_cell = True
            self._cell_text = []

    def handle_endtag(self, tag):
        if not self._in_table:
            return
        if tag in ('td', 'th') and self._in_cell:
            text = clean_text(''.join(self._cell_text))
            self._current_row.append(text)
            self._in_cell = False
            self._cell_text = []
        elif tag == 'tr' and self._in_row:
            if self._current_row:
                self._current_table.append(self._current_row)
            self._in_row = False
        elif tag == 'table':
            self._table_depth -= 1
            if self._table_depth == 0:
                self.tables.append(self._current_table)
                self._current_table = []
                self._in_table = False

    def handle_data(self, data):
        if self._in_cell:
            self._cell_text.append(data)


def clean_text(text):
    text = unescape(text or '')
    text = re.sub(r'\[[^\]]+\]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def fetch_text(url, timeout=(10, 30)):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def parse_tables(url):
    parser = WikiTableParser()
    parser.feed(fetch_text(url))
    return parser.tables


def extract_sp500_constituents():
    tables = parse_tables(SP500_URL)
    table = next((t for t in tables if t and t[0][:2] == ['Symbol', 'Security']), None)
    if not table:
        raise RuntimeError('Could not parse S&P 500 constituents table')
    rows = []
    for row in table[1:]:
        if len(row) < 4:
            continue
        symbol = row[0].strip()
        company = row[1].strip()
        sector = row[2].strip()
        yahoo_symbol = symbol.replace('.', '-')
        rows.append({
            'market': 'S&P 500',
            'index': 'S&P 500',
            'display_symbol': symbol,
            'symbol': yahoo_symbol,
            'company': company,
            'sector': sector,
            'currency': 'USD',
            'source': SP500_URL,
        })
    return rows


def parse_market_cap(value):
    if not value:
        return 0.0
    value = value.replace(',', '').replace('$', '').strip()
    try:
        return float(value)
    except Exception:
        return 0.0


def extract_asx100_constituents():
    tables = parse_tables(ASX200_URL)
    table = next((t for t in tables if t and t[0][:4] == ['Code', 'Company', 'Sector', 'Market Capitalisation (A$)']), None)
    if not table:
        raise RuntimeError('Could not parse ASX200 constituents table')
    rows = []
    for row in table[1:]:
        if len(row) < 4:
            continue
        code = row[0].strip().upper()
        company = row[1].strip()
        sector = row[2].strip()
        market_cap = parse_market_cap(row[3])
        rows.append({
            'market': 'ASX 100',
            'index': 'ASX 100',
            'display_symbol': code,
            'symbol': f'{code}.AX',
            'company': company,
            'sector': sector,
            'currency': 'AUD',
            'market_cap_aud': market_cap,
            'source': ASX200_URL,
        })
    rows.sort(key=lambda r: r.get('market_cap_aud', 0), reverse=True)
    return rows[:100]


def pct_change(current, base):
    if current is None or base in (None, 0):
        return None
    try:
        return (float(current) / float(base) - 1.0) * 100.0
    except Exception:
        return None


def safe_float(value):
    if value is None:
        return None
    try:
        value = float(value)
        if math.isnan(value):
            return None
        return value
    except Exception:
        return None


def build_bars(result):
    quote = (result.get('indicators') or {}).get('quote') or []
    if not quote:
        return []
    quote = quote[0]
    opens = quote.get('open') or []
    closes = quote.get('close') or []
    ts = result.get('timestamp') or []
    bars = []
    for i, stamp in enumerate(ts):
        o = safe_float(opens[i] if i < len(opens) else None)
        c = safe_float(closes[i] if i < len(closes) else None)
        if o is None and c is None:
            continue
        bars.append({'timestamp': stamp, 'open': o, 'close': c})
    return bars


def ref_close(bars, sessions_back):
    if len(bars) <= sessions_back:
        return None
    idx = -(sessions_back + 1)
    return safe_float(bars[idx]['close'])


def fetch_symbol_metrics(item):
    symbol = item['symbol']

    def chart_url(sym):
        return f'https://query1.finance.yahoo.com/v8/finance/chart/{quote(sym)}?range=1y&interval=1d&includePrePost=false'

    def resolve_symbol_fallback(base_item):
        try:
            q = base_item.get('company') or base_item.get('display_symbol') or base_item['symbol']
            search_url = 'https://query1.finance.yahoo.com/v1/finance/search?q=' + quote(q)
            r = requests.get(search_url, headers=HEADERS, timeout=(10, 20))
            r.raise_for_status()
            payload = r.json()
            for row in payload.get('quotes', []):
                sym = row.get('symbol')
                exch = row.get('exchange') or row.get('exchDisp') or ''
                if not sym:
                    continue
                if base_item['market'] == 'ASX 100' and (sym.endswith('.AX') or exch == 'ASX' or 'Australian' in str(exch)):
                    return sym
                if base_item['market'] == 'S&P 500' and not sym.endswith('.AX'):
                    return sym
        except Exception:
            return None
        return None

    url = chart_url(symbol)
    last_err = None
    for attempt in range(4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=(10, 30))
            r.raise_for_status()
            payload = r.json()
            result = (payload.get('chart') or {}).get('result') or []
            if not result:
                raise RuntimeError(f'No chart result for {symbol}')
            result = result[0]
            meta = result.get('meta') or {}
            bars = build_bars(result)
            if len(bars) < 2:
                raise RuntimeError(f'Not enough bars for {symbol}')

            current_price = safe_float(meta.get('regularMarketPrice'))
            if current_price is None:
                current_price = safe_float(bars[-1]['close'])
            today_open = safe_float(bars[-1]['open'])
            prev_open = safe_float(bars[-2]['open'])
            prev_close = safe_float(bars[-2]['close'])
            week_ref = ref_close(bars, 5)
            month_ref = ref_close(bars, 21)
            high_52w = safe_float(meta.get('fiftyTwoWeekHigh'))
            market_time = meta.get('regularMarketTime')

            out = dict(item)
            out.update({
                'resolved_symbol': symbol,
                'long_name': meta.get('longName') or item.get('company') or meta.get('shortName') or symbol,
                'current_price': current_price,
                'today_open': today_open,
                'prev_open': prev_open,
                'prev_close': prev_close,
                'week_ref_close': week_ref,
                'month_ref_close': month_ref,
                'high_52w': high_52w,
                'market_time': market_time,
                'market_time_iso': datetime.fromtimestamp(market_time, tz=timezone.utc).isoformat() if market_time else None,
                'currency': meta.get('currency') or item.get('currency'),
                'exchange': meta.get('fullExchangeName') or meta.get('exchangeName') or '',
                'quote_url': f'https://finance.yahoo.com/quote/{quote(symbol)}',
                'open_vs_prev_close_pct': pct_change(today_open, prev_close),
                'prev_close_vs_prev_open_pct': pct_change(prev_close, prev_open),
                'today_open_vs_prev_open_pct': pct_change(today_open, prev_open),
                'week_change_pct': pct_change(current_price, week_ref),
                'month_change_pct': pct_change(current_price, month_ref),
                'drawdown_52w_pct': pct_change(current_price, high_52w),
            })
            return out
        except Exception as e:
            last_err = e
            if attempt == 0:
                fallback_symbol = resolve_symbol_fallback(item)
                if fallback_symbol and fallback_symbol != symbol:
                    symbol = fallback_symbol
                    url = chart_url(symbol)
                    continue
            time.sleep(1 + attempt * 2)
    failed = dict(item)
    failed['error'] = str(last_err)
    return failed


def bucket_label(value, kind='short'):
    if value is None or value >= 0:
        return ''
    drop = abs(value)
    if kind == 'long':
        if drop >= 40:
            return '40%+ down'
        if drop >= 25:
            return '25–40% down'
        if drop >= 10:
            return '10–25% down'
        return '<10% down'
    if drop >= 30:
        return '30%+ down'
    if drop >= 20:
        return '20–30% down'
    if drop >= 10:
        return '10–20% down'
    if drop >= 5:
        return '5–10% down'
    if drop >= 2:
        return '2–5% down'
    return '<2% down'


def format_pct(value):
    if value is None:
        return ''
    return f'{value:+.2f}%'


def format_price(value, currency):
    if value is None:
        return ''
    symbol = '$' if currency in ('USD', 'AUD') else ''
    return f'{symbol}{value:,.2f}'


def esc(text):
    text = '' if text is None else str(text)
    return (text
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;'))


def format_sydney(dt):
    return dt.astimezone(SYDNEY_TZ).strftime('%a %d %b %Y, %-I:%M %p Sydney')


def upcoming_refreshes(start_utc, limit=20):
    items = []
    for day_offset in range(0, 21):
        for schedule in REFRESH_SCHEDULES:
            tz = ZoneInfo(schedule['timezone'])
            local_start = start_utc.astimezone(tz)
            local_date = (local_start + timedelta(days=day_offset)).date()
            candidate_local = datetime(local_date.year, local_date.month, local_date.day, schedule['hour'], schedule['minute'], tzinfo=tz)
            if candidate_local.weekday() not in schedule['weekdays']:
                continue
            candidate_utc = candidate_local.astimezone(timezone.utc)
            if candidate_utc <= start_utc:
                continue
            items.append({
                'name': schedule['name'],
                'timezone': schedule['timezone'],
                'utc_iso': candidate_utc.isoformat(),
                'sydney_label': format_sydney(candidate_utc),
            })
    items.sort(key=lambda x: x['utc_iso'])
    return items[:limit]


def metric_rows(rows, metric_key):
    threshold = SECTION_THRESHOLD[metric_key]
    keep = [r for r in rows if r.get(metric_key) is not None and r[metric_key] <= threshold]
    keep.sort(key=lambda r: r[metric_key])
    return keep


def count_bucket_rows(rows, metric_key, kind='short'):
    counts = {}
    for r in rows:
        value = r.get(metric_key)
        label = bucket_label(value, kind=kind)
        if not label:
            continue
        counts[label] = counts.get(label, 0) + 1
    return counts


def render_html(summary, rows, path):
    generated_at = summary['generated_at_utc']
    refresh_schedule_json = esc(json.dumps(summary['refresh_schedule']))
    upcoming_refreshes_json = esc(json.dumps(summary['upcoming_refreshes']))
    html = ['<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">']
    html.append('<title>Stock Dip Dashboard</title>')
    html.append('''<style>
    body{font-family:Inter,system-ui,Arial,sans-serif;background:#0b1020;color:#e8ecf3;margin:0;padding:24px}
    .wrap{max-width:1600px;margin:0 auto}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px;margin:20px 0}.meta-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px;margin:20px 0}
    .card{background:#121a2b;border:1px solid #24304a;border-radius:16px;padding:18px}.big{font-size:28px;font-weight:700}.muted{color:#9eb0cf}
    .btn{display:inline-block;background:#22304c;border:1px solid #385178;color:#e8ecf3;padding:10px 14px;border-radius:10px;text-decoration:none;cursor:pointer;font-size:14px}.btn:hover{background:#2b3e63}.btn-row{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px}
    table{width:100%;border-collapse:collapse;background:#121a2b;border-radius:16px;overflow:hidden}th,td{padding:10px 12px;border-bottom:1px solid #24304a;text-align:left;font-size:14px;vertical-align:top}
    th{background:#182238;position:sticky;top:0}th.sortable{cursor:pointer;user-select:none}th.sortable:hover{background:#22304c}th.sortable::after{content:" ↕";color:#7084a8;font-size:12px}th.sort-asc::after{content:" ↑";color:#cfe1ff}th.sort-desc::after{content:" ↓";color:#cfe1ff}
    a{color:#8cc2ff;text-decoration:none}.pill{display:inline-block;padding:4px 8px;border-radius:999px;background:#1b2740;color:#bcd3ff;font-size:12px}
    .pill-short{background:#3b1f1f;color:#ffd7d7}.pill-long{background:#2e2a13;color:#ffe8a3}.good{color:#7ee787}.bad{color:#ff9b9b}.section{margin-top:28px}
    .bucket-row{display:flex;flex-wrap:wrap;gap:10px;margin:10px 0 14px}.bucket{background:#121a2b;border:1px solid #24304a;border-radius:999px;padding:8px 12px;font-size:13px;color:#cfe1ff}
    .small{font-size:12px}.market-asx{color:#7dd3fc}.market-us{color:#c4b5fd}
    @media (max-width:1100px){.grid,.meta-grid{grid-template-columns:repeat(2,minmax(0,1fr));}} @media (max-width:700px){.grid,.meta-grid{grid-template-columns:1fr;}}
    </style></head><body><div class="wrap">''')
    html.append('<h1>Stock Dip Dashboard</h1>')
    html.append('<div class="muted">ASX 100 + S&amp;P 500 dip scanner for overnight gaps, same-day damage, short swing drawdowns, and 52-week-high drawdown</div>')
    html.append(f'<div class="small muted" style="margin-top:8px">Generated (UTC): {esc(generated_at)}</div>')
    html.append('<div class="meta-grid">')
    html.append(f'<div class="card"><div class="muted">Last refreshed</div><div class="big" id="last-refresh-sydney">{esc(summary["generated_at_sydney"])}</div><div class="small muted">Sydney time</div></div>')
    html.append(f'<div class="card"><div class="muted">Next scheduled refresh</div><div class="big" id="next-refresh-sydney">{esc(summary["next_refresh_sydney"])}</div><div class="small muted" id="next-refresh-label">{esc(summary["next_refresh_name"])}</div></div>')
    html.append(f'<div class="card"><div class="muted">Refresh cadence</div><div class="small">{esc(summary["refresh_schedule"][0]["description"])}<br>{esc(summary["refresh_schedule"][1]["description"])}<br><span class="muted">All times shown to you in Sydney time</span></div></div>')
    html.append('<div class="card"><div class="muted">Manual refresh</div><div class="small">Reload the latest published page, or open the GitHub workflow to run a real manual refresh.</div><div class="btn-row"><button class="btn" id="reload-dashboard-btn" type="button">Reload latest page</button><a class="btn" href="https://github.com/samjozz/stock-dip-dashboard/actions/workflows/refresh-stock-dip-dashboard.yml" target="_blank" rel="noopener">Refresh now in GitHub</a></div></div>')
    html.append('</div>')
    html.append('<div class="grid">')
    html.append(f'<div class="card"><div class="muted">Total stocks</div><div class="big">{summary["row_count"]}</div></div>')
    html.append(f'<div class="card"><div class="muted">ASX 100 rows</div><div class="big">{summary["market_counts"].get("ASX 100",0)}</div></div>')
    html.append(f'<div class="card"><div class="muted">S&amp;P 500 rows</div><div class="big">{summary["market_counts"].get("S&P 500",0)}</div></div>')
    html.append(f'<div class="card"><div class="muted">Errors skipped</div><div class="big">{summary["error_count"]}</div></div>')
    html.append('</div>')

    html.append('<div class="section"><h2>All stocks</h2>')
    html.append('<table class="sortable-table"><thead><tr>')
    for col in ['Market','Ticker','Company','Sector','Current','Open vs prev close','Prev close vs prev open','Today open vs prev open','1 week','1 month','52W high drawdown']:
        html.append(f'<th>{col}</th>')
    html.append('</tr></thead><tbody>')
    for r in rows:
        market_cls = 'market-asx' if r['market'] == 'ASX 100' else 'market-us'
        html.append('<tr>')
        html.append(f'<td><span class="{market_cls}">{esc(r["market"])}</span></td>')
        html.append(f'<td><a href="{esc(r["quote_url"])}">{esc(r["display_symbol"])}</a></td>')
        html.append(f'<td>{esc(r["company"])} </td>')
        html.append(f'<td>{esc(r.get("sector",""))}</td>')
        html.append(f'<td>{format_price(r.get("current_price"), r.get("currency"))}</td>')
        for metric in ['open_vs_prev_close_pct','prev_close_vs_prev_open_pct','today_open_vs_prev_open_pct','week_change_pct','month_change_pct','drawdown_52w_pct']:
            val = r.get(metric)
            cls = 'bad' if val is not None and val < 0 else 'good' if val is not None and val > 0 else ''
            html.append(f'<td><span class="{cls}">{esc(format_pct(val))}</span></td>')
        html.append('</tr>')
    html.append('</tbody></table></div>')

    for metric_key, title, subtitle in METRIC_ORDER:
        kind = 'long' if metric_key == 'drawdown_52w_pct' else 'short'
        keep = metric_rows(rows, metric_key)
        counts = count_bucket_rows(keep, metric_key, kind=kind)
        html.append(f'<div class="section"><h2>{esc(title)}</h2><div class="muted">{esc(subtitle)}</div>')
        html.append('<div class="bucket-row">')
        for label in ['30%+ down','20–30% down','10–20% down','5–10% down','2–5% down'] if kind == 'short' else ['40%+ down','25–40% down','10–25% down']:
            html.append(f'<div class="bucket">{esc(label)}: <strong>{counts.get(label,0)}</strong></div>')
        html.append('</div>')
        html.append('<table class="sortable-table"><thead><tr>')
        for col in ['Market','Ticker','Company','Bucket','Metric','Current','Today open','Prev open','Prev close','52W high','Sector']:
            html.append(f'<th>{col}</th>')
        html.append('</tr></thead><tbody>')
        if not keep:
            html.append('<tr><td colspan="11" class="muted">No stocks currently in the highlighted drop buckets for this metric.</td></tr>')
        for r in keep:
            val = r.get(metric_key)
            bucket = bucket_label(val, kind=kind)
            pill_cls = 'pill-long' if kind == 'long' else 'pill-short'
            market_cls = 'market-asx' if r['market'] == 'ASX 100' else 'market-us'
            html.append('<tr>')
            html.append(f'<td><span class="{market_cls}">{esc(r["market"])}</span></td>')
            html.append(f'<td><a href="{esc(r["quote_url"])}">{esc(r["display_symbol"])}</a></td>')
            html.append(f'<td>{esc(r["company"])} </td>')
            html.append(f'<td><span class="pill {pill_cls}">{esc(bucket)}</span></td>')
            html.append(f'<td><span class="bad">{esc(format_pct(val))}</span></td>')
            html.append(f'<td>{format_price(r.get("current_price"), r.get("currency"))}</td>')
            html.append(f'<td>{format_price(r.get("today_open"), r.get("currency"))}</td>')
            html.append(f'<td>{format_price(r.get("prev_open"), r.get("currency"))}</td>')
            html.append(f'<td>{format_price(r.get("prev_close"), r.get("currency"))}</td>')
            html.append(f'<td>{format_price(r.get("high_52w"), r.get("currency"))}</td>')
            html.append(f'<td>{esc(r.get("sector",""))}</td>')
            html.append('</tr>')
        html.append('</tbody></table></div>')

    js = r'''<script>
    (() => {
      const refreshSchedule = JSON.parse(__SCHEDULE_JSON__);
      const upcomingRefreshes = JSON.parse(__UPCOMING_JSON__);
      const nextRefreshEl = document.getElementById('next-refresh-sydney');
      const nextRefreshLabelEl = document.getElementById('next-refresh-label');
      const reloadBtn = document.getElementById('reload-dashboard-btn');
      const now = Date.now();
      const nextItem = upcomingRefreshes.find((item) => new Date(item.utc_iso).getTime() > now - 60000) || upcomingRefreshes[0];
      if (nextItem && nextRefreshEl) {
        nextRefreshEl.textContent = nextItem.sydney_label;
        if (nextRefreshLabelEl) nextRefreshLabelEl.textContent = nextItem.name;
      }
      if (reloadBtn) {
        reloadBtn.addEventListener('click', () => {
          const u = new URL(window.location.href);
          u.searchParams.set('_ts', String(Date.now()));
          window.location.href = u.toString();
        });
      }
      const parseCell = (cell) => {
        const text = (cell.innerText || cell.textContent || '').trim();
        const numeric = text.replace(/[$,%\s,]/g, '');
        if (/^-?\d+(?:\.\d+)?$/.test(numeric)) {
          return { type: 'number', value: parseFloat(numeric) };
        }
        return { type: 'text', value: text.toLowerCase() };
      };
      document.querySelectorAll('table.sortable-table').forEach((table) => {
        const headers = Array.from(table.querySelectorAll('thead th'));
        const tbody = table.tBodies[0];
        headers.forEach((header, index) => {
          header.classList.add('sortable');
          header.addEventListener('click', () => {
            const current = header.dataset.sortDir === 'asc' ? 'asc' : (header.dataset.sortDir === 'desc' ? 'desc' : 'none');
            const next = current === 'asc' ? 'desc' : 'asc';
            headers.forEach((h) => {
              h.dataset.sortDir = '';
              h.classList.remove('sort-asc', 'sort-desc');
            });
            header.dataset.sortDir = next;
            header.classList.add(next === 'asc' ? 'sort-asc' : 'sort-desc');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            rows.sort((a, b) => {
              const av = parseCell(a.children[index] || a.lastElementChild);
              const bv = parseCell(b.children[index] || b.lastElementChild);
              let cmp = 0;
              if (av.type === 'number' && bv.type === 'number') cmp = av.value - bv.value;
              else cmp = String(av.value).localeCompare(String(bv.value));
              return next === 'asc' ? cmp : -cmp;
            });
            rows.forEach((row) => tbody.appendChild(row));
          });
        });
      });
    })();
    </script>'''
    js = js.replace('__SCHEDULE_JSON__', json.dumps(json.dumps(summary['refresh_schedule']))).replace('__UPCOMING_JSON__', json.dumps(json.dumps(summary['upcoming_refreshes'])))
    html.append(js)
    html.append('</div></body></html>')
    Path(path).write_text(''.join(html))


def main():
    sp500 = extract_sp500_constituents()
    asx100 = extract_asx100_constituents()
    constituents = asx100 + sp500

    rows = []
    errors = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_symbol_metrics, item): item for item in constituents}
        for fut in as_completed(futures):
            row = fut.result()
            if row.get('error'):
                errors.append(row)
            else:
                rows.append(row)

    rows.sort(key=lambda r: (r['market'], r['display_symbol']))
    generated_now = datetime.now(timezone.utc)
    next_items = upcoming_refreshes(generated_now, limit=20)
    summary = {
        'generated_at_utc': generated_now.strftime('%Y-%m-%d %H:%M:%S UTC'),
        'generated_at_utc_iso': generated_now.isoformat(),
        'generated_at_sydney': format_sydney(generated_now),
        'row_count': len(rows),
        'error_count': len(errors),
        'market_counts': {
            'ASX 100': sum(1 for r in rows if r['market'] == 'ASX 100'),
            'S&P 500': sum(1 for r in rows if r['market'] == 'S&P 500'),
        },
        'metric_counts': {},
        'refresh_schedule': REFRESH_SCHEDULES,
        'upcoming_refreshes': next_items,
        'next_refresh_name': next_items[0]['name'] if next_items else '',
        'next_refresh_sydney': next_items[0]['sydney_label'] if next_items else '',
        'sources': {
            'sp500': SP500_URL,
            'asx100': ASX200_URL + ' (top 100 by market cap)',
            'prices': 'Yahoo Finance chart endpoint',
        }
    }
    for metric_key, title, _subtitle in METRIC_ORDER:
        kind = 'long' if metric_key == 'drawdown_52w_pct' else 'short'
        keep = metric_rows(rows, metric_key)
        summary['metric_counts'][metric_key] = count_bucket_rows(keep, metric_key, kind=kind)

    (DATA_DIR / 'constituents.json').write_text(json.dumps(constituents, indent=2))
    (DATA_DIR / 'stocks.json').write_text(json.dumps(rows, indent=2))
    (DATA_DIR / 'errors.json').write_text(json.dumps(errors, indent=2))
    (DATA_DIR / 'summary.json').write_text(json.dumps(summary, indent=2))
    render_html(summary, rows, ROOT / 'index.html')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
