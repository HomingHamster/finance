from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import signal
import sqlite3
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from decimal import Decimal, ROUND_DOWN

import feedparser
import requests
from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm

from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest
from alpaca.trading.enums import AssetClass, AssetStatus, OrderSide, TimeInForce
from alpaca.trading.requests import GetAssetsRequest, MarketOrderRequest


load_dotenv('.env')


@dataclass
class BotConfig:
    openai_model: str = os.getenv('OPENAI_MODEL', 'gpt-5.4-nano')
    alpaca_paper: bool = os.getenv('ALPACA_PAPER', 'false').lower() == 'true'
    dry_run: bool = os.getenv('DRY_RUN', 'false').lower() == 'true'
    allow_fractional: bool = os.getenv('ALLOW_FRACTIONAL', 'true').lower() == 'true'

    feed_poll_seconds: int = int(os.getenv('FEED_POLL_SECONDS', '240'))
    rebalance_poll_seconds: int = int(os.getenv('REBALANCE_POLL_SECONDS', '120'))
    market_check_seconds: int = int(os.getenv('MARKET_CHECK_SECONDS', '20'))
    plan_refresh_with_ingest: bool = os.getenv('PLAN_REFRESH_WITH_INGEST', 'False').lower() == 'true'
    debug_loop_logging: bool = os.getenv('DEBUG_LOOP_LOGGING', 'true').lower() == 'true'
    rebalance_force_when_closed: bool = os.getenv('REBALANCE_FORCE_WHEN_CLOSED', 'false').lower() == 'true'
    per_feed_limit: int = int(os.getenv('PER_FEED_LIMIT', '15'))
    total_headline_limit: int = int(os.getenv('TOTAL_HEADLINE_LIMIT', '350'))
    scoring_batch_size: int = int(os.getenv('SCORING_BATCH_SIZE', '12'))
    history_retention_hours: int = int(os.getenv('HISTORY_RETENTION_HOURS', '72'))

    min_headline_count: int = int(os.getenv('MIN_HEADLINE_COUNT', '2'))
    min_source_count: int = int(os.getenv('MIN_SOURCE_COUNT', '1'))
    min_sentiment: float = float(os.getenv('MIN_SENTIMENT', '0.58'))
    sentiment_sell_floor: float = float(os.getenv('SENTIMENT_SELL_FLOOR', '0.50'))
    max_position_weight: float = float(os.getenv('MAX_POSITION_WEIGHT', '0.50'))
    cash_buffer_weight: float = float(os.getenv('CASH_BUFFER_WEIGHT', '0.03'))
    drift_threshold: float = float(os.getenv('DRIFT_THRESHOLD', '0.02'))
    signal_change_threshold: float = float(os.getenv('SIGNAL_CHANGE_THRESHOLD', '0.03'))
    min_trade_notional: float = float(os.getenv('MIN_TRADE_NOTIONAL', '4'))
    slippage_bps: float = float(os.getenv('SLIPPAGE_BPS', '10'))
    sell_fee_rate: float = float(os.getenv('SELL_FEE_RATE', '0.00002'))
    cooldown_minutes_buy: int = int(os.getenv('COOLDOWN_MINUTES_BUY', '120'))
    cooldown_minutes_sell: int = int(os.getenv('COOLDOWN_MINUTES_SELL', '30'))

    top_n_positions: int = int(os.getenv('TOP_N_POSITIONS', '5'))
    fallback_weight: float = float(os.getenv('FALLBACK_WEIGHT', '0.50'))
    fallback_min_positions: int = int(os.getenv('FALLBACK_MIN_POSITIONS', '3'))
    etf_fallback_ticker: str = os.getenv('ETF_FALLBACK_TICKER', 'SPY').upper()
    buying_power_reserve_pct: float = float(os.getenv('BUYING_POWER_RESERVE_PCT', '0.00'))

    decay_half_life_hours: float = float(os.getenv('DECAY_HALF_LIFE_HOURS', '6'))
    negative_sentiment_multiplier: float = float(os.getenv('NEGATIVE_SENTIMENT_MULTIPLIER', '1.25'))

    state_path: str = os.getenv('BOT_STATE_PATH', 'rebalance_state_async4.json')
    article_store_path: str = os.getenv('ARTICLE_STORE_PATH', 'article_store4.json')
    scored_headlines_path: str = os.getenv('SCORED_HEADLINES_PATH', 'financial_headlines_scored4.json')
    ticker_averages_path: str = os.getenv('TICKER_AVERAGES_PATH', 'financial_ticker_averages4.json')
    rebalance_plan_path: str = os.getenv('REBALANCE_PLAN_PATH', 'rebalance_plan4.json')
    live_ticker_state_path: str = os.getenv('LIVE_TICKER_STATE_PATH', 'live_ticker_state4.json')
    asset_db_path: str = os.getenv('ALPACA_ASSET_DB_PATH', 'alpaca_assets_2.db')
    unresolved_companies_path: str = os.getenv('UNRESOLVED_COMPANIES_PATH', 'unresolved_companies4.json')
    ticker_resolution_debug_path: str = os.getenv('TICKER_RESOLUTION_DEBUG_PATH', 'ticker_resolution_debug4.json')
    raw_positions_debug_path: str = os.getenv('RAW_POSITIONS_DEBUG_PATH', 'raw_positions_debug4.json')
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')


RSS_FEEDS = [
    {'name': 'CNBC Top News', 'url': 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114', 'weight': 0.9},
    {'name': 'CNBC Finance', 'url': 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664', 'weight': 0.88},
    {'name': 'CNBC World News', 'url': 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362', 'weight': 0.86},
    {'name': 'MarketWatch Top Stories', 'url': 'https://feeds.content.dowjones.io/public/rss/mw_topstories', 'weight': 0.9},
    {'name': 'MarketWatch MarketPulse', 'url': 'https://feeds.content.dowjones.io/public/rss/mw_marketpulse', 'weight': 0.88},
    {'name': 'MarketWatch RealTime Headlines', 'url': 'https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines', 'weight': 0.86},
    {'name': 'Yahoo Finance', 'url': 'https://finance.yahoo.com/news/rssindex', 'weight': 0.85},
    {'name': 'Investing.com General News', 'url': 'https://www.investing.com/rss/news_25.rss', 'weight': 0.82},
    {'name': 'Investing.com Stock Market', 'url': 'https://www.investing.com/rss/news_285.rss', 'weight': 0.82},
    {'name': 'Investing.com Forex', 'url': 'https://www.investing.com/rss/news_1.rss', 'weight': 0.8},
    {'name': 'Investing.com Commodities', 'url': 'https://www.investing.com/rss/news_11.rss', 'weight': 0.8},
    {'name': 'Investing.com Economy', 'url': 'https://www.investing.com/rss/news_14.rss', 'weight': 0.8},
    {'name': 'Seeking Alpha Market News', 'url': 'https://seekingalpha.com/market_currents.xml', 'weight': 0.8},
    {'name': 'Seeking Alpha Feed', 'url': 'https://seekingalpha.com/feed.xml', 'weight': 0.78},
    {'name': 'Benzinga', 'url': 'https://www.benzinga.com/feed', 'weight': 0.78},
    {'name': 'SEC Press Releases', 'url': 'https://www.sec.gov/news/pressreleases.rss', 'weight': 1.0},
    {'name': 'SEC News', 'url': 'https://www.sec.gov/rss/news/press.xml', 'weight': 0.95},
    {'name': 'SEC Litigation', 'url': 'https://www.sec.gov/enforcement-litigation/litigation-releases/rss', 'weight': 0.92},
    {'name': 'Federal Reserve Press', 'url': 'https://www.federalreserve.gov/feeds/press_all.xml', 'weight': 1.0},
    {'name': 'Federal Reserve Speeches', 'url': 'https://www.federalreserve.gov/feeds/speeches.xml', 'weight': 0.95},
    {'name': 'Federal Reserve Testimony', 'url': 'https://www.federalreserve.gov/feeds/testimony.xml', 'weight': 0.95},
    {'name': 'Federal Reserve FEDS Notes', 'url': 'https://www.federalreserve.gov/feeds/feds_notes.xml', 'weight': 0.88},
    {'name': 'IMF News', 'url': 'https://www.imf.org/en/News/RSS', 'weight': 0.88},
    {'name': 'BIS Press Releases', 'url': 'https://www.bis.org/doclist/all_pressrels.rss', 'weight': 0.9},
    {'name': 'BIS Statistics', 'url': 'https://www.bis.org/doclist/all_statistics.rss', 'weight': 0.88},
    {'name': 'BIS Central Bank Speeches', 'url': 'https://www.bis.org/doclist/cbspeeches.rss', 'weight': 0.88},
]


CONFIG = BotConfig()


class TqdmLoggingHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('openai').setLevel(logging.WARNING)
logger = logging.getLogger('async_rss_rebalance_bot')
logger.handlers.clear()
logger.setLevel(getattr(logging, CONFIG.log_level.upper(), logging.INFO))
logger.propagate = False
handler = TqdmLoggingHandler()
handler.setLevel(getattr(logging, CONFIG.log_level.upper(), logging.INFO))
handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
logger.addHandler(handler)


client = OpenAI()
trading_client: Optional[TradingClient] = None
asset_cache: Optional[List[Any]] = None
asset_lookup_by_symbol: Dict[str, Any] = {}
asset_fts_ready = False
run_lock = asyncio.Lock()
shutdown_event = asyncio.Event()


REQUEST_HEADERS = {
    'User-Agent': 'FelixRSSBot/2.0 (+https://circuspam.coffee)',
    'Accept': 'application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8',
}

COMMON_SUFFIXES = {
    'inc', 'inc.', 'corp', 'corp.', 'corporation', 'co', 'co.', 'company',
    'ltd', 'ltd.', 'limited', 'plc', 'ag', 'sa', 'nv', 'n.v.', 'spa', 's.p.a.',
    'group', 'holdings', 'holding', 'the', 'bank', 'adr', 'ads',
}

TRACKING_QUERY_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'utm_id', 'utm_name', 'utm_reader', 'utm_referrer',
    'gclid', 'fbclid', 'mc_cid', 'mc_eid', 'guccounter', '__source', 'src',
    'ncid', 'cmpid', 'ocid', 'ref', 'rss', 'rssid',
}


@dataclass
class PendingHeadline:
    article_id: str
    timestamp: str
    title: str
    source: str
    source_weight: float
    link: str


class ProgressTracker:
    def __init__(self) -> None:
        self.bar: Optional[tqdm] = None
        self.total = 0
        self.completed = 0

    def start(self, total: int, desc: str, unit: str) -> None:
        self.close()
        self.total = max(0, int(total))
        self.completed = 0
        self.bar = tqdm(total=self.total, desc=desc, unit=unit, dynamic_ncols=True, leave=False)

    def add_total(self, count: int) -> None:
        if count <= 0:
            return
        self.total += count
        if self.bar is not None:
            self.bar.total = self.total
            self.bar.refresh()

    def update(self, count: int = 1, label: str = '') -> None:
        self.completed += count
        if self.bar is not None:
            if label:
                self.bar.set_postfix_str(label, refresh=False)
            self.bar.update(count)

    def close(self) -> None:
        if self.bar is not None:
            self.bar.close()
        self.bar = None
        self.total = 0
        self.completed = 0


feed_progress = ProgressTracker()
openai_progress = ProgressTracker()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_json(path: str, default: Any) -> Any:
    file_path = Path(path)
    if not file_path.exists():
        return default
    try:
        with open(file_path, 'r', encoding='utf-8') as handle:
            return json.load(handle)
    except Exception:
        return default


def save_json(path: str, payload: Any) -> None:
    with open(path, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def append_json_list(path: str, rows: List[dict]) -> None:
    if not rows:
        return
    current = load_json(path, [])
    if not isinstance(current, list):
        current = []
    current.extend(rows)
    save_json(path, current)


def normalize_title(title: str) -> str:
    value = (title or '').strip()
    value = re.sub(r'\s+', ' ', value)
    value = value.replace('\u2019', "'").replace('\u2018', "'")
    value = value.replace('\u201c', '"').replace('\u201d', '"')
    return value


def normalized_title_key(title: str) -> str:
    norm = normalize_title(title).lower()
    norm = re.sub(r'[^a-z0-9\s]', ' ', norm)
    norm = re.sub(r'\b(update|live|exclusive|analysis|breaking|watch|opinion)\b', ' ', norm)
    norm = re.sub(r'\s+', ' ', norm).strip()
    return norm


def canonicalize_link(link: str) -> str:
    raw = (link or '').strip()
    if not raw:
        return ''
    try:
        parts = urlsplit(raw)
        query_pairs = parse_qsl(parts.query, keep_blank_values=True)
        filtered_pairs = [(k, v) for k, v in query_pairs if k.lower() not in TRACKING_QUERY_PARAMS]
        clean_query = urlencode(filtered_pairs, doseq=True)
        clean = urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path or '', clean_query, ''))
        return clean.rstrip('/').lower()
    except Exception:
        value = raw.lower()
        value = re.sub(r'#.*$', '', value)
        value = re.sub(r'[?&]+$', '', value)
        return value


def article_id_for_entry(entry: Any, source: str, title: str, link: str) -> str:
    raw_id = str(entry.get('id') or entry.get('guid') or '').strip().lower()
    clean_link = canonicalize_link(link)
    norm_title = normalized_title_key(title)
    clean_source = source.strip().lower()
    if raw_id and raw_id not in {'none', 'null'}:
        stable = f'{clean_source}|id|{raw_id}'
    elif clean_link:
        stable = f'{clean_source}|link|{clean_link}'
    else:
        stable = f'{clean_source}|title|{norm_title}'
    return hashlib.sha256(stable.encode('utf-8')).hexdigest()


def normalize_company_name(name: str) -> str:
    value = (name or '').lower().strip()
    value = value.replace('&', ' and ')
    value = re.sub(r'[\u2018\u2019\u201c\u201d]', ' ', value)
    value = re.sub(r'[^a-z0-9\s]', ' ', value)
    tokens = [token for token in value.split() if token and token not in COMMON_SUFFIXES]
    return ' '.join(tokens)


def is_valid_symbol(symbol: Any) -> bool:
    if symbol is None:
        return False
    clean = str(symbol).strip().upper()
    if not clean or clean in {'NULL', 'NONE', 'NAN'}:
        return False
    return bool(re.fullmatch(r'[A-Z][A-Z0-9\.\-]{0,9}', clean))


def sanitize_symbol(symbol: Any) -> Optional[str]:
    clean = str(symbol).strip().upper() if symbol is not None else ''
    return clean if is_valid_symbol(clean) else None


def normalize_position_side(side: Any) -> str:
    if side is None:
        return ''
    if hasattr(side, 'value'):
        return str(side.value).strip().lower()
    value = str(side).strip().lower()
    if value.endswith('.long'):
        return 'long'
    if value.endswith('.short'):
        return 'short'
    return value


def parse_entry_time(entry: Any) -> str:
    for field in ('published', 'updated', 'created'):
        raw = entry.get(field)
        if not raw:
            continue
        try:
            dt = parsedate_to_datetime(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return to_iso(dt)
        except Exception:
            continue
    return to_iso(utc_now())


def age_hours(timestamp: str, now: Optional[datetime] = None) -> float:
    now_dt = now or utc_now()
    age_seconds = max(0.0, (now_dt - parse_iso(timestamp)).total_seconds())
    return age_seconds / 3600.0


def decay_weight(timestamp: str, now: Optional[datetime] = None) -> float:
    current = now or utc_now()
    hours = age_hours(timestamp, current)
    half_life = max(0.5, CONFIG.decay_half_life_hours)
    return math.exp(-math.log(2.0) * hours / half_life)


def polarity_weight(sentiment: float) -> float:
    return CONFIG.negative_sentiment_multiplier if sentiment < 0.5 else 1.0


def ensure_article_store() -> Dict[str, Any]:
    store = load_json(CONFIG.article_store_path, {'articles': {}, 'last_ingest_at': None})
    if not isinstance(store, dict):
        store = {'articles': {}, 'last_ingest_at': None}
    store.setdefault('articles', {})
    store.setdefault('last_ingest_at', None)
    return store


def load_state() -> Dict[str, Any]:
    state = load_json(CONFIG.state_path, {
        'last_trade_times': {},
        'last_signal_scores': {},
        'last_target_weights': {},
        'last_run': None,
    })
    if not isinstance(state, dict):
        state = {}
    state.setdefault('last_trade_times', {})
    state.setdefault('last_signal_scores', {})
    state.setdefault('last_target_weights', {})
    state.setdefault('last_run', None)
    return state


def save_state(state: Dict[str, Any]) -> None:
    save_json(CONFIG.state_path, state)


def clone_signal_scores(state: Dict[str, Any]) -> Dict[str, float]:
    raw = state.get('last_signal_scores', {}) or {}
    snapshot: Dict[str, float] = {}
    for symbol, value in raw.items():
        clean = sanitize_symbol(symbol)
        if not clean:
            continue
        try:
            snapshot[clean] = float(value)
        except Exception:
            continue
    return snapshot


def snapshot_signal_state(rows: List[dict]) -> Dict[str, float]:
    snapshot: Dict[str, float] = {}
    for row in rows:
        symbol = sanitize_symbol(row.get('ticker'))
        if not symbol:
            continue
        try:
            snapshot[symbol] = float(row.get('decayed_sentiment', 0.5))
        except Exception:
            snapshot[symbol] = 0.5
    return snapshot


def get_trading_client() -> TradingClient:
    global trading_client
    if trading_client is None:
        api_key = os.getenv('ALPACA_API_KEY')
        secret_key = os.getenv('ALPACA_SECRET_KEY')
        if not api_key or not secret_key:
            raise RuntimeError('Missing ALPACA_API_KEY or ALPACA_SECRET_KEY in environment.')
        trading_client = TradingClient(api_key=api_key, secret_key=secret_key, paper=CONFIG.alpaca_paper)
    return trading_client


def get_all_assets_cached() -> List[Any]:
    global asset_cache
    if asset_cache is None:
        params = GetAssetsRequest(status=AssetStatus.ACTIVE, asset_class=AssetClass.US_EQUITY)
        asset_cache = get_trading_client().get_all_assets(params)
    return asset_cache


def get_asset_db() -> sqlite3.Connection:
    connection = sqlite3.connect(CONFIG.asset_db_path)
    connection.row_factory = sqlite3.Row
    return connection


def rebuild_asset_search_index() -> None:
    global asset_fts_ready, asset_lookup_by_symbol
    assets = get_all_assets_cached()
    asset_lookup_by_symbol = {
        str(getattr(asset, 'symbol', '')).upper(): asset
        for asset in assets
        if getattr(asset, 'symbol', None)
    }

    connection = get_asset_db()
    cursor = connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS asset_meta')
    cursor.execute('DROP TABLE IF EXISTS asset_fts')
    cursor.execute(
        'CREATE TABLE asset_meta ('
        'symbol TEXT PRIMARY KEY, '
        'name TEXT NOT NULL, '
        'normalized_name TEXT NOT NULL, '
        'exchange TEXT, '
        'tradable INTEGER NOT NULL, '
        'fractionable INTEGER NOT NULL, '
        'status TEXT'
        ')'
    )
    cursor.execute(
        "CREATE VIRTUAL TABLE asset_fts USING fts5("
        "symbol, name, normalized_name, content='asset_meta', content_rowid='rowid'"
        ')'
    )

    rows = []
    for asset in assets:
        symbol = str(getattr(asset, 'symbol', '') or '').upper().strip()
        name = str(getattr(asset, 'name', '') or '').strip()
        if not symbol or not name:
            continue
        rows.append((
            symbol,
            name,
            normalize_company_name(name),
            str(getattr(asset, 'exchange', '') or ''),
            1 if bool(getattr(asset, 'tradable', False)) else 0,
            1 if bool(getattr(asset, 'fractionable', False)) else 0,
            str(getattr(asset, 'status', '') or ''),
        ))

    cursor.executemany(
        'INSERT INTO asset_meta(symbol, name, normalized_name, exchange, tradable, fractionable, status) VALUES (?, ?, ?, ?, ?, ?, ?)',
        rows,
    )
    cursor.execute('INSERT INTO asset_fts(rowid, symbol, name, normalized_name) SELECT rowid, symbol, name, normalized_name FROM asset_meta')
    connection.commit()
    connection.close()
    asset_fts_ready = True


def ensure_asset_search_index() -> None:
    global asset_fts_ready
    if asset_fts_ready and Path(CONFIG.asset_db_path).exists():
        return
    rebuild_asset_search_index()


def search_assets_fts(search_string: str, limit: int = 25) -> List[Any]:
    ensure_asset_search_index()
    normalized = normalize_company_name(search_string)
    if not normalized:
        return []
    tokens = [token for token in normalized.split() if token]
    if not tokens:
        return []
    query = ' '.join(f'{token}*' for token in tokens)
    connection = get_asset_db()
    cursor = connection.cursor()
    cursor.execute(
        'SELECT m.symbol '
        'FROM asset_fts f '
        'JOIN asset_meta m ON m.rowid = f.rowid '
        'WHERE asset_fts MATCH ? '
        'ORDER BY bm25(asset_fts), m.tradable DESC, m.fractionable DESC '
        'LIMIT ?',
        (query, limit),
    )
    symbols = [row['symbol'] for row in cursor.fetchall()]
    connection.close()
    return [asset_lookup_by_symbol[symbol] for symbol in symbols if symbol in asset_lookup_by_symbol]


def get_ticker_by_company_name(search_string: str) -> List[Any]:
    search_lower = search_string.lower().strip()
    if not search_lower:
        return []
    results: List[Any] = []
    seen: set[str] = set()
    for asset in search_assets_fts(search_string, limit=25):
        symbol = str(getattr(asset, 'symbol', '') or '').upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            results.append(asset)
    for asset in get_all_assets_cached():
        asset_name = (getattr(asset, 'name', '') or '').lower()
        tradable = bool(getattr(asset, 'tradable', False))
        status = str(getattr(asset, 'status', '')).lower()
        symbol = str(getattr(asset, 'symbol', '') or '').upper()
        if search_lower in asset_name and tradable and status == 'active' and symbol not in seen:
            seen.add(symbol)
            results.append(asset)
    return results


def build_financial_messages(title: str) -> List[Dict[str, str]]:
    system_text = (
        'You are an expert financial analyst. Analyze the provided news headline. '
        'Identify all companies mentioned. Rate the market sentiment toward those companies '
        'on a strict scale from 0 (extremely negative/bearish) to 1 (extremely positive/bullish). '
        'If the headline is macroeconomic and mentions no specific company, return an empty company list '
        'and a neutral score unless there is clear company-specific impact. Return valid JSON only.'
    )
    schema_hint = {'companies_mentioned': ['Company A', 'Company B'], 'sentiment_score_0_to_1': 0.5}
    user_text = (
        f'Headline to analyze: {title}\n\n'
        'Return JSON in exactly this shape:\n'
        f'{json.dumps(schema_hint)}'
    )
    return [
        {'role': 'system', 'content': system_text},
        {'role': 'user', 'content': user_text},
    ]


def match_ticker_with_ai(headline: str, alpaca_matches: List[Any], model: str) -> Optional[str]:
    if not alpaca_matches:
        openai_progress.update(1, 'ticker-skip')
        return None
    candidate_lines = []
    for index, asset in enumerate(alpaca_matches[:30]):
        candidate_lines.append(
            f'Choice {index}: Symbol: {getattr(asset, "symbol", None)} | '
            f'Name: {getattr(asset, "name", None)} | '
            f'Exchange: {getattr(asset, "exchange", None)} | '
            f'Tradable: {getattr(asset, "tradable", None)}'
        )
    system_text = (
        'You are an expert financial data parser. Read the headline and analyze the list of candidate '
        'stock assets provided. Pick the SINGLE best ticker symbol that matches the headline context. '
        'Prioritize assets where tradable is True. Pick standard common stock over preferred stock or OTC assets '
        'unless the headline specifically targets them. If no candidates match the headline context, return null. '
        'Return valid JSON only.'
    )
    schema_hint = {'selected_symbol': 'TICKER_HERE', 'confidence_score_0_to_1': 0.95}
    user_text = (
        f'Headline: {headline}\n\n'
        f'Candidate Options from Alpaca:\n' + '\n'.join(candidate_lines) + '\n\n'
        'Return JSON in exactly this shape:\n'
        f'{json.dumps(schema_hint)}'
    )
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {'role': 'system', 'content': system_text},
                {'role': 'user', 'content': user_text},
            ],
            response_format={'type': 'json_object'},
            temperature=0,
        )
        openai_progress.update(1, 'ticker')
        parsed_json = json.loads(response.choices[0].message.content)
        return sanitize_symbol(parsed_json.get('selected_symbol'))
    except Exception as exc:
        openai_progress.update(1, 'ticker-error')
        logger.warning('AI ticker selection failed: %s', exc)
        for asset in alpaca_matches:
            if getattr(asset, 'tradable', False):
                return getattr(asset, 'symbol', None)
        return getattr(alpaca_matches[0], 'symbol', None) if alpaca_matches else None


def score_and_attach_headline(pending: PendingHeadline) -> Dict[str, Any]:
    try:
        response = client.chat.completions.create(
            model=CONFIG.openai_model,
            messages=build_financial_messages(pending.title),
            response_format={'type': 'json_object'},
            temperature=0,
        )
        openai_progress.update(1, 'headline')
        payload = json.loads(response.choices[0].message.content)
        companies = payload.get('companies_mentioned', [])
        sentiment = float(payload.get('sentiment_score_0_to_1', 0.5))
        sentiment = max(0.0, min(1.0, sentiment))
    except Exception as exc:
        openai_progress.update(1, 'headline-error')
        logger.warning("Headline scoring failed for '%s': %s", pending.title, exc)
        companies = []
        sentiment = 0.5

    ensure_asset_search_index()
    ticker_map: Dict[str, str] = {}
    unresolved_rows: List[dict] = []
    debug_rows: List[dict] = []
    openai_progress.add_total(len(companies))

    for company in companies:
        candidates = get_ticker_by_company_name(company)
        debug_rows.append({
            'timestamp': pending.timestamp,
            'source': pending.source,
            'title': pending.title,
            'company': company,
            'candidate_count': len(candidates),
            'candidates': [
                {
                    'symbol': getattr(candidate, 'symbol', None),
                    'name': getattr(candidate, 'name', None),
                    'exchange': str(getattr(candidate, 'exchange', None)),
                    'tradable': bool(getattr(candidate, 'tradable', False)),
                    'fractionable': bool(getattr(candidate, 'fractionable', False)),
                }
                for candidate in candidates[:10]
            ],
        })
        final_ticker = match_ticker_with_ai(pending.title, candidates, model=CONFIG.openai_model)
        if final_ticker:
            ticker_map[company] = final_ticker
        else:
            unresolved_rows.append({
                'timestamp': pending.timestamp,
                'source': pending.source,
                'title': pending.title,
                'company': company,
            })

    append_json_list(CONFIG.unresolved_companies_path, unresolved_rows)
    append_json_list(CONFIG.ticker_resolution_debug_path, debug_rows)

    now_iso = to_iso(utc_now())
    return {
        'article_id': pending.article_id,
        'timestamp': pending.timestamp,
        'title': pending.title,
        'source': pending.source,
        'weight': pending.source_weight,
        'link': pending.link,
        'companies': companies,
        'sentiment': sentiment,
        'company_ticker_mapping': ticker_map,
        'scored_at': now_iso,
        'last_seen_at': now_iso,
    }


def fetch_feed_entries(feed: dict, timeout: int = 10) -> List[Any]:
    try:
        response = requests.get(feed['url'], headers=REQUEST_HEADERS, timeout=timeout)
        response.raise_for_status()
        parsed = feedparser.parse(BytesIO(response.content))
        if getattr(parsed, 'bozo', 0):
            logger.warning('Feed parse warning for %s: %s', feed['name'], getattr(parsed, 'bozo_exception', None))
        return getattr(parsed, 'entries', []) or []
    except requests.Timeout:
        logger.warning('Feed timeout: %s', feed['name'])
        return []
    except requests.RequestException as exc:
        logger.warning('Feed HTTP failure: %s | %s', feed['name'], exc)
        return []
    except Exception as exc:
        logger.warning('Feed parse failure: %s | %s', feed['name'], exc)
        return []


def collect_latest_feed_items() -> List[PendingHeadline]:
    items: List[PendingHeadline] = []
    seen_ids: set[str] = set()
    feed_progress.start(len(RSS_FEEDS), desc='Parsing feeds', unit='feed')
    try:
        for feed in RSS_FEEDS:
            entries = fetch_feed_entries(feed, timeout=10)
            feed_progress.update(1, feed['name'])
            for entry in entries[: CONFIG.per_feed_limit]:
                title = normalize_title(entry.get('title', ''))
                if not title:
                    continue
                timestamp = parse_entry_time(entry)
                link = entry.get('link', '')
                article_id = article_id_for_entry(entry, feed['name'], title, link)
                if article_id in seen_ids:
                    continue
                seen_ids.add(article_id)
                items.append(PendingHeadline(
                    article_id=article_id,
                    timestamp=timestamp,
                    title=title,
                    source=feed['name'],
                    source_weight=float(feed['weight']),
                    link=link,
                ))
    finally:
        feed_progress.close()
    items.sort(key=lambda item: item.timestamp, reverse=True)
    return items[: CONFIG.total_headline_limit]


def prune_article_store(store: Dict[str, Any]) -> None:
    articles = store.get('articles', {})
    now = utc_now()
    retention = timedelta(hours=CONFIG.history_retention_hours)
    keep: Dict[str, Any] = {}
    for article_id, article in articles.items():
        timestamp = article.get('timestamp')
        if not timestamp:
            continue
        try:
            if now - parse_iso(timestamp) <= retention:
                keep[article_id] = article
        except Exception:
            continue
    store['articles'] = keep


def live_articles(store: Dict[str, Any]) -> List[Dict[str, Any]]:
    articles = list(store.get('articles', {}).values())
    articles.sort(key=lambda row: row.get('timestamp', ''), reverse=True)
    return articles


def ingest_new_headlines_once() -> Dict[str, Any]:
    store = ensure_article_store()
    current_articles = store['articles']
    candidates = collect_latest_feed_items()
    new_items: List[PendingHeadline] = []
    touched = 0
    now_iso = to_iso(utc_now())

    for item in candidates:
        existing = current_articles.get(item.article_id)
        if existing:
            existing['last_seen_at'] = now_iso
            touched += 1
            continue
        new_items.append(item)

    try:
        openai_progress.start(len(new_items), desc='OpenAI requests', unit='req')
        for start in range(0, len(new_items), CONFIG.scoring_batch_size):
            batch = new_items[start:start + CONFIG.scoring_batch_size]
            for item in batch:
                current_articles[item.article_id] = score_and_attach_headline(item)
    finally:
        openai_progress.close()

    prune_article_store(store)
    store['last_ingest_at'] = now_iso
    articles = live_articles(store)
    save_json(CONFIG.article_store_path, store)
    save_json(CONFIG.scored_headlines_path, articles)
    return {
        'new_articles': len(new_items),
        'touched_articles': touched,
        'total_cached_articles': len(store['articles']),
    }


def get_asset_by_symbol(symbol: str) -> Optional[Any]:
    if not symbol:
        return None
    clean = symbol.upper().strip()
    return asset_lookup_by_symbol.get(clean) or next((asset for asset in get_all_assets_cached() if getattr(asset, 'symbol', None) == clean), None)


def asset_is_active(asset: Any) -> bool:
    status = getattr(asset, 'status', None)
    return (
        status == AssetStatus.ACTIVE
        or (isinstance(status, str) and status.lower() == 'active')
        or (hasattr(status, 'value') and str(status.value).lower() == 'active')
    )


def is_asset_tradeable_for_target(symbol: str) -> bool:
    asset = get_asset_by_symbol(symbol)
    if asset is None:
        return False
    if not bool(getattr(asset, 'tradable', False)):
        return False
    return asset_is_active(asset)


def can_submit_sell(symbol: str, positions: Dict[str, dict]) -> Tuple[bool, str]:
    position = positions.get(symbol)
    if not position:
        return False, 'no_position'
    if position.get('qty', 0.0) <= 0 or position.get('side') != 'long':
        return False, 'no_long_inventory'
    asset = get_asset_by_symbol(symbol)
    if asset is None:
        return False, 'asset_not_found'
    if not bool(getattr(asset, 'tradable', False)):
        return False, 'asset_not_tradable'
    if not asset_is_active(asset):
        return False, 'asset_not_active'
    return True, 'ok'


#def get_latest_trade_price(symbol: str) -> Optional[float]:
#    try:
#        trade = get_trading_client().get_latest_trade(symbol)
#        price = float(getattr(trade, 'price', None) or 0.0)
#        return price if price > 0 else None
#    except Exception as exc:
#        logger.warning("latest trade lookup failed for %s: %r", symbol, exc)
#        return None

stock_data_client: Optional[StockHistoricalDataClient] = None

def get_stock_data_client() -> StockHistoricalDataClient:
    global stock_data_client
    if stock_data_client is None:
        api_key = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_SECRET_KEY")
        if not api_key or not secret_key:
            raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY in environment.")
        stock_data_client = StockHistoricalDataClient(api_key, secret_key)
    return stock_data_client

def get_latest_trade_price(symbol: str) -> Optional[float]:
    try:
        request = StockLatestTradeRequest(symbol_or_symbols=symbol)
        trade_map = get_stock_data_client().get_stock_latest_trade(request)
        trade = trade_map.get(symbol)
        price = float(getattr(trade, "price", 0) or 0)
        return price if price > 0 else None
    except Exception as exc:
        logger.warning("latest trade lookup failed for %s: %r", symbol, exc)
        return None

def get_account_snapshot() -> Tuple[dict, Dict[str, dict]]:
    client_instance = get_trading_client()
    account = client_instance.get_account()
    positions = client_instance.get_all_positions()
    raw_debug: List[dict] = []
    position_map: Dict[str, dict] = {}
    for position in positions:
        raw_symbol = getattr(position, 'symbol', None)
        symbol = sanitize_symbol(raw_symbol)
        entry = {
            'raw_symbol': raw_symbol,
            'symbol': symbol,
            'market_value': float(getattr(position, 'market_value', 0) or 0),
            'qty': float(getattr(position, 'qty', 0) or 0),
            'side': normalize_position_side(getattr(position, 'side', '')),
            'avg_entry_price': float(getattr(position, 'avg_entry_price', 0) or 0),
            'asset_marginable': bool(getattr(position, 'asset_marginable', False)) if hasattr(position, 'asset_marginable') else None,
        }
        raw_debug.append(entry)
        if not symbol:
            continue
        position_map[symbol] = {
            'market_value': abs(entry['market_value']),
            'qty': entry['qty'],
            'side': entry['side'],
            'avg_entry_price': entry['avg_entry_price'],
        }
    save_json(CONFIG.raw_positions_debug_path, raw_debug)
    snapshot = {
        'equity': float(account.equity),
        'buying_power': float(account.buying_power),
        'cash': float(account.cash),
        'trading_blocked': bool(account.trading_blocked),
    }
    return snapshot, position_map


def current_weights(equity: float, positions: Dict[str, dict]) -> Dict[str, float]:
    if equity <= 0:
        return {}
    weights: Dict[str, float] = {}
    for symbol, data in positions.items():
        clean = sanitize_symbol(symbol)
        if not clean:
            continue
        if float(data.get('market_value', 0)) <= 0:
            continue
        if normalize_position_side(data.get('side', '')) != 'long':
            continue
        weights[clean] = float(data['market_value']) / equity
    return weights


def estimate_trade_cost(notional: float, side: str) -> float:
    slippage = notional * (CONFIG.slippage_bps / 10000.0)
    sell_fee = notional * CONFIG.sell_fee_rate if side.lower() == 'sell' else 0.0
    return slippage + sell_fee


def validate_portfolio_state(snapshot: dict, positions: Dict[str, dict], current_w: Dict[str, float]) -> List[dict]:
    issues: List[dict] = []
    if snapshot.get('equity', 0) > 0 and snapshot.get('cash', 0) < 0 and not positions:
        issues.append({'reason': 'negative_cash_but_no_positions_visible'})
    if snapshot.get('equity', 0) > 0 and snapshot.get('cash', 0) < 0 and not current_w:
        issues.append({'reason': 'negative_cash_but_no_current_weights'})
    return issues


def symbol_in_cooldown(symbol: str, side: str, state: Dict[str, Any]) -> bool:
    last_trade = state.get('last_trade_times', {}).get(symbol)
    if not last_trade:
        return False
    try:
        last_dt = parse_iso(last_trade)
    except Exception:
        return False
    cooldown_minutes = CONFIG.cooldown_minutes_sell if side == 'sell' else CONFIG.cooldown_minutes_buy
    return utc_now() < last_dt + timedelta(minutes=cooldown_minutes)


def aggregate_live_ticker_state() -> List[dict]:
    store = ensure_article_store()
    articles = live_articles(store)
    now = utc_now()
    buckets: Dict[str, dict] = defaultdict(lambda: {
        'ticker': None,
        'headline_count': 0,
        'source_count': 0,
        'sources': set(),
        'companies_seen': set(),
        'net_sentiment_sum': 0.0,
        'effective_weight_total': 0.0,
        'positive_mass': 0.0,
        'negative_mass': 0.0,
        'effective_headline_mass': 0.0,
        'headlines': [],
    })

    for article in articles:
        mapping = article.get('company_ticker_mapping', {}) or {}
        if not mapping:
            continue
        timestamp = article.get('timestamp')
        sentiment = float(article.get('sentiment', 0.5))
        source_weight = float(article.get('weight', 1.0))
        freshness = decay_weight(timestamp, now)
        polarity = polarity_weight(sentiment)
        effective_weight = source_weight * freshness * polarity
        signed_sentiment = sentiment - 0.5

        for company, ticker in mapping.items():
            clean_ticker = sanitize_symbol(ticker)
            if not clean_ticker or not is_asset_tradeable_for_target(clean_ticker):
                continue
            row = buckets[clean_ticker]
            row['ticker'] = clean_ticker
            row['headline_count'] += 1
            row['sources'].add(article.get('source', ''))
            row['companies_seen'].add(company)
            row['net_sentiment_sum'] += signed_sentiment * effective_weight
            row['effective_weight_total'] += effective_weight
            row['effective_headline_mass'] += freshness
            if signed_sentiment >= 0:
                row['positive_mass'] += signed_sentiment * effective_weight
            else:
                row['negative_mass'] += abs(signed_sentiment) * effective_weight
            row['headlines'].append({
                'timestamp': timestamp,
                'source': article.get('source'),
                'title': article.get('title'),
                'sentiment': sentiment,
                'weight': source_weight,
                'freshness_weight': round(freshness, 6),
                'effective_weight': round(effective_weight, 6),
                'link': article.get('link'),
                'company': company,
            })

    results: List[dict] = []
    for row in buckets.values():
        row['source_count'] = len(row['sources'])
        row['sources'] = sorted(value for value in row['sources'] if value)
        row['companies_seen'] = sorted(row['companies_seen'])
        signed_score = row['net_sentiment_sum'] / row['effective_weight_total'] if row['effective_weight_total'] > 0 else 0.0
        decayed_sentiment = max(0.0, min(1.0, 0.5 + signed_score))
        row['decayed_sentiment'] = round(decayed_sentiment, 6)
        row['signal_strength'] = round(max(0.0, decayed_sentiment - CONFIG.min_sentiment), 6)
        row['negative_pressure'] = round(row['negative_mass'], 6)
        row['positive_pressure'] = round(row['positive_mass'], 6)
        row['effective_headline_mass'] = round(row['effective_headline_mass'], 6)
        row['is_investable'] = (
            row['headline_count'] >= CONFIG.min_headline_count
            and row['source_count'] >= CONFIG.min_source_count
            and decayed_sentiment >= CONFIG.min_sentiment
        )
        del row['net_sentiment_sum']
        del row['effective_weight_total']
        results.append(row)

    results.sort(
        key=lambda item: (
            item['is_investable'],
            item['decayed_sentiment'],
            item['effective_headline_mass'],
            -item['negative_pressure'],
        ),
        reverse=True,
    )
    return results


def conviction_score(row: dict) -> float:
    sentiment = float(row.get('decayed_sentiment', 0.5))
    headline_mass = float(row.get('effective_headline_mass', 0.0))
    sources = int(row.get('source_count', 0))
    if int(row.get('headline_count', 0)) < CONFIG.min_headline_count:
        return 0.0
    if sources < CONFIG.min_source_count:
        return 0.0
    if sentiment <= CONFIG.min_sentiment:
        return 0.0
    edge = sentiment - CONFIG.min_sentiment
    evidence = math.log1p(max(0.0, headline_mass)) * (1.0 + 0.15 * max(0, sources - 1))
    penalty = 1.0 / (1.0 + float(row.get('negative_pressure', 0.0)))
    return edge * evidence * penalty


def can_meet_min_notional(symbol: str, weight: float, equity: float) -> bool:
    asset = get_asset_by_symbol(symbol)
    if asset is None:
        return False
    planned_notional = weight * equity
    fractionable = bool(getattr(asset, 'fractionable', False))
    if fractionable and CONFIG.allow_fractional:
        return planned_notional >= CONFIG.min_trade_notional
    reference_price = get_latest_trade_price(symbol)
    if not reference_price or reference_price <= 0:
        return False
    return planned_notional >= max(CONFIG.min_trade_notional, reference_price)


def build_target_weights(rows: List[dict], equity: float) -> Tuple[Dict[str, float], List[dict]]:
    investable_rows = [row for row in rows if row.get('is_investable')]
    ranked_rows = sorted(
        investable_rows,
        key=lambda row: (
            float(row.get('decayed_sentiment', 0.0)),
            float(row.get('effective_headline_mass', 0.0)),
            -float(row.get('negative_pressure', 0.0)),
        ),
        reverse=True,
    )

    chosen: List[Tuple[str, float]] = []
    skipped: List[dict] = []
    investable_weight = max(0.0, 1.0 - CONFIG.cash_buffer_weight)

    for row in ranked_rows:
        symbol = sanitize_symbol(row.get('ticker'))
        if not symbol:
            skipped.append({'symbol': row.get('ticker'), 'reason': 'invalidsymbol'})
            continue
        score = conviction_score(row)
        if score <= 0:
            skipped.append({'symbol': symbol, 'reason': 'nonpositivescore'})
            continue
        trial = chosen + [(symbol, score)]
        total_score = sum(s for _, s in trial)
        trial_weights = {sym: min(CONFIG.max_position_weight, (s / total_score) * investable_weight) for sym, s in trial}
        total_alloc = sum(trial_weights.values())
        if total_alloc > 0:
            scale = min(1.0, investable_weight / total_alloc)
            trial_weights = {sym: w * scale for sym, w in trial_weights.items()}
        if not can_meet_min_notional(symbol, trial_weights[symbol], equity):
            skipped.append({
                'symbol': symbol,
                'reason': 'belowmintradenotionalafterweighting',
                'plannednotional': round(trial_weights[symbol] * equity, 2),
            })
            continue
        chosen = trial
        if len(chosen) >= CONFIG.top_n_positions:
            break

    if not chosen:
        return {CONFIG.etf_fallback_ticker: min(CONFIG.fallback_weight, investable_weight)}, skipped

    total_score = sum(s for _, s in chosen)
    target_weights = {sym: min(CONFIG.max_position_weight, (s / total_score) * investable_weight) for sym, s in chosen}
    total_alloc = sum(target_weights.values())
    if total_alloc > 0:
        scale = min(1.0, investable_weight / total_alloc)
        target_weights = {sym: round(w * scale, 6) for sym, w in target_weights.items()}

    if len([w for w in target_weights.values() if w > 0]) < CONFIG.fallback_min_positions:
        remaining = investable_weight - sum(target_weights.values())
        if remaining > 0:
            target_weights[CONFIG.etf_fallback_ticker] = round(
                target_weights.get(CONFIG.etf_fallback_ticker, 0.0) + min(CONFIG.fallback_weight, remaining),
                6,
            )
    return target_weights, skipped


def filter_and_reweight_target_weights(equity: float, target_weights: Dict[str, float]) -> Tuple[Dict[str, float], List[dict]]:
    filtered: Dict[str, float] = {}
    skipped: List[dict] = []
    if equity <= 0:
        return {}, [{'reason': 'non_positive_equity'}]
    kept_weight_total = 0.0
    original_total = sum(float(w) for w in target_weights.values() if float(w) > 0)
    if original_total <= 0:
        return {}, skipped

    for symbol, weight in target_weights.items():
        clean = sanitize_symbol(symbol)
        if not clean or weight <= 0:
            skipped.append({'symbol': symbol, 'reason': 'invalid_or_non_positive_weight'})
            continue
        asset = get_asset_by_symbol(clean)
        if asset is None or not is_asset_tradeable_for_target(clean):
            skipped.append({'symbol': clean, 'reason': 'asset_not_tradable_for_target'})
            continue
        fractionable = bool(getattr(asset, 'fractionable', False))
        planned_notional = weight * equity
        if fractionable and CONFIG.allow_fractional:
            if planned_notional < CONFIG.min_trade_notional:
                skipped.append({'symbol': clean, 'reason': 'below_min_trade_notional_after_weighting', 'planned_notional': round(planned_notional, 2)})
                continue
            filtered[clean] = weight
            kept_weight_total += weight
            continue
        reference_price = get_latest_trade_price(clean)
        if not reference_price or reference_price <= 0:
            skipped.append({'symbol': clean, 'reason': 'no_price_for_non_fractionable_target'})
            continue
        if planned_notional < max(CONFIG.min_trade_notional, reference_price):
            skipped.append({
                'symbol': clean,
                'reason': 'below_one_share_notional',
                'planned_notional': round(planned_notional, 2),
                'reference_price': round(reference_price, 2),
            })
            continue
        filtered[clean] = weight
        kept_weight_total += weight

    if not filtered or kept_weight_total <= 0:
        return {}, skipped
    scale = original_total / kept_weight_total
    reweighted = {symbol: round(weight * scale, 6) for symbol, weight in filtered.items()}
    total_reweighted = sum(reweighted.values())
    if total_reweighted > 0:
        correction = original_total / total_reweighted
        reweighted = {symbol: round(weight * correction, 6) for symbol, weight in reweighted.items()}
    return reweighted, skipped


def build_rebalance_plan(
    snapshot: dict,
    positions: Dict[str, dict],
    current_w: Dict[str, float],
    target_weights: Dict[str, float],
    state: Dict[str, Any],
    live_signal_rows: List[dict],
    filtered_target_skips: List[dict],
    previous_signal_scores: Dict[str, float],
) -> Dict[str, Any]:
    equity = snapshot['equity']
    sells: List[dict] = []
    buys: List[dict] = []
    skipped: List[dict] = list(filtered_target_skips)
    signal_lookup = {row['ticker']: row for row in live_signal_rows if row.get('ticker')}
    all_symbols = set(current_w) | set(target_weights)

    for symbol in sorted(all_symbols):
        current_weight = float(current_w.get(symbol, 0.0))
        target_weight = float(target_weights.get(symbol, 0.0))
        drift = target_weight - current_weight
        signal_row = signal_lookup.get(symbol, {})
        live_sentiment = float(signal_row.get('decayed_sentiment', 0.5))

        previous_sentiment = float((previous_signal_scores or {}).get(symbol, live_sentiment))
        signal_delta = live_sentiment - previous_sentiment
        should_sell_for_signal = current_weight > 0 and live_sentiment <= CONFIG.sentiment_sell_floor
        should_trade_for_drift = abs(drift) >= CONFIG.drift_threshold
        should_trade_for_signal_change = abs(signal_delta) >= CONFIG.signal_change_threshold

        if not (should_sell_for_signal or should_trade_for_drift or should_trade_for_signal_change):
            continue

        side = 'buy' if drift > 0 and not should_sell_for_signal else 'sell'
        if side == 'buy' and target_weight <= current_weight:
            continue
        if side == 'sell' and current_weight <= 0:
            continue
        if symbol_in_cooldown(symbol, side, state):
            skipped.append({
                'symbol': symbol,
                'reason': 'cooldown',
                'side': side,
                'current_weight': round(current_weight, 6),
                'target_weight': round(target_weight, 6),
                'live_sentiment': round(live_sentiment, 6),
                'signal_delta': round(signal_delta, 6),
            })
            continue

        raw_notional = current_weight * equity if should_sell_for_signal else abs(drift) * equity
        if raw_notional < CONFIG.min_trade_notional:
            skipped.append({'symbol': symbol, 'reason': 'below_min_notional', 'raw_notional': round(raw_notional, 2)})
            continue

        if side == 'sell':
            ok, reason = can_submit_sell(symbol, positions)
            if not ok:
                skipped.append({'symbol': symbol, 'reason': reason, 'current_weight': round(current_weight, 6), 'target_weight': round(target_weight, 6)})
                continue

        estimated_cost = estimate_trade_cost(raw_notional, side)
        adjusted_notional = max(0.0, raw_notional - estimated_cost)
        if adjusted_notional < CONFIG.min_trade_notional:
            skipped.append({'symbol': symbol, 'reason': 'below_min_after_cost', 'notional': round(adjusted_notional, 2)})
            continue

        asset = get_asset_by_symbol(symbol)
        fractionable = bool(getattr(asset, 'fractionable', False)) if asset else False
        reference_price = get_latest_trade_price(symbol)
        order = {
            'symbol': symbol,
            'side': side,
            'target_weight': round(target_weight, 6),
            'current_weight': round(current_weight, 6),
            'drift': round(drift, 6),
            'raw_notional': round(raw_notional, 2),
            'estimated_cost': round(estimated_cost, 2),
            'notional': round(adjusted_notional, 2),
            'live_sentiment': round(live_sentiment, 6),
            'signal_delta': round(signal_delta, 6),
            'sell_due_to_sentiment_floor': should_sell_for_signal,
            'fractionable': fractionable,
            'reference_price': round(reference_price, 6) if reference_price else None,
        }

        if side == 'sell':
            position = positions.get(symbol, {})
            held_qty = float(position.get('qty', 0.0) or 0.0)
            order['held_qty'] = held_qty
            if not fractionable:
                if not reference_price or reference_price <= 0:
                    skipped.append({'symbol': symbol, 'reason': 'no_price_for_non_fractional_sell', 'held_qty': held_qty})
                    continue
                sell_qty = int(math.floor(held_qty)) if should_sell_for_signal else int(math.floor(adjusted_notional / reference_price))
                sell_qty = min(sell_qty, int(math.floor(held_qty)))
                if sell_qty <= 0:
                    skipped.append({'symbol': symbol, 'reason': 'sell_qty_zero', 'held_qty': held_qty, 'notional': round(adjusted_notional, 2)})
                    continue
                order['qty'] = sell_qty
            sells.append(order)
            continue

        if side == 'buy' and not fractionable:
            if not reference_price or reference_price <= 0:
                skipped.append({'symbol': symbol, 'reason': 'no_price_for_non_fractional_buy'})
                continue
            buy_qty = int(math.floor(adjusted_notional / reference_price))
            if buy_qty <= 0:
                skipped.append({'symbol': symbol, 'reason': 'below_one_share_notional', 'notional': round(adjusted_notional, 2), 'reference_price': round(reference_price, 6)})
                continue
            order['qty'] = buy_qty
        buys.append(order)

    sells.sort(key=lambda row: (bool(row.get('sell_due_to_sentiment_floor')), abs(float(row.get('drift', 0.0))), float(row.get('notional', 0.0))), reverse=True)
    buys.sort(key=lambda row: abs(float(row.get('drift', 0.0))), reverse=True)
    return {
        'config': asdict(CONFIG),
        'timestamp': to_iso(utc_now()),
        'account': snapshot,
        'target_weights': target_weights,
        'current_weights': current_w,
        'positions_snapshot': positions,
        'portfolio_issues': [],
        'live_tickers_considered': len(live_signal_rows),
        'sell_orders': sells,
        'buy_orders': buys,
        'skipped_orders': skipped,
        'buying_power_check': {},
        'buying_power_trimmed_orders': [],
        'submitted': {'sells': [], 'buys': []},
    }


def apply_buying_power_check(buy_orders: List[dict], account: Any) -> Tuple[List[dict], Dict[str, Any], List[dict]]:
    available_bp = float(account.buying_power)
    safe_bp = max(0.0, available_bp * (1.0 - CONFIG.buying_power_reserve_pct))
    ordered = sorted(buy_orders, key=lambda row: abs(float(row.get('drift', 0.0))), reverse=True)
    kept: List[dict] = []
    trimmed: List[dict] = []
    used = 0.0

    for order in ordered:
        notional = float(order['notional'])
        if notional < CONFIG.min_trade_notional:
            trimmed.append({'symbol': order['symbol'], 'reason': 'below_min_trade_notional'})
            continue
        if used + notional <= safe_bp:
            kept.append(order)
            used += notional
            continue
        remaining = safe_bp - used
        if remaining >= CONFIG.min_trade_notional:
            reduced = dict(order)
            reduced['original_notional'] = order['notional']
            reduced['notional'] = round(remaining, 2)
            kept.append(reduced)
            trimmed.append({'symbol': order['symbol'], 'reason': 'trimmed_to_fit_buying_power', 'original_notional': order['notional'], 'new_notional': reduced['notional']})
            used += remaining
        else:
            trimmed.append({'symbol': order['symbol'], 'reason': 'dropped_insufficient_buying_power', 'requested_notional': order['notional']})
            break

    if len(ordered) > len(kept):
        kept_symbols = {(row['symbol'], row.get('notional')) for row in kept}
        for order in ordered:
            if (order['symbol'], order.get('notional')) in kept_symbols:
                continue
            if not any(trimmed_item.get('symbol') == order['symbol'] for trimmed_item in trimmed):
                trimmed.append({'symbol': order['symbol'], 'reason': 'dropped_lower_priority_after_buying_power_limit', 'requested_notional': order['notional']})

    info = {
        'buying_power': round(available_bp, 2),
        'safe_buying_power': round(safe_bp, 2),
        'used_buying_power_for_buys': round(used, 2),
        'planned_buy_notional_before_check': round(sum(float(order['notional']) for order in buy_orders), 2),
        'planned_buy_notional_after_check': round(sum(float(order['notional']) for order in kept), 2),
        'buying_power_reserve_pct': CONFIG.buying_power_reserve_pct,
        'scaled_or_trimmed': sum(float(order['notional']) for order in kept) < sum(float(order['notional']) for order in buy_orders),
    }
    return kept, info, trimmed


def submit_orders(orders: List[dict], positions: Dict[str, dict], equity: Optional[float] = None) -> List[dict]:
    client_instance = get_trading_client()
    submitted: List[dict] = []
    executable_orders: List[dict] = []
    skipped_precheck: List[dict] = []

    for raw_order in orders:
        order = dict(raw_order)
        symbol = order['symbol']
        side = order['side']
        asset = get_asset_by_symbol(symbol)
        if asset is None:
            skipped_precheck.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'asset_not_found'})
            continue
        if not bool(getattr(asset, 'tradable', False)):
            skipped_precheck.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'asset_not_tradable'})
            continue
        if not asset_is_active(asset):
            skipped_precheck.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'asset_not_active'})
            continue
        fractionable = bool(getattr(asset, 'fractionable', False))
        order['fractionable'] = fractionable
        reference_price = order.get('reference_price') or get_latest_trade_price(symbol)
        if (not fractionable or order.get('qty') is not None) and (not reference_price or reference_price <= 0):
            skipped_precheck.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'no_price'})
            continue
        order['reference_price'] = float(reference_price) if reference_price else None
        if side == 'sell':
            ok, reason = can_submit_sell(symbol, positions)
            if not ok:
                skipped_precheck.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': reason})
                continue
        executable_orders.append(order)

    if not executable_orders:
        return skipped_precheck

    total_effective_weight = sum(max(0.0, float(order.get('current_weight', 0.0))) for order in executable_orders)

    for order in executable_orders:
        symbol = order['symbol']
        side = order['side']
        fractionable = bool(order.get('fractionable', False))
        reference_price = order.get('reference_price')
        current_weight = float(order.get('current_weight', 0.0))
        order['effective_current_weight'] = current_weight / total_effective_weight if total_effective_weight > 0 and current_weight > 0 else current_weight

        if order.get('qty') is not None:
            quantity = float(order['qty'])
            if quantity <= 0:
                submitted.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'qty_zero'})
                continue
            if side == 'sell':
                held_qty = float((positions.get(symbol) or {}).get('qty', 0.0) or 0.0)
                quantity = min(quantity, held_qty)
                if quantity <= 0:
                    submitted.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'qty_exceeds_inventory'})
                    continue
            if fractionable and CONFIG.allow_fractional and not float(quantity).is_integer():
                request = MarketOrderRequest(symbol=symbol, qty=quantity, side=OrderSide.BUY if side == 'buy' else OrderSide.SELL, time_in_force=TimeInForce.DAY)
                payload = {'symbol': symbol, 'side': side, 'mode': 'qty', 'qty': quantity, 'reference_price': reference_price, 'effective_current_weight': round(order['effective_current_weight'], 6)}
            else:
                quantity_int = int(math.floor(quantity))
                if quantity_int <= 0:
                    submitted.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'qty_zero'})
                    continue
                request = MarketOrderRequest(symbol=symbol, qty=quantity_int, side=OrderSide.BUY if side == 'buy' else OrderSide.SELL, time_in_force=TimeInForce.DAY)
                payload = {'symbol': symbol, 'side': side, 'mode': 'qty', 'qty': quantity_int, 'reference_price': reference_price, 'effective_current_weight': round(order['effective_current_weight'], 6)}


        elif side == 'sell' and fractionable and CONFIG.allow_fractional and (
            bool(order.get('sell_due_to_sentiment_floor')) or float(order.get('target_weight', 0.0)) <= 0.0
        ):
            held_qty = float((positions.get(symbol) or {}).get('qty', 0.0) or 0.0)
            if held_qty <= 0:
                submitted.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'qty_exceeds_inventory'})
                continue
            request = MarketOrderRequest(
                symbol=symbol,
                qty=held_qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
            )
            payload = {
                'symbol': symbol,
                'side': side,
                'mode': 'qty',
                'qty': held_qty,
                'reference_price': reference_price,
                'effective_current_weight': round(order['effective_current_weight'], 6),
            }


        elif fractionable and CONFIG.allow_fractional:
            notional = float(order.get('notional', 0.0))
            if notional < CONFIG.min_trade_notional:
                submitted.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'below_min_trade_notional'})
                continue
            request = MarketOrderRequest(symbol=symbol, notional=notional, side=OrderSide.BUY if side == 'buy' else OrderSide.SELL, time_in_force=TimeInForce.DAY)
            payload = {'symbol': symbol, 'side': side, 'mode': 'notional', 'notional': notional, 'effective_current_weight': round(order['effective_current_weight'], 6)}
        else:
            if not reference_price or reference_price <= 0:
                submitted.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'no_price'})
                continue
            quantity_int = int(float(order.get('notional', 0.0)) // float(reference_price))
            if quantity_int <= 0:
                submitted.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'qty_zero'})
                continue
            if side == 'sell':
                held_qty = int(math.floor(float((positions.get(symbol) or {}).get('qty', 0.0) or 0.0)))
                quantity_int = min(quantity_int, held_qty)
                if quantity_int <= 0:
                    submitted.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'qty_exceeds_inventory'})
                    continue
            implied_notional = quantity_int * float(reference_price)
            if implied_notional < CONFIG.min_trade_notional:
                submitted.append({'symbol': symbol, 'side': side, 'skipped': True, 'reason': 'below_min_trade_notional'})
                continue
            request = MarketOrderRequest(symbol=symbol, qty=quantity_int, side=OrderSide.BUY if side == 'buy' else OrderSide.SELL, time_in_force=TimeInForce.DAY)
            payload = {'symbol': symbol, 'side': side, 'mode': 'qty', 'qty': quantity_int, 'reference_price': reference_price, 'implied_notional': round(implied_notional, 2), 'effective_current_weight': round(order['effective_current_weight'], 6)}

        response = client_instance.submit_order(order_data=request)
        payload['alpaca_order_id'] = str(response.id)
        submitted.append(payload)

    submitted.extend(skipped_precheck)
    return submitted


def update_trade_state(state: Dict[str, Any], submitted: Dict[str, List[dict]], signal_rows: List[dict], target_weights: Dict[str, float]) -> None:
    now_iso = to_iso(utc_now())
    state.setdefault('last_trade_times', {})
    for side in ('sells', 'buys'):
        for order in submitted.get(side, []):
            if order.get('skipped'):
                continue
            symbol = sanitize_symbol(order.get('symbol'))
            if symbol:
                state['last_trade_times'][symbol] = now_iso
    state['last_signal_scores'] = {
        sanitize_symbol(row.get('ticker')): float(row.get('decayed_sentiment', 0.5))
        for row in signal_rows
        if sanitize_symbol(row.get('ticker'))
    }
    state['last_target_weights'] = {
        sanitize_symbol(symbol): float(weight)
        for symbol, weight in target_weights.items()
        if sanitize_symbol(symbol)
    }
    state['last_run'] = now_iso
    save_state(state)


def update_signal_state(state: Dict[str, Any], signal_scores: Dict[str, float], target_weights: Dict[str, float]) -> None:
    clean_scores: Dict[str, float] = {}
    for symbol, value in (signal_scores or {}).items():
        clean = sanitize_symbol(symbol)
        if not clean:
            continue
        try:
            clean_scores[clean] = round(float(value), 6)
        except Exception:
            continue

    state['last_signal_scores'] = clean_scores
    state['last_target_weights'] = {
        sanitize_symbol(symbol): round(float(weight), 6)
        for symbol, weight in (target_weights or {}).items()
        if sanitize_symbol(symbol) and float(weight) > 0
    }
    state['last_run'] = to_iso(utc_now())
    save_state(state)


def update_trade_timestamps(state: Dict[str, Any], submitted: Dict[str, List[dict]]) -> None:
    now_iso = to_iso(utc_now())
    state.setdefault('last_trade_times', {})
    for side in ('sells', 'buys'):
        for order in submitted.get(side, []):
            if order.get('skipped'):
                continue
            symbol = sanitize_symbol(order.get('symbol'))
            if symbol:
                state['last_trade_times'][symbol] = now_iso
    save_state(state)



def market_is_open() -> bool:
    clock = get_trading_client().get_clock()
    return bool(clock.is_open)


def evaluate_and_rebalance_once(refresh_ingest: bool = False) -> Dict[str, Any]:
    ensure_asset_search_index()
    if refresh_ingest:
        ingest_new_headlines_once()

    state = load_state()
    previous_signal_scores = clone_signal_scores(state)
    snapshot, positions = get_account_snapshot()
    if snapshot['trading_blocked']:
        raise RuntimeError('Account is trading blocked.')

    current_weight_map = current_weights(snapshot['equity'], positions)
    portfolio_issues = validate_portfolio_state(snapshot, positions, current_weight_map)

    live_signal_rows = aggregate_live_ticker_state()
    save_json(CONFIG.live_ticker_state_path, live_signal_rows)
    save_json(CONFIG.ticker_averages_path, live_signal_rows)

    raw_target_weights, target_skipped = build_target_weights(live_signal_rows, snapshot['equity'])
    target_weights, target_weight_filter_skipped = filter_and_reweight_target_weights(snapshot['equity'], raw_target_weights)

    result = build_rebalance_plan(
        snapshot,
        positions,
        current_weight_map,
        target_weights,
        state,
        live_signal_rows,
        target_skipped + target_weight_filter_skipped,
        previous_signal_scores,
    )
    result['portfolio_issues'] = portfolio_issues
    result['ingest_refreshed_for_plan'] = bool(refresh_ingest)
    result['signal_state_before_plan'] = previous_signal_scores
    result['signal_state_current'] = snapshot_signal_state(live_signal_rows)
    result['state_last_ingest_at'] = ensure_article_store().get('last_ingest_at')

    if not CONFIG.dry_run:
        if result['sell_orders']:
            result['submitted']['sells'] = submit_orders(result['sell_orders'], positions, snapshot['equity'])
            time.sleep(3)
        refreshed_account = get_trading_client().get_account()
        checked_buy_orders, bp_info, bp_trimmed = apply_buying_power_check(result['buy_orders'], refreshed_account)
        result['buy_orders'] = checked_buy_orders
        result['buying_power_check'] = bp_info
        result['buying_power_trimmed_orders'] = bp_trimmed
        if checked_buy_orders:
            result['submitted']['buys'] = submit_orders(checked_buy_orders, positions, snapshot['equity'])
    else:
        preview_account = get_trading_client().get_account()
        checked_buy_orders, bp_info, bp_trimmed = apply_buying_power_check(result['buy_orders'], preview_account)
        result['buy_orders'] = checked_buy_orders
        result['buying_power_check'] = bp_info
        result['buying_power_trimmed_orders'] = bp_trimmed

    update_trade_timestamps(state, result['submitted'])
    current_signal_scores = result['signal_state_current']
    update_signal_state(state, current_signal_scores, result['target_weights'])
    result['persisted_signal_state'] = clone_signal_scores(load_state())
    save_json(CONFIG.rebalance_plan_path, result)
    return result


def log_cycle_summary(result: Dict[str, Any]) -> None:
    logger.info(
        'Cycle complete. tickers=%s target_symbols=%s sells=%s buys=%s dry_run=%s',
        result.get('live_tickers_considered', 0),
        len(result.get('target_weights', {})),
        len(result.get('sell_orders', [])),
        len(result.get('buy_orders', [])),
        CONFIG.dry_run,
    )


def debug_loop_state(loop_name: str, **fields: Any) -> None:
    if not CONFIG.debug_loop_logging:
        return
    payload = ', '.join(f"{key}={value!r}" for key, value in fields.items())
    logger.info('[%s] %s', loop_name, payload)


async def feed_loop() -> None:
    iteration = 0
    while not shutdown_event.is_set():
        iteration += 1
        started_at = time.time()
        try:
            debug_loop_state('feed-loop:start', iteration=iteration, poll_seconds=CONFIG.feed_poll_seconds, shutdown=shutdown_event.is_set())
            async with run_lock:
                debug_loop_state('feed-loop:lock-acquired', iteration=iteration)
                stats = await asyncio.to_thread(ingest_new_headlines_once)
                state = load_state(); live_signal_rows = aggregate_live_ticker_state()
                update_signal_state(state, snapshot_signal_state(live_signal_rows), build_target_weights(live_signal_rows, 1.0)[0])
            debug_loop_state(
                'feed-loop:ingest-complete',
                iteration=iteration,
                elapsed_seconds=round(time.time() - started_at, 3),
                new_articles=stats.get('new_articles'),
                touched_articles=stats.get('touched_articles'),
                total_cached_articles=stats.get('total_cached_articles'),
            )
        except Exception as exc:
            logger.exception('Feed loop failed on iteration %s: %r', iteration, exc)
        try:
            debug_loop_state('feed-loop:sleep', iteration=iteration, timeout=CONFIG.feed_poll_seconds)
            await asyncio.wait_for(shutdown_event.wait(), timeout=max(1, CONFIG.feed_poll_seconds))
        except asyncio.TimeoutError:
            debug_loop_state('feed-loop:wake-timeout', iteration=iteration)
            continue


async def rebalance_loop() -> None:
    iteration = 0
    while not shutdown_event.is_set():
        iteration += 1
        started_at = time.time()
        try:
            market_open = await asyncio.to_thread(market_is_open)
            should_run = market_open or CONFIG.rebalance_force_when_closed
            debug_loop_state(
                'rebalance-loop:start',
                iteration=iteration,
                poll_seconds=CONFIG.rebalance_poll_seconds,
                market_open=market_open,
                forced=CONFIG.rebalance_force_when_closed,
                should_run=should_run,
            )
            if not should_run:
                logger.info('Market is closed; skipping rebalance check.')
            else:
                async with run_lock:
                    debug_loop_state('rebalance-loop:lock-acquired', iteration=iteration)
                    result = await asyncio.to_thread(evaluate_and_rebalance_once, CONFIG.plan_refresh_with_ingest)
                debug_loop_state(
                    'rebalance-loop:cycle-complete',
                    iteration=iteration,
                    elapsed_seconds=round(time.time() - started_at, 3),
                    live_tickers_considered=result.get('live_tickers_considered'),
                    sell_orders=len(result.get('sell_orders', [])),
                    buy_orders=len(result.get('buy_orders', [])),
                    skipped_orders=len(result.get('skipped_orders', [])),
                )
                log_cycle_summary(result)
        except Exception as exc:
            logger.exception('Rebalance loop failed on iteration %s: %r', iteration, exc)
        try:
            debug_loop_state('rebalance-loop:sleep', iteration=iteration, timeout=CONFIG.rebalance_poll_seconds)
            await asyncio.wait_for(shutdown_event.wait(), timeout=max(1, CONFIG.rebalance_poll_seconds))
        except asyncio.TimeoutError:
            debug_loop_state('rebalance-loop:wake-timeout', iteration=iteration)
            continue


async def run_async_bot() -> None:
    ensure_asset_search_index()
    feed_task = asyncio.create_task(feed_loop(), name='feed-loop')
    rebalance_task = asyncio.create_task(rebalance_loop(), name='rebalance-loop')
    await shutdown_event.wait()
    await asyncio.gather(feed_task, rebalance_task, return_exceptions=True)


def request_shutdown(signum: int, frame: Any) -> None:
    logger.info('Received signal %s, shutting down async bot.', signum)
    shutdown_event.set()


def run_once_cli() -> None:
    result = evaluate_and_rebalance_once(refresh_ingest=True)
    print(json.dumps(result, indent=2))


def run_async_cli() -> None:
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    logger.info(
        'Starting async bot: feed poll=%ss, rebalance poll=%ss, top_n=%s, drift_threshold=%s, plan_refresh_with_ingest=%s, debug_loop_logging=%s, rebalance_force_when_closed=%s',
        CONFIG.feed_poll_seconds,
        CONFIG.rebalance_poll_seconds,
        CONFIG.top_n_positions,
        CONFIG.drift_threshold,
        CONFIG.plan_refresh_with_ingest,
        CONFIG.debug_loop_logging,
        CONFIG.rebalance_force_when_closed,
    )
    asyncio.run(run_async_bot())


if __name__ == '__main__':
    mode = os.getenv('BOT_MODE', 'async').lower()
    if mode == 'once':
        run_once_cli()
    else:
        run_async_cli()
