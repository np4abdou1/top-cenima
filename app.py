#!/usr/bin/env python3
"""
TopCinema Advanced Web Scraper UI (v2.3)

- Flask Web Server with a minimal, premium, live-updating UI.
- High-performance, 50-worker parallel fetcher.
- Safe, single-thread database writer using a queue.
- Polymorphic DB schema (movies link servers to shows, series link to episodes).
- Progress is saved to the DB, allowing the script to be stopped and resumed.
- FIX (User Request): Now scrapes season pages directly, avoiding broken /list/ pagination.
- FIX (User Request): Robustly parses special (0), decimal (X.Y), and merged (X و Y) episode numbers.
- FIX (User Request): Uses `source_url` as the UNIQUE key for shows, allowing duplicate titles.
- NEW (User Request): Added "Sync" feature to update database from a sitemap URL.
- UI FIX (User Request): Added granular, colored logging and improved log scrolling.
- NEW (User Request): Added /db page to explore database tables.
- NEW (User Request): Added theme switcher (Green, Blue, Amber) with localStorage.
- NEW (User Request): Added "Download DB" button.
"""

import json
import os
import re
import time
import sqlite3
import threading
import logging
import queue
from typing import List, Dict, Optional, Any, Tuple
from urllib.parse import urlparse, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue
from collections import deque

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, Response, request, send_file, render_template_string

# --- Configuration ---

# Suppress all terminal logging except for our one startup message
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

DB_PATH = "data/scrapped.db"
JSON_FILES = ["data/movies.json", "data/series_animes.json"]
FETCHER_WORKERS_MOVIES = 50 # Fast speed for movies
FETCHER_WORKERS_SERIES = 15 # Reduced speed for series to prevent thread errors
FETCHER_WORKERS_ANIME = 15 # Reduced speed for anime to prevent thread errors
SERVER_PORT = 8080

# --- Global State for UI ---

# This dictionary is the single source of truth for the web UI
GLOBAL_STATE = {
    "scraper_running": False,
    "current_scrape_type": None,  # 'movies', 'series', 'anime', 'sync', or 'all'
    "scrape_queue": [],  # Queue of types to scrape in order
    "status_message": "Idle. Ready to start.",
    "progress": {
        "pending": 0,
        "completed": 0,
        "failed": 0,
        "total": 0
    },
    "counts": {
        "movies": 0,
        "series": 0,
        "anime": 0
    },
    "live_db_log": "...",
    "live_fetch_logs": deque(maxlen=500) # Increased for longer log history
}

DATA_QUEUE = Queue()
STOP_EVENT = threading.Event()
SCRAPER_THREAD = None
SYNC_THREAD = None # Thread for the sync operation

# --- Networking Setup ---

# Base headers for GET requests (browsing pages)
BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://topcinema.pro/",
}

# Magic headers for the server-fetching POST request
SERVER_POST_HEADERS = {
    'Accept': '*/*',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
}

REQUEST_TIMEOUT = 15
REQUEST_DELAY = 0.1  # Minimal delay for VPS
VERIFY_SSL = False

# Setup persistent session for GET requests
SESSION = requests.Session()
SESSION.headers.update(BASE_HEADERS)
retry_strategy = requests.packages.urllib3.util.retry.Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retry_strategy)
SESSION.mount('https://', adapter)
SESSION.mount('http://', adapter)
if not VERIFY_SSL:
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- Regex & Constants ---

REGEX_PATTERNS = {
    'number': re.compile(r'(\d+)'),
    'movie': re.compile(r'(\/فيلم-|\/film-|\/movie-|%d9%81%d9%8a%d9%84%d9%85)', re.IGNORECASE),
    'episode_complex': re.compile(r'(?:الحلقة|Episode)\s*([\d\.\sو]+)', re.IGNORECASE), # FIX: Handles "12 و 13" or "11.5"
    'episode_special': re.compile(r'الخاصة|Special', re.IGNORECASE), # FIX: Detects special episodes
    'episode_zero': re.compile(r'(?:الحلقة|Episode)\s+0\s*', re.IGNORECASE), # NEW: Detects episode 0
    'episode_decimal': re.compile(r'(\d+(?:\.\d+)?)'), # FIX: Extracts first number, including decimals
    'watch_suffix': re.compile(r'/watch/?$'),
    'episode_id': re.compile(r'"id"\s*:\s*"(\d+)"'),
    'title_clean_prefix': re.compile(r'^\s*(فيلم|انمي|مسلسل|anime|film|movie|series)\s+', re.IGNORECASE | re.UNICODE),
    'title_clean_suffix': re.compile(r'\s+(مترجم|اون\s*لاين|اونلاين|online|مترجمة|مدبلج|مدبلجة)(\s+|$)', re.IGNORECASE | re.UNICODE),
    'base_show_url': re.compile(r'(https?:\/\/[^\/]+\/(?:مسلسل|انمي|series|anime)-[^\/]+)\/') # NEW: For sitemap parser
}

ARABIC_ORDINALS = {
    "الاول": 1, "الأول": 1, "الثاني": 2, "ثاني": 2, "الثالث": 3, "ثالث": 3,
    "الابع": 4, "رابع": 4, "الخامس": 5, "خامس": 5, "السادس": 6, "sادس": 6,
    "السابع": 7, "سابع": 7, "الثامن": 8, "ثامن": 8, "التاسع": 9, "تاسع": 9,
    "العاشر": 10, "عاشر": 10,
}

# --- UI Logging ---

def log_to_ui(log_type: str, message: str):
    """Updates the GLOBAL_STATE for the UI to read."""
    if log_type == "db":
        GLOBAL_STATE["live_db_log"] = message
    elif log_type == "fetch":
        GLOBAL_STATE["live_fetch_logs"].append(message)
    elif log_type == "status":
        GLOBAL_STATE["status_message"] = message

# --- Utility Functions ---

def slugify(text: str) -> str:
    """Create a safe filename from a title."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text[:100]

def fetch_html(url: str) -> Optional[BeautifulSoup]:
    """Fetches and parses HTML from a URL."""
    if STOP_EVENT.is_set(): return None
    if not url.startswith(('http://', 'https://')):
        return None
    try:
        time.sleep(REQUEST_DELAY)
        resp = SESSION.get(url, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        # Don't flood the UI log with request failures
        pass
    return None

def extract_number_from_text(text: str) -> Optional[int]:
    if not text: return None
    m = REGEX_PATTERNS['number'].search(text)
    if m: return int(m.group(1))
    lower = text.replace("ي", "ى").replace("أ", "ا").replace("إ", "ا").strip()
    for word, num in ARABIC_ORDINALS.items():
        if word in lower: return num
    return None

def clean_title(title: str) -> str:
    if not title: return title
    cleaned = REGEX_PATTERNS['title_clean_prefix'].sub('', title)
    prev = ""
    while prev != cleaned:
        prev = cleaned
        cleaned = REGEX_PATTERNS['title_clean_suffix'].sub(' ', cleaned)
    cleaned = ' '.join(cleaned.split()).strip(' -–—|:،؛')
    return cleaned

def get_sort_key(ep_str: Optional[str]) -> float:
    """
    Converts an episode string (e.g., "22-23", "144.5", "0") into a float
    for correct sorting.
    """
    if ep_str is None:
        return 99999.0
    # Try to get the first number (float or int) from the string
    match = REGEX_PATTERNS['episode_decimal'].search(ep_str)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return 99999.0
    # Fallback for "Special" or other non-numeric, or if parse fails
    if ep_str.lower() == "special" or ep_str == "0":
        return 0.0
    return 99999.0

# --- Database Initialization ---

def init_database(db_path: str = DB_PATH):
    """Create 4-table POLYMORPHIC database schema"""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS shows (
            id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL,
            type TEXT NOT NULL CHECK(type IN ('movie', 'series', 'anime')),
            poster TEXT, synopsis TEXT, imdb_rating REAL, trailer TEXT, year INTEGER,
            genres TEXT, cast TEXT, directors TEXT, country TEXT, language TEXT, duration TEXT,
            source_url TEXT UNIQUE NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""") # FIX: Removed UNIQUE on title/slug, added UNIQUE on source_url
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS seasons (
            id INTEGER PRIMARY KEY AUTOINCREMENT, show_id INTEGER NOT NULL, season_number INTEGER NOT NULL,
            poster TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (show_id) REFERENCES shows(id) ON DELETE CASCADE, UNIQUE(show_id, season_number)
        )""")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS episodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, season_id INTEGER NOT NULL, 
            episode_number TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (season_id) REFERENCES seasons(id) ON DELETE CASCADE, 
            UNIQUE(season_id, episode_number)
        )""") # FIX: Changed episode_number from INTEGER to TEXT
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS servers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, embed_url TEXT NOT NULL, server_number INTEGER NOT NULL,
            parent_type TEXT NOT NULL CHECK(parent_type IN ('movie', 'episode')),
            parent_id INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS scrape_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE NOT NULL,
            status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'completed', 'failed')),
            show_id INTEGER, error_message TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (show_id) REFERENCES shows(id) ON DELETE SET NULL
        )""")
        conn.commit()

# --- Core Scraping Logic ---

def get_trailer_embed_url(page_url: str, form_url: str) -> Optional[str]:
    """Fetches trailer embed URL via Ajax POST request."""
    if STOP_EVENT.is_set(): return None
    try:
        base = "https://topcinema.pro"
        trailer_endpoint = base + "/wp-content/themes/movies2023/Ajaxat/Home/LoadTrailer.php"
        data_str = f"href={quote(form_url, safe=':/')}"
        trailer_headers = {
            "accept": "*/*", 
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-requested-with": "XMLHttpRequest", 
            "referer": quote(page_url, safe=':/')
        }
        resp = SESSION.post(trailer_endpoint, headers=trailer_headers, data=data_str.encode('utf-8'),
                          timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        iframe = soup.find("iframe")
        if iframe and iframe.get("src"):
            trailer_url = iframe["src"].strip()
            if trailer_url and trailer_url.startswith(('http://', 'https://')):
                return trailer_url
    except Exception:
        pass
    return None

def get_episode_servers(episode_id: str, referer: Optional[str] = None, total_servers: int = 10) -> List[Dict]:
    """Fetches all server embed URLs using the 4-header POST request fix."""
    if STOP_EVENT.is_set(): return []
    servers: List[Dict] = []
    server_url = "https://topcinema.pro/wp-content/themes/movies2023/Ajaxat/Single/Server.php"
    
    # Use the 4 magic headers for the POST request
    server_headers = SERVER_POST_HEADERS.copy()
    server_headers["Referer"] = quote(referer, safe=':/') if referer else "https://topcinema.pro/"

    def fetch_one(i: int):
        if STOP_EVENT.is_set(): return None
        try:
            data = {"id": str(episode_id), "i": str(i)}
            # Use requests.post directly to send a clean request
            resp = requests.post(server_url, headers=server_headers, data=data, timeout=5, verify=VERIFY_SSL)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            iframe = soup.find("iframe")
            if iframe and iframe.get("src") and iframe.get("src").strip():
                return {"server_number": i, "embed_url": iframe.get("src").strip()}
        except Exception:
            pass
        return None

    # Fetch all servers in parallel (reduced workers to prevent thread errors)
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(fetch_one, i): i for i in range(total_servers)}
        for fut in as_completed(futures):
            if STOP_EVENT.is_set():
                ex.shutdown(wait=False, cancel_futures=True)
                break
            res = fut.result()
            if res:
                servers.append(res)

    servers.sort(key=lambda x: x.get("server_number", 0))
    return servers

def extract_episode_id_from_watch_page(soup: BeautifulSoup) -> Optional[str]:
    """Finds the internal episode ID from a /watch/ page."""
    if not soup: return None
    li = soup.select_one(".watch--servers--list li.server--item[data-id]")
    if li and li.has_attr("data-id"):
        return li["data-id"].strip()
    for script in soup.find_all("script"):
        if script.string:
            m = REGEX_PATTERNS['episode_id'].search(script.string)
            if m: return m.group(1)
    return None

def scrape_season_episodes(season_url: str) -> List[Dict]:
    """Scrapes all episodes and their servers for a given season URL."""
    if STOP_EVENT.is_set(): return []
    
    # 1. Fetch season page directly (no /list/ or pagination)
    soup = fetch_html(season_url)
    if not soup: 
        log_to_ui("fetch", f"🔥 [ERROR]   > Failed to fetch season page: {season_url}")
        return []

    # Add anchors from page 1
    all_anchors = soup.select('.allepcont .row > a')
    if not all_anchors:
        all_anchors = [x for x in soup.find_all('a') if (x.find(class_='epnum') or (x.get('title') and ('الحلقة' in x.get('title') or 'Episode' in x.get('title'))))]
    
    episodes: List[Dict] = []
    seen = set()
    
    log_to_ui("fetch", f"➡️ [DEBUG]   > Found {len(all_anchors)} total episodes.")

    def process_episode(a):
        if STOP_EVENT.is_set(): return None
        try:
            raw_href = a.get('href')
            if not raw_href: return None
            
            ep_title = a.get('title', '').strip()
            ep_num_text = a.get_text(" ", strip=True)
            full_text_for_parse = f"{ep_title} {ep_num_text}"
            
            key = (ep_title.strip() or raw_href.strip())
            if not key: return None
            if key in seen: return None
            seen.add(key)

            # --- New Episode Number Logic (FIX) ---
            ep_num_str: Optional[str] = None
            
            # Priority 1: Check for "Special" or "Episode 0"
            if REGEX_PATTERNS['episode_zero'].search(full_text_for_parse) or REGEX_PATTERNS['episode_special'].search(full_text_for_parse):
                ep_num_str = "0"
            
            # Priority 2: If not special, check for complex numbers (e.g., "22 و 23", "1115.5")
            if ep_num_str is None:
                complex_match = REGEX_PATTERNS['episode_complex'].search(full_text_for_parse)
                if complex_match:
                    num_str = complex_match.group(1).strip() # e.g., "12 و 13", "1115.5"
                    # Clean the string: "12 و 13" -> "12-13", "1115.5" -> "1115.5"
                    num_str = num_str.replace('و', '-').strip()
                    num_str = re.sub(r'\s+', '', num_str)
                    
                    # Final check it's a valid-looking number string
                    if re.search(r'[\d\.-]', num_str):
                        ep_num_str = num_str

            # Priority 3: Fallback to simple number extraction
            if ep_num_str is None:
                 ep_num_int = (extract_number_from_text(ep_title) or extract_number_from_text(ep_num_text))
                 if ep_num_int is not None:
                    ep_num_str = str(ep_num_int)

            # If still not found, log it and skip
            if ep_num_str is None:
                log_to_ui("fetch", f"⚠️ [WARN]   > Could not parse ep num for: {ep_title}")
                return None
            # --- End New Logic ---

            watch_url = raw_href.rstrip('/') + '/watch/'
            ep_watch_soup = fetch_html(watch_url)
            episode_id = extract_episode_id_from_watch_page(ep_watch_soup) if ep_watch_soup else None
            
            server_list: List[Dict] = []
            if episode_id:
                server_list = get_episode_servers(episode_id, referer=watch_url, total_servers=10)
            
            return {"episode_number": ep_num_str, "servers": server_list}
        except Exception as e:
            log_to_ui("fetch", f"🔥 [ERROR]   > processing episode {a.get('href')}: {e}")
            return None

    # Fetch all episodes in parallel (reduced workers to prevent thread errors)
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = [ex.submit(process_episode, a) for a in all_anchors]
        for fut in as_completed(futures):
            if STOP_EVENT.is_set():
                ex.shutdown(wait=False, cancel_futures=True)
                break
            res = fut.result()
            if res:
                episodes.append(res)

    # Sort episodes based on the numeric value of their new string-based number
    episodes.sort(key=lambda e: get_sort_key(e.get("episode_number")))
    # Keep all episodes
    return episodes

def extract_media_details(soup: BeautifulSoup) -> Dict:
    """Extracts common details (title, poster, synopsis) from a page."""
    details = {
        "title": "Unknown", "poster": None, "synopsis": "",
        "imdb_rating": None, "metadata": {}
    }
    try:
        title_el = soup.find("h1", class_="post-title")
        if title_el:
            details["title"] = clean_title(title_el.get_text(strip=True))
        
        poster_wrap = soup.find('div', class_='image')
        if poster_wrap:
            img_tag = poster_wrap.find('img')
            if img_tag: details["poster"] = img_tag.get('src') or img_tag.get('data-src')
        
        story = soup.find('div', class_='story')
        if story:
            p = story.find('p')
            if p: details["synopsis"] = p.get_text(strip=True)
        
        imdb_box = soup.select_one(".UnderPoster .imdbR")
        if imdb_box:
            sp = imdb_box.find("span")
            if sp:
                try: details["imdb_rating"] = float(sp.get_text(strip=True))
                except ValueError: pass
        
        tax = soup.find('ul', class_='RightTaxContent')
        if tax:
            for li in tax.find_all('li'):
                key_el = li.find('span')
                if key_el:
                    key = key_el.get_text(strip=True).replace(':', '')
                    links = [a.get_text(strip=True) for a in li.find_all('a') if a.get_text(strip=True)]
                    details["metadata"][key] = links if links else li.find('strong').get_text(strip=True) if li.find('strong') else ""
    except Exception:
        pass
    
    key_mapping = {
        "قسم المسلسل": "category", "قسم الفيلم": "category", "نوع المسلسل": "genres",
        "نوع الفيلم": "genres", "النوع": "genres", "جودة المسلسل": "quality",
        "جودة الفيلم": "quality", "عدد الحلقات": "episode_count", "توقيت المسلسل": "duration",
        "توقيت الفيلم": "duration", "مدة الفيلم": "duration", "موعد الصدور": "release_year",
        "سنة الانتاج": "release_year", "لغة المسلسل": "language", "لغة الفيلم": "language",
        "دولة المسلسل": "country", "دولة الفيلم": "country", "المخرجين": "directors",
        "المخرج": "directors", "بطولة": "cast"
    }
    
    mapped_metadata = {}
    for k, v in details["metadata"].items():
        clean_key = k.strip().rstrip(':')
        new_key = key_mapping.get(clean_key, clean_key)
        if new_key in key_mapping.values():
            mapped_metadata[new_key] = v
    details["metadata"] = mapped_metadata
    return details

def scrape_series(url: str) -> Optional[Dict]:
    """Scrapes a full series or anime, including all seasons and episodes."""
    if STOP_EVENT.is_set(): return None
    soup = fetch_html(url)
    if not soup: return None
    
    details = extract_media_details(soup)
    seasons: List[Dict] = []
    season_urls: Dict[int, str] = {}
    seen_urls = set()
    
    # Find season links
    for s_el in soup.select('div.Small--Box.Season'):
        a_el = s_el.find('a')
        if not a_el or not a_el.get('href'): continue
        s_url = a_el.get('href')
        if s_url in seen_urls: continue
        seen_urls.add(s_url)
        s_title = a_el.get('title') or a_el.get_text(strip=True) or ""
        s_num = extract_number_from_text(s_title) or 1
        s_poster = img.get('src') or img.get('data-src') if (img := a_el.find('img')) else None
        season_urls[s_num] = s_url
        seasons.append({"season_number": s_num, "poster": s_poster, "episodes": []})

    if not seasons: # Fallback
        for a_el in soup.find_all('a', href=True):
            href = a_el['href']
            if ('/series/' in href or '/anime/' in href) and ('الموسم' in href or 'season' in a_el.get_text(strip=True, default='').lower()):
                if href in seen_urls: continue
                seen_urls.add(href)
                s_title = a_el.get('title') or a_el.get_text(strip=True) or ""
                s_num = extract_number_from_text(s_title) or extract_number_from_text(href) or 1
                season_urls[s_num] = href
                seasons.append({"season_number": s_num, "poster": None, "episodes": []})

    seasons.sort(key=lambda s: s.get('season_number', 0))

    if not seasons: # Single-season show
        season_urls[1] = url
        seasons.append({"season_number": 1, "poster": details["poster"], "episodes": []})
    
    log_to_ui("fetch", f"➡️ [DEBUG]   > Found {len(seasons)} seasons.")

    # Scrape episodes for each season
    for season in seasons:
        if STOP_EVENT.is_set(): break
        s_num = season["season_number"]
        if s_num in season_urls:
            season["episodes"] = scrape_season_episodes(season_urls[s_num])

    # Get trailer
    trailer_url = None
    if season_urls:
        first_season_url = list(season_urls.values())[0]
        temp_soup = fetch_html(first_season_url)
        if temp_soup and (first_ep_link := temp_soup.select_one(".allepcont .row > a")):
            trailer_url = get_trailer_embed_url(url, first_ep_link.get("href"))
    if not trailer_url:
        trailer_url = get_trailer_embed_url(url, url)

    return {
        "title": details["title"], "type": "series", "imdb_rating": details["imdb_rating"],
        "poster": details["poster"], "synopsis": details["synopsis"], "metadata": details["metadata"],
        "trailer": trailer_url, "source_url": url, "seasons": seasons
    }

def scrape_movie(url: str) -> Optional[Dict]:
    """Scrapes a movie and its streaming servers."""
    if STOP_EVENT.is_set(): return None
    
    details_soup = fetch_html(url)
    if not details_soup: return None
    
    details = extract_media_details(details_soup)
    
    watch_url = url.rstrip('/') + '/watch/'
    watch_soup = fetch_html(watch_url)
    if not watch_soup: return None
        
    episode_id = extract_episode_id_from_watch_page(watch_soup)
    servers = []
    if episode_id:
        servers = get_episode_servers(episode_id, referer=watch_url)
        log_to_ui("fetch", f"➡️ [DEBUG]   > Found {len(servers)} servers.")
    else:
        log_to_ui("fetch", f"⚠️ [WARN]   > No EpisodeID found.")
        
    trailer_url = get_trailer_embed_url(url, url)

    # Add year from title if not in metadata
    year = None
    if details["metadata"].get("release_year"):
        match = re.search(r'(\d{4})', str(details["metadata"]["release_year"]))
        if match: year = int(match.group(1))
    if not year:
        year = extract_number_from_text(details["title"])

    return {
        "title": details["title"], "type": "movie", "year": year,
        "imdb_rating": details["imdb_rating"], "poster": details["poster"],
        "synopsis": details["synopsis"], "metadata": details["metadata"],
        "trailer": trailer_url, "source_url": url,
        "streaming_servers": servers, "scraped_at": datetime.now().isoformat()
    }

def run_single(url_input: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Main function to detect type and scrape.
    Returns (result, error_message)
    """
    if STOP_EVENT.is_set(): return (None, "Stopped")
    url = url_input.strip()
    log_to_ui("fetch", f"START: {url.split('/')[-2]}")
    
    result: Optional[Dict] = None
    show_type = "unknown"
    error_message = None
    
    try:
        if "فيلم" in url or REGEX_PATTERNS['movie'].search(url):
            show_type = 'movie'
            result = scrape_movie(url)
        elif "انمي" in url or "anime" in url:
            show_type = 'anime'
            result = scrape_series(url)
            if result:
                result['type'] = 'anime' # Ensure type is correctly set
        elif "مسلسل" in url or "series" in url:
            show_type = 'series'
            result = scrape_series(url)
            if result:
                result['type'] = 'series' # Ensure type is correctly set
        else:
            # Fallback
            result = scrape_series(url)
            if result:
                has_episodes = any(s.get("episodes") for s in result.get("seasons", []))
                if not has_episodes:
                     result = scrape_movie(url)
                     show_type = 'movie'
                else:
                     show_type = 'series'
                     result['type'] = 'series'
            else:
                result = scrape_movie(url)
                show_type = 'movie'
        
        if result and 'type' not in result:
            result['type'] = show_type

        # --- NEW: Improved Redflag Filter ---
        if result and result.get("type") in ["series", "anime"]:
            seasons = result.get("seasons", [])
            if not seasons:
                error_message = "Redflag: No seasons found."
            else:
                total_episodes = 0
                for s in seasons:
                    episodes = s.get("episodes", [])
                    total_episodes += len(episodes)
                
                if total_episodes == 0:
                    error_message = "Redflag: No episodes found in any season."
                else:
                    # Check if at least some episodes have servers
                    has_any_server = False
                    for s in seasons:
                        for ep in s.get("episodes", []):
                            if ep.get("servers"):
                                has_any_server = True
                                break
                        if has_any_server:
                            break
                    
                    if not has_any_server:
                        error_message = "Redflag: No servers found for any episode."
            
            if error_message:
                log_to_ui("fetch", f"🟠 [REDFLAG] {result.get('title', 'Show')} - {error_message}")
                result = None # Discard the result

    except Exception as e:
        error_message = str(e)
        result = None

    if not result and not error_message:
        error_message = "Scrape failed"
        
    return (result, error_message)

# --- Database Class & Writer Thread ---

class Database:
    """Database class to handle all DB operations in the writer thread."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        # Each thread MUST create its own connection.
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.conn.execute("PRAGMA foreign_keys = ON")
        except Exception as e:
            print(f"[DB ERROR] Could not connect to DB at {db_path}: {e}")
            self.conn = None

    def close(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def insert_show(self, show_data: Dict) -> Optional[int]:
        """Insert show and return ID"""
        if not self.conn: return None
        cursor = self.conn.cursor()
        try:
            title = show_data.get("title")
            source_url = show_data.get("source_url") # FIX: Get source_url
            metadata = show_data.get("metadata", {})
            
            def to_string(value):
                if isinstance(value, list):
                    return ", ".join(str(v) for v in value if v)
                return str(value) if value else None
            
            year = show_data.get("year")
            if not year:
                year_str = metadata.get("release_year") or metadata.get("year")
                if year_str:
                    match = re.search(r'(\d{4})', str(year_str))
                    if match:
                        year = int(match.group(1))
            
            show_type = show_data.get("type", "series")

            cursor.execute("""
            INSERT INTO shows (title, type, poster, synopsis, imdb_rating, trailer, year, 
                             genres, cast, directors, country, language, duration, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                title, show_type,
                show_data.get("poster"), show_data.get("synopsis"),
                show_data.get("imdb_rating"), show_data.get("trailer"), year,
                to_string(metadata.get("genres")), to_string(metadata.get("cast")),
                to_string(metadata.get("directors")), to_string(metadata.get("country")),
                to_string(metadata.get("language")), to_string(metadata.get("duration")),
                source_url # FIX: Insert source_url
            ))
            show_id = cursor.lastrowid
            return show_id
        except sqlite3.IntegrityError:
            # FIX: Check based on source_url
            cursor.execute("SELECT id FROM shows WHERE source_url = ?", (source_url,))
            result = cursor.fetchone()
            return result["id"] if result else None
        except Exception as e:
            log_to_ui("db", f"ERROR inserting show: {e}")
            return None

    def insert_seasons_episodes_servers(self, show_id: int, seasons_data: List[Dict]):
        """Inserts seasons, episodes, and servers for a show."""
        if not self.conn: return
        cursor = self.conn.cursor()
        try:
            for season in seasons_data:
                season_num = season.get("season_number", 1)
                cursor.execute("INSERT OR IGNORE INTO seasons (show_id, season_number, poster) VALUES (?, ?, ?)", 
                               (show_id, season_num, season.get("poster")))
                
                season_id = cursor.lastrowid
                if season_id == 0: # Already exists
                    cursor.execute("SELECT id FROM seasons WHERE show_id = ? AND season_number = ?", (show_id, season_num))
                    result = cursor.fetchone()
                    if result: season_id = result[0]
                
                if not season_id: continue

                for episode in season.get("episodes", []):
                    cursor.execute("INSERT OR IGNORE INTO episodes (season_id, episode_number) VALUES (?, ?)", 
                                   (season_id, episode.get("episode_number")))
                    
                    episode_id = cursor.lastrowid
                    if episode_id == 0: # Already exists
                        cursor.execute("SELECT id FROM episodes WHERE season_id = ? AND episode_number = ?", (season_id, episode.get("episode_number")))
                        result = cursor.fetchone()
                        if result: episode_id = result[0]
                    
                    if not episode_id: continue
                    
                    # Delete old servers for this episode to refresh them
                    cursor.execute("DELETE FROM servers WHERE parent_type = 'episode' AND parent_id = ?", (episode_id,))

                    for server in episode.get("servers", []):
                        cursor.execute("""
                        INSERT INTO servers (embed_url, server_number, parent_type, parent_id) 
                        VALUES (?, ?, 'episode', ?)
                        """, (server.get("embed_url"), server.get("server_number"), episode_id))
        except Exception as e:
            log_to_ui("db", f"ERROR writing seasons: {e}")

    def insert_movie_servers(self, show_id: int, servers_data: List[Dict]):
        """Inserts servers for a movie, linking directly to the show."""
        if not self.conn: return
        cursor = self.conn.cursor()
        try:
            # Delete old servers for this movie to refresh them
            cursor.execute("DELETE FROM servers WHERE parent_type = 'movie' AND parent_id = ?", (show_id,))
            
            for server in servers_data:
                cursor.execute("""
                INSERT INTO servers (embed_url, server_number, parent_type, parent_id) 
                VALUES (?, ?, 'movie', ?)
                """, (server.get("embed_url"), server.get("server_number"), show_id))
        except Exception as e:
            log_to_ui("db", f"ERROR writing movie servers: {e}")

    def mark_progress(self, url: str, status: str, show_id: Optional[int] = None, error: Optional[str] = None):
        if not self.conn: return
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
            UPDATE scrape_progress SET status = ?, show_id = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
            WHERE url = ?
            """, (status, show_id, error, url))
        except Exception as e:
            log_to_ui("db", f"ERROR marking progress: {e}")

    def populate_and_get_pending_urls(self, scrape_type: str = "all") -> List[str]:
        """
        Reads all URLs from JSON files, injects them into the DB,
        updates the UI stats from the DB, and returns the pending URLs filtered by type.
        scrape_type: 'all', 'movies', 'series', or 'anime'
        """
        if not self.conn: return []
        cursor = self.conn.cursor()
        all_urls = []
        log_to_ui("status", "Reading source JSON files...")
        
        # Read URLs from BOTH files
        for file_path in JSON_FILES:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    file_urls = data.get("urls", [])
                    if file_urls:
                        all_urls.extend(file_urls)
            except Exception as e:
                log_to_ui("status", f"Error reading {file_path}: {e}")
        
        log_to_ui("status", f"Found {len(all_urls)} total URLs. Injecting new URLs into DB...")
        
        # Inject all URLs into progress table
        if all_urls:
            urls_to_insert = [(url,) for url in all_urls if url]
            cursor.executemany("INSERT OR IGNORE INTO scrape_progress (url) VALUES (?)", urls_to_insert)
            self.conn.commit()
        log_to_ui("status", "Database populated. Calculating stats...")

        # Get all stats from the DB
        cursor.execute("SELECT COUNT(*) FROM scrape_progress")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM scrape_progress WHERE status = 'pending'")
        pending_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM scrape_progress WHERE status = 'completed'")
        completed = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM scrape_progress WHERE status = 'failed'")
        failed = cursor.fetchone()[0]

        # Get pending URLs
        cursor.execute("SELECT url FROM scrape_progress WHERE status = 'pending'")
        all_pending_urls = [row[0] for row in cursor.fetchall()]
        
        # Filter URLs by type if specified
        pending_urls = []
        movies_count = 0
        series_count = 0
        anime_count = 0
        
        for url in all_pending_urls:
            url_type = None
            if "فيلم" in url or "movie" in url:
                url_type = "movies"
                movies_count += 1
            elif "انمي" in url or "anime" in url:
                url_type = "anime"
                anime_count += 1
            elif "مسلسل" in url or "series" in url:
                url_type = "series"
                series_count += 1
            
            # Add to pending_urls based on scrape_type filter
            if scrape_type == "all" or scrape_type == url_type:
                pending_urls.append(url)
                
        # Update GLOBAL_STATE
        GLOBAL_STATE["progress"]["total"] = total
        GLOBAL_STATE["progress"]["pending"] = len(pending_urls) if scrape_type != "all" else pending_count
        GLOBAL_STATE["progress"]["completed"] = completed
        GLOBAL_STATE["progress"]["failed"] = failed
        GLOBAL_STATE["counts"]["movies"] = movies_count
        GLOBAL_STATE["counts"]["series"] = series_count
        GLOBAL_STATE["counts"]["anime"] = anime_count
        
        log_to_ui("status", f"Ready to scrape {len(pending_urls)} pending {scrape_type} items.")
        return pending_urls
    
    def get_initial_stats(self):
        """Just read stats from DB without populating. Used on script launch."""
        if not self.conn: return
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM scrape_progress")
            total = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM scrape_progress WHERE status = 'pending'")
            pending_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM scrape_progress WHERE status = 'completed'")
            completed = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM scrape_progress WHERE status = 'failed'")
            failed = cursor.fetchone()[0]
            
            cursor.execute("SELECT url FROM scrape_progress WHERE status = 'pending'")
            pending_urls = [row[0] for row in cursor.fetchall()]
            
            movies_count = 0
            series_count = 0
            anime_count = 0
            for url in pending_urls:
                if "فيلم" in url or "movie" in url:
                    movies_count += 1
                elif "انمي" in url:
                    anime_count += 1
                elif "مسلسل" in url or "series" in url:
                    series_count += 1

            GLOBAL_STATE["progress"]["total"] = total
            GLOBAL_STATE["progress"]["pending"] = pending_count
            GLOBAL_STATE["progress"]["completed"] = completed
            GLOBAL_STATE["progress"]["failed"] = failed
            GLOBAL_STATE["counts"]["movies"] = movies_count
            GLOBAL_STATE["counts"]["series"] = series_count
            GLOBAL_STATE["counts"]["anime"] = anime_count
            log_to_ui("status", f"Idle. {pending_count} items pending.")
        except Exception as e:
            log_to_ui("status", f"Error loading initial stats: {e}")

    def get_all_urls_from_progress(self) -> set:
        """Helper to get all URLs currently in the progress table."""
        if not self.conn: return set()
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT url FROM scrape_progress")
            return set(row[0] for row in cursor.fetchall())
        except Exception as e:
            log_to_ui("db", f"ERROR getting all URLs: {e}")
            return set()
            
    # --- NEW DB Explorer Functions ---
    def get_table_names(self) -> List[str]:
        """Returns a list of all table names in the DB."""
        if not self.conn: return []
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"[DB ERROR] Failed to get table names: {e}")
            return []

    def get_table_data(self, table_name: str) -> Tuple[List[str], List[dict]]:
        """Returns headers and rows for a given table, limited to 100."""
        if not self.conn: return [], []
        
        # --- Security Check ---
        # Validate table_name against a known good list to prevent SQL injection
        known_tables = self.get_table_names()
        if table_name not in known_tables:
            return ["Error"], [{"Error": f"Table '{table_name}' does not exist."}]
        
        cursor = self.conn.cursor()
        try:
            # Safe to use f-string now after validation
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 100;")
            
            headers = [desc[0] for desc in cursor.description]
            rows = [dict(row) for row in cursor.fetchall()]
            return headers, rows
        except Exception as e:
            print(f"[DB ERROR] Failed to get data for table {table_name}: {e}")
            return ["Error"], [{"Error": str(e)}]

def writer_thread_task(db: Database):
    """
    The single, dedicated database writer thread.
    Reads from DATA_QUEUE and performs all DB writes sequentially.
    NOW ACCEPTS the DB object.
    """
    commit_counter = 0
    running = True
    
    while running:
        try:
            item = DATA_QUEUE.get(timeout=3) # Wait 3s for new items
            if item is None: # Stop signal
                running = False
                DATA_QUEUE.task_done()
                break
            
            if not db.conn:
                log_to_ui("db", "DB connection lost. Writer thread stopping.")
                running = False
                DATA_QUEUE.task_done()
                break

            url = item.get("url")
            result = item.get("result")
            error_msg = item.get("error")
            title = result.get("title", "Unknown") if result else "Unknown"
            current_type = GLOBAL_STATE["current_scrape_type"] # Check current scrape type
            
            log_to_ui("db", f"WRITING: {title}")
            
            if result:
                show_id = db.insert_show(result)
                if show_id:
                    if result.get("type") in ["series", "anime"]:
                        db.insert_seasons_episodes_servers(show_id, result.get("seasons", []))
                        # FIX: Only decrement counts if NOT in sync mode
                        if current_type != "sync":
                            if result.get("type") == "anime":
                                GLOBAL_STATE["counts"]["anime"] -= 1
                            else:
                                GLOBAL_STATE["counts"]["series"] -= 1
                    else:
                        db.insert_movie_servers(show_id, result.get("streaming_servers", []))
                         # FIX: Only decrement counts if NOT in sync mode
                        if current_type != "sync":
                            GLOBAL_STATE["counts"]["movies"] -= 1
                    
                    db.mark_progress(url, "completed", show_id)
                    GLOBAL_STATE["progress"]["completed"] += 1
                else:
                    db.mark_progress(url, "failed", error="Duplicate or DB insert error")
                    GLOBAL_STATE["progress"]["failed"] += 1
            else:
                # This is a failure (scrape fail OR redflag)
                db.mark_progress(url, "failed", error=error_msg)
                GLOBAL_STATE["progress"]["failed"] += 1
                # FIX: Only decrement counts if NOT in sync mode
                if current_type != "sync":
                    if "فيلم" in url or "movie" in url:
                        GLOBAL_STATE["counts"]["movies"] -= 1
                    elif "انمي" in url or "anime" in url: # FIX: Added 'or "anime" in url'
                        GLOBAL_STATE["counts"]["anime"] -= 1
                    elif "مسلسل" in url or "series" in url:
                        GLOBAL_STATE["counts"]["series"] -= 1

            GLOBAL_STATE["progress"]["pending"] -= 1
            commit_counter += 1
            if commit_counter >= 20: # Commit every 20 writes
                db.conn.commit()
                commit_counter = 0
                
            DATA_QUEUE.task_done()
            
        except queue.Empty:
            # --- FIX: This is the critical fix for the race condition ---
            # Check if fetchers are done *only if* the main scraper thread is no longer running
            if not GLOBAL_STATE["scraper_running"] and DATA_QUEUE.qsize() == 0:
                # This check ensures we only stop if the main thread has confirmed all fetchers are done
                running = False
            # If scraper is still running, just loop and wait for more items
            pass
        except Exception as e:
            log_to_ui("db", f"WRITER ERROR: {e}")
            if 'item' in locals() and item:
                DATA_QUEUE.task_done()

    log_to_ui("status", "Writer thread committing and shutting down.")
    db.close() # Commits final changes and closes connection

# --- Main Scraper Control ---

def fetcher_task(url: str):
    """The task for each fetcher thread. Scrapes one URL."""
    try:
        if STOP_EVENT.is_set(): return
        
        result, error = run_single(url)
        
        if STOP_EVENT.is_set(): return

        if result:
            title = result.get("title", "Unknown")
            if result.get("type") == "movie":
                log_to_ui("fetch", f"✅ [SUCCESS] Scraped {title} ({len(result.get('streaming_servers', []))} servers)")
            else:
                log_to_ui("fetch", f"✅ [SUCCESS] Scraped {title} ({len(result.get('seasons', []))} seasons)")
            DATA_QUEUE.put({"url": url, "result": result, "error": None})
        else:
            if error and not error.startswith("Redflag"):
                log_to_ui("fetch", f"🔥 [ERROR] ✗ FAILED: {url.split('/')[-2]}")
            DATA_QUEUE.put({"url": url, "result": None, "error": error})
    except Exception as e:
        log_to_ui("fetch", f"🔥 [ERROR] ✗ ERROR: {url.split('/')[-2]} ({e})")
        DATA_QUEUE.put({"url": url, "result": None, "error": str(e)})

def start_scraper_thread(pending_urls: List[str], scrape_type: str = "all"):
    """Main control function to start the writer and fetcher pool."""
    
    # Determine worker count based on scrape type
    if scrape_type == "movies":
        worker_count = FETCHER_WORKERS_MOVIES
    elif scrape_type in ["series", "anime", "sync"]: # FIX: Added sync
        worker_count = FETCHER_WORKERS_SERIES
    else:  # "all"
        # Use lower count for safety when scraping all types
        worker_count = FETCHER_WORKERS_SERIES
    
    # 1. Start the single writer thread
    # It needs its own DB connection.
    db = Database(DB_PATH) 
    if not db.conn:
        log_to_ui("status", "FATAL: Could not start writer thread. DB connection failed.")
        GLOBAL_STATE["scraper_running"] = False
        GLOBAL_STATE["current_scrape_type"] = None
        return
        
    writer = threading.Thread(target=writer_thread_task, args=(db,), name="WriterThread")
    writer.start()
        
    if not pending_urls:
        log_to_ui("status", f"No pending {scrape_type} URLs found.")
    else:
        # 2. Start Fetchers
        log_to_ui("status", f"Scraping {len(pending_urls)} {scrape_type} URLs with {worker_count} workers...")
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="Fetcher") as executor:
            try:
                # Submit tasks
                for url in pending_urls:
                    if STOP_EVENT.is_set():
                        break
                    executor.submit(fetcher_task, url)
                
                # Wait for tasks to complete (or be cancelled)
                executor.shutdown(wait=True)

            except KeyboardInterrupt:
                log_to_ui("status", "Stop signal received! Shutting down fetchers...")
                STOP_EVENT.set()
                executor.shutdown(wait=False, cancel_futures=True)

        log_to_ui("status", f"Finished scraping {scrape_type}. Waiting for writer...")

    # 3. Signal and Stop
    DATA_QUEUE.put(None) # Signal writer thread to stop
    writer.join() # Wait for writer to finish
    
    # 4. Check if there's a next type to scrape
    # FIX: Do not auto-chain if this was a 'sync' task
    if GLOBAL_STATE["scrape_queue"] and not STOP_EVENT.is_set() and scrape_type != "sync":
        next_type = GLOBAL_STATE["scrape_queue"].pop(0)
        log_to_ui("status", f"Auto-starting next scrape type: {next_type}")
        GLOBAL_STATE["current_scrape_type"] = next_type
        
        # Load next batch of URLs
        try:
            db = Database(DB_PATH)
            next_pending_urls = db.populate_and_get_pending_urls(next_type)
            db.close()
            
            # Continue with next type
            start_scraper_thread(next_pending_urls, next_type)
        except Exception as e:
            log_to_ui("status", f"Failed to start next scrape type: {e}")
            GLOBAL_STATE["scraper_running"] = False
            GLOBAL_STATE["current_scrape_type"] = None
            GLOBAL_STATE["scrape_queue"] = []
    else:
        # All done
        GLOBAL_STATE["scraper_running"] = False
        GLOBAL_STATE["current_scrape_type"] = None
        GLOBAL_STATE["scrape_queue"] = []
        log_to_ui("status", "All scraping tasks completed!")

def sync_thread_task(sitemap_url: str):
    """Fetches sitemap, parses URLs, finds new/updated shows, and starts scraper."""
    try:
        log_to_ui("status", f"Starting sync from {sitemap_url}...")
        soup = fetch_html(sitemap_url)
        
        if not soup:
            log_to_ui("status", f"ERROR: Could not fetch sitemap URL.")
            GLOBAL_STATE["scraper_running"] = False
            GLOBAL_STATE["current_scrape_type"] = None
            return

        urls_to_scrape = set()
        sitemap_links = soup.select("#content table tbody tr a")

        log_to_ui("status", f"Parsing {len(sitemap_links)} links from sitemap...")

        for link in sitemap_links:
            href = link.get('href')
            if not href: continue

            if "فيلم" in href or REGEX_PATTERNS['movie'].search(href):
                urls_to_scrape.add(href)
            else:
                match = REGEX_PATTERNS['base_show_url'].search(href)
                if match:
                    base_url = match.group(1) + '/'
                    urls_to_scrape.add(base_url)
        
        log_to_ui("status", f"Found {len(urls_to_scrape)} unique shows/movies to sync.")
        if not urls_to_scrape:
            log_to_ui("status", "Sync complete. No items found.")
            GLOBAL_STATE["scraper_running"] = False
            GLOBAL_STATE["current_scrape_type"] = None
            return

        db = Database(DB_PATH)
        if not db.conn:
             log_to_ui("status", "FATAL: Could not start sync. DB connection failed.")
             GLOBAL_STATE["scraper_running"] = False
             GLOBAL_STATE["current_scrape_type"] = None
             return
             
        cursor = db.conn.cursor()
        existing_urls = db.get_all_urls_from_progress()
        
        pending_urls = []
        new_item_count = 0

        for url in urls_to_scrape:
            if url not in existing_urls:
                cursor.execute("INSERT OR IGNORE INTO scrape_progress (url) VALUES (?)", (url,))
                pending_urls.append(url)
                new_item_count += 1
            else:
                # It's an existing show, re-scrape it to check for new episodes
                pending_urls.append(url)
        
        db.conn.commit()
        db.close()

        log_to_ui("status", f"Found {new_item_count} new items. Syncing {len(pending_urls)} total items...")
        
        # Reload stats to update totals
        load_initial_stats()
        # Override pending count to just what we are scraping
        GLOBAL_STATE["progress"]["pending"] = len(pending_urls)
        
        # Call the main scraper engine with our prepared list
        start_scraper_thread(pending_urls, "sync")

    except Exception as e:
        log_to_ui("status", f"ERROR during sync: {e}")
        GLOBAL_STATE["scraper_running"] = False
        GLOBAL_STATE["current_scrape_type"] = None

    
# --- Flask Web Server ---

app = Flask(__name__)

# --- HTML Templates ---

# Main dashboard template
MAIN_PAGE_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SCRAPER-TERMINAL v2.3</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=VT323&family=Share+Tech+Mono&display=swap');
        
        :root {
            --bg-color: #0a0e14;
            --terminal-bg: #0f1419;
            --border-color: #00ff41;
            --text-color: #00ff41;
            --text-dim: #00aa33;
            --accent-color: #00ffff;
            --success-color: #00ff41;
            --fail-color: #ff0055;
            --warn-color: #ffaa00;
            --redflag-color: #ff6e00;
            --glow: 0 0 10px #00ff41, 0 0 20px #00ff41;
        }

        body.theme-blue {
            --border-color: #00ffff;
            --text-color: #00ffff;
            --text-dim: #00aaaa;
            --accent-color: #00ff41;
            --success-color: #00ff41;
            --fail-color: #ff5555;
            --warn-color: #ffff00;
            --redflag-color: #ffaa00;
            --glow: 0 0 10px #00ffff, 0 0 20px #00ffff;
        }

        body.theme-amber {
            --border-color: #ffc400;
            --text-color: #ffc400;
            --text-dim: #b38a00;
            --accent-color: #00ffff;
            --success-color: #00ff41;
            --fail-color: #ff4444;
            --warn-color: #ffaa00;
            --redflag-color: #ff6e00;
            --glow: 0 0 10px #ffc400, 0 0 20px #ffc400;
        }
        
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }
        
        body {
            font-family: 'Share Tech Mono', monospace;
            background: var(--bg-color);
            color: var(--text-color);
            overflow-x: hidden;
            background-image: 
                repeating-linear-gradient(0deg, rgba(0,255,65,0.03) 0px, transparent 1px, transparent 2px, rgba(0,255,65,0.03) 3px),
                repeating-linear-gradient(90deg, rgba(0,255,65,0.03) 0px, transparent 1px, transparent 2px, rgba(0,255,65,0.03) 3px);
            transition: color 0.3s, background-color 0.3s;
        }

        body.theme-blue {
            background-image: 
                repeating-linear-gradient(0deg, rgba(0,255,255,0.03) 0px, transparent 1px, transparent 2px, rgba(0,255,255,0.03) 3px),
                repeating-linear-gradient(90deg, rgba(0,255,255,0.03) 0px, transparent 1px, transparent 2px, rgba(0,255,255,0.03) 3px);
        }
        body.theme-amber {
            background-image: 
                repeating-linear-gradient(0deg, rgba(255,196,0,0.03) 0px, transparent 1px, transparent 2px, rgba(255,196,0,0.03) 3px),
                repeating-linear-gradient(90deg, rgba(255,196,0,0.03) 0px, transparent 1px, transparent 2px, rgba(255,196,0,0.03) 3px);
        }
        
        .scanline {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: linear-gradient(to bottom, transparent 50%, rgba(0,255,65,0.02) 51%);
            background-size: 100% 4px;
            pointer-events: none;
            z-index: 9999;
            animation: scanline 8s linear infinite;
        }
        
        @keyframes scanline {
            0% { background-position: 0 0; }
            100% { background-position: 0 100%; }
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
        
        header {
            border: 2px solid var(--border-color);
            padding: 20px;
            margin-bottom: 20px;
            background: var(--terminal-bg);
            box-shadow: var(--glow);
            animation: flicker 0.15s infinite alternate;
            transition: border-color 0.3s, box-shadow 0.3s;
        }
        
        @keyframes flicker {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.97; }
        }
        
        .terminal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
            margin-bottom: 15px;
        }
        
        h1 {
            font-family: 'VT323', monospace;
            font-size: 32px;
            letter-spacing: 2px;
            text-shadow: var(--glow);
            animation: glitch 3s infinite;
            transition: text-shadow 0.3s;
        }
        
        @keyframes glitch {
            0%, 90%, 100% { text-shadow: var(--glow); }
            92% { text-shadow: 2px 0 0 var(--fail-color), -2px 0 0 var(--accent-color); }
            94% { text-shadow: -2px 0 0 var(--fail-color), 2px 0 0 var(--accent-color); }
        }

        .header-utils {
            display: flex;
            align-items: center;
            gap: 15px;
        }

        .header-utils a {
            font-family: 'Share Tech Mono', monospace;
            font-size: 14px;
            padding: 8px 12px;
            border: 1px solid var(--border-color);
            background: var(--terminal-bg);
            color: var(--text-color);
            text-decoration: none;
            border-radius: 8px;
            transition: all 0.2s;
        }
        .header-utils a:hover {
            background: var(--border-color);
            color: var(--terminal-bg);
            box-shadow: 0 0 15px var(--border-color);
        }

        .theme-selector {
            display: flex;
            gap: 5px;
            border: 1px solid var(--text-dim);
            border-radius: 8px;
            padding: 5px;
        }
        .theme-selector span {
            padding: 5px 8px;
            cursor: pointer;
            border-radius: 5px;
            transition: all 0.2s;
            font-size: 12px;
        }
        .theme-selector span:hover {
            background: var(--text-dim);
            color: var(--terminal-bg);
        }
        .theme-selector span.active {
            background: var(--border-color);
            color: var(--terminal-bg);
            font-weight: bold;
        }

        .controls {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-bottom: 15px; /* Added margin */
        }
        
        .controls button {
            font-family: 'Share Tech Mono', monospace;
            font-size: 14px;
            padding: 12px 20px;
            border: 2px solid var(--border-color);
            background: var(--terminal-bg);
            color: var(--text-color);
            cursor: pointer;
            transition: all 0.2s;
            text-transform: uppercase;
            letter-spacing: 1px;
            position: relative;
            overflow: hidden;
            border-radius: 8px;
        }
        
        .controls button::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: rgba(255,255,255,0.2);
            transition: left 0.3s;
        }
        
        .controls button:hover:not(:disabled)::before {
            left: 100%;
        }
        
        .controls button:hover:not(:disabled) {
            box-shadow: 0 0 15px var(--border-color);
            transform: translateY(-2px);
        }
        
        .controls button:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        
        .controls button.running {
            animation: pulse 1s infinite;
            border-color: var(--accent-color);
            color: var(--accent-color);
        }
        
        @keyframes pulse {
            0%, 100% { box-shadow: 0 0 5px var(--accent-color); }
            50% { box-shadow: 0 0 20px var(--accent-color); }
        }
        
        .controls button.stop-btn {
            border-color: var(--fail-color);
            color: var(--fail-color);
        }
        
        .controls button.stop-btn:hover {
            background: var(--fail-color);
            color: var(--terminal-bg);
            box-shadow: 0 0 15px var(--fail-color);
        }
        
        .controls button.stopped {
            display: none;
        }

        /* NEW: Sitemap controls */
        .controls-sitemap {
            display: flex;
            gap: 10px;
            width: 100%;
        }
        .controls-sitemap input[type="text"] {
            flex: 1;
            background: var(--terminal-bg);
            border: 2px solid var(--border-color);
            color: var(--text-color);
            padding: 12px 20px;
            font-family: 'Share Tech Mono', monospace;
            font-size: 14px;
            border-radius: 8px;
            transition: border-color 0.3s;
        }
        .controls-sitemap input[type="text"]::placeholder {
            color: var(--text-dim);
        }
        .controls-sitemap button {
            font-family: 'Share Tech Mono', monospace;
            font-size: 14px;
            padding: 12px 20px;
            border: 2px solid var(--warn-color);
            background: var(--terminal-bg);
            color: var(--warn-color);
            cursor: pointer;
            transition: all 0.2s;
            text-transform: uppercase;
            letter-spacing: 1px;
            border-radius: 8px;
        }
        .controls-sitemap button:hover:not(:disabled) {
            background: var(--warn-color);
            color: var(--terminal-bg);
            box-shadow: 0 0 15px var(--warn-color);
        }
        .controls-sitemap button:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        
        .terminal-panel {
            border: 2px solid var(--border-color);
            background: var(--terminal-bg);
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 0 10px rgba(0,255,65,0.3);
            transition: border-color 0.3s, box-shadow 0.3s;
        }
        
        .panel-header {
            font-size: 18px;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid var(--text-dim);
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: border-color 0.3s;
        }
        
        .panel-header::before {
            content: '> ';
            color: var(--accent-color);
        }
        
        .status-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: var(--text-dim);
            margin-left: 10px;
            animation: blink 2s infinite;
        }
        
        .status-indicator.active {
            background: var(--success-color);
            box-shadow: 0 0 10px var(--success-color);
        }
        
        @keyframes blink {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        
        .stat-box {
            border: 1px solid var(--text-dim);
            padding: 15px;
            text-align: center;
            background: rgba(0,0,0,0.1);
            transition: all 0.3s;
        }
        
        .stat-box:hover {
            border-color: var(--border-color);
            background: rgba(0,0,0,0.2);
            transform: scale(1.05);
        }
        
        .stat-box strong {
            display: block;
            font-size: 32px;
            font-family: 'VT323', monospace;
            margin-bottom: 5px;
            text-shadow: 0 0 10px currentColor;
        }
        
        .stat-box span {
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-dim);
        }
        
        .progress-container {
            margin-top: 20px;
            border: 1px solid var(--border-color);
            height: 30px;
            position: relative;
            overflow: hidden;
            background: rgba(0,0,0,0.5);
            transition: border-color 0.3s;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, var(--success-color), var(--accent-color));
            transition: width 0.5s, background 0.3s;
            position: relative;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            color: var(--terminal-bg);
            text-shadow: none;
        }
        
        .progress-fill::after {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent);
            animation: shimmer 2s infinite;
        }
        
        @keyframes shimmer {
            0% { transform: translateX(-100%); }
            100% { transform: translateX(100%); }
        }
        
        .logs-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }
        
        @media (max-width: 968px) {
            .logs-grid {
                grid-template-columns: 1fr;
            }
        }
        
        .log-panel {
            border: 2px solid var(--border-color);
            background: var(--terminal-bg);
            padding: 15px;
            height: 400px;
            display: flex;
            flex-direction: column;
            box-shadow: 0 0 10px rgba(0,255,65,0.3);
            transition: border-color 0.3s, box-shadow 0.3s;
        }
        
        .log-header {
            font-size: 16px;
            margin-bottom: 10px;
            padding-bottom: 8px;
            border-bottom: 1px solid var(--text-dim);
            color: var(--accent-color);
            transition: border-color 0.3s, color 0.3s;
        }
        
        .log-content {
            flex: 1;
            overflow-y: auto;
            overflow-x: hidden;
            font-size: 13px;
            line-height: 1.6;
            padding: 10px;
            background: rgba(0,0,0,0.3);
            border: 1px solid var(--text-dim);
            transition: border-color 0.3s;
        }
        
        .log-content::-webkit-scrollbar {
            width: 10px;
        }
        
        .log-content::-webkit-scrollbar-track {
            background: var(--terminal-bg);
            border-left: 1px solid var(--text-dim);
        }
        
        .log-content::-webkit-scrollbar-thumb {
            background: var(--border-color);
            box-shadow: inset 0 0 5px rgba(255,255,255,0.5);
        }
        
        .log-content::-webkit-scrollbar-thumb:hover {
            background: var(--success-color);
        }
        
        #live-fetch-logs {
            display: flex;
            flex-direction: column;
        }
        
        .log-line {
            padding: 2px 0;
            white-space: pre-wrap;
            word-break: break-all;
            animation: fadeIn 0.3s;
        }
        
        @keyframes fadeIn {
            from { opacity: 0; transform: translateX(-10px); }
            to { opacity: 1; transform: translateX(0); }
        }
        
        .log-line.success { color: var(--success-color); }
        .log-line.success::before { content: '✅ '; }
        
        .log-line.error { color: var(--fail-color); }
        .log-line.error::before { content: '🔥 '; }
        
        .log-line.warn { color: var(--warn-color); }
        .log-line.warn::before { content: '⚠️ '; }

        .log-line.redflag { color: var(--redflag-color); }
        .log-line.redflag::before { content: '🟠 '; }

        .log-line.debug { color: var(--accent-color); }
        .log-line.debug::before { content: '➡️ '; }

        .log-line.start { color: var(--text-color); font-weight: bold; }
        .log-line.start::before { content: 'START: '; color: var(--text-dim); }

        .log-line.info { color: var(--text-dim); }
        .log-line.info::before { content: '$ '; color: var(--text-dim); }
        
        #live-db-log {
            padding: 10px;
            background: rgba(0,0,0,0.3);
            border: 1px solid var(--text-dim);
            color: var(--accent-color);
            font-size: 14px;
            min-height: 50px;
            display: flex;
            align-items: center;
            transition: border-color 0.3s, color 0.3s;
        }
        
        #live-db-log::before {
            content: '>>> ';
            color: var(--success-color);
            font-weight: bold;
        }
        
        .typing-cursor::after {
            content: '▊';
            animation: blink 1s infinite;
        }
        
        .queue-info {
            font-size: 12px;
            color: var(--warn-color);
            margin-top: 10px;
            padding: 8px;
            border: 1px dashed var(--text-dim);
            background: rgba(255,170,0,0.05);
        }
        
        .queue-info::before {
            content: '⚡ AUTO-CHAIN QUEUE: ';
            font-weight: bold;
        }
    </style>
</head>
<body class="theme-green">
    <div class="scanline"></div>
    <div class="container">
        <header>
            <div class="terminal-header">
                <h1>█ SCRAPER-TERMINAL v2.3 █</h1>
                <div class="header-utils">
                    <div class="theme-selector">
                        <span id="theme-green" class="active" onclick="setTheme('theme-green')">Green</span>
                        <span id="theme-blue" onclick="setTheme('theme-blue')">Blue</span>
                        <span id="theme-amber" onclick="setTheme('theme-amber')">Amber</span>
                    </div>
                    <a href="/db" target="_blank">🗂️ DB Explorer</a>
                    <a href="/api/download_db">💾 Download DB</a>
                </div>
            </div>
            <div class="controls">
                <button id="start-movies-btn">▶ MOVIES</button>
                <button id="start-series-btn">▶ SERIES</button>
                <button id="start-anime-btn">▶ ANIME</button>
                <button id="stop-btn" class="stop-btn stopped">⏹ ABORT</button>
            </div>
            <div class="controls-sitemap">
                <input type="text" id="sitemap-url-input" placeholder="https://topcinema.pro/sitemap-pt-post-2025-11.html">
                <button id="start-sync-btn">⟳ SYNC</button>
            </div>
        </header>

        <div class="terminal-panel">
            <div class="panel-header">
                <span id="status-message" class="typing-cursor">SYSTEM IDLE</span>
                <span class="status-indicator" id="status-led"></span>
            </div>
            <div class="stats-grid">
                <div class="stat-box">
                    <strong id="pending" style="color: var(--warn-color)">0</strong>
                    <span>PENDING</span>
                </div>
                <div class="stat-box">
                    <strong id="completed" style="color: var(--success-color)">0</strong>
                    <span>COMPLETED</span>
                </div>
                <div class="stat-box">
                    <strong id="failed" style="color: var(--fail-color)">0</strong>
                    <span>FAILED</span>
                </div>
                <div class="stat-box">
                    <strong id="movies" style="color: var(--accent-color)">0</strong>
                    <span>MOVIES</span>
                </div>
                <div class="stat-box">
                    <strong id="series" style="color: var(--accent-color)">0</strong>
                    <span>SERIES</span>
                </div>
                <div class="stat-box">
                    <strong id="anime" style="color: var(--accent-color)">0</strong>
                    <span>ANIME</span>
                </div>
            </div>
            <div class="progress-container">
                <div class="progress-fill" id="progress-fill" style="width: 0%;">0%</div>
            </div>
            <div class="queue-info" id="queue-info" style="display: none;"></div>
        </div>

        <div class="logs-grid">
            <div class="log-panel">
                <div class="log-header">▸ FETCH OPERATIONS</div>
                <div class="log-content" id="live-fetch-logs">
                    <div class="log-line info">Awaiting commands...</div>
                </div>
            </div>
            <div class="log-panel">
                <div class="log-header">▸ DATABASE WRITER</div>
                <div class="log-content">
                    <div id="live-db-log">Idle...</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        const startMoviesBtn = document.getElementById('start-movies-btn');
        const startSeriesBtn = document.getElementById('start-series-btn');
        const startAnimeBtn = document.getElementById('start-anime-btn');
        const startSyncBtn = document.getElementById('start-sync-btn');
        const sitemapUrlInput = document.getElementById('sitemap-url-input');
        const stopBtn = document.getElementById('stop-btn');
        const statusMsg = document.getElementById('status-message');
        const statusLed = document.getElementById('status-led');
        const queueInfo = document.getElementById('queue-info');
        
        const pendingEl = document.getElementById('pending');
        const completedEl = document.getElementById('completed');
        const failedEl = document.getElementById('failed');
        
        const moviesEl = document.getElementById('movies');
        const seriesEl = document.getElementById('series');
        const animeEl = document.getElementById('anime');
        
        const progressFill = document.getElementById('progress-fill');
        const dbLogEl = document.getElementById('live-db-log');
        const fetchLogEl = document.getElementById('live-fetch-logs');
        
        let userScrolledUp = false;

        // --- Theme ---
        function setTheme(themeName) {
            document.body.className = themeName;
            localStorage.setItem('scraperTheme', themeName);
            // Update active button
            document.querySelectorAll('.theme-selector span').forEach(span => {
                span.classList.remove('active');
            });
            document.getElementById(themeName.replace('.', '')).classList.add('active');
        }
        
        // On page load, apply saved theme
        document.addEventListener('DOMContentLoaded', () => {
            const savedTheme = localStorage.getItem('scraperTheme') || 'theme-green';
            setTheme(savedTheme);
            
            // Set default sitemap URL
            const now = new Date();
            const year = now.getFullYear();
            const month = (now.getMonth() + 1).toString().padStart(2, '0');
            sitemapUrlInput.value = `https://topcinema.pro/sitemap-pt-post-${year}-${month}.html`;
        });

        // Update time display
        function updateTime() {
            // Not needed for terminal theme
        }
        
        // Auto-scroll management
        fetchLogEl.addEventListener('scroll', () => {
            const isAtBottom = fetchLogEl.scrollHeight - fetchLogEl.scrollTop <= fetchLogEl.clientHeight + 50;
            userScrolledUp = !isAtBottom;
        });

        function updateUI(data) {
            // Update buttons based on running state
            const isRunning = data.scraper_running;
            const currentType = data.current_scrape_type;
            
            // Disable all start buttons if running
            startMoviesBtn.disabled = isRunning;
            startSeriesBtn.disabled = isRunning;
            startAnimeBtn.disabled = isRunning;
            startSyncBtn.disabled = isRunning;
            
            if (isRunning) {
                statusLed.classList.add('active');
                stopBtn.classList.remove('stopped');
                
                if (currentType === 'movies') {
                    startMoviesBtn.classList.add('running');
                } else if (currentType === 'series') {
                    startSeriesBtn.classList.add('running');
                } else if (currentType === 'anime') {
                    startAnimeBtn.classList.add('running');
                } else if (currentType === 'sync') {
                    startSyncBtn.classList.add('running');
                    startSyncBtn.textContent = 'SYNCING...';
                }
            } else {
                statusLed.classList.remove('active');
                stopBtn.classList.add('stopped');
                startMoviesBtn.classList.remove('running');
                startSeriesBtn.classList.remove('running');
                startAnimeBtn.classList.remove('running');
                startSyncBtn.classList.remove('running');
                startSyncBtn.textContent = '⟳ SYNC';
            }
            
            statusMsg.textContent = data.status_message.toUpperCase();
            
            if (data.scrape_queue && data.scrape_queue.length > 0) {
                queueInfo.style.display = 'block';
                queueInfo.textContent = '⚡ AUTO-CHAIN QUEUE: ' + data.scrape_queue.map(t => t.toUpperCase()).join(' → ');
            } else {
                queueInfo.style.display = 'none';
            }

            const progress = data.progress;
            pendingEl.textContent = progress.pending < 0 ? 0 : progress.pending;
            completedEl.textContent = progress.completed;
            failedEl.textContent = progress.failed;
            
            const counts = data.counts;
            moviesEl.textContent = counts.movies < 0 ? 0 : counts.movies;
            seriesEl.textContent = counts.series < 0 ? 0 : counts.series;
            animeEl.textContent = counts.anime < 0 ? 0 : counts.anime;

            let percent = 0;
            if (progress.total > 0) {
                percent = ((progress.completed + progress.failed) / progress.total) * 100;
            }
            progressFill.style.width = percent + '%';
            progressFill.textContent = Math.round(percent) + '%';
            
            dbLogEl.textContent = data.live_db_log || 'Idle...';
            
            // --- New Log Parsing ---
            const wasAtBottom = fetchLogEl.scrollHeight - fetchLogEl.scrollTop <= fetchLogEl.clientHeight + 50;

            fetchLogEl.innerHTML = (data.live_fetch_logs || []).map(log => {
                let level = 'info'; // Default
                if (log.startsWith('✅ [SUCCESS]')) {
                    level = 'success';
                } else if (log.startsWith('🔥 [ERROR]')) {
                    level = 'error';
                } else if (log.startsWith('⚠️ [WARN]')) {
                    level = 'warn';
                } else if (log.startsWith('🟠 [REDFLAG]')) {
                    level = 'redflag';
                } else if (log.startsWith('➡️ [DEBUG]')) {
                    level = 'debug';
                } else if (log.startsWith('START:')) {
                    level = 'start';
                }
                
                // Clean the log message (remove prefix for display)
                const displayLog = log.replace(/^\[\w+\]\s*/, '').replace(/^(✅|🔥|⚠️|🟠|➡️)\s*/, ''); 
                
                return `<div class="log-line ${level}">${displayLog}</div>`;
            }).join('');
            
            if (!userScrolledUp && wasAtBottom) {
                fetchLogEl.scrollTop = fetchLogEl.scrollHeight;
            }
        }

        async function fetchStatus() {
            try {
                const response = await fetch('/api/status');
                if (!response.ok) return;
                const data = await response.json();
                updateUI(data);
            } catch (e) {
                // Server might be restarting
            }
        }

        async function startScraper(type) {
            try {
                const response = await fetch(`/api/start/${type}`, { method: 'POST' });
                const result = await response.json();
                if (!result.success) {
                    statusMsg.textContent = result.message.toUpperCase();
                }
                fetchStatus();
            } catch (e) {
                statusMsg.textContent = `ERROR STARTING ${type.toUpperCase()} SCRAPER`;
            }
        }
        
        async function startSync() {
            const url = sitemapUrlInput.value;
            if (!url) {
                statusMsg.textContent = "SITEMAP URL IS REQUIRED";
                return;
            }
            if (!url.startsWith('http')) {
                statusMsg.textContent = "INVALID SITEMAP URL";
                return;
            }
            
            try {
                const response = await fetch(`/api/sync`, { 
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ url: url })
                });
                const result = await response.json();
                if (!result.success) {
                    statusMsg.textContent = result.message.toUpperCase();
                }
                fetchStatus();
            } catch (e) {
                statusMsg.textContent = `ERROR STARTING SYNC`;
            }
        }

        startMoviesBtn.addEventListener('click', async () => await startScraper('movies'));
        startSeriesBtn.addEventListener('click', async () => await startScraper('series'));
        startAnimeBtn.addEventListener('click', async () => await startScraper('anime'));
        startSyncBtn.addEventListener('click', async () => await startSync());

        stopBtn.addEventListener('click', async () => {
            try {
                stopBtn.disabled = true;
                await fetch('/api/stop', { method: 'POST' });
                fetchStatus();
            } catch (e) {
                statusMsg.textContent = 'ERROR STOPPING SCRAPER';
            } finally {
                setTimeout(() => stopBtn.disabled = false, 1000);
            }
        });

        setInterval(fetchStatus, 1000);
        fetchStatus();
    </script>
</body>
</html>
"""

# DB Explorer base page template
DB_PAGE_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DB Explorer</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=VT323&family=Share+Tech+Mono&display=swap');
        :root { --bg-color: #0a0e14; --terminal-bg: #0f1419; --border-color: #00ff41; --text-color: #00ff41; --text-dim: #00aa33; }
        body { font-family: 'Share Tech Mono', monospace; background: var(--bg-color); color: var(--text-color); }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        header { border: 2px solid var(--border-color); padding: 20px; margin-bottom: 20px; background: var(--terminal-bg); }
        h1 { font-family: 'VT323', monospace; font-size: 32px; letter-spacing: 2px; }
        .table-list { list-style: none; padding: 0; }
        .table-list li { margin: 10px 0; }
        .table-list a { color: var(--text-color); font-size: 18px; text-decoration: none; padding: 8px; border: 1px solid var(--text-dim); border-radius: 8px; transition: all 0.2s; }
        .table-list a:hover { background: var(--border-color); color: var(--terminal-bg); border-color: var(--border-color); }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🗂️ DB Explorer</h1>
        </header>
        <h2>Tables:</h2>
        <ul class="table-list">
            {% for table in tables %}
            <li><a href="/db/{{ table }}">{{ table }}</a></li>
            {% endfor %}
        </ul>
    </div>
</body>
</html>
"""

# DB Explorer table view template
DB_TABLE_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DB Explorer: {{ table_name }}</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=VT323&family=Share+Tech+Mono&display=swap');
        :root { --bg-color: #0a0e14; --terminal-bg: #0f1419; --border-color: #00ff41; --text-color: #00ff41; --text-dim: #00aa33; }
        body { font-family: 'Share Tech Mono', monospace; background: var(--bg-color); color: var(--text-color); }
        .container { max-width: 95%; margin: 0 auto; padding: 20px; }
        header { border: 2px solid var(--border-color); padding: 20px; margin-bottom: 20px; background: var(--terminal-bg); }
        h1 { font-family: 'VT323', monospace; font-size: 32px; letter-spacing: 2px; }
        a { color: var(--border-color); }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; border: 1px solid var(--text-dim); }
        th, td { border: 1px solid var(--text-dim); padding: 10px; text-align: left; max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        th { background: var(--terminal-bg); color: var(--border-color); font-size: 14px; }
        td { font-size: 12px; }
        tr:nth-child(even) { background: var(--terminal-bg); }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🗂️ DB Explorer: {{ table_name }}</h1>
            <a href="/db">&larr; Back to Tables</a>
        </header>
        <table>
            <thead>
                <tr>
                    {% for header in headers %}
                    <th>{{ header }}</th>
                    {% endfor %}
                </tr>
            </thead>
            <tbody>
                {% for row in rows %}
                <tr>
                    {% for header in headers %}
                    <td>{{ row[header] }}</td>
                    {% endfor %}
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</body>
</html>
"""


@app.route('/')
def index():
    """Main dashboard with retro hacker terminal theme"""
    return render_template_string(MAIN_PAGE_TEMPLATE)

@app.route('/api/status')
def api_status():
    """Returns the current state of the scraper."""
    # Convert deque to list for JSON serialization
    state_copy = GLOBAL_STATE.copy()
    state_copy["live_fetch_logs"] = list(GLOBAL_STATE["live_fetch_logs"])
    return jsonify(state_copy)

@app.route('/api/start/<scrape_type>', methods=['POST'])
def api_start(scrape_type):
    """
    Starts the scraper in a background thread for specific type.
    Auto-chains to the next type (movies -> series -> anime)
    scrape_type: 'movies', 'series', 'anime'
    """
    global SCRAPER_THREAD
    
    # Validate scrape_type
    if scrape_type not in ['movies', 'series', 'anime']:
        return jsonify({"success": False, "message": "Invalid scrape type. Use 'movies', 'series', or 'anime'."}), 400
    
    if not GLOBAL_STATE['scraper_running']:
        GLOBAL_STATE["scraper_running"] = True
        GLOBAL_STATE["current_scrape_type"] = scrape_type
        
        # Set up auto-chain queue: next types to scrape after current one
        if scrape_type == "movies":
            GLOBAL_STATE["scrape_queue"] = ["series", "anime"]
        elif scrape_type == "series":
            GLOBAL_STATE["scrape_queue"] = ["anime", "movies"]
        elif scrape_type == "anime":
            GLOBAL_STATE["scrape_queue"] = ["movies", "series"]
        
        STOP_EVENT.clear()
        # GLOBAL_STATE["live_fetch_logs"].clear() # FIX: Don't clear logs
        GLOBAL_STATE["live_db_log"] = "..."
        
        # Load stats in the main thread to prevent race condition
        pending_urls = []
        try:
            db = Database(DB_PATH)
            pending_urls = db.populate_and_get_pending_urls(scrape_type) # Filter by type
            db.close() # Close the temp connection
        except Exception as e:
            log_to_ui("status", f"Failed to get pending URLs: {e}")
            GLOBAL_STATE["scraper_running"] = False
            GLOBAL_STATE["current_scrape_type"] = None
            GLOBAL_STATE["scrape_queue"] = []
            return jsonify({"success": False, "message": "Failed to load URLs from DB."})
        
        # Pass the pre-fetched list to the scraper thread
        SCRAPER_THREAD = threading.Thread(target=start_scraper_thread, args=(pending_urls, scrape_type), daemon=True)
        SCRAPER_THREAD.start()
        
        return jsonify({"success": True, "message": f"Scraper started for {scrape_type}. Will auto-chain to next types."})
    return jsonify({"success": False, "message": "Scraper already running."})

@app.route('/api/sync', methods=['POST'])
def api_sync():
    """
    Starts the sitemap sync and update process.
    """
    global SYNC_THREAD
    
    sitemap_url = request.json.get('url')
    if not sitemap_url:
        return jsonify({"success": False, "message": "Sitemap URL is required."}), 400
    
    if not GLOBAL_STATE['scraper_running']:
        GLOBAL_STATE["scraper_running"] = True
        GLOBAL_STATE["current_scrape_type"] = "sync"
        GLOBAL_STATE["scrape_queue"] = [] # Sync does not chain
        
        STOP_EVENT.clear()
        # GLOBAL_STATE["live_fetch_logs"].clear() # FIX: Don't clear logs
        GLOBAL_STATE["live_db_log"] = "..."
        
        # Start the sync process in a new thread
        SYNC_THREAD = threading.Thread(target=sync_thread_task, args=(sitemap_url,), daemon=True)
        SYNC_THREAD.start()
        
        return jsonify({"success": True, "message": f"Sync started from {sitemap_url}."})
    return jsonify({"success": False, "message": "Scraper already running."})


@app.route('/api/stop', methods=['POST'])
def api_stop():
    """Sets the stop event to gracefully shut down the scraper and clear the queue."""
    if GLOBAL_STATE['scraper_running']:
        log_to_ui("status", "Stop signal received... finishing current tasks...")
        STOP_EVENT.set()
        GLOBAL_STATE["scrape_queue"] = []  # Clear the auto-chain queue
        return jsonify({"success": True, "message": "Stop signal sent."})
    return jsonify({"success": False, "message": "Scraper not running."})

# --- NEW: DB Download Route ---
@app.route('/api/download_db')
def download_db():
    """Provides the database file for download."""
    try:
        return send_file(DB_PATH, as_attachment=True, download_name='scrapped.db')
    except Exception as e:
        log_to_ui("status", f"Error downloading DB: {e}")
        return "Error: Could not find or read database file.", 404

# --- NEW: DB Explorer Routes ---
@app.route('/db')
def db_explorer():
    """Displays a list of all tables in the database."""
    db = Database(DB_PATH)
    if not db.conn:
        return "Error: Could not connect to database.", 500
    tables = db.get_table_names()
    db.close()
    return render_template_string(DB_PAGE_TEMPLATE, tables=tables)

@app.route('/db/<table>')
def db_view_table(table):
    """Displays the data for a specific table."""
    db = Database(DB_PATH)
    if not db.conn:
        return "Error: Could not connect to database.", 500
    headers, rows = db.get_table_data(table)
    db.close()
    return render_template_string(DB_TABLE_TEMPLATE, table_name=table, headers=headers, rows=rows)

# --- Main Execution ---

def load_initial_stats():
    """Loads stats from the DB on startup to populate the UI."""
    try:
        db = Database(DB_PATH)
        db.get_initial_stats()
        db.close() # Close the connection immediately
    except Exception as e:
        log_to_ui("status", f"Failed to load initial stats: {e}")

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    
    # Create dummy JSON files if they don't exist
    for f in JSON_FILES:
        if not os.path.exists(f):
            try:
                with open(f, 'w') as new_file:
                    json.dump({"urls": []}, new_file)
            except Exception as e:
                print(f"[ERROR] Could not create file {f}: {e}")

    # Initialize DB schema
    try:
        init_database(DB_PATH)
    except Exception as e:
        print(f"[FATAL] Could not initialize database: {e}")
        exit(1)
    
    # --- FIX: Load stats on launch ---
    load_initial_stats()
    
    # Suppress all terminal output
    print(f"\n--- WEB UI WORKING ! ---")
    print(f"Access at: http://127.0.0.1:{SERVER_PORT}")
    print(f"DB Explorer at: http://127.0.0.1:{SERVER_PORT}/db")
    print("------------------------")
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=SERVER_PORT, debug=False, use_reloader=False)

