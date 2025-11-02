"""
Enhanced TopCinema scraper with web dashboard, retry logic, and improved error handling
Features: Single-line progress, web status server on port 8080, failed URLs export
"""
import json
import os
import re
import time
import sqlite3
import sys
from typing import List, Dict, Optional
from urllib.parse import unquote, urlparse, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
import logging

import requests
from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.table import Table

console = Console()
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings()

# Global stats for web dashboard
STATS = {
    'current_file': '',
    'current_url': '',
    'total_urls': 0,
    'completed': 0,
    'failed': 0,
    'success': 0,
    'start_time': time.time(),
    'failed_urls': [],
    'current_show': '',
    'episodes_found': 0,
    'servers_found': 0
}

class Database:
    def __init__(self, db_path: str = "data/scraper.db"):
        self.db_path = db_path
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    
    def slugify(self, text: str) -> str:
        """Convert text to URL-friendly slug"""
        import re
        text = text.lower().strip()
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[-\s]+', '-', text)
        return text[:100]
    
    def insert_show(self, show_data: Dict) -> Optional[int]:
        """Insert show and return ID"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            title = show_data.get("title")
            slug = self.slugify(title)
            metadata = show_data.get("metadata", {})
            
            # Convert lists to comma-separated strings
            def to_string(value):
                if isinstance(value, list):
                    return ", ".join(str(v) for v in value if v)
                return str(value) if value else None
            
            # Extract year from metadata or title
            year = show_data.get("year")
            if not year:
                # Try to get from metadata
                year_str = metadata.get("release_year") or metadata.get("year")
                if year_str:
                    import re
                    match = re.search(r'(\d{4})', str(year_str))
                    if match:
                        year = int(match.group(1))
            
            cursor.execute("""
            INSERT INTO shows (title, slug, type, poster, synopsis, imdb_rating, trailer, year, 
                             genres, cast, directors, country, language, duration, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                title,
                slug,
                show_data.get("type"),
                show_data.get("poster"),
                show_data.get("synopsis"),
                show_data.get("imdb_rating"),
                show_data.get("trailer"),
                year,
                to_string(metadata.get("genres")),
                to_string(metadata.get("cast")),
                to_string(metadata.get("directors")),
                to_string(metadata.get("country")),
                to_string(metadata.get("language")),
                to_string(metadata.get("duration")),
                show_data.get("source_url")
            ))
            show_id = cursor.lastrowid
            
            conn.commit()
            return show_id
        except sqlite3.IntegrityError as e:
            console.print(f"[yellow]Show '{show_data.get('title')}' already exists[/yellow]")
            return None
        finally:
            conn.close()
    
    def insert_seasons_and_episodes(self, show_id: int, seasons: List[Dict]):
        """Insert seasons, episodes and servers for a show"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            for season in seasons:
                season_num = season.get("season_number", 1)
                season_poster = season.get("poster")
                
                # Insert season
                cursor.execute("""
                INSERT OR IGNORE INTO seasons (show_id, season_number, poster)
                VALUES (?, ?, ?)
                """, (show_id, season_num, season_poster))
                
                season_id = cursor.lastrowid
                if season_id == 0:  # Already exists, get the ID
                    cursor.execute("""
                    SELECT id FROM seasons WHERE show_id = ? AND season_number = ?
                    """, (show_id, season_num))
                    result = cursor.fetchone()
                    if result:
                        season_id = result[0]
                
                # Insert episodes
                for episode in season.get("episodes", []):
                    cursor.execute("""
                    INSERT OR IGNORE INTO episodes (season_id, episode_number)
                    VALUES (?, ?)
                    """, (season_id, episode.get("episode_number")))
                    
                    episode_id = cursor.lastrowid
                    if episode_id == 0:  # Already exists, get the ID
                        cursor.execute("""
                        SELECT id FROM episodes WHERE season_id = ? AND episode_number = ?
                        """, (season_id, episode.get("episode_number")))
                        result = cursor.fetchone()
                        if result:
                            episode_id = result[0]
                    
                    # Insert servers
                    for server in episode.get("servers", []):
                        cursor.execute("""
                        INSERT INTO servers (episode_id, server_number, embed_url)
                        VALUES (?, ?, ?)
                        """, (episode_id, server.get("server_number"), server.get("embed_url")))
            
            conn.commit()
        finally:
            conn.close()
    
    def insert_movie_servers(self, show_id: int, servers: List[Dict]):
        """Insert servers for a movie (movies don't have seasons/episodes)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Create a dummy season and episode for movies
            cursor.execute("""
            INSERT OR IGNORE INTO seasons (show_id, season_number)
            VALUES (?, 1)
            """, (show_id,))
            
            season_id = cursor.lastrowid
            if season_id == 0:
                cursor.execute("SELECT id FROM seasons WHERE show_id = ? AND season_number = 1", (show_id,))
                result = cursor.fetchone()
                if result:
                    season_id = result[0]
            
            cursor.execute("""
            INSERT OR IGNORE INTO episodes (season_id, episode_number)
            VALUES (?, 1)
            """, (season_id,))
            
            episode_id = cursor.lastrowid
            if episode_id == 0:
                cursor.execute("SELECT id FROM episodes WHERE season_id = ? AND episode_number = 1", (season_id,))
                result = cursor.fetchone()
                if result:
                    episode_id = result[0]
            
            for server in servers:
                cursor.execute("""
                INSERT INTO servers (episode_id, server_number, embed_url)
                VALUES (?, ?, ?)
                """, (episode_id, server.get("server_number"), server.get("embed_url")))
            
            conn.commit()
        finally:
            conn.close()
    

    def mark_progress(self, url: str, status: str, show_id: Optional[int] = None, error: Optional[str] = None):
        """Update scraping progress"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
            INSERT OR REPLACE INTO scrape_progress (url, status, show_id, error_message, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (url, status, show_id, error))
            conn.commit()
        finally:
            conn.close()
    
    def get_pending_urls(self, json_file: str) -> List[str]:
        """Get URLs that haven't been scraped yet from a JSON file"""
        import json
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        all_urls = data if isinstance(data, list) else data.get("urls", [])
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Get completed URLs
        cursor.execute("SELECT url FROM scrape_progress WHERE status = 'completed'")
        completed = {row[0] for row in cursor.fetchall()}
        conn.close()
        
        # Return only pending URLs
        return [url for url in all_urls if url not in completed]
    
    def init_progress(self, urls: List[str]):
        """Initialize progress tracking for URLs"""
        conn = self.get_connection()
        cursor = conn.cursor()
        for url in urls:
            cursor.execute("INSERT OR IGNORE INTO scrape_progress (url, status) VALUES (?, 'pending')", (url,))
        conn.commit()
        conn.close()

class StatusHandler(BaseHTTPRequestHandler):
    """Web dashboard handler"""
    def log_message(self, format, *args):
        pass  # Suppress server logs
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.send_header('Refresh', '2')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        elapsed = time.time() - STATS['start_time']
        hours, minutes, seconds = int(elapsed // 3600), int((elapsed % 3600) // 60), int(elapsed % 60)
        progress_pct = (STATS['completed'] / STATS['total_urls'] * 100) if STATS['total_urls'] > 0 else 0
        success_rate = (STATS['success'] / STATS['completed'] * 100) if STATS['completed'] > 0 else 0
        
        html = f"""<!DOCTYPE html><html><head><title>Scraper Status</title><style>
body{{font-family:Arial;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:#fff;padding:20px;margin:0}}
.container{{max-width:1200px;margin:0 auto;background:rgba(255,255,255,0.1);backdrop-filter:blur(10px);border-radius:20px;padding:30px;box-shadow:0 8px 32px 0 rgba(31,38,135,0.37)}}
h1{{text-align:center;margin-bottom:30px;font-size:2.5em;text-shadow:2px 2px 4px rgba(0,0,0,0.3)}}
.stats-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:20px;margin-bottom:30px}}
.stat-card{{background:rgba(255,255,255,0.2);padding:20px;border-radius:15px;text-align:center}}
.stat-value{{font-size:2.5em;font-weight:bold;margin:10px 0}}
.stat-label{{font-size:0.9em;opacity:0.9}}
.progress-bar{{width:100%;height:40px;background:rgba(255,255,255,0.2);border-radius:20px;overflow:hidden;margin:20px 0}}
.progress-fill{{height:100%;background:linear-gradient(90deg,#00d2ff 0%,#3a47d5 100%);transition:width 0.3s ease;display:flex;align-items:center;justify-content:center;font-weight:bold}}
.current-info{{background:rgba(255,255,255,0.15);padding:20px;border-radius:15px;margin-top:20px}}
.current-info h3{{margin-top:0;border-bottom:2px solid rgba(255,255,255,0.3);padding-bottom:10px}}
.info-row{{margin:10px 0;padding:8px;background:rgba(0,0,0,0.2);border-radius:8px}}
.failed-list{{max-height:200px;overflow-y:auto;background:rgba(0,0,0,0.2);padding:15px;border-radius:10px;margin-top:10px}}
.failed-item{{padding:5px;margin:5px 0;background:rgba(255,0,0,0.2);border-left:3px solid #f44;border-radius:5px;font-size:0.9em}}
.success{{color:#4ade80}}.error{{color:#f87171}}
</style></head><body><div class="container">
<h1>🎬 TopCinema Scraper Dashboard</h1>
<div class="stats-grid">
<div class="stat-card"><div class="stat-label">Total URLs</div><div class="stat-value">{STATS['total_urls']}</div></div>
<div class="stat-card"><div class="stat-label">Completed</div><div class="stat-value success">{STATS['completed']}</div></div>
<div class="stat-card"><div class="stat-label">Success</div><div class="stat-value success">{STATS['success']}</div></div>
<div class="stat-card"><div class="stat-label">Failed</div><div class="stat-value error">{STATS['failed']}</div></div>
<div class="stat-card"><div class="stat-label">Success Rate</div><div class="stat-value">{success_rate:.1f}%</div></div>
<div class="stat-card"><div class="stat-label">Elapsed Time</div><div class="stat-value">{hours:02d}:{minutes:02d}:{seconds:02d}</div></div>
</div>
<div class="progress-bar"><div class="progress-fill" style="width:{progress_pct}%">{progress_pct:.1f}%</div></div>
<div class="current-info">
<h3>📂 Current File</h3><div class="info-row">{STATS['current_file']}</div>
<h3>🔗 Current URL</h3><div class="info-row" style="word-break:break-all;font-size:0.85em">{STATS['current_url']}</div>
<h3>🎭 Current Show</h3><div class="info-row">{STATS['current_show']}</div>
<div class="info-row">📺 Episodes: {STATS['episodes_found']} | 🖥️ Servers: {STATS['servers_found']}</div>
</div>
{f'<div class="current-info"><h3 class="error">❌ Failed URLs ({len(STATS["failed_urls"])})</h3><div class="failed-list">{"".join([f"<div class=\\'failed-item\\'>{url}</div>" for url in STATS["failed_urls"][-20:]])}</div></div>' if STATS['failed_urls'] else ''}
</div></body></html>"""
        
        self.wfile.write(html.encode('utf-8'))

def start_web_server():
    """Start web server in background"""
    server = HTTPServer(('0.0.0.0', 8080), StatusHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server

db = Database()

REGEX_PATTERNS = {
    'number': re.compile(r'(\d+)'),
    'movie': re.compile(r'(\/فيلم-|\/film-|\/movie-|%d9%81%d9%8a%d9%84%d9%85)', re.IGNORECASE),
    'episode': re.compile(r'(?:الحلقة|Episode)\s*(\d+)'),
    'watch_suffix': re.compile(r'/watch/?$'),
    'title_prefix': re.compile(r'^(انمي|مسلسل)\s+'),
    'episode_id': re.compile(r'"id"\s*:\s*"(\d+)"'),
    'title_clean_prefix': re.compile(
        r'^\s*(فيلم|انمي|مسلسل|anime|film|movie|series)\s+',
        re.IGNORECASE | re.UNICODE
    ),
    'title_clean_suffix': re.compile(
        r'\s+(مترجم|اون\s*لاين|اونلاين|online|مترجمة|مدبلج|مدبلجة)(\s+|$)',
        re.IGNORECASE | re.UNICODE
    )
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": "https://web7.topcinema.cam/",
    "Upgrade-Insecure-Requests": "1",
}

REQUEST_TIMEOUT = 15
REQUEST_DELAY = 0.3

SESSION = requests.Session()
try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    retry_strategy = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(
        pool_connections=50,
        pool_maxsize=100,
        max_retries=retry_strategy,
        pool_block=False
    )
    SESSION.mount('https://', adapter)
    SESSION.mount('http://', adapter)
    
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception as e:
    logger.warning(f"Failed to configure session adapter: {e}")

VERIFY_SSL = os.environ.get('VERIFY_SSL', 'false').lower() == 'true'

ARABIC_ORDINALS = {
    "الاول": 1, "الأول": 1, "الثاني": 2, "ثاني": 2,
    "الثالث": 3, "ثالث": 3, "الرابع": 4, "رابع": 4,
    "الخامس": 5, "خامس": 5, "السادس": 6, "سادس": 6,
    "السابع": 7, "سابع": 7, "الثامن": 8, "ثامن": 8,
    "التاسع": 9, "تاسع": 9, "العاشر": 10, "عاشر": 10,
}

def log(msg: str, level: str = "info") -> None:
    """Enhanced logging with Rich"""
    timestamp = time.strftime("%H:%M:%S")
    
    if level == "info":
        console.print(f"[{timestamp}] {msg}", style="dim")
    elif level == "success":
        console.print(f"[{timestamp}] {msg}", style="bold green")
    elif level == "warning":
        console.print(f"[{timestamp}] {msg}", style="bold yellow")
    elif level == "error":
        console.print(f"[{timestamp}] {msg}", style="bold red")
    elif level == "debug":
        pass

def fetch_html(url: str) -> Optional[BeautifulSoup]:
    """Fetch and parse HTML"""
    if not url.startswith(('http://', 'https://')):
        logger.error(f"Invalid URL scheme: {url}")
        return None
        
    try:
        time.sleep(REQUEST_DELAY)
        resp = SESSION.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        log(f"Request failed for {url}: {str(e)[:50]}", level="error")
    return None

def extract_number_from_text(text: str) -> Optional[int]:
    """Extract number from Arabic or English text"""
    if not text:
        return None
    m = REGEX_PATTERNS['number'].search(text)
    if m:
        return int(m.group(1))
    lower = text.replace("ي", "ى").replace("أ", "ا").replace("إ", "ا").strip()
    for word, num in ARABIC_ORDINALS.items():
        if word in lower:
            return num
    return None

def clean_title(title: str) -> str:
    """Remove prefixes and suffixes from titles"""
    if not title:
        return title
    
    cleaned = REGEX_PATTERNS['title_clean_prefix'].sub('', title)
    
    prev = ""
    while prev != cleaned:
        prev = cleaned
        cleaned = REGEX_PATTERNS['title_clean_suffix'].sub(' ', cleaned)
    
    cleaned = ' '.join(cleaned.split())
    cleaned = cleaned.strip(' -–—|:،؛')
    
    return cleaned

def get_trailer_embed_url(page_url: str, form_url: str) -> Optional[str]:
    """Fetch trailer with separate URLs for page and form data"""
    try:
        p = urlparse(page_url)
        base = f"{p.scheme}://{p.netloc}"
        trailer_endpoint = base + "/wp-content/themes/movies2023/Ajaxat/Home/LoadTrailer.php"
        
        # URL-encode the form_url for the POST data
        # Use quote with safe=':/' to preserve URL structure but encode Arabic chars
        encoded_form_url = quote(form_url, safe=':/')
        data_str = f"href={encoded_form_url}"
        data_bytes = data_str.encode('utf-8')
        
        # Exact headers that worked in curl
        # Ensure referer is ASCII-safe by encoding it properly
        safe_referer = quote(page_url, safe=':/')
        trailer_headers = {
            "accept": "*/*",
            "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "sec-ch-ua": "\"Google Chrome\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "x-requested-with": "XMLHttpRequest",
            "referer": safe_referer
        }
        
        resp = SESSION.post(trailer_endpoint, 
                          headers=trailer_headers, 
                          data=data_bytes,
                          timeout=REQUEST_TIMEOUT,
                          verify=VERIFY_SSL)
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.text, "html.parser")
        iframe = soup.find("iframe")
        if iframe and iframe.get("src"):
            trailer_src = iframe["src"].strip()
            return trailer_src
        
        return None
    except Exception as e:
        try:
            error_msg = repr(e).encode('utf-8', errors='replace').decode('utf-8')[:50]
        except:
            error_msg = "Unknown error"
        log(f"Trailer fetch error: {error_msg}", level="warning")
        return None

def get_episode_servers(episode_id: str, referer: Optional[str] = None, total_servers: int = 10) -> List[Dict]:
    """Fetch streaming servers for an episode"""
    servers: List[Dict] = []
    if referer:
        p = urlparse(referer)
        base = f"{p.scheme}://{p.netloc}"
    else:
        base = "https://web7.topcinema.cam"
    
    server_url = base + "/wp-content/themes/movies2023/Ajaxat/Single/Server.php"
    server_headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest",
    }
    if referer:
        server_headers["Referer"] = referer

    def fetch_one(i: int):
        max_retries = 3
        retry_delay = 0.5
        
        for attempt in range(max_retries):
            try:
                data = {"id": str(episode_id), "i": str(i)}
                resp = SESSION.post(server_url, headers=server_headers, data=data, timeout=5, verify=VERIFY_SSL)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                iframe = soup.find("iframe")
                if iframe and iframe.get("src") and iframe.get("src").strip():
                    return {"server_number": i, "embed_url": iframe.get("src").strip()}
                return None
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    return None
            except Exception:
                return None
        
        return None

    with ThreadPoolExecutor(max_workers=min(12, total_servers)) as ex:
        futures = {ex.submit(fetch_one, i): i for i in range(total_servers)}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                servers.append(res)

    servers.sort(key=lambda x: x.get("server_number", 0))
    return servers

def extract_episode_id_from_watch_page(soup: BeautifulSoup) -> Optional[str]:
    """Extract episode ID from watch page HTML"""
    if not soup:
        return None
    
    li = soup.select_one(".watch--servers--list li.server--item[data-id]")
    if li and li.has_attr("data-id"):
        return li["data-id"].strip()
    
    for script in soup.find_all("script"):
        if script.string:
            m = REGEX_PATTERNS['episode_id'].search(script.string)
            if m:
                return m.group(1)
    return None

def scrape_season_episodes(season_url: str) -> List[Dict]:
    """Scrape all episodes from a season page"""
    list_url = season_url.rstrip('/') + '/list/' if not season_url.endswith('/list/') else season_url
    
    soup = fetch_html(list_url)
    if not soup:
        return []

    episodes: List[Dict] = []
    seen = set()

    anchors = soup.select('.allepcont .row > a')
    if not anchors:
        anchors = [x for x in soup.find_all('a') if (x.find(class_='epnum') or (x.get('title') and 'الحلقة' in x.get('title')))]

    log(f"Found {len(anchors)} episode links to process", level="info")

    def process_episode(a):
        try:
            raw_href = a.get('href')
            ep_title = a.get('title', '')
            
            ep_num = None
            em = a.find('em')
            if em:
                m = REGEX_PATTERNS['number'].search(em.get_text())
                if m:
                    ep_num = int(m.group(1))
            
            if not ep_num:
                epdiv = a.find(class_='epnum')
                if epdiv:
                    m = REGEX_PATTERNS['number'].search(epdiv.get_text())
                    if m:
                        ep_num = int(m.group(1))
            
            if not ep_num:
                src_text = ep_title or ''
                h2 = a.find('h2')
                if h2 and not src_text:
                    src_text = h2.get_text(strip=True)
                m = REGEX_PATTERNS['episode'].search(src_text)
                if m:
                    ep_num = int(m.group(1))
            
            if not ep_num or ep_num == 0:
                ep_num = 999
            
            ep_poster = None
            img = a.find('img')
            if img:
                ep_poster = img.get('src') or img.get('data-src')
            
            key = (str(ep_num) or ep_title or raw_href or '').strip()
            if key in seen:
                return None
            seen.add(key)
            
            watch_url = raw_href
            if not REGEX_PATTERNS['watch_suffix'].search(raw_href):
                watch_url = raw_href.rstrip('/') + '/watch/'
            
            ep_watch_soup = fetch_html(watch_url)
            episode_id = None
            
            if ep_watch_soup:
                episode_id = extract_episode_id_from_watch_page(ep_watch_soup)
            
            server_list: List[Dict] = []
            if episode_id:
                server_list = get_episode_servers(episode_id, referer=watch_url, total_servers=10)
            
            if ep_num != 999:
                console.print(f"Episode {ep_num:2d} - {len(server_list):2d} servers", style="cyan")
            
            return {
                "episode_number": ep_num,
                "servers": server_list
            }
        except Exception as e:
            log(f"Error processing episode: {str(e)[:50]}", level="error")
            return None

    with ThreadPoolExecutor(max_workers=10) as ex:
        try:
            for res in ex.map(process_episode, anchors):
                if res:
                    episodes.append(res)
        except Exception as e:
            log(f"Error in thread pool: {str(e)}", level="error")
            raise

    episodes.sort(key=lambda e: e.get("episode_number", 999))
    episodes = [e for e in episodes if e.get("episode_number", 999) != 999]
    
    return episodes

def scrape_series(url: str, force_type: str = "series") -> Optional[Dict]:
    """Scrape series"""
    soup = fetch_html(url)
    if not soup:
        return None
    
    details = extract_media_details(soup, force_type)
    STATS['current_show'] = details.get('title', 'Unknown')

    seasons: List[Dict] = []
    season_urls: Dict[int, str] = {}
    seen_urls = set()
    
    for s_el in soup.select('div.Small--Box.Season'):
        a_el = s_el.find('a')
        if not a_el or not a_el.get('href'):
            continue
        s_url = a_el.get('href')
        if s_url in seen_urls:
            continue
        seen_urls.add(s_url)
        s_title = a_el.get('title') or a_el.get_text(strip=True) or ""
        s_num = None
        en = s_el.find('div', class_='epnum')
        if en:
            s_num = extract_number_from_text(en.get_text(" ", strip=True))
        if not s_num:
            s_num = extract_number_from_text(s_title)
        s_num = s_num or 1
        
        s_poster = None
        season_soup = fetch_html(s_url)
        if season_soup:
            poster_img = season_soup.select_one('.MainSingle .left .image img')
            if poster_img:
                s_poster = poster_img.get('src') or poster_img.get('data-src')
        
        season_urls[s_num] = s_url
        seasons.append({
            "season_number": s_num,
            "poster": s_poster,
            "episodes": []
        })

    if not seasons:
        for a_el in soup.find_all('a', href=True):
            href = a_el['href']
            if '/series/' in href and 'الموسم' in href:
                if href in seen_urls:
                    continue
                seen_urls.add(href)
                s_title = a_el.get('title') or a_el.get_text(strip=True) or ""
                s_num = extract_number_from_text(s_title) or extract_number_from_text(href) or 1
                
                s_poster = None
                season_soup = fetch_html(href)
                if season_soup:
                    poster_img = season_soup.select_one('.MainSingle .left .image img')
                    if poster_img:
                        s_poster = poster_img.get('src') or poster_img.get('data-src')
                
                season_urls[s_num] = href
                seasons.append({
                    "season_number": s_num,
                    "poster": s_poster,
                    "episodes": []
                })

    seasons.sort(key=lambda s: s.get('season_number', 0))

    if not seasons:
        season_urls[1] = url
        seasons.append({
            "season_number": 1,
            "poster": details["poster"],
            "episodes": []
        })

    # Scrape episodes and track stats
    total_episodes = 0
    total_servers = 0
    for season in seasons:
        s_num = season["season_number"]
        if s_num in season_urls:
            season["episodes"] = scrape_season_episodes(season_urls[s_num])
            total_episodes += len(season["episodes"])
            for ep in season["episodes"]:
                total_servers += len(ep.get("servers", []))
    
    STATS['episodes_found'] = total_episodes
    STATS['servers_found'] = total_servers

    episode_page_url = None
    if seasons:
        first_season_url = list(seen_urls)[0] if seen_urls else None
        if first_season_url:
            temp_soup = fetch_html(first_season_url)
            if temp_soup:
                first_ep_link = temp_soup.select_one(".allepcont .row > a")
                if first_ep_link:
                    episode_page_url = first_ep_link.get("href")
    
    trailer_url = None
    if episode_page_url:
        trailer_url = get_trailer_embed_url(url, episode_page_url)
    
    if trailer_url:
        log("Trailer found", level="success")
    else:
        log("No trailer available", level="info")

    return {
        "title": details["title"],
        "type": force_type,
        "imdb_rating": details["imdb_rating"],
        "poster": details["poster"],
        "synopsis": details["synopsis"],
        "metadata": details["metadata"],
        "trailer": trailer_url,
        "year": None,
        "source_url": url,
        "seasons": seasons
    }

def scrape_movie(url: str) -> Optional[Dict]:
    """Scrape movie"""
    if not REGEX_PATTERNS['movie'].search(url):
        log(f"URL {url} doesn't appear to be a movie", level="error")
        return None
    
    log(f"Fetching movie: {url}")
    details_soup = fetch_html(url)
    if not details_soup:
        return None
    
    details = extract_media_details(details_soup, "movie")
    
    watch_url = url.rstrip('/') + '/watch/'
    watch_soup = fetch_html(watch_url)
    if not watch_soup:
        return None
        
    episode_id = extract_episode_id_from_watch_page(watch_soup)
    servers = get_episode_servers(episode_id, referer=watch_url) if episode_id else []
    
    trailer_url = get_trailer_embed_url(url, url)
    
    if trailer_url:
        log("Trailer found", level="success")
    else:
        log("No trailer available", level="info")

    title = details["title"]
    imdb_rating = details["imdb_rating"]
    poster_url = details["poster"]
    story_txt = details["synopsis"]

    return {
        "title": title,
        "type": "movie",
        "year": extract_number_from_text(title),
        "imdb_rating": imdb_rating,
        "poster": poster_url,
        "synopsis": story_txt,
        "metadata": details["metadata"],
        "trailer": trailer_url,
        "source_url": url,
        "streaming_servers": servers,
        "scraped_at": datetime.now().isoformat()
    }

def extract_media_details(soup: BeautifulSoup, media_type: str) -> Dict:
    """Shared function for extracting media details"""
    details = {
        "title": "Unknown",
        "poster": None,
        "synopsis": "",
        "imdb_rating": None,
        "metadata": {}
    }
    
    try:
        title_el = soup.find("h1", class_="post-title")
        if title_el:
            title = title_el.get_text(strip=True)
            details["title"] = clean_title(title)
        
        poster_wrap = soup.find('div', class_='image')
        if poster_wrap:
            img_tag = poster_wrap.find('img')
            if img_tag:
                details["poster"] = img_tag.get('src') or img_tag.get('data-src')
        
        story = soup.find('div', class_='story')
        if story:
            p = story.find('p')
            if p:
                details["synopsis"] = p.get_text(strip=True)
        
        imdb_box = soup.select_one(".UnderPoster .imdbR")
        if imdb_box:
            sp = imdb_box.find("span")
            if sp:
                try:
                    details["imdb_rating"] = float(sp.get_text(strip=True))
                except ValueError:
                    pass
        
        tax = soup.find('ul', class_='RightTaxContent')
        if tax:
            for li in tax.find_all('li'):
                key_el = li.find('span')
                if key_el:
                    key = key_el.get_text(strip=True).replace(':', '')
                    links = [a.get_text(strip=True) for a in li.find_all('a') if a.get_text(strip=True)]
                    details["metadata"][key] = links if links else li.find('strong').get_text(strip=True) if li.find('strong') else ""
    except Exception as e:
        log(f"Error extracting details: {str(e)}", level="error")
    
    key_mapping = {
        "قسم المسلسل": "category",
        "قسم الفيلم": "category",
        "نوع المسلسل": "genres",
        "نوع الفيلم": "genres",
        "النوع": "genres",
        "جودة المسلسل": "quality",
        "جودة الفيلم": "quality",
        "عدد الحلقات": "episode_count",
        "توقيت المسلسل": "duration",
        "توقيت الفيلم": "duration",
        "مدة الفيلم": "duration",
        "موعد الصدور": "release_year",
        "سنة الانتاج": "release_year",
        "لغة المسلسل": "language",
        "لغة الفيلم": "language",
        "دولة المسلسل": "country",
        "دولة الفيلم": "country",
        "المخرجين": "directors",
        "المخرج": "directors",
        "بطولة": "cast"
    }
    
    mapped_metadata = {}
    for k, v in details["metadata"].items():
        clean_key = k.strip().rstrip(':')
        new_key = key_mapping.get(clean_key, clean_key)
        if new_key in key_mapping.values():
            mapped_metadata[new_key] = v
    details["metadata"] = mapped_metadata
    
    return details

def run_single(url_input: str, force_type: str = None) -> Optional[Dict]:
    """Main scraping function"""
    url = url_input.strip()
    
    if REGEX_PATTERNS['movie'].search(url):
        return scrape_movie(url)
    else:
        return scrape_series(url, force_type=force_type or "series")

def cleanup():
    """Cleanup resources before exit"""
    try:
        SESSION.close()
        log("Resources cleaned up", level="debug")
    except Exception as e:
        logger.warning(f"Cleanup error: {e}")

if __name__ == "__main__":
    # Get local IP
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except:
        local_ip = "localhost"
    
    # Clear screen and show minimal output
    os.system('cls' if os.name == 'nt' else 'clear')
    print(f"WORKING° check {local_ip}:8080")
    sys.stdout.flush()
    
    # Start web dashboard
    server = start_web_server()
    
    # Process both JSON files
    json_files = [
        ("data/series_animes.json", "series"),
        ("data/movies.json", "movie")
    ]
    
    STATS['start_time'] = time.time()
    
    # Count total URLs first
    for json_file, _ in json_files:
        if os.path.exists(json_file):
            urls = db.get_pending_urls(json_file)
            STATS['total_urls'] += len(urls)
    
    # Process each file
    for json_file, force_type in json_files:
        if not os.path.exists(json_file):
            continue
        
        STATS['current_file'] = json_file
        urls = db.get_pending_urls(json_file)
        
        if not urls:
            continue
        
        db.init_progress(urls)
        
        # Process each URL (silently)
        for idx, url in enumerate(urls, 1):
            STATS['current_url'] = url
            
            try:
                result = run_single(url, force_type=force_type)
                
                if result:
                    show_id = db.insert_show(result)
                    if show_id:
                        if result.get("type") in ["series", "anime"]:
                            db.insert_seasons_and_episodes(show_id, result.get("seasons", []))
                        else:
                            db.insert_movie_servers(show_id, result.get("streaming_servers", []))
                        
                        db.mark_progress(url, "completed", show_id)
                        STATS['success'] += 1
                    else:
                        db.mark_progress(url, "failed", error="Duplicate")
                        STATS['failed'] += 1
                        STATS['failed_urls'].append(url)
                else:
                    db.mark_progress(url, "failed", error="No data")
                    STATS['failed'] += 1
                    STATS['failed_urls'].append(url)
            except Exception as e:
                db.mark_progress(url, "failed", error=str(e)[:100])
                STATS['failed'] += 1
                STATS['failed_urls'].append(url)
            
            STATS['completed'] += 1
    
    # Export failed URLs (silently)
    if STATS['failed_urls']:
        failed_file = f"data/failed_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(failed_file, 'w', encoding='utf-8') as f:
            json.dump({
                'failed_count': len(STATS['failed_urls']),
                'timestamp': datetime.now().isoformat(),
                'urls': STATS['failed_urls']
            }, f, indent=2, ensure_ascii=False)
    
    cleanup()
    
    # Keep server running (silently)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.shutdown()
