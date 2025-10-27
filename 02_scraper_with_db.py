"""
Enhanced TopCinema scraper that saves to SQLite database with resumable progress
"""
import json
import os
import re
import time
import sqlite3
from typing import List, Dict, Optional
from urllib.parse import unquote, urlparse, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

import requests
from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.table import Table

console = Console()
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

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

def scrape_series(url: str) -> Optional[Dict]:
    """Scrape series"""
    soup = fetch_html(url)
    if not soup:
        return None
    
    details = extract_media_details(soup, "series")

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

    for season in seasons:
        s_num = season["season_number"]
        if s_num in season_urls:
            season["episodes"] = scrape_season_episodes(season_urls[s_num])

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
        "type": "series",
        "imdb_rating": details["imdb_rating"],
        "poster": details["poster"],
        "synopsis": details["synopsis"],
        "metadata": details["metadata"],
        "trailer": trailer_url,
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

def run_single(url_input: str) -> Optional[Dict]:
    """Main scraping function"""
    url = url_input.strip()
    
    if REGEX_PATTERNS['movie'].search(url):
        return scrape_movie(url)
    else:
        return scrape_series(url)

def cleanup():
    """Cleanup resources before exit"""
    try:
        SESSION.close()
        log("Resources cleaned up", level="debug")
    except Exception as e:
        logger.warning(f"Cleanup error: {e}")

if __name__ == "__main__":
    import sys
    
    # Process both JSON files in order
    json_files = [
        "data/series_animes.json",
        "data/movies.json"
    ]
    
    total_success = 0
    total_errors = 0
    overall_start = time.time()
    
    for json_file in json_files:
        if not os.path.exists(json_file):
            console.print(f"[yellow]Warning: {json_file} not found, skipping...[/yellow]")
            continue
        
        console.print(f"\n[bold magenta]{'='*60}[/bold magenta]")
        console.print(f"[bold magenta]Processing: {json_file}[/bold magenta]")
        console.print(f"[bold magenta]{'='*60}[/bold magenta]\n")
        
        # Get pending URLs (skip already completed)
        urls = db.get_pending_urls(json_file)
        
        if not urls:
            console.print(f"[green]✓ All URLs from {json_file} already scraped![/green]")
            continue
        
        console.print(f"[bold blue]Found {len(urls)} pending URLs to scrape[/bold blue]")
        
        # Initialize progress tracking
        db.init_progress(urls)
        
        # Scrape each URL
        start_time = time.time()
        success_count = 0
        error_count = 0
        
        for idx, url in enumerate(urls, 1):
            console.print(f"\n[bold cyan][{idx}/{len(urls)}] Scraping: {url}[/bold cyan]")
            
            try:
                result = run_single(url)
                if result:
                    show_id = db.insert_show(result)
                    if show_id:
                        if result.get("type") in ["series", "anime"]:
                            db.insert_seasons_and_episodes(show_id, result.get("seasons", []))
                        else:
                            db.insert_movie_servers(show_id, result.get("streaming_servers", []))
                        
                        db.mark_progress(url, "completed", show_id)
                        success_count += 1
                        total_success += 1
                        console.print(f"[green]✓ Saved to database[/green]")
                    else:
                        db.mark_progress(url, "failed", error="Duplicate show")
                        error_count += 1
                        total_errors += 1
                else:
                    db.mark_progress(url, "failed", error="Scraping returned no data")
                    error_count += 1
                    total_errors += 1
                    console.print(f"[red]✗ Failed to scrape[/red]")
            except Exception as e:
                db.mark_progress(url, "failed", error=str(e))
                error_count += 1
                total_errors += 1
                console.print(f"[red]✗ Error: {str(e)[:100]}[/red]")
        
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
        
        console.print(f"\n[bold]{json_file} Summary:[/bold]")
        console.print(f"[green]✓ Completed: {success_count}[/green]")
        console.print(f"[red]✗ Failed: {error_count}[/red]")
        console.print(f"[cyan]⏱ Time: {time_str}[/cyan]")
    
    # Overall summary
    total_elapsed = time.time() - overall_start
    total_minutes = int(total_elapsed // 60)
    total_seconds = int(total_elapsed % 60)
    total_time_str = f"{total_minutes}m {total_seconds}s" if total_minutes > 0 else f"{total_seconds}s"
    
    console.print(f"\n[bold magenta]{'='*60}[/bold magenta]")
    console.print(f"[bold magenta]OVERALL SUMMARY[/bold magenta]")
    console.print(f"[bold magenta]{'='*60}[/bold magenta]")
    console.print(f"[green]✓ Total Completed: {total_success}[/green]")
    console.print(f"[red]✗ Total Failed: {total_errors}[/red]")
    console.print(f"[cyan]⏱ Total Time: {total_time_str}[/cyan]")
    
    cleanup()
