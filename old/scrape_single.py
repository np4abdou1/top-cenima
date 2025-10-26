import json
import os
import re
import time
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from urllib.parse import unquote, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Console colors (Arabic output) with graceful fallback if colorama isn't installed
try:
    from colorama import init as colorama_init, Fore, Style
    colorama_init(autoreset=True)
except Exception:  # pragma: no cover - fallback when colorama is missing
    class _Dummy:
        RESET_ALL = ""
        RED = GREEN = YELLOW = CYAN = BLUE = MAGENTA = WHITE = ""

    Fore = Style = _Dummy()


# Common headers and timeouts
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ar,en-US;q=0.7,en;q=0.3",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

REQUEST_TIMEOUT = 10  # seconds, for GET/POST HTML endpoints

# Reuse a single session with connection pooling for speed
SESSION = requests.Session()
try:
    # Increase connection pool for heavy parallel IO
    adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=0)
    SESSION.mount('https://', adapter)
    SESSION.mount('http://', adapter)
except Exception:
    pass


def log(msg: str, color: Optional[str] = None) -> None:
    try:
        if color:
            print(color + msg + Style.RESET_ALL, flush=True)
        else:
            print(msg, flush=True)
    except Exception:
        pass


def percent_decode(url: str) -> str:
    try:
        return unquote(url)
    except Exception:
        return url


def is_anime_or_series(url_decoded: str) -> bool:
    text = url_decoded or ""
    return ("انمي" in text) or ("مسلسل" in text)





# Arabic ordinal words to numbers for robust season/episode parsing
ARABIC_ORDINALS = {
    "الاول": 1, "الأول": 1,
    "الثاني": 2, "ثاني": 2,
    "الثالث": 3, "ثالث": 3,
    "الرابع": 4, "رابع": 4,
    "الخامس": 5, "خامس": 5,
    "السادس": 6, "سادس": 6,
    "السابع": 7, "سابع": 7,
    "الثامن": 8, "ثامن": 8,
    "التاسع": 9, "تاسع": 9,
    "العاشر": 10, "عاشر": 10,
}


def extract_number_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"(\d+)", text)
    if m:
        return m.group(1)
    # try ordinal mapping
    lower = text.replace("ي", "ى").replace("أ", "ا").replace("إ", "ا").strip()
    for word, num in ARABIC_ORDINALS.items():
        if word in lower:
            return str(num)
    return None


def get_trailer_embed_url(page_url: str, page_soup: Optional[BeautifulSoup] = None) -> Optional[str]:
    """Try to load trailer via AJAX, fallback to parse iframe from page HTML.
    Ensures same-origin by using the input page domain as base and reusing session cookies.
    """
    try:
        p = urlparse(page_url)
        base = f"{p.scheme}://{p.netloc}"
        trailer_url = base + "/wp-content/themes/movies2023/Ajaxat/Home/LoadTrailer.php"
        trailer_headers = {
            "accept": "*/*",
            "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-requested-with": "XMLHttpRequest",
            "Referer": page_url,
        }
        data = {"href": page_url}
        resp = SESSION.post(trailer_url, headers=trailer_headers, data=data, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        iframe = soup.find("iframe")
        if iframe and iframe.get("src"):
            return iframe.get("src")
    except Exception:
        pass
    # Fallback: parse trailer iframe directly from page HTML if provided/available
    try:
        psoup = page_soup or fetch_html(page_url)
        if psoup:
            ifr = psoup.find("iframe", attrs={"id": re.compile(r"trailer", re.I)}) or \
                  psoup.find("iframe", src=re.compile(r"(youtube|vid|trailer)", re.I))
            if ifr and ifr.get("src"):
                return ifr.get("src")
    except Exception:
        pass
    return None


def get_episode_servers(episode_id: str, referer: Optional[str] = None, total_servers: int = 10, indices: Optional[List[int]] = None) -> List[Dict]:
    servers: List[Dict] = []
    # Use same-origin (base) from referer if available to avoid cross-domain
    if referer:
        p = urlparse(referer)
        base = f"{p.scheme}://{p.netloc}"
    else:
        base = "https://web7.topcinema.cam"
    server_url = base + "/wp-content/themes/movies2023/Ajaxat/Single/Server.php"
    server_headers = {
        "accept": "*/*",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest",
    }
    if referer:
        server_headers["Referer"] = referer
    idxs = indices if isinstance(indices, list) and len(indices) > 0 else list(range(total_servers))

    def fetch_one(i: int):
        try:
            data = {"id": str(episode_id), "i": str(i)}
            resp = SESSION.post(server_url, headers=server_headers, data=data, timeout=8)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            iframe = soup.find("iframe")
            if iframe and iframe.get("src") and iframe.get("src").strip():
                return {"server_number": i, "embed_url": iframe.get("src").strip()}
        except Exception:
            return None
        return None

    # Fetch server embeds concurrently (IO-bound), limit workers for politeness
    with ThreadPoolExecutor(max_workers=min(12, max(1, len(idxs)))) as ex:
        future_map = {ex.submit(fetch_one, i): i for i in idxs}
        for fut in as_completed(future_map):
            res = fut.result()
            if res:
                servers.append(res)

    servers.sort(key=lambda x: x.get("server_number", 0))
    return servers


def extract_episode_id_from_watch_page_html(soup: BeautifulSoup) -> Optional[str]:
    # Prefer robust extraction via data-id on servers list
    li = soup.select_one(".watch--servers--list li.server--item[data-id]")
    if li and li.has_attr("data-id"):
        return li["data-id"].strip()
    # Fallback: scan scripts for patterns used previously
    for script in soup.find_all("script"):
        if script.string:
            m = re.search(r'"id"\s*:\s*"(\d+)"', script.string)
            if m:
                return m.group(1)
            m = re.search(r'episode[_-]?id["\s:]*?(\d+)', script.string, re.IGNORECASE)
            if m:
                return m.group(1)
    return None


def fetch_html(url: str) -> Optional[BeautifulSoup]:
    try:
        resp = SESSION.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return BeautifulSoup(resp.content, "html.parser")
    except Exception:
        return None


def get_available_server_indices(watch_soup: BeautifulSoup) -> List[int]:
    idxs: List[int] = []
    try:
        for li in watch_soup.select('.watch--servers--list li.server--item[data-server]'):
            val = li.get('data-server')
            if val is not None and str(val).isdigit():
                idxs.append(int(val))
    except Exception:
        pass
    # Deduplicate and sort
    return sorted(set(idxs))


def find_watch_url_from_detail(detail_url: str) -> Optional[str]:
    """Given an episode detail URL (not /watch/), find the corresponding /watch/ URL on that page."""
    log(f"- فتح صفحة التفاصيل لاستخراج رابط المشاهدة: {detail_url}")
    detail_soup = fetch_html(detail_url)
    if not detail_soup:
        return None
    # Prefer explicit anchors to /watch/
    a = detail_soup.find('a', href=re.compile(r'/watch/?$'))
    if a and a.get('href'):
        return a.get('href')
    # Try buttons/links that likely point to watch
    cand = detail_soup.select_one('a.watch, a.watchAndDownlaod, .WatchBar a[href*="/watch/"]')
    if cand and cand.get('href'):
        return cand.get('href')
    # Fallback: any anchor containing /watch/
    any_watch = detail_soup.find('a', href=re.compile(r'/watch/'))
    if any_watch and any_watch.get('href'):
        return any_watch.get('href')
    return None


def parse_breadcrumbs(soup: BeautifulSoup) -> List[str]:
    items: List[str] = []
    bc_root = soup.select_one(".breadcrumbs #mpbreadcrumbs")
    if not bc_root:
        return items
    for span in bc_root.find_all("span"):
        # Prefer inner text of nested span
        txt = span.get_text(strip=True)
        if txt:
            items.append(txt)
    return items


def scrape_series_details(url: str) -> Optional[Dict]:
    log(f"جلب الصفحة الرئيسية: {url}", color=Fore.CYAN)
    soup = fetch_html(url)
    if not soup:
        return None

    # Title
    title_el = soup.find("h1", class_="post-title")
    title_raw = title_el.get_text(strip=True) if title_el else "Unknown Title"
    title_clean = re.sub(r'^(انمي|مسلسل)\s+', '', title_raw).strip()

    # Type
    series_type = "anime" if ("انمي" in url or "انمي" in title_raw) else "series"

    # IMDb rating and label
    imdb_rating, imdb_label = None, None
    imdb_box = soup.select_one(".UnderPoster .imdbR")
    if imdb_box:
        sp = imdb_box.find("span")
        em = imdb_box.find("em")
        if sp:
            imdb_rating = sp.get_text(strip=True)
        if em:
            imdb_label = em.get_text(strip=True)

    # Poster
    poster_url = None
    poster_wrap = soup.find('div', class_='image')
    if poster_wrap:
        img_tag = poster_wrap.find('img')
        if img_tag:
            poster_url = img_tag.get('src') or img_tag.get('data-src')

    # Story
    story_txt = ""
    story = soup.find('div', class_='story')
    if story:
        p = story.find('p')
        if p:
            story_txt = p.get_text(strip=True)

    # Details (as shown in RightTaxContent)
    details: Dict[str, object] = {}
    tax = soup.find('ul', class_='RightTaxContent')
    if tax:
        for li in tax.find_all('li'):
            key_el = li.find('span')
            if not key_el:
                continue
            key = key_el.get_text(strip=True).replace(':', '')
            links = [a.get_text(strip=True) for a in li.find_all('a') if a.get_text(strip=True)]
            if links:
                details[key] = links
            else:
                strong = li.find('strong')
                if strong:
                    details[key] = strong.get_text(strip=True)

    # Trailer (use soup to avoid extra GET on fallback)
    trailer_embed = get_trailer_embed_url(url, soup)

    # Seasons
    seasons: List[Dict] = []
    seen_urls = set()
    # Primary: CSS selector requiring both classes
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
        s_num = s_num or ""
        s_poster = None
        s_img = s_el.find('img')
        if s_img:
            s_poster = s_img.get('src') or s_img.get('data-src')
        seasons.append({
            "season_number": s_num,
            "title": s_title,
            "url": s_url,
            "poster": s_poster,
            "episodes": []
        })

    # Fallback: scan anchors that look like season pages (contain "الموسم")
    if not seasons:
        for a_el in soup.find_all('a', href=True):
            href = a_el['href']
            if '/series/' in href and 'الموسم' in href:
                if href in seen_urls:
                    continue
                seen_urls.add(href)
                s_title = a_el.get('title') or a_el.get_text(strip=True) or ""
                s_num = extract_number_from_text(s_title) or extract_number_from_text(href) or ""
                seasons.append({
                    "season_number": s_num,
                    "title": s_title,
                    "url": href,
                    "poster": None,
                    "episodes": []
                })

    # Sort seasons by number if available, else keep order
    def _season_key(s: Dict):
        sn = s.get('season_number')
        try:
            return int(sn) if sn and sn.isdigit() else 9999
        except Exception:
            return 9999
    seasons.sort(key=_season_key)

    # Fallback single-season handling
    if not seasons:
        # Create one season placeholder; episodes will be attempted from the same page later
        seasons.append({
            "season_number": "1",
            "title": title_clean,
            "url": url,
            "poster": poster_url,
            "episodes": []
        })

    data = {
        "title": title_clean,
        "type": series_type,
        # Exclude original_url from final output later; keep internally
        "_original_url": url,
        "imdb": {"rating": imdb_rating, "label": imdb_label},
        "poster": poster_url,
        "story": story_txt,
        "details": details,
        "trailer_url": trailer_embed,
        "breadcrumbs": parse_breadcrumbs(soup),
        "seasons": seasons,
    }
    return data


def normalize_to_list_page(season_url: str) -> str:
    if season_url.endswith('/list/'):
        return season_url
    if season_url.endswith('/'):
        return season_url + 'list/'
    return season_url + '/list/'


def scrape_season_episodes(season_url: str) -> List[Dict]:
    def extract_from_soup(_soup: BeautifulSoup) -> List[Dict]:
        eps: List[Dict] = []
        seen = set()
        # Collect anchors from typical episodes listing blocks
        anchors = []
        anchors.extend(_soup.select('.allepcont .row > a'))
        # If none found, also accept any anchor that looks like episode entry (has epnum or title contains "الحلقة")
        if not anchors:
            anchors = [x for x in _soup.find_all('a') if (x.find(class_='epnum') or (x.get('title') and 'الحلقة' in x.get('title')))]
        # As a last resort, take direct /watch/ links
        if not anchors:
            anchors = _soup.find_all('a', href=re.compile(r'/watch/?$'))

        # Pre-parse metadata for all anchors
        items: List[Tuple[str, str, str, Optional[str]]] = []  # (raw_href, ep_num, ep_title, ep_poster)
        for a in anchors:
            raw_href = a.get('href')
            ep_title = a.get('title', '')
            # Episode number: try several places
            ep_num = None
            # 1) <em> text (anime list layout)
            em = a.find('em')
            if em:
                m = re.search(r'(\d+)', em.get_text())
                if m:
                    ep_num = m.group(1)
            # 2) <div class="epnum"> content
            if not ep_num:
                epdiv = a.find(class_='epnum')
                if epdiv:
                    m = re.search(r'(\d+)', epdiv.get_text())
                    if m:
                        ep_num = m.group(1)
            # 3) Extract from title attribute or nested h2 text
            if not ep_num:
                src_text = ep_title or ''
                h2 = a.find('h2')
                if h2 and not src_text:
                    src_text = h2.get_text(strip=True)
                m = re.search(r'(?:الحلقة|Episode)\s*(\d+)', src_text)
                if m:
                    ep_num = m.group(1)
            if not ep_num:
                ep_num = ""

            # Poster
            ep_poster = None
            img = a.find('img')
            if img:
                ep_poster = img.get('data-src') or img.get('src')

            # Deduplicate by episode number if already seen on this page
            key = (ep_num or ep_title or raw_href or '').strip()
            if key in seen:
                continue
            seen.add(key)

            items.append((raw_href, ep_num, ep_title, ep_poster))

        def process_episode(item: Tuple[str, str, str, Optional[str]]) -> Dict:
            raw_href, ep_num, ep_title, ep_poster = item
            # Fetch watch page to get episode id
            episode_id = None
            watch_url = None
            if raw_href:
                if re.search(r'/watch/?$', raw_href):
                    watch_url = raw_href
                else:
                    # Fast path: guess the watch URL by appending /watch/
                    watch_url = raw_href.rstrip('/') + '/watch/'

            ep_watch_soup = None
            if watch_url:
                ep_watch_soup = fetch_html(watch_url)
                # If guessed watch URL failed, fallback to discovering from the detail page
                if not ep_watch_soup and raw_href and not re.search(r'/watch/?$', raw_href):
                    alt_watch = find_watch_url_from_detail(raw_href)
                    if alt_watch and alt_watch != watch_url:
                        watch_url = alt_watch
                        ep_watch_soup = fetch_html(watch_url)

            server_indices: List[int] = []
            if ep_watch_soup:
                episode_id = extract_episode_id_from_watch_page_html(ep_watch_soup)
                server_indices = get_available_server_indices(ep_watch_soup)

            server_list: List[Dict] = []
            if episode_id:
                server_list = get_episode_servers(episode_id, referer=watch_url, total_servers=10, indices=server_indices)
            try:
                log(f"  - حلقة {ep_num or '?'}: تم العثور على {len(server_list)} سيرفرات", color=Fore.GREEN)
            except Exception:
                pass

            return {
                "episode_number": ep_num,
                "title": ep_title,
                "_watch_url": watch_url or raw_href,
                "_episode_id": episode_id,
                "poster": ep_poster,
                "servers": server_list,
            }

        # Process episodes concurrently per page for a big speedup
        with ThreadPoolExecutor(max_workers=12) as ex:
            for res in ex.map(process_episode, items):
                eps.append(res)

        # Sort episodes by episode_number if numeric
        def _ep_key(e: Dict):
            val = e.get("episode_number") or ""
            try:
                return int(val)
            except Exception:
                # fallback: try extract digits from title or watch url
                t = e.get("title") or e.get("_watch_url") or ""
                m = re.search(r"(\d+)", t)
                return int(m.group(1)) if m else 99999
        eps.sort(key=_ep_key)
        return eps

    def iterate_pages_collect(start_url: str) -> List[Dict]:
        all_eps: List[Dict] = []
        seen_pages = set()
        url = start_url
        while url and url not in seen_pages:
            seen_pages.add(url)
            s = fetch_html(url)
            if not s:
                break
            all_eps.extend(extract_from_soup(s))
            # Find next page (prefer rel=next)
            next_link = s.find('link', rel='next')
            next_url = next_link.get('href') if next_link else None
            if not next_url:
                # fallback: pagination anchors
                a_next = s.select_one('a.next, .paginate a.next, ul.page-numbers a.next')
                if a_next and a_next.get('href'):
                    next_url = a_next.get('href')
            url = next_url
        # De-duplicate across pages by episode_number (or watch url fallback)
        unique: Dict[str, Dict] = {}
        order: List[str] = []
        for ep in all_eps:
            key = ep.get("episode_number") or (ep.get("_watch_url") or ep.get("title") or "")
            if key not in unique:
                unique[key] = ep
                order.append(key)
            else:
                # Prefer one with more servers
                if len(ep.get("servers", [])) > len(unique[key].get("servers", [])):
                    unique[key] = ep
        deduped = [unique[k] for k in order]
        # Final numeric sort
        def _ep_key(e: Dict):
            val = e.get("episode_number") or ""
            try:
                return int(val)
            except Exception:
                t = e.get("title") or e.get("_watch_url") or ""
                m = re.search(r"(\d+)", t)
                return int(m.group(1)) if m else 99999
        deduped.sort(key=_ep_key)
        return deduped

    # 1) Try the /list/ page first (with pagination)
    list_url = normalize_to_list_page(season_url)
    episodes = iterate_pages_collect(list_url)
    if episodes:
        return episodes

    # 2) Fallback: try parsing episodes directly from the season page
    episodes = iterate_pages_collect(season_url)
    if episodes:
        return episodes

    return []


def sanitize_series_payload(series: Dict) -> Dict:
    # Remove any topcinema URLs except image posters (we keep poster links as-is)
    clean = {
        "title": series.get("title"),
        "type": series.get("type"),
        "imdb": series.get("imdb"),
        "poster": series.get("poster"),
        "story": series.get("story"),
        "details": series.get("details", {}),
        "trailer_url": series.get("trailer_url"),
        "breadcrumbs": series.get("breadcrumbs", []),
        "seasons": [],
    }

    total_episodes = 0
    # Sort and de-dup seasons by season_number
    seasons_list = list(series.get("seasons", []))
    def _season_key(s: Dict):
        sn = s.get('season_number')
        try:
            return int(sn) if sn and str(sn).isdigit() else 9999
        except Exception:
            return 9999
    seasons_list.sort(key=_season_key)

    seen_seasons = set()
    for s in seasons_list:
        s_num = s.get("season_number")
        if s_num in seen_seasons:
            continue
        seen_seasons.add(s_num)
        season_obj = {
            "season_number": s_num,
            "title": s.get("title"),
            "poster": s.get("poster"),  # poster allowed
            "episodes": [],
        }
        # De-dup episodes within a season by episode_number
        seen_ep = set()
        for ep in s.get("episodes", []):
            ep_no = ep.get("episode_number")
            if ep_no in seen_ep:
                continue
            seen_ep.add(ep_no)
            season_obj["episodes"].append({
                "episode_number": ep.get("episode_number"),
                "title": ep.get("title"),
                "poster": ep.get("poster"),  # poster allowed
                "servers": ep.get("servers", []),
            })
            total_episodes += 1
        clean["seasons"].append(season_obj)

    # Attach episodes_count at top-level
    clean["episodes_count"] = total_episodes
    return clean


def append_to_title_db(item: Dict, db_path: str = os.path.join("data", "title_db.json"), minify: bool = True) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    data = {"total_count": 0, "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"), "series": []}
    if os.path.exists(db_path):
        try:
            with open(db_path, "r", encoding="utf-8") as f:
                data = json.load(f) or data
        except Exception:
            pass

    # Upsert by title+type
    replaced = False
    for i, existing in enumerate(data.get("series", [])):
        if existing.get("title") == item.get("title") and existing.get("type") == item.get("type"):
            data["series"][i] = item
            replaced = True
            break
    if not replaced:
        data.setdefault("series", []).append(item)

    data["total_count"] = len(data.get("series", []))
    data["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")

    with open(db_path, "w", encoding="utf-8") as f:
        if minify:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
        else:
            json.dump(data, f, ensure_ascii=False, indent=2)


def run_single(url_input: str) -> Optional[Dict]:
    # Percent-decode and validate
    decoded = percent_decode(url_input.strip())
    if not is_anime_or_series(decoded):
        log("الرابط لا يبدو خاصاً بأنمي أو مسلسل (يجب أن يحتوي على: انمي/مسلسل).", color=Fore.RED)
        return None

    # Scrape base details
    series = scrape_series_details(decoded)
    if not series:
        print("تعذر جلب تفاصيل الصفحة.")
        return None

    # Fill episodes for each season (parallel for speed)
    seasons = series.get("seasons", [])
    def process_season(s: Dict) -> Tuple[str, List[Dict]]:
        s_url = s.get("url")
        sn = s.get('season_number', '?')
        if not s_url:
            return sn, []
        log(f"معالجة الموسم {sn}: {s_url}", color=Fore.CYAN)
        episodes = scrape_season_episodes(s_url)
        log(f"✓ اكتمل الموسم {sn} بعدد حلقات: {len(episodes)}", color=Fore.GREEN)
        return sn, episodes

    with ThreadPoolExecutor(max_workers=min(6, max(1, len(seasons)))) as ex:
        futures = {ex.submit(process_season, s): s for s in seasons}
        for fut in as_completed(futures):
            s = futures[fut]
            try:
                sn, eps = fut.result()
                s["episodes"] = eps
                # Persist incrementally after each season to avoid losing progress on long runs
                interim = sanitize_series_payload(series)
                append_to_title_db(interim, minify=True)
                log(f"💾 تم حفظ تقدم الموسم {sn} مؤقتاً", color=Fore.YELLOW)
            except Exception as e:
                log(f"تعذر معالجة موسم {s.get('season_number','?')}: {e}", color=Fore.RED)

    # Sanitize payload (remove site URLs except poster images)
    cleaned = sanitize_series_payload(series)
    return cleaned


def main():
    try:
        url_input = input(Fore.CYAN + "ضع رابط الأنمي/المسلسل من توب سينما: " + Style.RESET_ALL).strip()
    except EOFError:
        log("لم يتم إدخال رابط.", color=Fore.RED)
        return

    log("بدء الاستخراج ...", color=Fore.CYAN)
    result = run_single(url_input)
    if not result:
        return

    append_to_title_db(result, minify=True)
    log(f"\n✓ تم الحفظ في data/title_db.json: {result.get('title')} ({result.get('type')})", color=Fore.GREEN)
    log(f"المواسم: {len(result.get('seasons', []))} | عدد الحلقات: {result.get('episodes_count', 0)}", color=Fore.GREEN)


if __name__ == "__main__":
    main()
