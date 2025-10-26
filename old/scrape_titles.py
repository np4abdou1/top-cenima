import json
import requests
from bs4 import BeautifulSoup
import urllib.parse
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote, unquote

# Headers for web scraping
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ar,en-US;q=0.7,en;q=0.3",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

def get_trailer_embed_url(url):
    """Get trailer embed URL from the anime trailer API"""
    try:
        trailer_url = "https://web7.topcinema.cam/wp-content/themes/movies2023/Ajaxat/Home/LoadTrailer.php"
        trailer_headers = {
            "accept": "*/*",
            "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-requested-with": "XMLHttpRequest",
            "Referer": url
        }
        
        data = {"href": url}
        
        response = requests.post(trailer_url, headers=trailer_headers, data=data, timeout=30)
        response.raise_for_status()
        
        # Parse the response to extract iframe src
        soup = BeautifulSoup(response.text, 'html.parser')
        iframe = soup.find('iframe')
        if iframe and iframe.get('src'):
            return iframe.get('src')
        return None
        
    except Exception as e:
        print(f"Error getting trailer for {url}: {e}")
        return None

def get_episode_servers(episode_id, total_servers=10):
    """Get streaming servers for an episode"""
    servers = []
    server_url = "https://web7.topcinema.cam/wp-content/themes/movies2023/Ajaxat/Single/Server.php"
    
    server_headers = {
        "accept": "*/*",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest",
    }
    
    for i in range(total_servers):
        try:
            data = {
                "id": str(episode_id),
                "i": str(i)
            }
            
            response = requests.post(server_url, headers=server_headers, data=data, timeout=15)
            response.raise_for_status()
            
            # Parse iframe src from response
            soup = BeautifulSoup(response.text, 'html.parser')
            iframe = soup.find('iframe')
            if iframe and iframe.get('src') and iframe.get('src').strip():
                servers.append({
                    "server_number": i,
                    "embed_url": iframe.get('src').strip()
                })
            
        except Exception as e:
            print(f"Error getting server {i} for episode {episode_id}: {e}")
    
    return servers

def extract_episode_id_from_watch_url(watch_url):
    """Extract episode ID from the watch page URL"""
    try:
        response = requests.get(watch_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Look for the episode ID in the HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Try to find episode ID in data attributes or JavaScript
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string:
                # Look for patterns that might contain episode ID
                id_match = re.search(r'"id":\s*"(\d+)"', script.string)
                if id_match:
                    return id_match.group(1)
                
                # Alternative pattern
                id_match = re.search(r'episode[_-]?id["\s:]*(\d+)', script.string, re.IGNORECASE)
                if id_match:
                    return id_match.group(1)
        
        return None
        
    except Exception as e:
        print(f"Error extracting episode ID from {watch_url}: {e}")
        return None

def scrape_series_details(url):
    """Scrape series/anime details from the main page"""
    try:
        print(f"Scraping: {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract basic information
        title_element = soup.find('h1', class_='post-title')
        title = title_element.get_text().strip() if title_element else "Unknown Title"
        
        # Remove any "انمي" or "مسلسل" prefix and clean title
        clean_title = re.sub(r'^(انمي|مسلسل)\s+', '', title).strip()
        
        # Determine type
        series_type = "anime" if "انمي" in url or "انمي" in title else "series"
        
        # Extract IMDb rating
        imdb_rating = None
        imdb_element = soup.find('div', class_='imdbR')
        if imdb_element:
            rating_span = imdb_element.find('span')
            if rating_span:
                imdb_rating = rating_span.get_text().strip()
        
        # Extract poster URL
        poster_url = None
        poster_img = soup.find('div', class_='image')
        if poster_img:
            img_tag = poster_img.find('img')
            if img_tag:
                poster_url = img_tag.get('src') or img_tag.get('data-src')
        
        # Extract story
        story = ""
        story_element = soup.find('div', class_='story')
        if story_element:
            story_p = story_element.find('p')
            if story_p:
                story = story_p.get_text().strip()
        
        # Extract details from RightTaxContent
        details = {}
        tax_content = soup.find('ul', class_='RightTaxContent')
        if tax_content:
            items = tax_content.find_all('li')
            for item in items:
                span = item.find('span')
                if span:
                    key = span.get_text().replace(':', '').strip()
                    # Get all links in this item
                    links = item.find_all('a')
                    if links:
                        values = [link.get_text().strip() for link in links]
                        details[key] = values
                    else:
                        # Check for strong tag (for duration, etc.)
                        strong = item.find('strong')
                        if strong:
                            details[key] = strong.get_text().strip()
        
        # Get trailer URL
        trailer_url = get_trailer_embed_url(url)
        
        # Extract seasons/episodes
        seasons = []
        
        # Look for seasons in the page
        season_elements = soup.find_all('div', class_='Small--Box Season')
        for season_element in season_elements:
            season_link = season_element.find('a')
            if season_link:
                season_url = season_link.get('href')
                season_title = season_link.get('title') or ""
                
                # Extract season number
                season_num_element = season_element.find('div', class_='epnum')
                season_number = "1"
                if season_num_element:
                    season_text = season_num_element.get_text()
                    season_match = re.search(r'(\d+)', season_text)
                    if season_match:
                        season_number = season_match.group(1)
                
                # Get season poster
                season_poster = None
                season_img = season_element.find('img')
                if season_img:
                    season_poster = season_img.get('src') or season_img.get('data-src')
                
                seasons.append({
                    "season_number": season_number,
                    "title": season_title,
                    "url": season_url,
                    "poster": season_poster,
                    "episodes": []  # Will be filled later
                })
        
        # If no seasons found, this might be a single season series
        if not seasons and not url.endswith('/'):
            # Try to find episodes link
            episodes_link = soup.find('a', href=re.compile(r'/list/?$'))
            if episodes_link:
                seasons.append({
                    "season_number": "1",
                    "title": clean_title,
                    "url": url,
                    "poster": poster_url,
                    "episodes": []
                })
        
        # Create the main entry
        series_data = {
            "title": clean_title,
            "type": series_type,
            "original_url": url,
            "imdb_rating": imdb_rating,
            "poster": poster_url,
            "story": story,
            "details": details,
            "trailer_url": trailer_url,
            "seasons": seasons
        }
        
        return series_data
        
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

def scrape_season_episodes(season_url):
    """Scrape episodes from a season page"""
    try:
        # Convert to list page if needed
        if not season_url.endswith('/list/'):
            if season_url.endswith('/'):
                season_url += 'list/'
            else:
                season_url += '/list/'
        
        print(f"Scraping episodes from: {season_url}")
        response = requests.get(season_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        episodes = []
        
        # Look for episode links
        episode_elements = soup.find_all('a', href=re.compile(r'/watch/?$'))
        
        for episode_element in episode_elements:
            episode_url = episode_element.get('href')
            
            # Extract episode info
            episode_title = episode_element.get('title', '')
            
            # Extract episode number
            episode_number = "1"
            ep_num_element = episode_element.find('em')
            if ep_num_element:
                ep_text = ep_num_element.get_text()
                ep_match = re.search(r'(\d+)', ep_text)
                if ep_match:
                    episode_number = ep_match.group(1)
            
            # Get episode poster
            episode_poster = None
            ep_img = episode_element.find('img')
            if ep_img:
                episode_poster = ep_img.get('src') or ep_img.get('data-src')
            
            # Extract episode ID and get servers
            episode_id = extract_episode_id_from_watch_url(episode_url)
            servers = []
            if episode_id:
                servers = get_episode_servers(episode_id)
            
            episodes.append({
                "episode_number": episode_number,
                "title": episode_title,
                "watch_url": episode_url,
                "poster": episode_poster,
                "episode_id": episode_id,
                "servers": servers
            })
        
        return episodes
        
    except Exception as e:
        print(f"Error scraping episodes from {season_url}: {e}")
        return []

def filter_anime_series_urls(urls):
    """Filter URLs to only include anime and series"""
    filtered_urls = []
    for url in urls:
        # Check if URL contains anime or series keywords in Arabic
        if "انمي" in url or "مسلسل" in url:
            # Skip season-specific URLs, we'll get them from the main series page
            if not re.search(r'(الموسم|الحلقة)', url):
                filtered_urls.append(url)
    
    return filtered_urls

def process_series(url):
    """Process a single series/anime"""
    try:
        # Scrape main series data
        series_data = scrape_series_details(url)
        if not series_data:
            return None
        
        # Process each season to get episodes
        for season in series_data['seasons']:
            if season['url']:
                episodes = scrape_season_episodes(season['url'])
                season['episodes'] = episodes
                
                # Add a small delay to be respectful
                time.sleep(1)
        
        return series_data
        
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return None

def main():
    print("Starting anime and series scraping...")
    
    # Load URLs from titles.json
    try:
        with open('data/titles.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            all_urls = data.get('urls', [])
    except Exception as e:
        print(f"Error loading titles.json: {e}")
        return
    
    print(f"Total URLs loaded: {len(all_urls)}")
    
    # Filter for anime and series only
    filtered_urls = filter_anime_series_urls(all_urls)
    print(f"Filtered anime/series URLs: {len(filtered_urls)}")
    
    # Process URLs (limit for testing)
    test_limit = 50  # Change this to process more
    urls_to_process = filtered_urls[:test_limit]
    
    print(f"Processing {len(urls_to_process)} URLs...")
    
    all_series_data = []
    
    # Process with threading for better performance
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_url = {executor.submit(process_series, url): url for url in urls_to_process}
        
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                if result:
                    all_series_data.append(result)
                    print(f"✓ Processed: {result['title']}")
                else:
                    print(f"✗ Failed: {url}")
            except Exception as e:
                print(f"✗ Error processing {url}: {e}")
    
    # Save to title_db.json
    output_data = {
        "total_count": len(all_series_data),
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "series": all_series_data
    }
    
    try:
        with open('data/title_db.json', 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Successfully saved {len(all_series_data)} series to title_db.json")
        print(f"Total anime: {len([s for s in all_series_data if s['type'] == 'anime'])}")
        print(f"Total series: {len([s for s in all_series_data if s['type'] == 'series'])}")
        
    except Exception as e:
        print(f"Error saving title_db.json: {e}")

if __name__ == "__main__":
    main()
