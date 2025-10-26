#!/usr/bin/env python3
"""
Fast multi-threaded movie URL scraper
Scrapes movie URLs from TopCinema pagination pages
"""

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Set
import json
import time
import re
from urllib.parse import unquote

# Configuration
BASE_URL = "https://web7.topcinema.cam/movies/page/{page}/"
START_PAGE = 1
END_PAGE = 50
MAX_WORKERS = 50  # High concurrency for speed
TIMEOUT = 10

# Headers matching scrape_single_test.py to avoid 403 errors
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": "https://web7.topcinema.cam/",
    "Upgrade-Insecure-Requests": "1",
}

# Session with connection pooling
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Configure connection pool
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

retry_strategy = Retry(
    total=2,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
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

# Disable SSL warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def scrape_page(page_num: int) -> List[str]:
    """Scrape movie URLs from a single page"""
    url = BASE_URL.format(page=page_num)
    urls = []
    
    try:
        response = SESSION.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all movie links in the Posts--List
        posts_list = soup.find('ul', class_='Posts--List')
        
        if posts_list:
            # Find all <a> tags with href inside Small--Box divs
            for small_box in posts_list.find_all('div', class_='Small--Box'):
                a_tag = small_box.find('a', href=True)
                if a_tag and a_tag.get('href'):
                    href = a_tag['href']
                    # Decode URL-encoded Arabic characters
                    decoded_href = unquote(href)
                    # Only include movie URLs (containing فيلم or /film- or /movie-)
                    if any(pattern in decoded_href for pattern in ['فيلم', '/film-', '/movie-']):
                        urls.append(decoded_href)
        
        print(f"✓ Page {page_num:2d}: {len(urls)} movies")
        return urls
        
    except Exception as e:
        print(f"✗ Page {page_num:2d}: Error - {str(e)[:50]}")
        return []

def scrape_all_pages(start: int, end: int, workers: int = MAX_WORKERS) -> List[str]:
    """Scrape all pages in parallel"""
    
    print(f"🚀 Starting movie URL scraper")
    print(f"📄 Pages: {start} to {end} ({end - start + 1} pages)")
    print(f"👷 Workers: {workers}")
    print("=" * 60)
    
    all_urls: Set[str] = set()
    start_time = time.time()
    
    # Use ThreadPoolExecutor for parallel scraping
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all page scraping tasks
        future_to_page = {
            executor.submit(scrape_page, page): page 
            for page in range(start, end + 1)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                urls = future.result()
                all_urls.update(urls)
            except Exception as e:
                print(f"✗ Page {page} failed: {str(e)[:50]}")
    
    elapsed = time.time() - start_time
    
    # Remove duplicates and sort
    unique_urls = sorted(list(all_urls))
    
    print("\n" + "=" * 60)
    print(f"🏁 Scraping completed!")
    print(f"📊 Total unique movies: {len(unique_urls)}")
    print(f"⏱️  Time taken: {elapsed:.2f}s ({elapsed/60:.2f}m)")
    print(f"⚡ Speed: {len(unique_urls)/elapsed:.1f} movies/second")
    
    return unique_urls

def save_urls(urls: List[str], output_file: str = 'data/movies.json'):
    """Save URLs to JSON file"""
    import os
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save as JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({"urls": urls}, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Saved to: {output_file}")
    
    # Also save as plain text (one URL per line)
    txt_file = output_file.replace('.json', '.txt')
    with open(txt_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(urls))
    
    print(f"📝 Also saved as: {txt_file}")

def main():
    """Main function"""
    
    # Scrape all pages
    urls = scrape_all_pages(START_PAGE, END_PAGE, MAX_WORKERS)
    
    if urls:
        # Save results
        save_urls(urls)
        
        # Show sample
        print(f"\n📋 Sample URLs (first 5):")
        for url in urls[:5]:
            print(f"   {url}")
        
        if len(urls) > 5:
            print(f"   ... and {len(urls) - 5} more")
    else:
        print("❌ No URLs found!")

if __name__ == '__main__':
    main()
