import requests
import xml.etree.ElementTree as ET
import json
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def fetch_sitemap_urls(sitemap_number):
    """Fetch URLs from a single sitemap"""
    url = f"https://web7.topcinema.cam/series-sitemap{sitemap_number}.xml"
    
    try:
        print(f"Fetching sitemap {sitemap_number}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse XML
        root = ET.fromstring(response.content)
        
        # Extract URLs from XML namespace
        urls = []
        for url_element in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
            loc_element = url_element.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
            if loc_element is not None:
                # Decode percent-encoded URL
                decoded_url = urllib.parse.unquote(loc_element.text)
                urls.append(decoded_url)
        
        print(f"Sitemap {sitemap_number}: Found {len(urls)} URLs")
        return urls
        
    except Exception as e:
        print(f"Error fetching sitemap {sitemap_number}: {e}")
        return []

def main():
    print("Starting to fetch URLs from all sitemaps...")
    start_time = time.time()
    
    all_urls = []
    
    # Use ThreadPoolExecutor for concurrent requests
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all sitemap requests
        future_to_sitemap = {
            executor.submit(fetch_sitemap_urls, i): i 
            for i in range(1, 10)  # sitemaps 1-9
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_sitemap):
            sitemap_num = future_to_sitemap[future]
            try:
                urls = future.result()
                all_urls.extend(urls)
            except Exception as e:
                print(f"Error processing sitemap {sitemap_num}: {e}")
    
    # Remove duplicates while preserving order
    unique_urls = list(dict.fromkeys(all_urls))
    
    print(f"\nTotal URLs found: {len(all_urls)}")
    print(f"Unique URLs: {len(unique_urls)}")
    
    # Save to JSON file
    output_data = {"urls": unique_urls}
    
    try:
        with open('data/titles.json', 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"URLs saved to data/titles.json")
        print(f"Total execution time: {time.time() - start_time:.2f} seconds")
        
        # Show sample of decoded URLs
        print(f"\nSample URLs (first 5):")
        for i, url in enumerate(unique_urls[:5], 1):
            print(f"{i}. {url}")
            
    except Exception as e:
        print(f"Error saving to JSON: {e}")

if __name__ == "__main__":
    main()
