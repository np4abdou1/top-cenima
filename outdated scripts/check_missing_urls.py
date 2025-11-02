#!/usr/bin/env python3
"""
Check Missing URLs - Verify all URLs from JSON files are in scrape_progress table
"""
import json
import sqlite3

DB_PATH = "data/scrapped.db"
JSON_FILES = ["data/movies.json", "data/series_animes.json"]

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def check_missing_urls():
    print("=" * 80)
    print("🔍 CHECKING FOR MISSING URLs IN SCRAPE_PROGRESS")
    print("=" * 80)
    
    # Load all URLs from JSON files
    all_json_urls = set()
    
    for json_file in JSON_FILES:
        print(f"\n📂 Loading URLs from {json_file}...")
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if isinstance(data, list):
                urls = data
            elif isinstance(data, dict):
                urls = data.get('urls', []) or data.get('items', []) or list(data.values())
            else:
                print(f"  ⚠️  Unknown format in {json_file}")
                continue
            
            # Extract URLs from various possible formats
            count = 0
            for item in urls:
                if isinstance(item, str):
                    all_json_urls.add(item.strip())
                    count += 1
                elif isinstance(item, dict):
                    url = item.get('url') or item.get('link') or item.get('href')
                    if url:
                        all_json_urls.add(url.strip())
                        count += 1
            
            print(f"  ✅ Found {count} URLs")
        
        except FileNotFoundError:
            print(f"  ❌ File not found: {json_file}")
        except json.JSONDecodeError as e:
            print(f"  ❌ JSON decode error: {e}")
    
    print(f"\n📊 Total unique URLs from JSON files: {len(all_json_urls)}")
    
    # Get all URLs from database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT url FROM scrape_progress")
    db_urls = set(row['url'] for row in cursor.fetchall())
    
    print(f"📊 Total URLs in scrape_progress table: {len(db_urls)}")
    
    # Find missing URLs
    missing_urls = all_json_urls - db_urls
    extra_urls = db_urls - all_json_urls
    
    print("\n" + "=" * 80)
    print("📋 ANALYSIS RESULTS")
    print("=" * 80)
    
    if missing_urls:
        print(f"\n⚠️  MISSING URLs (in JSON but NOT in DB): {len(missing_urls)}")
        print("\nFirst 20 missing URLs:")
        for i, url in enumerate(sorted(missing_urls)[:20], 1):
            print(f"  {i}. {url}")
        
        if len(missing_urls) > 20:
            print(f"  ... and {len(missing_urls) - 20} more")
        
        # Option to add them
        print("\n" + "=" * 80)
        response = input("\n❓ Do you want to add these missing URLs to scrape_progress? (YES/no): ").strip()
        
        if response.upper() in ['YES', 'Y', '']:
            print("\n📝 Adding missing URLs to scrape_progress...")
            added = 0
            for url in missing_urls:
                try:
                    cursor.execute("""
                        INSERT INTO scrape_progress (url, status)
                        VALUES (?, 'pending')
                    """, (url,))
                    added += 1
                    
                    if added % 100 == 0:
                        print(f"  Added {added}/{len(missing_urls)} URLs...")
                
                except sqlite3.IntegrityError:
                    pass  # URL already exists
            
            conn.commit()
            print(f"✅ Successfully added {added} URLs to scrape_progress")
    else:
        print("\n✅ No missing URLs! All JSON URLs are in the database.")
    
    if extra_urls:
        print(f"\n📌 EXTRA URLs (in DB but NOT in JSON): {len(extra_urls)}")
        print("(These might be from manual additions or previous runs)")
        print("\nFirst 10 extra URLs:")
        for i, url in enumerate(sorted(extra_urls)[:10], 1):
            print(f"  {i}. {url}")
    
    # Status breakdown
    print("\n" + "=" * 80)
    print("📊 SCRAPE_PROGRESS STATUS BREAKDOWN")
    print("=" * 80)
    
    cursor.execute("""
        SELECT status, COUNT(*) as count
        FROM scrape_progress
        GROUP BY status
        ORDER BY count DESC
    """)
    
    for row in cursor.fetchall():
        print(f"  {row['status'].upper()}: {row['count']}")
    
    # Coverage percentage
    if all_json_urls:
        coverage = (len(db_urls & all_json_urls) / len(all_json_urls)) * 100
        print(f"\n📈 Coverage: {coverage:.2f}% of JSON URLs are in database")
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ Check complete!")
    print("=" * 80)

if __name__ == "__main__":
    try:
        check_missing_urls()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
