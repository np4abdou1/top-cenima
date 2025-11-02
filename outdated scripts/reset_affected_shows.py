#!/usr/bin/env python3
"""
Reset Affected Shows - Delete and Mark for Re-scraping
This script deletes shows that have pagination or episode parsing issues
and marks their URLs as pending for re-scraping.
"""

import sqlite3
import json
import sys
from datetime import datetime

DB_PATH = "data/scrapped.db"
REDFLAG_FILE = "data/redflag_shows.json"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def reset_affected_shows():
    """
    Delete shows with:
    1. Seasons not starting at episode 1 (likely pagination issues)
    2. Seasons with episode gaps (likely special episode parsing issues)
    And mark them as pending for re-scraping.
    """
    
    print("=" * 80)
    print("🔄 RESETTING AFFECTED SHOWS FOR RE-SCRAPING")
    print("=" * 80)
    
    # Load redflag data
    print(f"\n📂 Loading redflag data from {REDFLAG_FILE}...")
    try:
        with open(REDFLAG_FILE, 'r', encoding='utf-8') as f:
            redflag_data = json.load(f)
    except FileNotFoundError:
        print(f"❌ ERROR: {REDFLAG_FILE} not found. Please run extract_redflags.py first.")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Collect all affected show IDs and URLs
    affected_shows = {}
    
    # 1. Shows with seasons not starting at episode 1
    print("\n1️⃣ Processing seasons not starting at episode 1...")
    category = redflag_data.get("categories", {}).get("seasons_not_starting_at_episode_1", {})
    for item in category.get("urls", []):
        show_id = item.get("show_id")
        url = item.get("url")
        title = item.get("title")
        if show_id and url:
            affected_shows[show_id] = {"url": url, "title": title, "reason": "Season not starting at ep 1"}
    
    print(f"   Found {len(category.get('urls', []))} shows")
    
    # 2. Shows with episode gaps
    print("\n2️⃣ Processing shows with episode gaps...")
    category = redflag_data.get("categories", {}).get("seasons_with_episode_gaps", {})
    for item in category.get("urls", []):
        show_id = item.get("show_id")
        url = item.get("url")
        title = item.get("title")
        if show_id and url:
            if show_id not in affected_shows:
                affected_shows[show_id] = {"url": url, "title": title, "reason": "Episode gaps"}
            else:
                affected_shows[show_id]["reason"] += " + Episode gaps"
    
    print(f"   Found {len(category.get('urls', []))} shows")
    
    # 3. Seasons without episodes (likely pagination issues)
    print("\n3️⃣ Processing seasons without episodes...")
    category = redflag_data.get("categories", {}).get("seasons_without_episodes", {})
    for item in category.get("urls", []):
        show_id = item.get("show_id")
        url = item.get("url")
        title = item.get("title")
        if show_id and url:
            if show_id not in affected_shows:
                affected_shows[show_id] = {"url": url, "title": title, "reason": "Season without episodes"}
            else:
                affected_shows[show_id]["reason"] += " + Season without episodes"
    
    print(f"   Found {len(category.get('urls', []))} shows")
    
    print(f"\n📊 Total unique affected shows: {len(affected_shows)}")
    
    if not affected_shows:
        print("\n✅ No affected shows to reset.")
        conn.close()
        return
    
    # Confirm action
    print("\n" + "=" * 80)
    print("⚠️  WARNING: This will DELETE the following shows and mark them as pending:")
    print("=" * 80)
    for i, (show_id, data) in enumerate(list(affected_shows.items())[:10], 1):
        print(f"  {i}. {data['title']} (ID: {show_id}) - {data['reason']}")
    if len(affected_shows) > 10:
        print(f"  ... and {len(affected_shows) - 10} more shows")
    
    print("\n" + "=" * 80)
    
    # Check for --force flag to skip confirmation
    force = "--force" in sys.argv
    
    if not force:
        response = input("Type 'YES' to proceed with deletion and reset: ")
        
        if response.strip().upper() != 'YES':
            print("\n❌ Operation cancelled.")
            conn.close()
            return
    else:
        print("🚀 Running in --force mode, skipping confirmation...")
    
    # Begin deletion and reset
    print("\n🗑️ Deleting affected shows from database...")
    deleted_count = 0
    
    for show_id, data in affected_shows.items():
        try:
            # Delete the show (CASCADE will delete seasons, episodes, and servers)
            cursor.execute("DELETE FROM shows WHERE id = ?", (show_id,))
            deleted_count += 1
            
            if deleted_count % 10 == 0:
                print(f"   Deleted {deleted_count}/{len(affected_shows)} shows...")
        except Exception as e:
            print(f"   ⚠️  Error deleting show ID {show_id}: {e}")
    
    conn.commit()
    print(f"✅ Deleted {deleted_count} shows from database")
    
    # Mark URLs as pending for re-scraping
    print("\n📝 Marking URLs as pending for re-scraping...")
    pending_count = 0
    
    for show_id, data in affected_shows.items():
        url = data["url"]
        try:
            # Update or insert into scrape_progress
            cursor.execute("""
                INSERT INTO scrape_progress (url, status, show_id, error_message, updated_at)
                VALUES (?, 'pending', NULL, 'Reset for re-scraping: ' || ?, CURRENT_TIMESTAMP)
                ON CONFLICT(url) DO UPDATE SET
                    status = 'pending',
                    show_id = NULL,
                    error_message = 'Reset for re-scraping: ' || excluded.error_message,
                    updated_at = CURRENT_TIMESTAMP
            """, (url, data["reason"]))
            pending_count += 1
            
            if pending_count % 10 == 0:
                print(f"   Marked {pending_count}/{len(affected_shows)} URLs as pending...")
        except Exception as e:
            print(f"   ⚠️  Error marking URL as pending {url}: {e}")
    
    conn.commit()
    print(f"✅ Marked {pending_count} URLs as pending")
    
    # Generate summary report
    print("\n" + "=" * 80)
    print("📊 RESET SUMMARY")
    print("=" * 80)
    print(f"  Total shows deleted: {deleted_count}")
    print(f"  Total URLs marked as pending: {pending_count}")
    print(f"  Ready for re-scraping: YES")
    print("=" * 80)
    print("\n✅ Reset complete! You can now run the scraper to re-scrape these shows.")
    
    # Save list of reset URLs to a file for reference
    reset_log = {
        "reset_at": datetime.now().isoformat(),
        "total_count": len(affected_shows),
        "shows": [
            {
                "show_id": show_id,
                "title": data["title"],
                "url": data["url"],
                "reason": data["reason"]
            }
            for show_id, data in affected_shows.items()
        ]
    }
    
    log_file = "data/reset_log.json"
    with open(log_file, 'w', encoding='utf-8') as f:
        json.dump(reset_log, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Reset log saved to: {log_file}")
    
    conn.close()

if __name__ == "__main__":
    try:
        reset_affected_shows()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
