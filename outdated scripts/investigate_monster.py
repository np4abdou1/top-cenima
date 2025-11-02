#!/usr/bin/env python3
"""
Check for Monster shows - both anime and series versions
"""
import sqlite3

conn = sqlite3.connect('data/scrapped.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("=" * 80)
print("CHECKING ALL MONSTER SHOWS")
print("=" * 80)

# Find ALL shows with "Monster" in the title
cursor.execute("""
    SELECT id, title, type, source_url, year, imdb_rating
    FROM shows
    WHERE LOWER(title) = 'monster'
    ORDER BY type, id
""")

monster_shows = cursor.fetchall()

print(f"\nFound {len(monster_shows)} show(s) with exact title 'Monster':\n")

for show in monster_shows:
    print(f"ID: {show['id']}")
    print(f"Title: {show['title']}")
    print(f"Type: {show['type']}")
    print(f"Year: {show['year']}")
    print(f"IMDB: {show['imdb_rating']}")
    print(f"URL: {show['source_url']}")
    
    # Get season and episode count
    cursor.execute("""
        SELECT COUNT(*) as season_count
        FROM seasons
        WHERE show_id = ?
    """, (show['id'],))
    season_count = cursor.fetchone()['season_count']
    
    cursor.execute("""
        SELECT COUNT(*) as episode_count
        FROM episodes
        WHERE season_id IN (SELECT id FROM seasons WHERE show_id = ?)
    """, (show['id'],))
    episode_count = cursor.fetchone()['episode_count']
    
    print(f"Seasons: {season_count}, Episodes: {episode_count}")
    print("-" * 80)

# Check if there's an anime Monster URL in the JSON files or scrape_progress
print("\n" + "=" * 80)
print("CHECKING FOR ANIME MONSTER URL")
print("=" * 80)

cursor.execute("""
    SELECT url, status, show_id, error_message
    FROM scrape_progress
    WHERE url LIKE '%انمي-monster%' OR url LIKE '%anime-monster%'
""")

anime_urls = cursor.fetchall()

if anime_urls:
    print(f"\nFound {len(anime_urls)} anime Monster URL(s):\n")
    for row in anime_urls:
        print(f"URL: {row['url']}")
        print(f"Status: {row['status']}")
        print(f"Show ID: {row['show_id']}")
        print(f"Error: {row['error_message'] or 'None'}")
        
        if row['show_id']:
            cursor.execute("SELECT title, type FROM shows WHERE id = ?", (row['show_id'],))
            show = cursor.fetchone()
            if show:
                print(f"Linked to: {show['title']} ({show['type']})")
        print()
else:
    print("\nNo anime Monster URLs found in scrape_progress")

# Check series Monster URL
cursor.execute("""
    SELECT url, status, show_id, error_message
    FROM scrape_progress
    WHERE url LIKE '%مسلسل-monster%' OR url LIKE '%series-monster%'
""")

series_urls = cursor.fetchall()

print("\n" + "=" * 80)
print("CHECKING FOR SERIES MONSTER URL")
print("=" * 80)

if series_urls:
    print(f"\nFound {len(series_urls)} series Monster URL(s):\n")
    for row in series_urls:
        print(f"URL: {row['url']}")
        print(f"Status: {row['status']}")
        print(f"Show ID: {row['show_id']}")
        print(f"Error: {row['error_message'] or 'None'}")
        
        if row['show_id']:
            cursor.execute("SELECT title, type FROM shows WHERE id = ?", (row['show_id'],))
            show = cursor.fetchone()
            if show:
                print(f"Linked to: {show['title']} ({show['type']})")
        print()
else:
    print("\nNo series Monster URLs found in scrape_progress")

conn.close()

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print("\nThere should be TWO separate Monster shows:")
print("1. Monster (anime) - The famous psychological thriller anime")
print("2. Monster (series) - A different live-action series")
print("\nIf we only have one, we need to check which URL is correct and")
print("potentially re-scrape the missing one.")
