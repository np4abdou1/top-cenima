#!/usr/bin/env python3
"""
Ultimate Pagination Checker - Detects shows with pagination issues in episode lists
This tool checks for:
1. Seasons with suspiciously round episode counts (10, 20, 30, 40, 50, 60, etc.)
2. Seasons where episodes don't start at 1 (indicating missing first page)
3. Seasons with unusual episode number jumps
4. Shows where total episodes seem too low for the type
"""

import sqlite3
from typing import List, Dict, Tuple

DB_PATH = "data/scrapped.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def check_pagination_issues():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("ULTIMATE PAGINATION CHECKER")
    print("=" * 80)
    
    issues = {
        "round_episode_counts": [],
        "not_starting_at_1": [],
        "large_gaps": [],
        "suspiciously_low_totals": [],
        "all_issues_combined": set()
    }
    
    # Get all series/anime with their seasons
    cursor.execute("""
        SELECT DISTINCT s.id as show_id, s.title, s.type, s.source_url
        FROM shows s
        WHERE s.type IN ('series', 'anime')
        ORDER BY s.title
    """)
    
    shows = cursor.fetchall()
    total_shows = len(shows)
    
    print(f"\nAnalyzing {total_shows} series/anime shows...\n")
    
    checked = 0
    for show in shows:
        checked += 1
        if checked % 100 == 0:
            print(f"  Checked {checked}/{total_shows} shows...")
        
        show_id = show['show_id']
        show_title = show['title']
        show_type = show['type']
        show_url = show['source_url']
        
        # Get all seasons for this show
        cursor.execute("""
            SELECT s.id as season_id, s.season_number
            FROM seasons s
            WHERE s.show_id = ?
            ORDER BY s.season_number
        """, (show_id,))
        
        seasons = cursor.fetchall()
        
        for season in seasons:
            season_id = season['season_id']
            season_num = season['season_number']
            
            # Get all episodes for this season
            cursor.execute("""
                SELECT episode_number
                FROM episodes
                WHERE season_id = ?
                ORDER BY episode_number
            """, (season_id,))
            
            episodes = [row['episode_number'] for row in cursor.fetchall()]
            
            if not episodes:
                continue
            
            episode_count = len(episodes)
            first_ep = min(episodes)
            last_ep = max(episodes)
            
            # Issue 1: Suspiciously round episode counts (multiples of 10)
            # This suggests pagination stopped at page boundary (10, 20, 30, 40, 50, 60, 70, 80, 90, 100)
            if episode_count in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100] and episode_count < 100:
                issues["round_episode_counts"].append({
                    "show_id": show_id,
                    "title": show_title,
                    "type": show_type,
                    "season": season_num,
                    "episode_count": episode_count,
                    "first_ep": first_ep,
                    "last_ep": last_ep,
                    "url": show_url,
                    "reason": f"Exactly {episode_count} episodes (suspiciously round)"
                })
                issues["all_issues_combined"].add(show_id)
            
            # Issue 2: Season doesn't start at episode 1 or 0
            if first_ep > 1 and season_num == 1:
                issues["not_starting_at_1"].append({
                    "show_id": show_id,
                    "title": show_title,
                    "type": show_type,
                    "season": season_num,
                    "episode_count": episode_count,
                    "first_ep": first_ep,
                    "last_ep": last_ep,
                    "url": show_url,
                    "reason": f"Season 1 starts at episode {first_ep}"
                })
                issues["all_issues_combined"].add(show_id)
            
            # Issue 3: Large gaps in episode numbers
            if len(episodes) > 1:
                episodes_int = sorted([int(e) for e in episodes])
                max_gap = 0
                gap_location = None
                
                for i in range(len(episodes_int) - 1):
                    gap = episodes_int[i + 1] - episodes_int[i]
                    if gap > max_gap:
                        max_gap = gap
                        gap_location = (episodes_int[i], episodes_int[i + 1])
                
                # If there's a gap larger than 10, likely pagination issue
                if max_gap > 10:
                    issues["large_gaps"].append({
                        "show_id": show_id,
                        "title": show_title,
                        "type": show_type,
                        "season": season_num,
                        "episode_count": episode_count,
                        "first_ep": first_ep,
                        "last_ep": last_ep,
                        "url": show_url,
                        "gap_size": max_gap,
                        "gap_location": gap_location,
                        "reason": f"Gap of {max_gap} episodes between {gap_location[0]} and {gap_location[1]}"
                    })
                    issues["all_issues_combined"].add(show_id)
            
            # Issue 4: Show seems to have too few total episodes
            # Anime typically has 12, 24, 26, 52, or more episodes
            # Series can vary but very popular/long-running ones should have more
            if show_type == 'anime':
                # Get total episodes across all seasons
                cursor.execute("""
                    SELECT COUNT(*) as total
                    FROM episodes
                    WHERE season_id IN (SELECT id FROM seasons WHERE show_id = ?)
                """, (show_id,))
                total_eps = cursor.fetchone()['total']
                
                # If anime has exactly 10, 20, 30, 40, 50, 60 episodes, suspicious
                if total_eps in [10, 20, 30, 40, 50, 60] and len(seasons) == 1:
                    issues["suspiciously_low_totals"].append({
                        "show_id": show_id,
                        "title": show_title,
                        "type": show_type,
                        "total_episodes": total_eps,
                        "seasons": len(seasons),
                        "url": show_url,
                        "reason": f"Anime with exactly {total_eps} episodes in 1 season (unusual)"
                    })
                    issues["all_issues_combined"].add(show_id)
    
    # Print results
    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80)
    
    print(f"\n1. Shows with ROUND episode counts (10, 20, 30, etc.): {len(issues['round_episode_counts'])}")
    if issues['round_episode_counts']:
        for item in issues['round_episode_counts'][:10]:
            print(f"   - {item['title']} (S{item['season']}): {item['episode_count']} eps, {item['reason']}")
        if len(issues['round_episode_counts']) > 10:
            print(f"   ... and {len(issues['round_episode_counts']) - 10} more")
    
    print(f"\n2. Season 1 NOT starting at episode 1: {len(issues['not_starting_at_1'])}")
    if issues['not_starting_at_1']:
        for item in issues['not_starting_at_1'][:10]:
            print(f"   - {item['title']} (S{item['season']}): {item['reason']}")
        if len(issues['not_starting_at_1']) > 10:
            print(f"   ... and {len(issues['not_starting_at_1']) - 10} more")
    
    print(f"\n3. Seasons with LARGE gaps in episode numbers: {len(issues['large_gaps'])}")
    if issues['large_gaps']:
        for item in issues['large_gaps'][:10]:
            print(f"   - {item['title']} (S{item['season']}): {item['reason']}")
        if len(issues['large_gaps']) > 10:
            print(f"   ... and {len(issues['large_gaps']) - 10} more")
    
    print(f"\n4. Shows with suspiciously LOW total episodes: {len(issues['suspiciously_low_totals'])}")
    if issues['suspiciously_low_totals']:
        for item in issues['suspiciously_low_totals'][:10]:
            print(f"   - {item['title']}: {item['reason']}")
        if len(issues['suspiciously_low_totals']) > 10:
            print(f"   ... and {len(issues['suspiciously_low_totals']) - 10} more")
    
    print("\n" + "=" * 80)
    print(f"TOTAL UNIQUE SHOWS WITH ISSUES: {len(issues['all_issues_combined'])}")
    print("=" * 80)
    
    # Save to file
    print("\nSaving detailed report to: data/pagination_issues.txt")
    
    with open("data/pagination_issues.txt", "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("PAGINATION ISSUES REPORT\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Total shows checked: {total_shows}\n")
        f.write(f"Shows with issues: {len(issues['all_issues_combined'])}\n\n")
        
        f.write("=" * 80 + "\n")
        f.write("1. ROUND EPISODE COUNTS (10, 20, 30, etc.)\n")
        f.write("=" * 80 + "\n")
        for item in issues['round_episode_counts']:
            f.write(f"\nTitle: {item['title']}\n")
            f.write(f"Type: {item['type']}\n")
            f.write(f"Season: {item['season']}\n")
            f.write(f"Episodes: {item['episode_count']} (ep {item['first_ep']}-{item['last_ep']})\n")
            f.write(f"Reason: {item['reason']}\n")
            f.write(f"URL: {item['url']}\n")
            f.write("-" * 80 + "\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("2. SEASON 1 NOT STARTING AT EPISODE 1\n")
        f.write("=" * 80 + "\n")
        for item in issues['not_starting_at_1']:
            f.write(f"\nTitle: {item['title']}\n")
            f.write(f"Type: {item['type']}\n")
            f.write(f"Season: {item['season']}\n")
            f.write(f"Episodes: {item['episode_count']} (ep {item['first_ep']}-{item['last_ep']})\n")
            f.write(f"Reason: {item['reason']}\n")
            f.write(f"URL: {item['url']}\n")
            f.write("-" * 80 + "\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("3. LARGE GAPS IN EPISODE NUMBERS\n")
        f.write("=" * 80 + "\n")
        for item in issues['large_gaps']:
            f.write(f"\nTitle: {item['title']}\n")
            f.write(f"Type: {item['type']}\n")
            f.write(f"Season: {item['season']}\n")
            f.write(f"Episodes: {item['episode_count']} (ep {item['first_ep']}-{item['last_ep']})\n")
            f.write(f"Reason: {item['reason']}\n")
            f.write(f"URL: {item['url']}\n")
            f.write("-" * 80 + "\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("4. SUSPICIOUSLY LOW TOTAL EPISODES\n")
        f.write("=" * 80 + "\n")
        for item in issues['suspiciously_low_totals']:
            f.write(f"\nTitle: {item['title']}\n")
            f.write(f"Type: {item['type']}\n")
            f.write(f"Total Episodes: {item['total_episodes']}\n")
            f.write(f"Seasons: {item['seasons']}\n")
            f.write(f"Reason: {item['reason']}\n")
            f.write(f"URL: {item['url']}\n")
            f.write("-" * 80 + "\n")
    
    print("\nDone!")
    
    # Offer to save show IDs for deletion
    if issues['all_issues_combined']:
        print("\n" + "=" * 80)
        response = input(f"\nSave {len(issues['all_issues_combined'])} show IDs to file for batch deletion? (YES/no): ").strip()
        
        if response.upper() in ['YES', 'Y', '']:
            import json
            
            # Get full details for all affected shows
            affected_shows = []
            for show_id in issues['all_issues_combined']:
                cursor.execute("""
                    SELECT id, title, type, source_url
                    FROM shows
                    WHERE id = ?
                """, (show_id,))
                show = cursor.fetchone()
                if show:
                    affected_shows.append({
                        "id": show['id'],
                        "title": show['title'],
                        "type": show['type'],
                        "url": show['source_url']
                    })
            
            with open("data/pagination_affected_shows.json", "w", encoding="utf-8") as f:
                json.dump(affected_shows, f, indent=2, ensure_ascii=False)
            
            print(f"Saved {len(affected_shows)} shows to: data/pagination_affected_shows.json")
            print("\nYou can use reset_affected_shows.py to delete and mark them as pending")
    
    conn.close()

if __name__ == "__main__":
    try:
        check_pagination_issues()
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
