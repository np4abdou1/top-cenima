#!/usr/bin/env python3
"""
Episode Numbering Validation Script
Checks for shows missing season 1, episodes not starting at 1, gaps in numbering, etc.
"""

import sqlite3
from collections import defaultdict

DB_PATH = "data/scrapped.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def check_numbering_issues():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("🔍 EPISODE NUMBERING VALIDATION")
    print("=" * 80)
    
    issues_found = False
    
    # === Check 1: Shows missing Season 1 ===
    print("\n📋 CHECK 1: Shows Missing Season 1")
    print("-" * 80)
    
    cursor.execute("""
        SELECT DISTINCT s.id, s.title, s.type
        FROM shows s
        JOIN seasons se ON s.id = se.show_id
        WHERE s.type IN ('series', 'anime')
        AND s.id NOT IN (
            SELECT show_id FROM seasons WHERE season_number = 1
        )
        AND s.id IN (SELECT show_id FROM seasons)
        ORDER BY s.type, s.title
    """)
    
    missing_season_1 = cursor.fetchall()
    
    if missing_season_1:
        issues_found = True
        print(f"⚠️  Found {len(missing_season_1)} shows without Season 1:")
        for row in missing_season_1:
            # Get which seasons they have
            cursor.execute("""
                SELECT season_number FROM seasons 
                WHERE show_id = ? 
                ORDER BY season_number
            """, (row['id'],))
            seasons = [str(s['season_number']) for s in cursor.fetchall()]
            print(f"  • [{row['id']}] {row['title']} ({row['type']}) - Has seasons: {', '.join(seasons)}")
    else:
        print("✅ All shows have Season 1!")
    
    # === Check 2: Seasons not starting at Episode 1 ===
    print("\n📋 CHECK 2: Seasons Not Starting at Episode 1")
    print("-" * 80)
    
    cursor.execute("""
        SELECT 
            sh.id as show_id,
            sh.title,
            sh.type,
            se.id as season_id,
            se.season_number,
            MIN(e.episode_number) as first_episode,
            COUNT(e.id) as episode_count
        FROM shows sh
        JOIN seasons se ON sh.id = se.show_id
        JOIN episodes e ON se.id = e.season_id
        WHERE sh.type IN ('series', 'anime')
        GROUP BY se.id
        HAVING first_episode != 1
        ORDER BY sh.title, se.season_number
    """)
    
    wrong_start = cursor.fetchall()
    
    if wrong_start:
        issues_found = True
        print(f"⚠️  Found {len(wrong_start)} seasons not starting at Episode 1:")
        for row in wrong_start:
            print(f"  • [{row['show_id']}] {row['title']} ({row['type']}) - Season {row['season_number']}")
            print(f"    └─ Starts at Episode {row['first_episode']}, has {row['episode_count']} episodes")
    else:
        print("✅ All seasons start at Episode 1!")
    
    # === Check 3: Gaps in episode numbering ===
    print("\n📋 CHECK 3: Gaps in Episode Numbering")
    print("-" * 80)
    
    cursor.execute("""
        SELECT DISTINCT
            sh.id as show_id,
            sh.title,
            sh.type,
            se.season_number
        FROM shows sh
        JOIN seasons se ON sh.id = se.show_id
        JOIN episodes e ON se.id = e.season_id
        WHERE sh.type IN ('series', 'anime')
        ORDER BY sh.title, se.season_number
    """)
    
    all_seasons = cursor.fetchall()
    gap_count = 0
    gap_examples = []
    
    for season in all_seasons:
        cursor.execute("""
            SELECT episode_number 
            FROM episodes 
            WHERE season_id = (
                SELECT id FROM seasons 
                WHERE show_id = ? AND season_number = ?
            )
            ORDER BY episode_number
        """, (season['show_id'], season['season_number']))
        
        episodes = [e['episode_number'] for e in cursor.fetchall()]
        
        if episodes:
            # Check for gaps
            expected = list(range(min(episodes), max(episodes) + 1))
            missing = set(expected) - set(episodes)
            
            if missing:
                gap_count += 1
                if len(gap_examples) < 20:
                    gap_examples.append({
                        'show_id': season['show_id'],
                        'title': season['title'],
                        'type': season['type'],
                        'season': season['season_number'],
                        'missing': sorted(missing),
                        'range': f"{min(episodes)}-{max(episodes)}"
                    })
    
    if gap_count > 0:
        issues_found = True
        print(f"⚠️  Found {gap_count} seasons with gaps in episode numbering:")
        for item in gap_examples[:20]:
            missing_str = ', '.join(str(x) for x in item['missing'][:10])
            if len(item['missing']) > 10:
                missing_str += f" ... and {len(item['missing']) - 10} more"
            print(f"  • [{item['show_id']}] {item['title']} ({item['type']}) - Season {item['season']}")
            print(f"    └─ Range: {item['range']}, Missing: {missing_str}")
        if gap_count > 20:
            print(f"  ... and {gap_count - 20} more seasons with gaps")
    else:
        print("✅ All seasons have continuous episode numbering!")
    
    # === Check 4: Duplicate episode numbers ===
    print("\n📋 CHECK 4: Duplicate Episode Numbers")
    print("-" * 80)
    
    cursor.execute("""
        SELECT 
            sh.id as show_id,
            sh.title,
            sh.type,
            se.season_number,
            e.episode_number,
            COUNT(*) as duplicate_count
        FROM shows sh
        JOIN seasons se ON sh.id = se.show_id
        JOIN episodes e ON se.id = e.season_id
        WHERE sh.type IN ('series', 'anime')
        GROUP BY se.id, e.episode_number
        HAVING duplicate_count > 1
        ORDER BY sh.title, se.season_number, e.episode_number
    """)
    
    duplicates = cursor.fetchall()
    
    if duplicates:
        issues_found = True
        print(f"⚠️  Found {len(duplicates)} duplicate episode numbers:")
        for row in duplicates[:20]:
            print(f"  • [{row['show_id']}] {row['title']} ({row['type']}) - Season {row['season_number']}")
            print(f"    └─ Episode {row['episode_number']} appears {row['duplicate_count']} times")
        if len(duplicates) > 20:
            print(f"  ... and {len(duplicates) - 20} more duplicates")
    else:
        print("✅ No duplicate episode numbers found!")
    
    # === Check 5: Shows with unusual episode ranges ===
    print("\n📋 CHECK 5: Unusual Episode Ranges (High Starting Numbers)")
    print("-" * 80)
    
    cursor.execute("""
        SELECT 
            sh.id as show_id,
            sh.title,
            sh.type,
            se.season_number,
            MIN(e.episode_number) as first_episode,
            MAX(e.episode_number) as last_episode,
            COUNT(e.id) as episode_count
        FROM shows sh
        JOIN seasons se ON sh.id = se.show_id
        JOIN episodes e ON se.id = e.season_id
        WHERE sh.type IN ('series', 'anime')
        GROUP BY se.id
        HAVING first_episode > 10
        ORDER BY first_episode DESC
        LIMIT 30
    """)
    
    unusual_ranges = cursor.fetchall()
    
    if unusual_ranges:
        issues_found = True
        print(f"⚠️  Found {len(unusual_ranges)} seasons with unusual episode ranges:")
        for row in unusual_ranges:
            print(f"  • [{row['show_id']}] {row['title']} ({row['type']}) - Season {row['season_number']}")
            print(f"    └─ Episodes {row['first_episode']}-{row['last_episode']} ({row['episode_count']} episodes)")
    else:
        print("✅ All episode ranges look normal!")
    
    # === Check 6: Shows with non-continuous season numbers ===
    print("\n📋 CHECK 6: Non-Continuous Season Numbers")
    print("-" * 80)
    
    cursor.execute("""
        SELECT DISTINCT s.id, s.title, s.type
        FROM shows s
        JOIN seasons se ON s.id = se.show_id
        WHERE s.type IN ('series', 'anime')
        AND s.id IN (SELECT show_id FROM seasons)
        ORDER BY s.title
    """)
    
    all_shows = cursor.fetchall()
    season_gap_count = 0
    season_gap_examples = []
    
    for show in all_shows:
        cursor.execute("""
            SELECT season_number 
            FROM seasons 
            WHERE show_id = ?
            ORDER BY season_number
        """, (show['id'],))
        
        seasons = [s['season_number'] for s in cursor.fetchall()]
        
        if seasons and len(seasons) > 1:
            # Check for gaps
            expected = list(range(min(seasons), max(seasons) + 1))
            missing = set(expected) - set(seasons)
            
            if missing:
                season_gap_count += 1
                if len(season_gap_examples) < 20:
                    season_gap_examples.append({
                        'show_id': show['id'],
                        'title': show['title'],
                        'type': show['type'],
                        'has_seasons': sorted(seasons),
                        'missing': sorted(missing)
                    })
    
    if season_gap_count > 0:
        issues_found = True
        print(f"⚠️  Found {season_gap_count} shows with gaps in season numbers:")
        for item in season_gap_examples[:20]:
            has_str = ', '.join(str(x) for x in item['has_seasons'])
            missing_str = ', '.join(str(x) for x in item['missing'])
            print(f"  • [{item['show_id']}] {item['title']} ({item['type']})")
            print(f"    ├─ Has seasons: {has_str}")
            print(f"    └─ Missing: {missing_str}")
        if season_gap_count > 20:
            print(f"  ... and {season_gap_count - 20} more shows with season gaps")
    else:
        print("✅ All shows have continuous season numbering!")
    
    # === Summary ===
    print("\n" + "=" * 80)
    print("📊 NUMBERING ISSUES SUMMARY")
    print("=" * 80)
    
    print(f"\n• Shows missing Season 1: {len(missing_season_1)}")
    print(f"• Seasons not starting at Episode 1: {len(wrong_start)}")
    print(f"• Seasons with episode gaps: {gap_count}")
    print(f"• Duplicate episode numbers: {len(duplicates)}")
    print(f"• Unusual episode ranges (starting >10): {len(unusual_ranges)}")
    print(f"• Shows with season gaps: {season_gap_count}")
    
    total_issues = (len(missing_season_1) + len(wrong_start) + gap_count + 
                   len(duplicates) + len(unusual_ranges) + season_gap_count)
    
    if total_issues == 0:
        print("\n✅ PERFECT! No numbering issues found!")
    else:
        print(f"\n⚠️  Total Issues Found: {total_issues}")
        print("\nNote: Some of these may be legitimate (e.g., sequel series, spinoffs)")
        print("or due to how the source website numbers their content.")
    
    print("\n" + "=" * 80)
    
    conn.close()

if __name__ == "__main__":
    try:
        check_numbering_issues()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
