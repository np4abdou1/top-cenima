#!/usr/bin/env python3
"""
Database Validation and Statistics Script
Scans the scrapped.db database to find broken shows, missing data, and generate comprehensive stats
"""

import sqlite3
import json
from collections import defaultdict
from datetime import datetime

DB_PATH = "data/scrapped.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def analyze_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("🔍 DATABASE VALIDATION & STATISTICS REPORT")
    print("=" * 80)
    print(f"⏰ Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # === BASIC STATS ===
    print("\n📊 BASIC STATISTICS")
    print("-" * 80)
    
    # Total shows
    cursor.execute("SELECT COUNT(*) as total FROM shows")
    total_shows = cursor.fetchone()['total']
    
    # By type
    cursor.execute("SELECT type, COUNT(*) as count FROM shows GROUP BY type")
    type_counts = {row['type']: row['count'] for row in cursor.fetchall()}
    
    print(f"Total Shows: {total_shows}")
    print(f"  ├─ Movies: {type_counts.get('movie', 0)}")
    print(f"  ├─ Series: {type_counts.get('series', 0)}")
    print(f"  └─ Anime: {type_counts.get('anime', 0)}")
    
    # Total seasons, episodes, servers
    cursor.execute("SELECT COUNT(*) as total FROM seasons")
    total_seasons = cursor.fetchone()['total']
    
    cursor.execute("SELECT COUNT(*) as total FROM episodes")
    total_episodes = cursor.fetchone()['total']
    
    cursor.execute("SELECT COUNT(*) as total FROM servers")
    total_servers = cursor.fetchone()['total']
    
    print(f"\nTotal Seasons: {total_seasons}")
    print(f"Total Episodes: {total_episodes}")
    print(f"Total Servers: {total_servers}")
    
    # Average IMDb ratings
    cursor.execute("SELECT AVG(imdb_rating) as avg_rating FROM shows WHERE imdb_rating IS NOT NULL")
    avg_rating = cursor.fetchone()['avg_rating']
    print(f"Average IMDb Rating: {avg_rating:.2f}" if avg_rating else "Average IMDb Rating: N/A")
    
    # === SCRAPE PROGRESS ===
    print("\n📈 SCRAPING PROGRESS")
    print("-" * 80)
    
    cursor.execute("SELECT status, COUNT(*) as count FROM scrape_progress GROUP BY status")
    progress_stats = {row['status']: row['count'] for row in cursor.fetchall()}
    
    total_urls = sum(progress_stats.values())
    completed = progress_stats.get('completed', 0)
    failed = progress_stats.get('failed', 0)
    pending = progress_stats.get('pending', 0)
    
    completion_rate = (completed / total_urls * 100) if total_urls > 0 else 0
    
    print(f"Total URLs: {total_urls}")
    print(f"  ├─ Completed: {completed} ({completion_rate:.1f}%)")
    print(f"  ├─ Failed: {failed} ({failed / total_urls * 100:.1f}%)")
    print(f"  └─ Pending: {pending} ({pending / total_urls * 100:.1f}%)")
    
    # === DATA QUALITY CHECKS ===
    print("\n🔧 DATA QUALITY ANALYSIS")
    print("-" * 80)
    
    issues = []
    
    # Check 1: Movies without servers
    cursor.execute("""
        SELECT COUNT(*) as count 
        FROM shows s
        WHERE s.type = 'movie'
        AND s.id NOT IN (
            SELECT DISTINCT parent_id FROM servers WHERE parent_type = 'movie'
        )
    """)
    movies_no_servers = cursor.fetchone()['count']
    if movies_no_servers > 0:
        issues.append(('Movies without servers', movies_no_servers))
        print(f"⚠️  Movies without servers: {movies_no_servers}")
    else:
        print(f"✅ All movies have servers!")
    
    # Check 2: Series/Anime without seasons
    cursor.execute("""
        SELECT COUNT(*) as count 
        FROM shows s
        WHERE s.type IN ('series', 'anime')
        AND s.id NOT IN (SELECT DISTINCT show_id FROM seasons)
    """)
    shows_no_seasons = cursor.fetchone()['count']
    if shows_no_seasons > 0:
        issues.append(('Series/Anime without seasons', shows_no_seasons))
        print(f"⚠️  Series/Anime without seasons: {shows_no_seasons}")
    else:
        print(f"✅ All series/anime have seasons!")
    
    # Check 3: Seasons without episodes
    cursor.execute("""
        SELECT COUNT(*) as count 
        FROM seasons s
        WHERE s.id NOT IN (SELECT DISTINCT season_id FROM episodes)
    """)
    seasons_no_episodes = cursor.fetchone()['count']
    if seasons_no_episodes > 0:
        issues.append(('Seasons without episodes', seasons_no_episodes))
        print(f"⚠️  Seasons without episodes: {seasons_no_episodes}")
    else:
        print(f"✅ All seasons have episodes!")
    
    # Check 4: Episodes without servers
    cursor.execute("""
        SELECT COUNT(*) as count 
        FROM episodes e
        WHERE e.id NOT IN (
            SELECT DISTINCT parent_id FROM servers WHERE parent_type = 'episode'
        )
    """)
    episodes_no_servers = cursor.fetchone()['count']
    if episodes_no_servers > 0:
        issues.append(('Episodes without servers', episodes_no_servers))
        print(f"⚠️  Episodes without servers: {episodes_no_servers}")
    else:
        print(f"✅ All episodes have servers!")
    
    # Check 5: Shows with missing metadata
    cursor.execute("""
        SELECT COUNT(*) as count FROM shows 
        WHERE poster IS NULL OR poster = ''
    """)
    no_poster = cursor.fetchone()['count']
    if no_poster > 0:
        print(f"⚠️  Shows without poster: {no_poster}")
    
    cursor.execute("""
        SELECT COUNT(*) as count FROM shows 
        WHERE synopsis IS NULL OR synopsis = ''
    """)
    no_synopsis = cursor.fetchone()['count']
    if no_synopsis > 0:
        print(f"⚠️  Shows without synopsis: {no_synopsis}")
    
    cursor.execute("""
        SELECT COUNT(*) as count FROM shows 
        WHERE imdb_rating IS NULL
    """)
    no_rating = cursor.fetchone()['count']
    if no_rating > 0:
        print(f"⚠️  Shows without IMDb rating: {no_rating}")
    
    # === DETAILED BREAKDOWNS ===
    print("\n📺 DETAILED BREAKDOWNS")
    print("-" * 80)
    
    # Movies with server count
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT s.id) as movie_count,
            AVG(server_count) as avg_servers
        FROM shows s
        LEFT JOIN (
            SELECT parent_id, COUNT(*) as server_count
            FROM servers
            WHERE parent_type = 'movie'
            GROUP BY parent_id
        ) srv ON s.id = srv.parent_id
        WHERE s.type = 'movie'
    """)
    movie_stats = cursor.fetchone()
    print(f"Movies: {movie_stats['movie_count']}")
    print(f"  └─ Average servers per movie: {movie_stats['avg_servers']:.1f}" if movie_stats['avg_servers'] else "  └─ Average servers per movie: 0")
    
    # Series statistics
    cursor.execute("""
        SELECT 
            s.id,
            s.title,
            COUNT(DISTINCT se.id) as season_count,
            COUNT(DISTINCT e.id) as episode_count,
            COUNT(DISTINCT srv.id) as server_count
        FROM shows s
        LEFT JOIN seasons se ON s.id = se.show_id
        LEFT JOIN episodes e ON se.id = e.season_id
        LEFT JOIN servers srv ON e.id = srv.parent_id AND srv.parent_type = 'episode'
        WHERE s.type = 'series'
        GROUP BY s.id
    """)
    series_data = cursor.fetchall()
    
    if series_data:
        total_series_seasons = sum(row['season_count'] for row in series_data)
        total_series_episodes = sum(row['episode_count'] for row in series_data)
        total_series_servers = sum(row['server_count'] for row in series_data)
        
        print(f"\nSeries: {len(series_data)}")
        print(f"  ├─ Total seasons: {total_series_seasons}")
        print(f"  ├─ Total episodes: {total_series_episodes}")
        print(f"  ├─ Average seasons per series: {total_series_seasons / len(series_data):.1f}")
        print(f"  ├─ Average episodes per series: {total_series_episodes / len(series_data):.1f}")
        print(f"  └─ Average servers per episode: {total_series_servers / total_series_episodes:.1f}" if total_series_episodes > 0 else "  └─ Average servers per episode: 0")
    
    # Anime statistics
    cursor.execute("""
        SELECT 
            s.id,
            s.title,
            COUNT(DISTINCT se.id) as season_count,
            COUNT(DISTINCT e.id) as episode_count,
            COUNT(DISTINCT srv.id) as server_count
        FROM shows s
        LEFT JOIN seasons se ON s.id = se.show_id
        LEFT JOIN episodes e ON se.id = e.season_id
        LEFT JOIN servers srv ON e.id = srv.parent_id AND srv.parent_type = 'episode'
        WHERE s.type = 'anime'
        GROUP BY s.id
    """)
    anime_data = cursor.fetchall()
    
    if anime_data:
        total_anime_seasons = sum(row['season_count'] for row in anime_data)
        total_anime_episodes = sum(row['episode_count'] for row in anime_data)
        total_anime_servers = sum(row['server_count'] for row in anime_data)
        
        print(f"\nAnime: {len(anime_data)}")
        print(f"  ├─ Total seasons: {total_anime_seasons}")
        print(f"  ├─ Total episodes: {total_anime_episodes}")
        print(f"  ├─ Average seasons per anime: {total_anime_seasons / len(anime_data):.1f}")
        print(f"  ├─ Average episodes per anime: {total_anime_episodes / len(anime_data):.1f}")
        print(f"  └─ Average servers per episode: {total_anime_servers / total_anime_episodes:.1f}" if total_anime_episodes > 0 else "  └─ Average servers per episode: 0")
    
    # === BROKEN SHOWS (DETAILED) ===
    if issues:
        print("\n🚨 BROKEN SHOWS - DETAILED LIST")
        print("-" * 80)
        
        # List movies without servers
        if movies_no_servers > 0:
            print(f"\n❌ Movies without servers ({movies_no_servers}):")
            cursor.execute("""
                SELECT id, title, source_url 
                FROM shows s
                WHERE s.type = 'movie'
                AND s.id NOT IN (
                    SELECT DISTINCT parent_id FROM servers WHERE parent_type = 'movie'
                )
                LIMIT 20
            """)
            for row in cursor.fetchall():
                print(f"  • [{row['id']}] {row['title']}")
            if movies_no_servers > 20:
                print(f"  ... and {movies_no_servers - 20} more")
        
        # List series/anime without seasons
        if shows_no_seasons > 0:
            print(f"\n❌ Series/Anime without seasons ({shows_no_seasons}):")
            cursor.execute("""
                SELECT id, title, type, source_url 
                FROM shows s
                WHERE s.type IN ('series', 'anime')
                AND s.id NOT IN (SELECT DISTINCT show_id FROM seasons)
                LIMIT 20
            """)
            for row in cursor.fetchall():
                print(f"  • [{row['id']}] {row['title']} ({row['type']})")
            if shows_no_seasons > 20:
                print(f"  ... and {shows_no_seasons - 20} more")
        
        # List seasons without episodes
        if seasons_no_episodes > 0:
            print(f"\n❌ Seasons without episodes ({seasons_no_episodes}):")
            cursor.execute("""
                SELECT s.id, sh.title, s.season_number
                FROM seasons s
                JOIN shows sh ON s.show_id = sh.id
                WHERE s.id NOT IN (SELECT DISTINCT season_id FROM episodes)
                LIMIT 20
            """)
            for row in cursor.fetchall():
                print(f"  • [{row['id']}] {row['title']} - Season {row['season_number']}")
            if seasons_no_episodes > 20:
                print(f"  ... and {seasons_no_episodes - 20} more")
    
    # === TOP PERFORMERS ===
    print("\n🏆 TOP PERFORMERS")
    print("-" * 80)
    
    # Top rated shows
    cursor.execute("""
        SELECT title, type, imdb_rating 
        FROM shows 
        WHERE imdb_rating IS NOT NULL 
        ORDER BY imdb_rating DESC 
        LIMIT 10
    """)
    print("\n⭐ Top 10 Highest Rated Shows:")
    for i, row in enumerate(cursor.fetchall(), 1):
        print(f"  {i}. {row['title']} ({row['type']}) - ⭐ {row['imdb_rating']}")
    
    # Series with most episodes
    cursor.execute("""
        SELECT 
            s.title,
            s.type,
            COUNT(DISTINCT e.id) as episode_count
        FROM shows s
        JOIN seasons se ON s.id = se.show_id
        JOIN episodes e ON se.id = e.season_id
        WHERE s.type IN ('series', 'anime')
        GROUP BY s.id
        ORDER BY episode_count DESC
        LIMIT 10
    """)
    print("\n📚 Top 10 Shows by Episode Count:")
    for i, row in enumerate(cursor.fetchall(), 1):
        print(f"  {i}. {row['title']} ({row['type']}) - {row['episode_count']} episodes")
    
    # === FAILED SCRAPES ===
    if failed > 0:
        print("\n❌ FAILED SCRAPES")
        print("-" * 80)
        cursor.execute("""
            SELECT url, error_message 
            FROM scrape_progress 
            WHERE status = 'failed'
            LIMIT 20
        """)
        print(f"\nShowing first 20 of {failed} failed URLs:")
        for row in cursor.fetchall():
            url_short = row['url'].split('/')[-2] if '/' in row['url'] else row['url']
            error = row['error_message'] or 'Unknown error'
            print(f"  • {url_short}: {error}")
        if failed > 20:
            print(f"  ... and {failed - 20} more failures")
    
    # === OVERALL HEALTH SCORE ===
    print("\n" + "=" * 80)
    print("💯 OVERALL HEALTH SCORE")
    print("=" * 80)
    
    # Calculate health metrics
    data_completeness = ((total_shows - movies_no_servers - shows_no_seasons) / total_shows * 100) if total_shows > 0 else 0
    scrape_success = (completed / total_urls * 100) if total_urls > 0 else 0
    server_coverage = ((total_servers / (type_counts.get('movie', 0) + total_episodes)) * 100) if (type_counts.get('movie', 0) + total_episodes) > 0 else 0
    
    health_score = (data_completeness * 0.4 + scrape_success * 0.4 + min(server_coverage, 100) * 0.2)
    
    print(f"\n📊 Data Completeness: {data_completeness:.1f}%")
    print(f"📊 Scrape Success Rate: {scrape_success:.1f}%")
    print(f"📊 Server Coverage: {min(server_coverage, 100):.1f}%")
    print(f"\n🎯 OVERALL HEALTH SCORE: {health_score:.1f}/100")
    
    if health_score >= 90:
        grade = "A+"
        emoji = "🌟"
        comment = "EXCELLENT! Database is in pristine condition!"
    elif health_score >= 80:
        grade = "A"
        emoji = "⭐"
        comment = "GREAT! Very few issues detected."
    elif health_score >= 70:
        grade = "B"
        emoji = "✨"
        comment = "GOOD! Some minor issues to address."
    elif health_score >= 60:
        grade = "C"
        emoji = "💫"
        comment = "FAIR. Several issues need attention."
    else:
        grade = "D"
        emoji = "⚠️"
        comment = "NEEDS WORK. Multiple issues detected."
    
    print(f"\n{emoji} GRADE: {grade} - {comment}")
    
    print("\n" + "=" * 80)
    print("✅ VALIDATION COMPLETE")
    print("=" * 80)
    
    conn.close()
    
    # Return health score for rating
    return health_score, {
        'total_shows': total_shows,
        'total_episodes': total_episodes,
        'total_servers': total_servers,
        'completion_rate': scrape_success,
        'issues': len(issues)
    }

def rate_scraper(health_score, stats):
    """Rate the scraper out of 10 based on performance"""
    print("\n" + "=" * 80)
    print("🎖️  SCRAPER PERFORMANCE RATING")
    print("=" * 80)
    
    # Rating criteria
    criteria_scores = []
    
    # 1. Data Quality (0-2.5 points)
    data_quality = (health_score / 100) * 2.5
    criteria_scores.append(data_quality)
    print(f"\n1. Data Quality & Completeness: {data_quality:.2f}/2.5")
    print(f"   └─ Health Score: {health_score:.1f}%")
    
    # 2. Volume of Data (0-2.5 points)
    total_content = stats['total_shows'] + stats['total_episodes']
    volume_score = min((total_content / 5000) * 2.5, 2.5)  # 5000+ items = max score
    criteria_scores.append(volume_score)
    print(f"\n2. Data Volume: {volume_score:.2f}/2.5")
    print(f"   ├─ Total Shows: {stats['total_shows']}")
    print(f"   └─ Total Episodes: {stats['total_episodes']}")
    
    # 3. Server Availability (0-2.0 points)
    server_ratio = stats['total_servers'] / max(stats['total_shows'] + stats['total_episodes'], 1)
    server_score = min((server_ratio / 3) * 2.0, 2.0)  # 3+ servers per item = max
    criteria_scores.append(server_score)
    print(f"\n3. Server Availability: {server_score:.2f}/2.0")
    print(f"   └─ Average servers per item: {server_ratio:.1f}")
    
    # 4. Scraping Efficiency (0-2.0 points)
    efficiency_score = (stats['completion_rate'] / 100) * 2.0
    criteria_scores.append(efficiency_score)
    print(f"\n4. Scraping Efficiency: {efficiency_score:.2f}/2.0")
    print(f"   └─ Completion Rate: {stats['completion_rate']:.1f}%")
    
    # 5. Reliability (0-1.0 points) - bonus for few issues
    reliability = max(1.0 - (stats['issues'] * 0.1), 0)
    criteria_scores.append(reliability)
    print(f"\n5. Reliability: {reliability:.2f}/1.0")
    print(f"   └─ Critical Issues: {stats['issues']}")
    
    final_rating = sum(criteria_scores)
    
    print("\n" + "-" * 80)
    print(f"🏆 FINAL RATING: {final_rating:.2f}/10.0")
    print("-" * 80)
    
    if final_rating >= 9.0:
        verdict = "🌟 OUTSTANDING! This is a professional-grade scraper!"
    elif final_rating >= 8.0:
        verdict = "⭐ EXCELLENT! Very impressive scraping system!"
    elif final_rating >= 7.0:
        verdict = "✨ VERY GOOD! Solid and reliable scraper!"
    elif final_rating >= 6.0:
        verdict = "💫 GOOD! Works well with minor improvements needed."
    elif final_rating >= 5.0:
        verdict = "👍 DECENT. Functional but needs refinement."
    else:
        verdict = "⚠️  NEEDS IMPROVEMENT. Several areas require work."
    
    print(f"\n{verdict}")
    print("\n" + "=" * 80)

if __name__ == "__main__":
    try:
        health_score, stats = analyze_database()
        rate_scraper(health_score, stats)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
