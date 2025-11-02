# Database Schema Rating & Analysis

## Current Rating: **6.5/10** ⭐⭐⭐⭐⭐⭐☆☆☆☆

---

## ✅ Strengths (What's Good)

### 1. **Normalized Structure** (Good)
- Proper separation of concerns: shows → seasons → episodes → servers
- Follows relational database best practices
- Prevents data duplication

### 2. **Foreign Key Constraints** (Good)
- CASCADE deletes ensure data integrity
- Proper relationships between tables

### 3. **Flexible Metadata System** (Good)
- EAV pattern for `show_metadata` allows dynamic attributes
- Can store genres, cast, directors, etc. without schema changes

### 4. **Scraping Progress Tracking** (Good)
- Resumable scraping with `scrape_progress` table
- Error tracking for debugging

---

## ❌ Critical Issues for Production Website

### 1. **Missing Indexes** ⚠️ CRITICAL
**Impact**: Slow search queries, poor performance with large datasets

**Missing indexes:**
```sql
-- Shows table
CREATE INDEX idx_shows_type ON shows(type);
CREATE INDEX idx_shows_title ON shows(title);
CREATE INDEX idx_shows_year ON shows(year);
CREATE INDEX idx_shows_imdb_rating ON shows(imdb_rating);

-- Metadata table (for filtering by genre, cast, etc.)
CREATE INDEX idx_metadata_key ON show_metadata(key);
CREATE INDEX idx_metadata_value ON show_metadata(value);
CREATE INDEX idx_metadata_show_id ON show_metadata(show_id);

-- Seasons/Episodes
CREATE INDEX idx_seasons_show_id ON seasons(show_id);
CREATE INDEX idx_episodes_season_id ON episodes(season_id);
CREATE INDEX idx_servers_episode_id ON servers(episode_id);

-- Scrape progress
CREATE INDEX idx_scrape_status ON scrape_progress(status);
```

### 2. **No Full-Text Search** ⚠️ CRITICAL
**Impact**: Cannot search titles/synopsis efficiently

**Solution needed:**
```sql
-- SQLite FTS5 virtual table
CREATE VIRTUAL TABLE shows_fts USING fts5(
    title, 
    synopsis, 
    content=shows, 
    content_rowid=id
);
```

### 3. **Missing URL/Slug Field** ⚠️ HIGH
**Impact**: Cannot generate SEO-friendly URLs

**Add to shows table:**
```sql
slug TEXT UNIQUE  -- e.g., "the-believers-2024"
source_url TEXT   -- Original scraping URL
```

### 4. **No View Count/Popularity Metrics** ⚠️ HIGH
**Impact**: Cannot sort by popularity or trending

**Add to shows table:**
```sql
view_count INTEGER DEFAULT 0
last_viewed_at TIMESTAMP
popularity_score REAL  -- Calculated field
```

### 5. **Metadata Not Normalized** ⚠️ MEDIUM
**Impact**: Difficult to filter by genre, cast, etc.

**Better approach:**
```sql
CREATE TABLE genres (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);

CREATE TABLE show_genres (
    show_id INTEGER,
    genre_id INTEGER,
    FOREIGN KEY (show_id) REFERENCES shows(id),
    FOREIGN KEY (genre_id) REFERENCES genres(id),
    PRIMARY KEY (show_id, genre_id)
);

-- Similar for cast, directors, countries
```

### 6. **Missing Episode Metadata** ⚠️ MEDIUM
**Impact**: Cannot display episode titles, air dates, descriptions

**Add to episodes table:**
```sql
title TEXT
air_date DATE
synopsis TEXT
duration INTEGER  -- in minutes
```

### 7. **No User-Related Tables** ⚠️ HIGH
**Impact**: Cannot track watchlists, favorites, watch history

**Needed tables:**
```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT UNIQUE,
    email TEXT UNIQUE,
    created_at TIMESTAMP
);

CREATE TABLE watchlist (
    user_id INTEGER,
    show_id INTEGER,
    added_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (show_id) REFERENCES shows(id),
    PRIMARY KEY (user_id, show_id)
);

CREATE TABLE watch_history (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    episode_id INTEGER,
    watched_at TIMESTAMP,
    progress_seconds INTEGER,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (episode_id) REFERENCES episodes(id)
);
```

### 8. **No Server Quality/Status Tracking** ⚠️ MEDIUM
**Impact**: Cannot prioritize working servers or track quality

**Add to servers table:**
```sql
quality TEXT  -- '1080p', '720p', '480p'
language TEXT  -- 'ar', 'en', 'dubbed'
is_active BOOLEAN DEFAULT 1
last_checked TIMESTAMP
response_time_ms INTEGER
```

### 9. **Missing Timestamps** ⚠️ LOW
**Impact**: Cannot track when content was updated

**Add to more tables:**
```sql
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
```

### 10. **No Caching/Performance Tables** ⚠️ MEDIUM
**Impact**: Repeated complex queries will be slow

**Suggested:**
```sql
CREATE TABLE trending_cache (
    show_id INTEGER PRIMARY KEY,
    score REAL,
    updated_at TIMESTAMP,
    FOREIGN KEY (show_id) REFERENCES shows(id)
);
```

---

## 📊 Detailed Breakdown

| Category | Score | Notes |
|----------|-------|-------|
| **Schema Design** | 7/10 | Good normalization, but missing key fields |
| **Indexing** | 2/10 | No indexes = slow queries |
| **Search Capability** | 3/10 | No FTS, limited search options |
| **Scalability** | 5/10 | Will struggle with 10k+ shows |
| **User Features** | 0/10 | No user-related tables |
| **SEO Readiness** | 3/10 | Missing slugs, metadata |
| **Performance** | 4/10 | No indexes or caching |
| **Data Integrity** | 8/10 | Good FK constraints |
| **Flexibility** | 7/10 | Metadata table is flexible |
| **Production Ready** | 4/10 | Needs significant work |

---

## 🎯 Priority Improvements for Website

### **Phase 1: Critical (Do First)** 🔴
1. Add all indexes (performance)
2. Add FTS5 for search
3. Add slug field for SEO URLs
4. Normalize genres/cast into separate tables

### **Phase 2: High Priority** 🟡
1. Add user tables (watchlist, history)
2. Add view count/popularity tracking
3. Add episode metadata (titles, descriptions)
4. Add server quality tracking

### **Phase 3: Nice to Have** 🟢
1. Add caching tables
2. Add rating/review system
3. Add recommendation engine tables
4. Add admin/moderation tables

---

## 🚀 Recommended Actions

### Immediate Next Steps:
1. **Run the improved schema** (see `01_init_database_improved.py` below)
2. **Migrate existing data** to new schema
3. **Add search API** with FTS5
4. **Implement caching layer** (Redis or in-DB)

---

## 💡 Example Queries Your Website Will Need

### Current Schema Limitations:
```sql
-- ❌ SLOW: Search by title (no index)
SELECT * FROM shows WHERE title LIKE '%believers%';

-- ❌ SLOW: Get all action shows (metadata not normalized)
SELECT DISTINCT s.* FROM shows s
JOIN show_metadata m ON s.id = m.show_id
WHERE m.key = 'genres' AND m.value LIKE '%Action%';

-- ❌ IMPOSSIBLE: Get trending shows (no view tracking)
-- Cannot do this with current schema

-- ❌ SLOW: Get all episodes for a show (no direct link)
SELECT e.* FROM episodes e
JOIN seasons s ON e.season_id = s.id
WHERE s.show_id = 1;
```

### With Improved Schema:
```sql
-- ✅ FAST: Search by title (with FTS5)
SELECT * FROM shows_fts WHERE shows_fts MATCH 'believers';

-- ✅ FAST: Get all action shows (normalized)
SELECT s.* FROM shows s
JOIN show_genres sg ON s.id = sg.show_id
JOIN genres g ON sg.genre_id = g.id
WHERE g.name = 'Action';

-- ✅ FAST: Get trending shows (with tracking)
SELECT * FROM shows 
ORDER BY view_count DESC, imdb_rating DESC 
LIMIT 10;

-- ✅ FAST: Get all episodes for a show (with index)
SELECT e.* FROM episodes e
JOIN seasons s ON e.season_id = s.id
WHERE s.show_id = 1
ORDER BY s.season_number, e.episode_number;
```

---

## 📝 Summary

**Current State**: Basic scraper database (6.5/10)
**Production Ready**: No (needs significant improvements)
**Estimated Work**: 2-3 days to implement critical improvements

**Bottom Line**: The schema is a good foundation but needs indexes, search capabilities, and user-related features before it can power a production website.
