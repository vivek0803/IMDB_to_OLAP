-- =============================================================================
-- ddl/create_tables.sql
-- ClickHouse DDL for the IMDb OLAP layer
--
-- Engine: MergeTree family
--   • Data is sorted on disk by ORDER BY at write/merge time, enabling a
--     sparse primary index (1 entry per index_granularity rows).
--   • PARTITION BY physically segregates files so queries skip entire
--     directories when the partition key is filtered.
--   • LowCardinality(String) stores repeated string columns as dictionary-
--     encoded integers — 4–10× less memory and faster GROUP BY / ORDER BY.
--
-- Auto-applied on first `docker compose up` because this file is mounted to
-- /docker-entrypoint-initdb.d/ in the ClickHouse container
-- =============================================================================

CREATE DATABASE IF NOT EXISTS imdb;

-- ── 1. imdb.titles ────────────────────────────────────────────────────────────
-- One row per IMDb title (movie, tvSeries, short, tvEpisode, etc.)
-- joined with its aggregated rating data.
--
-- PARTITION BY (titleType, intDiv(coalesce(startYear, 0), 10))
--   Dual-axis partition: category (titleType) × time (decade).
--   A query for "movies from the 1990s" reads exactly one partition cell.
--
-- ORDER BY (titleType, startYear, tconst)
--   Sparse primary index on the two most-filtered columns.

CREATE TABLE IF NOT EXISTS imdb.titles
(
    tconst          String                    COMMENT 'IMDb title identifier (e.g. tt0111161)',
    titleType       LowCardinality(String)    COMMENT 'movie | tvSeries | tvEpisode | short | …',
    primaryTitle    String                    COMMENT 'Promotional / common title',
    originalTitle   String                    COMMENT 'Title in original language',
    isAdult         UInt8                     COMMENT '0 = non-adult, 1 = adult',
    startYear       Nullable(UInt16)          COMMENT 'Release year (or series start year)',
    endYear         Nullable(UInt16)          COMMENT 'TV series end year; NULL for other types',
    runtimeMinutes  Nullable(UInt32)          COMMENT 'Primary runtime in minutes',
    genres          Array(String)             COMMENT 'Up to 3 genres e.g. [Action, Drama]',
    primaryGenre    LowCardinality(String)    COMMENT 'First genre from the genres array',
    decade          Nullable(UInt16)          COMMENT 'startYear rounded to decade (e.g. 1990)',
    averageRating   Nullable(Float32)         COMMENT 'Weighted average of IMDb user ratings',
    numVotes        Nullable(UInt32)          COMMENT 'Number of votes contributing to rating'
)
ENGINE = MergeTree()
PARTITION BY (titleType, intDiv(coalesce(startYear, 0), 10))
ORDER BY (titleType, coalesce(startYear, 0), tconst)
SETTINGS index_granularity = 8192;

-- Projection: pre-aggregated genre stats for instant genre analytics
ALTER TABLE imdb.titles
    ADD PROJECTION IF NOT EXISTS proj_genre_stats
    (
        SELECT primaryGenre, titleType, decade,
               avg(averageRating) AS avg_rating,
               sum(numVotes)      AS total_votes,
               count()            AS cnt
        GROUP BY primaryGenre, titleType, decade
    );

-- ── 2. imdb.people ────────────────────────────────────────────────────────────
-- One row per person (actor, director, writer, etc.) from name.basics.tsv.
--
-- PARTITION BY primaryProfession_bucket
--   Buckets the ~10 distinct profession categories so queries like
--   "all directors born after 1970" skip actor/writer partitions entirely.
--
-- ORDER BY (nconst)
--   nconst is the lookup key when joining with imdb.principals.

CREATE TABLE IF NOT EXISTS imdb.people
(
    nconst              String                  COMMENT 'IMDb person identifier (e.g. nm0000001)',
    primaryName         String                  COMMENT 'Most credited name',
    birthYear           Nullable(UInt16)        COMMENT 'Birth year',
    deathYear           Nullable(UInt16)        COMMENT 'Death year; NULL if still alive',
    primaryProfession   Array(String)           COMMENT 'Top-3 professions e.g. [actor, producer]',
    knownForTitles      Array(String)           COMMENT 'tconst list of notable titles'
)
ENGINE = MergeTree()
ORDER BY nconst
SETTINGS index_granularity = 8192;

-- ── 3. imdb.principals ────────────────────────────────────────────────────────
-- Cast and crew per title — links imdb.titles ↔ imdb.people.
-- Source: title.principals.tsv (~98 M rows).
--
-- PARTITION BY category
--   ~10 distinct job categories (actor, director, writer, producer, …).
--   Queries scoped to a category (e.g. "all directors of movies rated > 8")
--   skip every other partition.
--
-- ORDER BY (tconst, ordering)
--   Co-locates all cast/crew of a title together; `ordering` gives the
--   billed position (1 = top-billed actor / lead director).

CREATE TABLE IF NOT EXISTS imdb.principals
(
    tconst      String                  COMMENT 'IMDb title identifier',
    ordering    UInt8                   COMMENT 'Rank of this person for this title',
    nconst      String                  COMMENT 'IMDb person identifier',
    category    LowCardinality(String)  COMMENT 'actor | director | writer | producer | …',
    job         Nullable(String)        COMMENT 'Specific job title (e.g. "executive producer")',
    characters  Nullable(String)        COMMENT 'Character name(s) played (JSON array string)'
)
ENGINE = MergeTree()
PARTITION BY category
ORDER BY (tconst, ordering)
SETTINGS index_granularity = 8192;

-- ── 4. imdb.akas ──────────────────────────────────────────────────────────────
-- Alternate titles by region and language from title.akas.tsv (~55 M rows).
-- Useful for localisation queries: "what is the French title of this movie?"
--
-- PARTITION BY region
--   Region codes (US, GB, FR, DE, IN, …) are the primary filter.
--   A query for all French alternate titles reads only partition 'FR'.
--
-- ORDER BY (titleId, region, ordering)
--   Co-locates all alternate titles for a given tconst together.

CREATE TABLE IF NOT EXISTS imdb.akas
(
    titleId         String                  COMMENT 'IMDb title identifier (= tconst)',
    ordering        UInt16                  COMMENT 'Uniquely identifies rows for a given titleId',
    title           String                  COMMENT 'Localised title',
    region          LowCardinality(String)  COMMENT 'Region code (ISO 3166-1 alpha-2)',
    language        LowCardinality(String)  COMMENT 'Language code',
    types           Array(String)           COMMENT 'e.g. [imdbDisplay] or [alternative]',
    attributes      Array(String)           COMMENT 'e.g. [literal English title]',
    isOriginalTitle UInt8                   COMMENT '1 = this is the original-language title'
)
ENGINE = MergeTree()
PARTITION BY region
ORDER BY (titleId, region, ordering)
SETTINGS index_granularity = 8192;

-- ── Useful views ──────────────────────────────────────────────────────────────

-- Top-rated movies (commonly queried; no storage cost in ClickHouse)
CREATE VIEW IF NOT EXISTS imdb.top_movies AS
SELECT
    tconst,
    primaryTitle,
    startYear,
    primaryGenre,
    averageRating,
    numVotes,
    decade
FROM imdb.titles
WHERE
    titleType     = 'movie'
    AND numVotes  >= 25000
    AND averageRating IS NOT NULL
ORDER BY averageRating DESC, numVotes DESC;

-- Director filmography enriched with title ratings
CREATE VIEW IF NOT EXISTS imdb.director_filmography AS
SELECT
    p.nconst,
    p.primaryName,
    t.tconst,
    t.primaryTitle,
    t.startYear,
    t.primaryGenre,
    t.averageRating,
    t.numVotes
FROM imdb.principals pr
JOIN imdb.titles  t ON t.tconst = pr.tconst
JOIN imdb.people  p ON p.nconst = pr.nconst
WHERE pr.category = 'director';
