-- Migration: Phase 4 MVP Schema Updates
-- Adds: race_start_time to races, user_horse_lists table, user_settings table, search indexes
-- Date: 2026-05-06

-- 1. Add race_start_time column to races table
ALTER TABLE races ADD COLUMN IF NOT EXISTS race_start_time TIME;

-- 2. Create user_horse_lists table for favorites and watchlist
CREATE TABLE IF NOT EXISTS user_horse_lists (
    id SERIAL PRIMARY KEY,
    horse_id VARCHAR(10) NOT NULL REFERENCES horses(horse_id) ON DELETE CASCADE,
    list_type VARCHAR(20) NOT NULL,  -- 'favorites' or 'watchlist'
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_horse_list UNIQUE(horse_id, list_type)
);

CREATE INDEX IF NOT EXISTS idx_user_horse_lists_type ON user_horse_lists(list_type);
CREATE INDEX IF NOT EXISTS idx_user_horse_lists_horse_id ON user_horse_lists(horse_id);

-- 3. Create user_settings table for persistent settings across devices
CREATE TABLE IF NOT EXISTS user_settings (
    id INT PRIMARY KEY DEFAULT 1,
    settings_json JSONB NOT NULL DEFAULT '{}',
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT single_row CHECK (id = 1)
);

-- 4. Add indexes for horse name search (pg_trgm trigram search)
-- First, ensure pg_trgm extension is installed
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create GIN indexes for fast partial text matching on horse names
CREATE INDEX IF NOT EXISTS idx_horse_name_japanese_trgm
    ON horses USING gin(horse_name_japanese gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_horse_name_romaji_trgm
    ON horses USING gin(horse_name_romaji gin_trgm_ops);

-- Also add basic B-tree indexes for exact prefix matching
CREATE INDEX IF NOT EXISTS idx_horse_name_japanese_prefix
    ON horses (horse_name_japanese VARCHAR_PATTERN_OPS);

CREATE INDEX IF NOT EXISTS idx_horse_name_romaji_prefix
    ON horses (horse_name_romaji VARCHAR_PATTERN_OPS);

-- 5. Add columns for Phase 2+ features
ALTER TABLE bets_saved ADD COLUMN IF NOT EXISTS locked BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE horses ADD COLUMN IF NOT EXISTS horse_name_english VARCHAR(255);

-- 6. Create index for race date queries
CREATE INDEX IF NOT EXISTS idx_races_date_track
    ON races(race_date DESC, track_code);

-- 7. Create index for finding races by start time on a given date
CREATE INDEX IF NOT EXISTS idx_races_date_start_time
    ON races(race_date, race_start_time);
