-- UMAnager v2.0 PostgreSQL Schema

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    id INT PRIMARY KEY,
    version INT NOT NULL,
    jvlink_version VARCHAR(20) NOT NULL,
    sdk_version VARCHAR(20) NOT NULL,
    applied_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Horse master archive (競走馬マスタ)
CREATE TABLE IF NOT EXISTS horses (
    horse_id VARCHAR(10) PRIMARY KEY,
    horse_name_japanese TEXT,
    horse_name_romaji VARCHAR(255),
    birth_year INT,
    sire_id VARCHAR(10) REFERENCES horses(horse_id),
    dam_id VARCHAR(10) REFERENCES horses(horse_id),
    broodmare_sire_id VARCHAR(10) REFERENCES horses(horse_id),
    last_updated TIMESTAMPTZ,
    data_source VARCHAR(20) -- 'UM' or 'CK'
);

-- Race metadata
CREATE TABLE IF NOT EXISTS races (
    race_id VARCHAR(16) PRIMARY KEY,  -- YYYYMMDDPPNNNNNN
    race_date DATE,
    track_code VARCHAR(10),
    race_number INT,
    race_name_japanese TEXT,
    distance INT,                     -- meters
    surface VARCHAR(10),              -- 'turf' or 'dirt'
    grade VARCHAR(10),                -- 'G1', 'G2', 'listed', 'open', etc.
    race_conditions TEXT,             -- age/sex restrictions
    last_updated TIMESTAMPTZ
);

-- Who runs in each race
CREATE TABLE IF NOT EXISTS race_entries (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    race_id VARCHAR(16) REFERENCES races(race_id),
    horse_id VARCHAR(10) REFERENCES horses(horse_id),
    post_position INT,
    frame_number INT,
    horse_weight INT,
    jockey_name TEXT,
    trainer_name TEXT,
    morning_line_odds DECIMAL(10, 2),
    latest_odds DECIMAL(10, 2),
    finish_position INT,
    finish_time_hundredths INT,       -- time * 100 (in 1/100 seconds)
    payoff_win DECIMAL(10, 2),
    payoff_place DECIMAL(10, 2),
    payoff_show DECIMAL(10, 2),
    updated_at TIMESTAMPTZ
);

-- JV-Link synchronization state
CREATE TABLE IF NOT EXISTS sync_state (
    id INT PRIMARY KEY,
    last_timestamp_um BIGINT,         -- LastFileTimestamp from DIFN/DIFF fetch
    last_timestamp_races BIGINT,      -- LastFileTimestamp from TOKURACESNPN fetch
    last_sync_at TIMESTAMPTZ,
    last_error TEXT,
    sync_count INT
);

-- User's saved betting slips
CREATE TABLE IF NOT EXISTS bets_saved (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    race_id VARCHAR(16) REFERENCES races(race_id),
    bet_type VARCHAR(50),             -- 'win', 'exacta', 'trifecta', etc.
    horses_json TEXT,                 -- JSON array of horse IDs
    odds_json TEXT,                   -- JSON of odds at save time
    created_at TIMESTAMPTZ,
    exported_at TIMESTAMPTZ
);

-- Initialize sync_state with single row
INSERT INTO sync_state (id, last_timestamp_um, last_timestamp_races, last_sync_at, sync_count)
VALUES (1, 0, 0, CURRENT_TIMESTAMP, 0)
ON CONFLICT (id) DO NOTHING;

-- Initialize schema_version
INSERT INTO schema_version (id, version, jvlink_version, sdk_version)
VALUES (1, 1, '1.12', '4.9.0.1')
ON CONFLICT (id) DO NOTHING;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_race_entries_race_id ON race_entries(race_id);
CREATE INDEX IF NOT EXISTS idx_race_entries_horse_id ON race_entries(horse_id);
CREATE INDEX IF NOT EXISTS idx_horses_sire_id ON horses(sire_id);
CREATE INDEX IF NOT EXISTS idx_horses_dam_id ON horses(dam_id);
CREATE INDEX IF NOT EXISTS idx_races_race_date ON races(race_date);
CREATE INDEX IF NOT EXISTS idx_bets_saved_race_id ON bets_saved(race_id);
CREATE INDEX IF NOT EXISTS idx_bets_saved_created_at ON bets_saved(created_at);
