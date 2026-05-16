-- Reset UMAnager Phase 3D Database for Fresh Parsing Test
-- This script clears partial data from the aborted test run

-- Clear all parsed records (fresh start for Phase 3D test)
DELETE FROM race_entries;
DELETE FROM races;
DELETE FROM horses;

-- Reset UM records to unprocessed state (for fresh parsing test)
UPDATE raw_staging
SET is_processed = false
WHERE record_type = 'UM';

-- Verify cleanup
SELECT 'horses table count' as check_name, COUNT(*) as count FROM horses
UNION ALL
SELECT 'races table count', COUNT(*) FROM races
UNION ALL
SELECT 'race_entries table count', COUNT(*) FROM race_entries
UNION ALL
SELECT 'unprocessed UM records', COUNT(*) FROM raw_staging WHERE record_type = 'UM' AND is_processed = false;
