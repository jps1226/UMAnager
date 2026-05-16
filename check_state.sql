SELECT
    'horses' as table_name, COUNT(*) as row_count FROM horses
UNION ALL
SELECT 'races', COUNT(*) FROM races
UNION ALL
SELECT 'race_entries', COUNT(*) FROM race_entries
UNION ALL
SELECT 'raw_staging (UM)', COUNT(*) FROM raw_staging WHERE "RecordType" = 'UM'
UNION ALL
SELECT 'raw_staging (RA)', COUNT(*) FROM raw_staging WHERE "RecordType" = 'RA'
UNION ALL
SELECT 'raw_staging (SE)', COUNT(*) FROM raw_staging WHERE "RecordType" = 'SE'
UNION ALL
SELECT 'raw_staging (unprocessed UM)', COUNT(*) FROM raw_staging WHERE "RecordType" = 'UM' AND "IsProcessed" = false
UNION ALL
SELECT 'raw_staging (unprocessed RA)', COUNT(*) FROM raw_staging WHERE "RecordType" = 'RA' AND "IsProcessed" = false
UNION ALL
SELECT 'raw_staging (unprocessed SE)', COUNT(*) FROM raw_staging WHERE "RecordType" = 'SE' AND "IsProcessed" = false;
