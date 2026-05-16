SELECT
    'UM processed' as metric, COUNT(*) FROM raw_staging WHERE "RecordType" = 'UM' AND "IsProcessed" = true
UNION ALL
SELECT 'UM unprocessed', COUNT(*) FROM raw_staging WHERE "RecordType" = 'UM' AND "IsProcessed" = false
UNION ALL
SELECT 'RA processed', COUNT(*) FROM raw_staging WHERE "RecordType" = 'RA' AND "IsProcessed" = true
UNION ALL
SELECT 'RA unprocessed', COUNT(*) FROM raw_staging WHERE "RecordType" = 'RA' AND "IsProcessed" = false
UNION ALL
SELECT 'SE processed', COUNT(*) FROM raw_staging WHERE "RecordType" = 'SE' AND "IsProcessed" = true
UNION ALL
SELECT 'SE unprocessed', COUNT(*) FROM raw_staging WHERE "RecordType" = 'SE' AND "IsProcessed" = false;
