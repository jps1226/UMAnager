TRUNCATE TABLE horses CASCADE;
TRUNCATE TABLE races CASCADE;
TRUNCATE TABLE race_entries CASCADE;
UPDATE raw_staging SET "IsProcessed" = false;
SELECT COUNT(*) as um_unprocessed FROM raw_staging WHERE "RecordType" = 'UM' AND "IsProcessed" = false;
SELECT COUNT(*) as ra_unprocessed FROM raw_staging WHERE "RecordType" = 'RA' AND "IsProcessed" = false;
SELECT COUNT(*) as se_unprocessed FROM raw_staging WHERE "RecordType" = 'SE' AND "IsProcessed" = false;
