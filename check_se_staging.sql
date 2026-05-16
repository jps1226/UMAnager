-- Check upcoming race SE records at the CORRECT Oracle offsets (byte 360 len 4, byte 364 len 2)
SELECT
  encode(substring("RawBytes", 12, 16), 'escape') AS race_id,
  encode(substring("RawBytes", 29, 2), 'escape') AS post_pos,
  encode(substring("RawBytes", 360, 4), 'escape') AS odds_bytes,
  encode(substring("RawBytes", 364, 2), 'escape') AS fav_bytes
FROM raw_staging
WHERE "RecordType" = 'SE'
  AND substring("RawBytes"::bytea, 12, 8) = E'20260516'::bytea
LIMIT 10;
