SELECT
  COUNT(*) as total,
  COUNT(CASE WHEN "Odds" > 0 THEN 1 END) as has_odds,
  COUNT(CASE WHEN "FavRank" > 0 THEN 1 END) as has_fav,
  COUNT(CASE WHEN "FinishPos" > 0 THEN 1 END) as has_finish
FROM race_entries;

SELECT r."RaceDate", COUNT(*) as entries,
  COUNT(CASE WHEN re."Odds" > 0 THEN 1 END) as has_odds
FROM race_entries re
JOIN races r ON r."RaceId" = re."RaceId"
WHERE r."RaceDate" >= '2026-05-10'
GROUP BY r."RaceDate"
ORDER BY r."RaceDate";
