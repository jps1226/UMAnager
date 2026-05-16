# Pending Agent Queries — Phase 3D UPSERT Failures

**Status:** 156K UM, 161 RA, 2493 SE records parse but fail to insert (FK constraints suspected).

---

## ORACLE Query (JRA-VAN Official Docs)

**Format Required:** 3 bullet points max, answer only, no explanation.

**Q1: SireId/DamId Forward References**
In UM records, if SireId points to a horse not yet in the DIFN stream, is this:
- (A) Invalid/error condition?
- (B) Valid (orphaned ref, insert anyway, set FK to NULL)?
- (C) Valid (will be resolved in next stream)?

**Q2: SE Record FK Requirements**
SE records reference HorseId and RaceId. If either doesn't exist in current DIFN, should:
- (A) SE record be skipped/deleted?
- (B) SE record be inserted with NULL FK?
- (C) SE record insertion be delayed until dependencies exist?

**Q3: Null Pedigree Fields (SireId, DamId)**
If UM record has SireId=0000000000 or DamId=0000000000, is this:
- (A) Encoded NULL (store as NULL in DB)?
- (B) Error condition?
- (C) Valid (store "0000000000" as string)?

---

## LIBRARIAN Query (kmy-keiba Reference Implementation)

**Format Required:** Code snippets or bullet points only, no prose explanation.

**Q1: Transaction Boundaries for UM/RA/SE**
Show exact pattern: Does kmy-keiba call SaveChanges/Commit:
- After all UM records?
- After all RA records?
- After all SE records?
- Or after each batch regardless of type?

**Q2: SireId/DamId Orphan Handling**
Paste the exact lines where kmy-keiba handles horses with non-existent SireId/DamId:
- Does it validate FK before insert?
- Does it skip/delete the record?
- Does it set FK to NULL?

**Q3: ExecuteSqlRaw Exception Handling**
Show how kmy-keiba wraps batch INSERT/UPDATE operations:
- Try-catch around ExecuteSql call?
- Explicit exception logging before re-throw?
- Silent swallow of constraint violations?

**Q4: Actual Failed Record Example**
Show one real example from kmy-keiba logs of a horse/race/entry that failed to insert and why (paste exact error message if available).

---

## Pending — 2026-05-15 (Race-Card DataSpec)

**Format Required:** Cite exact JV-Link spec wording where possible. Bullet answers, ≤5 lines each.

**Q1: DataSpec for full weekly race-card archives**
Which JV-Link DataSpec delivers the complete set of RA + SE records for every JRA race in a date window (not diffs, not real-time)? List the candidate(s) and what each one's scope is. Distinguish between "setup" mode (option=1) and "normal" mode (option=2) for the chosen DataSpec.

**Q2: Behavior of `DIFN` for RA and SE records**
What exactly does `DIFN` deliver for record types `RA` and `SE`? Is it (a) full current-week race cards, (b) only changed/diff records since last fetch, or (c) something else? Cite the spec section.

**Q3: rc=-1 from JVOpen("RACE", from_time, 2)**
Under what conditions does `JVOpen` with DataSpec=`RACE`, option=2 return rc=-1? Is rc=-1 specifically "no new files since from_time," or can it also indicate a misconfigured DataSpec, an unsupported license tier, or an empty server-side store?

**Q4: How to obtain Saturday's race cards before race day**
What is the canonical JV-Link sequence (DataSpec, option, from_time semantics) used to retrieve the upcoming weekend's race cards (RA + SE) once they have been published by JRA? Include the typical publication day/time relative to race day if documented.

---

## Pending — 2026-05-15 (Round 2: JVStatus polling + DataStatus semantics)

**Context for Oracle:** We are building a Sidecar (x86) that wraps JV-Link and streams raw records to an x64 Nexus. We just confirmed canonical weekend-card retrieval uses `JVOpen("TOKURACESNPN", from_time, 2)`. Two implementation details remain unclear from the previous round.

**Format Required:** Cite exact spec section/wording. ≤5 lines per bullet.

**Q5: JVStatus polling between JVOpen and JVRead**
After `JVOpen` returns `DownloadCount > 0`, the spec mentions polling `JVStatus()` until downloaded files == `DownloadCount`. What exactly does `JVStatus()` return — a count of downloaded files, a percentage, a state code, or something else? What is the precise loop condition that signals "all files materialized, safe to JVRead"? What happens if we call `JVRead` *before* `JVStatus` reports completion — does it return rc=0 (premature EOF), rc=-1 (file boundary), or block?

**Q6: SE record fields when DataStatus=1 (nominations)**
You stated DataStatus=1 = 出走馬名表 (Thursday nominations) with blank bracket/post-position. List every SE field that is **guaranteed blank or zero** when DataStatus=1, vs. fields that are populated. Specifically: are Wakuban (bracket), Umaban (post), Futan (weight), KisyuCode/Name (jockey), Odds, and FavRank all blank, or only a subset? And when DataStatus transitions 1→2, is the existing record updated in place (same primary key), or is a new record emitted?

**Q7: from_time semantics for option=2 weekly pulls**
For `JVOpen("TOKURACESNPN", from_time, 2)`: does `from_time` act as (a) a filter — "give me files modified after this timestamp," or (b) a cursor — "resume from this position in the server's publication queue," or (c) something else? If we pass `from_time = "00000000000000"` (or unix epoch), do we get the full current week, or an error?
