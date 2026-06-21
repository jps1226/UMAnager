#!/usr/bin/env python3
"""
append_tuning_evidence.py — append one weekend's evidence block to tuning_hypotheses.md.

Reads the day's recap_data.json (Part B: Summary.ByType + per-race BetType/Net/RiskUsed) and,
when the Nexus is up, the enriched race card (/api/races?date=) to run the upset
catchable-vs-freak retro. Writes a filled evidence block under the AUTO-APPEND-MARKER in
tuning_hypotheses.md (newest first). Idempotent: skips if the date is already logged.

Usage:
  python tools/append_tuning_evidence.py                 # uses recap_data.json's Date
  python tools/append_tuning_evidence.py --date 2026-06-28
  python tools/append_tuning_evidence.py --dry-run       # print the block, don't write
  python tools/append_tuning_evidence.py --force         # append even if the date already exists

Designed to be run by hand each weekend, OR called as a final step by the 5am recap routine
(after it marks recap_data.json processed). No external side effects — only reads recap_data.json
+ the local API and appends to a local markdown file.
"""
import argparse, json, sys, os, urllib.request
from collections import defaultdict

# The Windows console is often cp932 (Shift-JIS) and can't encode ¥ / ◎ / em-dash. Force UTF-8
# stdout/stderr so prints never crash; the file itself is always written UTF-8 regardless.
for _s in (sys.stdout, sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RECAP    = os.path.join(ROOT, "recap_data.json")
LOG      = os.path.join(ROOT, "tuning_hypotheses.md")
MARKER   = "<!-- AUTO-APPEND-MARKER"
API_BASE = "http://localhost:5000"

def num(x):
    try:
        if x is None or x == "": return None
        return float(x)
    except (TypeError, ValueError):
        return None

def load_recap():
    with open(RECAP, encoding="utf-8-sig") as f:
        return json.load(f)

def fetch_card(date):
    """Enriched card for the date, or None if the Nexus isn't reachable."""
    try:
        with urllib.request.urlopen(f"{API_BASE}/api/races?date={date}", timeout=60) as r:
            d = json.loads(r.read().decode("utf-8-sig"))
        return d.get("races_by_date", {}).get(date)
    except Exception as e:
        print(f"  (card fetch failed — retro skipped: {e})", file=sys.stderr)
        return None

def rank_in_field(winner_id, field, key, higher_better=True):
    have = [(e["Horse_ID"], num(e.get(key))) for e in field if num(e.get(key)) is not None]
    have.sort(key=lambda t: t[1], reverse=higher_better)
    order = [hid for hid, _ in have]
    return (order.index(winner_id) + 1, len(have)) if winner_id in order else (None, len(have))

def upset_retro(card):
    """Classify each upset winner (FavRank>=4) as catchable/semi/freak from the engine's signals."""
    catch, semi, freak, catch_tracks = 0, 0, 0, []
    if not card:
        return None
    for race in card:
        field = race.get("entries") or []
        wins = [e for e in field if str(e.get("Finish")) == "1"]
        if not wins:
            continue
        win = wins[0]
        fav = num(win.get("Fav"))
        if fav is None or fav < 4:
            continue
        N = len(field)
        fr, _ = rank_in_field(win["Horse_ID"], field, "Form_Score")
        spr, _ = rank_in_field(win["Horse_ID"], field, "Sire_Place_Fit")
        sur = num(win.get("Surface_Place_Pct"))
        form_top = fr is not None and fr <= 4
        form_mid = fr is not None and fr <= max(4, N // 2)
        corrob = (sur is not None and sur >= 50) or (spr is not None and spr <= 3)
        if form_top:
            catch += 1
            catch_tracks.append(race["info"]["place_name"])
        elif form_mid and corrob:
            semi += 1
        else:
            freak += 1
    return {"catch": catch, "semi": semi, "freak": freak, "catch_tracks": catch_tracks}

def build_block(recap, card):
    date = recap["Date"]
    s = recap.get("Summary", {})
    staked = s.get("TotalStakedYen", 0)
    won = s.get("TotalWonYen", 0)
    net = s.get("NetYen", 0)
    marked = s.get("RacesMarked", 0)
    rec_pct = round(100 * won / staked) if staked else 0
    races = recap.get("Races") or []

    hon = sum(1 for r in races if (r.get("UserResult") or {}).get("HonmeiHit"))
    qb  = sum(1 for r in races if (r.get("UserResult") or {}).get("QBoxHit"))
    tb  = sum(1 for r in races if (r.get("UserResult") or {}).get("TBoxHit"))
    ups = sum(1 for r in races if (r.get("Result") or {}).get("Upset"))
    chk = sum(1 for r in races if (r.get("Result") or {}).get("ChalkFail"))
    total_races = len(races)

    bytype = sorted(s.get("ByType") or [], key=lambda b: b.get("Net", 0))
    bt_str = " · ".join(f"{b['Type']} ¥{b['Net']:,} ({b['Hits']}/{b['Races']})" for b in bytype) or "(none)"

    surf = defaultdict(lambda: [0, 0, 0, 0])  # races, honmeiHits, staked, won
    for r in races:
        sf = r.get("Surface") or "?"
        surf[sf][0] += 1
        if (r.get("UserResult") or {}).get("HonmeiHit"):
            surf[sf][1] += 1
        if r.get("Staked") is not None:
            surf[sf][2] += r.get("Staked") or 0
            surf[sf][3] += r.get("Won") or 0
    surf_str = " · ".join(
        f"{k} ¥{wn-st:,} (◎ {h}/{n})" for k, (n, h, st, wn) in
        sorted(surf.items(), key=lambda kv: kv[1][3]-kv[1][2])
    )

    risks = sorted({r.get("RiskUsed") for r in races if r.get("RiskUsed") is not None})
    risk_str = "/".join(str(int(x)) for x in risks) if risks else "?"

    retro = upset_retro(card)
    if retro:
        tracks = ", ".join(sorted(set(retro["catch_tracks"]))) if retro["catch_tracks"] else "—"
        retro_str = (f"CATCHABLE {retro['catch']} · SEMI {retro['semi']} · **FREAK {retro['freak']}** "
                     f"(catchable tracks: {tracks})")
    else:
        retro_str = "_(Nexus offline — run the upset catchable/freak pass manually)_"

    return (
        f"### {date} (JST)  ·  RISK {risk_str}\n"
        f"- **Result:** net **¥{net:,}** · ¥{won:,} back on ¥{staked:,} = **{rec_pct}% recovery** · "
        f"◎ {hon}/{marked} · Q {qb} · T {tb}.\n"
        f"- **Market chaos:** upsets (winner FavRank≥4) **{ups}/{total_races}** · "
        f"chalk-fails (fav1 out of top-3) **{chk}/{total_races}**.\n"
        f"- **ByType (worst-first):** {bt_str}.\n"
        f"- **Surface:** {surf_str}.\n"
        f"- **Upset retro:** {retro_str}.\n"
        f"- **Read / which hypotheses it moves:** _(fill in — does this corroborate H1/H2/H3 or contradict?)_\n"
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="override date (default: recap_data.json Date)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    recap = load_recap()
    if args.date:
        recap["Date"] = args.date
    date = recap["Date"]

    card = fetch_card(date)
    block = build_block(recap, card)

    if args.dry_run:
        print("----- DRY RUN: block that WOULD be inserted -----\n")
        print(block)
        return

    with open(LOG, encoding="utf-8") as f:
        existing = f.read()
    if f"### {date}" in existing and not args.force:
        print(f"'{date}' already logged in tuning_hypotheses.md — nothing to do (use --force to re-append).")
        return

    idx = existing.find(MARKER)
    if idx == -1:
        print("AUTO-APPEND-MARKER not found in tuning_hypotheses.md — aborting.", file=sys.stderr)
        sys.exit(1)
    insert_at = existing.find("\n", idx) + 1
    updated = existing[:insert_at] + "\n" + block + existing[insert_at:]
    with open(LOG, "w", encoding="utf-8") as f:
        f.write(updated)
    print(f"Appended evidence block for {date} to tuning_hypotheses.md.")

if __name__ == "__main__":
    main()
