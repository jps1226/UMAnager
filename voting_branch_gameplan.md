# Voting Branch Gameplan

This branch should be treated as a voting-workspace product pass, not just a UI cleanup. The priority is to make the workspace trustworthy first, then faster to use, then analytically useful.

## Branch Objectives

1. Persist strategy context with each race's saved marks.
2. Reconcile prediction-hit states with actual OrePro profitability.
3. Reduce header clutter and make sync state more obvious.
4. Upgrade navigation so the voting sidebar becomes the primary work surface.
5. Add analytics that explain which strategy and hit types actually generate profit.

## Current Constraints

1. `data/saved_marks.json` is still a flat `{ raceId_horseId: symbol }` map.
2. `GET /api/marks` and `POST /api/marks` in `routers/races.py` only support that flat map.
3. Risk strategy state lives only in current UI config (`data/config.json` -> `ui.riskSlider` and `ui.formulaWeights`) and is not attached to individual races.
4. OrePro sync persists actual race-level purchase/payout/profit in `data/orepro_results_history.json`, but there is no joined race evaluation model yet.
5. Search still navigates the main race tab model first and is not a true voting-sidebar-first jump flow.

## Phase 1: Marks Schema Upgrade

### Goal

Add race-level metadata without breaking current marks rendering.

### New `saved_marks.json` shape

Move from:

```json
{
  "202606020510_2023101234": "◎"
}
```

To:

```json
{
  "version": 2,
  "marks": {
    "202606020510_2023101234": "◎"
  },
  "raceMeta": {
    "202606020510": {
      "savedAt": "2026-03-16T12:05:10",
      "updatedAt": "2026-03-16T12:06:40",
      "markSource": "auto-pick",
      "strategySnapshot": {
        "riskSlider": 88,
        "riskLabel": "Max Chaos",
        "formulaWeights": {
          "oddsCap": 100,
          "formMultiplier": 100,
          "freshnessBonus": 3,
          "freshnessBreakeven": 10,
          "pedigreeMultiplier": 30
        }
      },
      "manualAdjustments": 1,
      "lockStateAtSave": false,
      "activeSymbols": ["◎", "〇", "▲", "△"]
    }
  }
}
```

### Backend changes

Files:

1. `routers/races.py`
2. `storage.py`
3. `config.py` only if a new file path constant is needed

Tasks:

1. Add a normalization helper that loads either legacy flat marks or the new versioned object.
2. Keep `GET /api/marks` backward-compatible for one step by returning both:
   - `marks`
   - `raceMeta`
   - `version`
3. Replace the current `POST /api/marks` with a typed payload model instead of raw `dict`.
4. Add migration-on-read logic so existing `saved_marks.json` is upgraded automatically the first time it is saved.

### Frontend changes

Files:

1. `static/script.js`

Tasks:

1. Replace `globalMarks = await marksRes.json();` with parsing that supports both legacy and new payloads.
2. Add `globalRaceMeta = {};`.
3. Replace `saveMarksToServer()` with a payload writer that includes metadata.
4. Capture strategy snapshot on these actions:
   - `autoPick(...)`
   - `reorderPicks(...)`
   - manual mark changes in `toggleMark(...)`
   - race-level clear actions
5. Define `markSource` values:
   - `manual`
   - `auto-pick`
   - `reordered`
   - `mixed`

### Definition of done

1. Marks still render normally after migration.
2. Existing users do not lose marks.
3. Each race has strategy metadata after any new edit.

## Phase 2: Canonical Race Evaluation Model

### Goal

Give each past race a single derived state that combines pick correctness with money outcome.

### New derived client/server model

For each race on a synced day, compute:

```json
{
  "raceId": "202606020510",
  "date": "2026-03-14",
  "track": "NAKAYAMA",
  "raceNumber": 10,
  "hit": {
    "honmei": true,
    "quinellaBox": true,
    "trioBox": false,
    "bestHitType": "quinella"
  },
  "economics": {
    "purchase": 10000,
    "payout": 52150,
    "profit": 42150,
    "roiPct": 521.5
  },
  "displayState": "profitable-hit",
  "strategy": {
    "riskSlider": 88,
    "riskLabel": "Max Chaos",
    "markSource": "mixed"
  }
}
```

### Display state rules

1. `miss-loss`: no hit, negative PnL
2. `miss-flat`: no hit, zero PnL
3. `pyrrhic-hit`: hit exists, but profit < 0
4. `breakeven-hit`: hit exists, profit == 0
5. `profitable-hit`: hit exists, profit > 0
6. `profit-no-hit`: rare fallback if OrePro data exists but mark hit logic says no

### Backend option

Preferred file:

1. `routers/orepro.py`

Add a new endpoint:

1. `GET /api/voting/performance?date=YYYY-MM-DD`

Response should join:

1. race info from existing race cache
2. mark metadata from `saved_marks.json`
3. OrePro day history from `orepro_results_history.json`
4. hit logic equivalent to current `evaluateRaceRecap(...)`

### Frontend option

Interim step:

1. Compute the joined model client-side in `static/script.js` first.
2. Move it server-side only if reuse/export needs it.

### Definition of done

1. Each voting card can answer both “was I right?” and “did I make money?”
2. Pyrrhic races are visually distinct from profitable hits.

## Phase 3: Voting Card Visual States

### Goal

Stop using hit badges alone as the dominant success language.

### Changes

Files:

1. `static/script.js`
2. `static/style.css`

Tasks:

1. Keep existing badges for:
   - `◎ Win`
   - `Q Box`
   - `T Box`
2. Add card-level classes driven by `displayState`:
   - `.race-state-profitable-hit`
   - `.race-state-pyrrhic-hit`
   - `.race-state-miss-loss`
3. Change hit badge treatment when negative PnL exists:
   - hollow/striped variants for pyrrhic hits
4. Keep PnL chips but also add a subtle border or header accent so the state is visible even before reading the chips.

### Definition of done

1. A profitable race and an unprofitable “correct” race no longer look equally successful.

## Phase 4: Header and Sync Workflow Cleanup

### Goal

Make the voting page feel like a focused workspace rather than a tool dump.

### Layout changes

Files:

1. `index.html`
2. `static/style.css`
3. `static/script.js`

Tasks:

1. Move `nkauth` and profile controls into a collapsed `Connection Settings` drawer.
2. Keep in the main action bar:
   - `Sync Results`
   - `Last Synced`
   - current selected date
3. Demote JST/Central clocks into a smaller utility strip.
4. Keep `Next Race In` as the prominent live workspace signal.

### Sync-state addition

Use existing timestamps from OrePro payload/history to show:

1. `Last synced 11:09 JST`
2. or relative wording like `Last synced 2m ago`

### Definition of done

1. OrePro connection controls no longer dominate the center of the page.
2. The user can confirm freshness without opening the details panel.

## Phase 5: Sidebar-First Navigation

### Goal

Reduce scroll friction when reviewing synced results.

### Changes

Files:

1. `static/script.js`
2. `index.html`
3. `static/style.css`

Tasks:

1. Promote horse search into a command-palette-like jump action.
2. Add direct race jump support for:
   - track + race number
   - horse name
   - only races with OrePro results
3. When in voting mode, jump should target the voting sidebar card first, not the main race-table tab body.
4. Add quick filters above sidebar:
   - all
   - hits
   - profitable
   - pyrrhic
   - misses

### Definition of done

1. The search/jump workflow feels native to the voting workspace.

## Phase 6: Strategy and Hit-Type Analytics

### Goal

Answer which styles and hit types actually produce profit.

### Minimum analytics tables

1. `Strategy Performance`

Columns:

1. strategy label
2. races
3. hit rate
4. purchase
5. payout
6. profit
7. ROI

2. `Hit-Type Economics`

Columns:

1. hit type (`◎`, `Q Box`, `T Box`)
2. race count
3. % of hits
4. total profit contribution
5. % of total profit

3. `Pyrrhic Review`

Columns:

1. date
2. race
3. hit type
4. purchase
5. payout
6. loss
7. strategy label

### Backend support

Preferred new endpoint:

1. `GET /api/voting/analytics?from=YYYY-MM-DD&to=YYYY-MM-DD`

### Definition of done

1. You can answer “Chaos gets more trio hits but worse ROI” from saved data instead of intuition.

## Recommended Pull Request Sequence

### PR 1

1. marks schema migration
2. race metadata capture
3. backward-compatible marks API

### PR 2

1. joined race evaluation model
2. pyrrhic/profitable/miss display states
3. last-synced indicator

### PR 3

1. connection settings collapse
2. header cleanup
3. sidebar-first jump/search improvements

### PR 4

1. strategy performance table
2. hit-type economics
3. pyrrhic review table

## Exact Code Touch List

### Backend

1. `routers/races.py`
2. `routers/orepro.py`
3. `storage.py`

### Frontend

1. `index.html`
2. `static/script.js`
3. `static/style.css`

### Data files affected

1. `data/saved_marks.json`
2. `data/orepro_results_history.json`
3. `data/config.json`

## Suggested Non-Goals For This Branch

1. Full charting library integration before the joined race model is stable.
2. Rebuilding the entire main races tab.
3. Automating OrePro authentication.
4. Replacing the existing mark symbols.

## Immediate Next Implementation Step

Start with PR 1 only:

1. upgrade `saved_marks.json`
2. add `globalRaceMeta`
3. capture strategy snapshots on mark save
4. keep all current vote rendering behavior intact

That is the smallest change that unlocks every later analytic and UI improvement.