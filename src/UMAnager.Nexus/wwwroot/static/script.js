let _bootMarks = []; // Boot-phase timing (dev mode only) — see bootMark() below.
function bootMark(label) {
    _bootMarks.push({ label, t: Math.round(performance.now()) });
}
bootMark('scriptStart'); // earliest point this script can run: end of HTML parse + script.js download

// QOL: collapsible sidebar (desktop only — see style.css .sidebar-collapse-toggle / body.sidebar-collapsed).
// Restored here, at the top of the file, so it applies as early as possible and avoids a flash of the
// expanded sidebar on load.
const SIDEBAR_COLLAPSED_KEY = 'umanager-sidebar-collapsed';
if (localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === '1') {
    document.body.classList.add('sidebar-collapsed');
    // The toggle button's HTML hardcodes the expanded icon (‹) — sync it if we're restoring collapsed.
    // Button element already exists: this script tag is at the end of <body>, after the button's markup.
    const _sidebarBtn = document.getElementById('sidebar-collapse-toggle');
    if (_sidebarBtn) { _sidebarBtn.textContent = '›'; _sidebarBtn.title = 'Expand sidebar'; }
}
function toggleSidebarCollapsed() {
    const collapsed = document.body.classList.toggle('sidebar-collapsed');
    localStorage.setItem(SIDEBAR_COLLAPSED_KEY, collapsed ? '1' : '0');
    const btn = document.getElementById('sidebar-collapse-toggle');
    if (btn) {
        btn.textContent = collapsed ? '›' : '‹';
        btn.title = collapsed ? 'Expand sidebar' : 'Collapse sidebar';
    }
    // Collapsing frees enough width that the compact-desktop column set (see isCompactDesktop) no
    // longer needs to apply — re-render so J%/T% etc. reappear immediately, not just on next refresh.
    try { rerenderAllRaceTables(); } catch (_) {}
}

let globalMarks = {};
let globalRaceMeta = {};
let globalMarksVersion = 2;
let listsData = { bloodlines: "", watchlist: "" };
let raceLocks = {}; // Per-race lock state for mark interactions
let upcomingRaces = []; // NEW: Stores our parsed race times
const BET_ESTIMATE_STORAGE_KEY = 'umanager-bet-estimate-cache-v1';
const BET_ESTIMATE_MAX_AGE_MS = 1000 * 60 * 60 * 12;
let globalRaceEntries = {}; // NEW: Stores local row data for instant sorting
let globalRaceClass = {};   // Phase: maiden detection. {r_id: {isMaiden, isDebut}}
let globalRaceInfo = {}; // NEW: Stores the Racetrack names and numbers
let globalRacesByDate = {}; // All race days organized by date for navigation and jump dropdowns
let globalAllRacesByDate = { upcoming: {}, past: {} }; // Full timeline buckets from API
let globalDateTimelineByDate = {}; // Maps YYYY-MM-DD -> "upcoming" | "past"
let currentTimelineTab = "upcoming";
let currentActiveDate = null;
let currentCalendarMonth = null;
let currentMainView = 'races';
let currentHorseId = null;
let sidebarRaceCollapseState = {};
let raceSorts = {}; // Per-race sort state — always kept in sync with globalSort.
let globalSort = { col: 'Default', asc: true }; // Single source of truth; all races mirror this.
let winningVotesFocusEnabled = false;
let searchableHorses = []; // Stores the database for the search bar
let currentSearchSelection = -1; // Tracks keyboard navigation in the dropdown
let appConfig = {}; // NEW: Stores app configuration
let globalVoteHistory = {}; // Phase 30: { horse_id: total count } — sidebar mini-count
let globalVoteHistoryRaces = {}; // Phase 30: { horse_id: [race_id,...] } — badge counts races OTHER than the current one
let globalVoteHistoryRecent = []; // Phase 30: recent 50 vote rows for sidebar history
let globalCalendarSkeleton = {}; // Phase 38: { 'YYYY-MM-DD': { count, timeline } } — all race days (lazy-load source)
let isFirstLoad = true; // NEW: Track if this is the first page load to auto-collapse past races
let renderedDates = new Set(); // Lazy render tracking — cleared on each full re-render
let _devFetchMs = 0;    // headers + body download + JSON.parse
let _devStateMs = 0;    // global state build loop (all races/dates)
let _devSidebarMs = 0;  // renderWeekendWatchlist + renderEnginePicks
let _devRenderMs = 0;   // renderDayTabsAndSchedules → renderDateTab → innerHTML
let raceNameDict = { stakes: {}, classNames: {} }; // Phase 21: Race name translation dictionary

const DEFAULT_RACE_COLUMNS = ["Shirushi", "BK", "PP", "Horse", "Record", "Last3", "J%", "T%", "Sire", "SF", "Dam", "BMS", "Odds", "Fav", "Finish"];

// JRA track codes → romaji names. Source: JRA-VAN spec.
const TRACK_NAMES = {
    "01": "Sapporo", "02": "Hakodate", "03": "Fukushima", "04": "Niigata",
    "05": "Tokyo",   "06": "Nakayama", "07": "Chukyo",    "08": "Kyoto",
    "09": "Hanshin", "10": "Kokura"
};
function trackName(code) {
    if (code === null || code === undefined) return '';
    const s = String(code).padStart(2, '0');
    return TRACK_NAMES[s] || s.toUpperCase();
}

const SCORE_TRACKED_HORSE = 1.0;
const SCORE_TRACKED_SIRE = 0.5;
const SCORE_TRACKED_DAM = 0.5;
const SCORE_TRACKED_BMS = 0.25;
const SCORE_WATCHLIST_HORSE = 1.0;
const SCORE_WATCHLIST_SIRE = 0.5;
const SCORE_WATCHLIST_DAM = 0.5;
const SCORE_WATCHLIST_BMS = 0.25;
const SCORE_MAX = 1.0;
const ICON_THRESHOLD_3STAR = 1.0;
const ICON_THRESHOLD_2STAR = 0.5;

const RACE_COLUMN_META = {
    Shirushi: { label: "Prediction", sortable: true, sortKey: "Shirushi", initialAsc: true },
    BK: { label: "BK", sortable: true, sortKey: "BK", initialAsc: true },
    PP: { label: "PP", sortable: true, sortKey: "PP", initialAsc: true },
    Horse: { label: "Horse", sortable: true, sortKey: "Horse", initialAsc: true },
    Record: { label: "W/S", sortable: true, sortKey: "Record", initialAsc: true },
    Last3: { label: "Form (fav)", sortable: true, sortKey: "Last3", initialAsc: false },
    "J%": { label: "J%", sortable: true, sortKey: "J%", initialAsc: false },
    "T%": { label: "T%", sortable: true, sortKey: "T%", initialAsc: false },
    Sire: { label: "Sire", sortable: true, sortKey: "Sire", initialAsc: true },
    SF: { label: "SF", sortable: true, sortKey: "SF", initialAsc: false },
    Dam: { label: "Dam", sortable: true, sortKey: "Dam", initialAsc: true },
    BMS: { label: "BMS", sortable: true, sortKey: "BMS", initialAsc: true },
    Odds: { label: "Odds", sortable: true, sortKey: "Odds", initialAsc: true },
    Fav: { label: "Fav", sortable: true, sortKey: "Fav", initialAsc: true },
    Finish: { label: "Fin", sortable: true, sortKey: "Finish", initialAsc: true }
};

// Phase 13 follow-up: separate visibility maps for desktop vs mobile. The single
// ordered column list (raceTableColumns) drives both order and desktop visibility;
// raceTableMobileVisibility is just a {key: bool} override applied when the viewport
// is below the mobile breakpoint. Mobile defaults below.
const MOBILE_DEFAULT_VISIBLE = new Set(["Shirushi", "PP", "Horse", "Odds", "Fav", "Finish"]);

// s60: a third tier — "compact desktop". Not phone-width, but the screen is small/dense enough that
// UI Scale sits below 85% (a real laptop, not a deliberate manual choice — see getUiScalePercent, which
// is per-device and includes any dev-only override, so an operator who overrides to e.g. 90% on a small
// screen opts back OUT of this). Drops the lowest-priority columns (jockey/trainer win% — a secondary
// stat, not the primary decision-making ones) to make room, same {key: bool} idea as
// MOBILE_DEFAULT_VISIBLE but scoped to just what's tight on a laptop, not a phone.
const COMPACT_HIDDEN_COLUMNS = new Set(["J%", "T%"]);
function isCompactDesktop() {
    // <= not < : the recalibrated auto-scale formula now lands laptop-class screens EXACTLY on 85
    // (see detectSuggestedUiScale) — a strict "<" would silently exclude the exact case this exists for.
    // Bypassed entirely when the sidebar is collapsed (see toggleSidebarCollapsed) — that frees up
    // roughly a sidebar-width's worth of room, often enough on its own to fit the dropped columns
    // again even though the screen itself hasn't changed.
    try {
        if (document.body.classList.contains('sidebar-collapsed')) return false;
        return !isMobileViewport() && getUiScalePercent() <= 85;
    } catch (_) { return false; }
}

function normalizeRaceColumnsLayout(layout) {
    const valid = new Set(DEFAULT_RACE_COLUMNS);
    const normalized = [];
    const seen = new Set();

    if (Array.isArray(layout)) {
        layout.forEach(item => {
            if (!item || !valid.has(item.key) || seen.has(item.key)) return;
            seen.add(item.key);
            normalized.push({ key: item.key, visible: item.visible !== false });
        });
    }

    DEFAULT_RACE_COLUMNS.forEach(key => {
        if (!seen.has(key)) normalized.push({ key: key, visible: true });
    });

    return normalized;
}

function normalizeMobileVisibilityMap(map) {
    const out = {};
    DEFAULT_RACE_COLUMNS.forEach(key => {
        if (map && typeof map === 'object' && key in map) {
            out[key] = !!map[key];
        } else {
            out[key] = MOBILE_DEFAULT_VISIBLE.has(key);
        }
    });
    return out;
}

function getRaceColumnsLayout() {
    if (!appConfig.ui) appConfig.ui = {};
    appConfig.ui.raceTableColumns = normalizeRaceColumnsLayout(appConfig.ui.raceTableColumns);
    return appConfig.ui.raceTableColumns;
}

function getMobileColumnVisibility() {
    if (!appConfig.ui) appConfig.ui = {};
    appConfig.ui.raceTableMobileVisibility = normalizeMobileVisibilityMap(appConfig.ui.raceTableMobileVisibility);
    return appConfig.ui.raceTableMobileVisibility;
}

function isMobileViewport() {
    // MOBILE_MQ is declared later in the file (TDZ-safe via typeof).
    if (typeof MOBILE_MQ !== 'undefined' && MOBILE_MQ) return MOBILE_MQ.matches;
    return window.matchMedia('(max-width: 768px)').matches;
}

function getVisibleRaceColumns() {
    const layout = getRaceColumnsLayout();
    if (isMobileViewport()) {
        const mob = getMobileColumnVisibility();
        return layout.filter(c => mob[c.key]).map(c => c.key);
    }
    let cols = layout.filter(c => c.visible).map(c => c.key);
    if (isCompactDesktop()) cols = cols.filter(c => !COMPACT_HIDDEN_COLUMNS.has(c));
    return cols;
}

function isVoteSortingEnabled() {
    return appConfig.ui?.voteSortingTop ?? true;
}

function isDebugConsoleEnabled() {
    return appConfig.ui?.debugConsole ?? false;
}

function isDevModeEnabled() {
    return appConfig.ui?.devMode ?? false;
}

// Phase 13: mobile drawer-sidebar toggle. CSS handles the slide animation;
// we just toggle the body class. Pass an explicit boolean to force open/close.
function toggleMobileSidebar(force) {
    const open = (typeof force === 'boolean') ? force : !document.body.classList.contains('mobile-sidebar-open');
    document.body.classList.toggle('mobile-sidebar-open', open);
}

// Phase 13 follow-up: reparent the search bar so it doesn't eat top-bar
// real estate on phones. On mobile it lives in the Race Database sidebar
// group; on desktop it sits in the main toolbar. Move on init + on
// viewport change (rotation, dev-tools resize, etc.).
const MOBILE_MQ = window.matchMedia('(max-width: 768px)');
function relocateSearchBar() {
    const search = document.querySelector('.search-container');
    if (!search) return;
    const mobileSlot = document.getElementById('mobile-search-slot');
    const desktopHome = document.querySelector('.main-toolbar-left');
    const target = MOBILE_MQ.matches ? mobileSlot : desktopHome;
    if (target && search.parentNode !== target) {
        target.insertBefore(search, target.firstChild);
    }
}
if (MOBILE_MQ.addEventListener) {
    MOBILE_MQ.addEventListener('change', () => { relocateSearchBar(); rerenderAllRaceTables(); });
}

// Re-render every visible race table (used when the viewport crosses the
// mobile breakpoint and the column-visibility set changes).
function rerenderAllRaceTables() {
    Object.keys(globalRaceEntries || {}).forEach(r_id => {
        const tbody = document.getElementById(`tbody-${r_id}`);
        const thead = document.getElementById(`thead-${r_id}`);
        if (tbody) tbody.innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);
        if (thead) thead.innerHTML = buildTableHeaderRow(r_id);
    });
}

function applyDevModeBodyClass() {
    const on = isDevModeEnabled();
    document.body.classList.toggle('dev-mode', on);
    // When dev mode flips off, also force the scrape console hidden in case a recent
    // append set its inline style to 'block'.
    if (!on) {
        const c = document.getElementById('scrape-console');
        if (c) c.style.display = 'none';
    }
}

function isAutoLockPastVotesEnabled() {
    return appConfig.ui?.autoLockPastVotes ?? false;
}

function isAutoLockAfterSubmitEnabled() {
    return appConfig.ui?.autoLockAfterSubmit ?? true;
}

function isAutoBetHighlightingEnabled() {
    return appConfig.ui?.highlightAutoBets ?? false;
}

// Phase 28 v3: BOX_OPTIMIZATION (default) takes top-4 by power score.
// TRADITIONAL_ROLES assigns ◎〇▲△ by role with the risk slider as an intensity filter.
function getVotingMarkMode() {
    const m = String(appConfig.ui?.votingMarkMode || 'BOX_OPTIMIZATION').toUpperCase();
    return m === 'TRADITIONAL_ROLES' ? 'TRADITIONAL_ROLES' : 'BOX_OPTIMIZATION';
}

function getOreProDefaultStake() {
    const v = parseInt(appConfig.ui?.oreproDefaultStake, 10);
    return Number.isFinite(v) && v > 0 ? v : 10000;
}

// ── Bet line types + presets (the composer) ──────────────────────────────────
// A race's bet is a COMPOSITION: up to 3 LINES. Each line = a line TYPE (ticket+method) plus
// a total ¥ for that line (the user enters the line total; per-combo = floor100(total/点数)).
// The COUNT of marks is decided by the engine; the composition reinterprets those N marks into
// stacked tickets. Chosen per-DAY (toolbar) and optionally overridden per-RACE (Voting tab).
// MUST mirror C# TemplateBetEvaluator line scoring — when applied, the built lines are FROZEN
// and the C# side scores that exact list, so the two never drift for placed bets.
const MAX_BET_LINES = 3;

// The supported line types. `combos(n)` = 点数 for n marks; `minMarks` gates availability.
// `pick` says which marked runners the line uses: 'honmei' (◎ only), 'all' (box), or
// 'opp' (◎ axis + the rest as opponents, for nagashi).
const BET_LINE_TYPES = {
    win:          { ticket: 'win',      method: 'normal',   label: 'Win (単勝)',           short: 'Win',          jpLabel: '単勝',        minMarks: 1, pick: 'honmei', combos: () => 1,
                    describe: () => 'Win (単勝) on ◎ — pays only if ◎ finishes 1st. Highest variance, simplest bet.' },
    place:        { ticket: 'place',    method: 'normal',   label: 'Place (複勝)',         short: 'Place',        jpLabel: '複勝',        minMarks: 1, pick: 'honmei', combos: () => 1,
                    describe: () => 'Place (複勝) on ◎ — cashes if ◎ finishes in the top 3. The safest single-horse net.' },
    quinella_box: { ticket: 'quinella', method: 'box',      label: 'Quinella BOX (馬連)',  short: 'Quinella box', jpLabel: '馬連',        minMarks: 2, pick: 'all',    combos: (n) => nCk(n, 2),
                    describe: () => 'Quinella BOX (馬連) — any 2 of your marks finish 1st AND 2nd (either order). Tougher than wide.' },
    wide_box:     { ticket: 'wide',     method: 'box',      label: 'Wide BOX (ワイド)',    short: 'Wide box',     jpLabel: 'ワイド',      minMarks: 2, pick: 'all',    combos: (n) => nCk(n, 2),
                    describe: () => 'Wide BOX (ワイド) — any 2 of your marks both finish top 3 (either order). The easiest box to cash.' },
    trio_box:     { ticket: 'trio',     method: 'box',      label: 'Trio BOX (3連複)',     short: 'Trio box',     jpLabel: '3連複',       minMarks: 3, pick: 'all',    combos: (n) => nCk(n, 3),
                    describe: () => 'Trio BOX (3連複) — any 3 of your marks fill the top 3 (any order).' },
    trio_nagashi: { ticket: 'trio',     method: 'nagashi1', label: 'Trio nagashi (3連複ながし)', short: 'Trio nagashi', jpLabel: '3連複ながし', minMarks: 3, pick: 'opp', combos: (n) => nCk(n - 1, 2),
                    describe: () => 'Trio nagashi (3連複 ◎ながし) — ◎ MUST finish top 3, plus any 2 others join it. Cheaper than a box.' },
};
function isValidLineType(t) { return !!t && Object.prototype.hasOwnProperty.call(BET_LINE_TYPES, t); }

// Preset bundles: each leads with a CHASE line + safer SAFETY net(s). Default ¥ are tuned for
// the common 4-mark card and total ¥10,000; they re-divide cleanly at other counts. Editable
// per-day/race in the composer. `main` flags the chase line (shown emphasized).
const BET_PRESETS = {
    balanced:      { label: 'Balanced (Win + Q + T)',        main: 0, lines: [{ type: 'win', yen: 5000 }, { type: 'quinella_box', yen: 3000 }, { type: 'trio_box', yen: 2000 }] },
    trio_chase:    { label: 'Trio chase + Wide net',         main: 0, lines: [{ type: 'trio_box', yen: 4000 }, { type: 'wide_box', yen: 6000 }] },
    quinella_wide: { label: 'Quinella + Wide net',           main: 0, lines: [{ type: 'quinella_box', yen: 5000 }, { type: 'wide_box', yen: 5000 }] },
    wide_safe:     { label: 'Wide safe + Win upside',        main: 0, lines: [{ type: 'wide_box', yen: 7000 }, { type: 'win', yen: 3000 }] },
    nagashi_chase: { label: 'Nagashi chase + Wide net',      main: 0, lines: [{ type: 'trio_nagashi', yen: 5000 }, { type: 'wide_box', yen: 5000 }] },
    win_place:     { label: 'Win + Place safety',            main: 0, lines: [{ type: 'win', yen: 6000 }, { type: 'place', yen: 4000 }] },
};
const DEFAULT_PRESET = 'balanced';

// Phase 35: a special DAY-preset choice — "let the engine pick the bet type per race from its field
// shape" (see SHAPE_TO_PRESET). It's not a real BET_PRESETS bundle, so the day composition stores a
// SENTINEL (presetId = this id; lines are a harmless placeholder so it persists/normalizes). The
// signal is presetId === AUTO_PER_RACE_ID; resolveBetComposition maps it to the per-race preset.
const AUTO_PER_RACE_ID = 'auto_per_race';
function autoPerRaceDayComposition() {
    return { presetId: AUTO_PER_RACE_ID, lines: BET_PRESETS[DEFAULT_PRESET].lines.map(l => ({ type: l.type, yen: l.yen })) };
}
function isAutoPerRaceComposition(comp) { return comp?.presetId === AUTO_PER_RACE_ID; }

// Phase 35: SMALL-FIELD TOKEN. Auto mode normally abstains on fields under MIN_FIELD (too few horses
// to model). Operator pref (2026-06-20): don't sit those out — throw a small 2-mark bet on the top 2
// just to have action. It's a tiny Quinella+Wide (馬連+ワイド) on the pair at a token stake. Not a
// real BET_PRESETS bundle (so it never appears in the day dropdown); identified by presetId.
const SMALL_FIELD_TOKEN_ID = 'small_token';
const SMALL_FIELD_TOKEN_STAKE = 2000;  // ¥ total — a token "get something on there" bet
function smallFieldTokenComposition() {
    return { presetId: SMALL_FIELD_TOKEN_ID, lines: [
        { type: 'quinella_box', yen: Math.round(SMALL_FIELD_TOKEN_STAKE / 2) },
        { type: 'wide_box',     yen: Math.round(SMALL_FIELD_TOKEN_STAKE / 2) },
    ] };
}
function isSmallFieldTokenComposition(comp) { return comp?.presetId === SMALL_FIELD_TOKEN_ID; }
// Map an auto-chosen preset id → its composition (handles the non-preset small-field token sentinel).
function compositionForAutoPreset(presetId) {
    return presetId === SMALL_FIELD_TOKEN_ID ? smallFieldTokenComposition() : compositionFromPreset(presetId);
}

// ── DISCIPLINE MODE (cold engine, §0 Step 2) ─────────────────────────────────
// An ALTERNATIVE TO THE RISK SLIDER, not a bet preset. The backtest bench proved we have no pick-side
// edge over the market — so Discipline mode stops trying to out-pick it. When ON it:
//   (1) OVERRIDES the slider with a market-trusting low risk (back the crowd's top choices), and
//   (2) OVERRIDES the day preset with a single PLACE (複勝) bet on the ◎ — the highest-recovery safe
//       bet the bench found (~83% vs ~73% for win+place/wide bundles; the win leg was the drag).
// Honest goal: LOSE LEAST, not profit — the ~20-25% takeout wall stands until better data (Group-B).
const DISCIPLINE_RISK = 10;            // pinned slider value when on — Ultra-Safe / trust the market
const DISCIPLINE_PLACE_STAKE = 10000;  // ¥ on the single 複勝 line (the per-race budget the presets use)
// Discipline is the DEFAULT mode (the overhaul made it the spine of betting). UNSET → ON; only an
// explicit `disciplineMode === false` (you flipped to Manual) turns it off. So a fresh config, or the
// brief window before /api/config loads, is Discipline.
function isDisciplineMode() {
    if (typeof appConfig !== 'object' || !appConfig || !appConfig.ui) return true;
    return appConfig.ui.disciplineMode !== false;
}
// Pure PLACE on the ◎. presetId is special (not in BET_PRESETS) so it never shows in the day dropdown
// and gets its own min/max=1 plan (no phantom 2nd mark — place only ever bets the ◎).
function disciplinePlaceComposition() {
    return { presetId: 'discipline_place', lines: [{ type: 'place', yen: DISCIPLINE_PLACE_STAKE }] };
}
function isDisciplinePlaceComposition(comp) { return !!comp && comp.presetId === 'discipline_place'; }
// The composition Discipline mode applies to a race: pure place on the ◎, or — on a field too small to
// model — the established token Q+Wide (operator pref) rather than abstaining.
function disciplineComposition(r_id) {
    const probe = getEngineMarkPlanForRace(r_id, { compositionOverride: compositionFromPreset('balanced') });
    return probe.shape === 'small-field' ? smallFieldTokenComposition() : disciplinePlaceComposition();
}

// ── SIDE BETS (loyalty bets — Discipline-era) ────────────────────────────────
// Explicit, ADDITIVE ¥1k bets on horses you like ("I backed it despite the odds"), riding ALONGSIDE
// the disciplined ◎ place — never replacing it. OPT-IN (s60+): nothing bets on a Favorite (👁 Watchlist)
// horse unless you explicitly add it — either the per-race Bets strip's ＋ chip, or the one-time
// Watchlist popup shown at Apply Day Votes time (showWatchlistSideBetPopup). One favorite → a ¥1k place
// (複勝); two+ in one race → ONE ¥1k Wide (ワイド) between them. The engine's ◎ is skipped (never double
// the spine). Frozen with kind:'side' so C# tracks their P/L APART and the Discipline recovery % stays
// honest. Whole feature off when appConfig.ui.sideBetsAuto === false.
const SIDE_BET_STAKE = 1000;
function sideBetsEnabled() { return isDisciplineMode() && (appConfig.ui?.sideBetsAuto !== false); }
// Favorite (Watchlist) horse-ids running in a race, MINUS the engine ◎ (don't double the spine).
function raceFavoriteHorseIds(rid) {
    const fav = parseListIds(listsData?.watchlist || '');
    if (!fav.size) return [];
    const honmei = (getUnconditionalAutoBetRankingsForRace(rid).find(p => p.symbol === '◎') || {}).h_id;
    const out = [];
    (globalRaceEntries[rid] || []).forEach(row => {
        const hid = String(row?.Horse_ID ?? '').split('.')[0].trim();
        if (hid && fav.has(hid) && hid !== String(honmei || '')) out.push(hid);
    });
    return out;
}
// The ACTIVE side-bet horse-ids for a race: an explicit per-race choice (raceMeta.sideBets, written by
// the per-race strip's ＋ chip or the Watchlist Apply-time popup). OPT-IN default (s60+) — nothing is
// active until you add it. Always [] when side bets are off. Re-filtered to favorites still in the
// field (a scratch drops it).
function activeSideBetHorseIds(rid) {
    if (!sideBetsEnabled()) return [];
    const favs = raceFavoriteHorseIds(rid);
    const ov = globalRaceMeta[rid]?.sideBets;
    if (Array.isArray(ov)) return ov.filter(h => favs.includes(h));
    return []; // opt-in: nothing bets until explicitly selected
}
// Toggle one favorite's side bet on/off for a race (materializes the auto-default first so a removal
// sticks), persist to the marks blob, and re-render the strip + sunk-cost panel.
function toggleSideBet(rid, hid) {
    const favs = raceFavoriteHorseIds(rid);
    const cur = activeSideBetHorseIds(rid);
    const next = cur.includes(hid) ? cur.filter(h => h !== hid) : [...cur, hid].filter(h => favs.includes(h));
    globalRaceMeta[rid] = { ...(globalRaceMeta[rid] || {}), sideBets: next };
    try { touchRaceMeta(rid); } catch (_) {}
    saveMarksToServer().catch(() => {});
    renderSideBetStrip(rid);
}
// Build this race's frozen side-bet LINES (kind:'side'): 1 favorite → ¥1k place; 2+ → one ¥1k Wide box.
function buildSideBetLines(rid) {
    const hids = activeSideBetHorseIds(rid);
    if (!hids.length) return [];
    const ppByHorse = {};
    (globalRaceEntries[rid] || []).forEach(row => {
        const hid = String(row?.Horse_ID ?? '').split('.')[0].trim();
        const pp = parseInt(row?.PP, 10);
        if (hid && Number.isFinite(pp) && pp > 0) ppByHorse[hid] = pp;
    });
    const pps = hids.map(h => ppByHorse[h]).filter(Boolean);
    if (!pps.length) return [];
    if (pps.length === 1)
        return [{ ticket: 'place', method: 'normal', label: '複勝', horses: [{ pp: pps[0] }], comboCount: 1, stakePerCombo: SIDE_BET_STAKE, kind: 'side' }];
    const c = nCk(pps.length, 2);
    const per = Math.max(100, Math.round((SIDE_BET_STAKE / c) / 100) * 100);
    return [{ ticket: 'wide', method: 'box', label: 'ワイド', horses: pps.map(pp => ({ pp })), comboCount: c, stakePerCombo: per, kind: 'side' }];
}

// Watchlist horses in the day's ELIGIBLE (about-to-apply) races that don't already have an explicit
// side bet — the candidate list for the Apply-time opt-in popup below. Skips any horse already added
// via the per-race strip (raceMeta.sideBets) so the popup never re-asks about a choice already made.
// Carries the full entry row so the popup can show real stats, not just "it's running".
function collectWatchlistSideBetCandidates(eligibleRaceIds) {
    const out = [];
    (eligibleRaceIds || []).forEach(r_id => {
        const favs = raceFavoriteHorseIds(r_id);
        if (!favs.length) return;
        const already = new Set(Array.isArray(globalRaceMeta[r_id]?.sideBets) ? globalRaceMeta[r_id].sideBets : []);
        const rowById = {};
        (globalRaceEntries[r_id] || []).forEach(row => { rowById[String(row?.Horse_ID ?? '').split('.')[0].trim()] = row; });
        favs.forEach(hid => {
            if (already.has(hid)) return;
            const row = rowById[hid] || {};
            out.push({ r_id, hid, name: row.Horse || hid, row });
        });
    });
    return out;
}

// Compact single-line stat summary for a Watchlist popup row — odds/fav rank, career record, last-3
// finishes, jockey win% — enough to judge "is this horse actually live" at a glance, not just its name.
function watchlistCandidateStatLine(row) {
    const dn = (v) => {
        if (v === null || v === undefined) return '—';
        const s = String(v).trim();
        return (s === '' || s === '0') ? '—' : s;
    };
    const odds = parseFloat(row?.Odds);
    const oddsStr = Number.isFinite(odds) && odds > 0 ? odds.toFixed(1) : '—';
    const jWin = (row?.Jockey_Win_Pct === null || row?.Jockey_Win_Pct === undefined) ? NaN : parseFloat(row.Jockey_Win_Pct);
    const jStr = Number.isFinite(jWin) ? `${(jWin * 100).toFixed(0)}%` : '—';
    const jName = row?.Jockey || row?.Jockey_Code || '';
    return `Odds ${oddsStr} (Fav ${dn(row?.Fav)}) · Record ${escapeHtml(dn(row?.Record))} · Last3 ${escapeHtml(dn(row?.Last3))} · ${escapeHtml(jName)} J${jStr}`;
}

// FINAL gate before Apply Day Votes actually submits anything to OrePro — shown after the day preview
// is confirmed. s60 incident: an earlier version let ✖/outside-click silently proceed with the main
// day apply regardless of choice (a fake cancel — Skip and ✖ were coded identically, and there was no
// real abort path once you'd reached this screen). Fixed: this is now the one true commit point when
// Watchlist candidates exist. Three explicit outcomes — Cancel (✖ / outside-click / Cancel button) =
// abort EVERYTHING, nothing is sent to OrePro; Skip = place the day's main bets, no Watchlist extras;
// Add selected = place the main bets PLUS a ¥1k place side bet on each checked Watchlist horse
// (unchecked by default — opt-in). Extras ride the existing side-bet OrePro pipeline (kind:'side',
// same anti-duplicate guards as the per-race strip). The disciplined ◎ spine bet is never affected by
// any of the three outcomes. Returns Promise<boolean>: true = proceed with the day apply, false =
// cancel everything. Silent pass-through (resolves true immediately, no popup) if there are no
// candidates — nothing to ask about.
function showWatchlistSideBetPopup(eligibleRaceIds, date) {
    const candidates = collectWatchlistSideBetCandidates(eligibleRaceIds);
    if (!candidates.length) return Promise.resolve(true);

    return new Promise(resolve => {
        const rows = candidates.map((c, i) => {
            const race = findRaceObjById(c.r_id);
            const label = race ? `${trackName(race.info.place)} R${race.info.race_number}` : c.r_id;
            const stat = watchlistCandidateStatLine(c.row);
            return `<label style="display:flex;align-items:flex-start;gap:8px;padding:7px 4px;border-bottom:1px solid #22283a;cursor:pointer;">
                <input type="checkbox" data-idx="${i}" style="width:16px;height:16px;flex-shrink:0;margin-top:3px;">
                <div style="flex:1;min-width:0;">
                    <div>👁 <b>${escapeHtml(c.name)}</b> <span style="color:#9fb2c8;font-size:0.85em;">· ${escapeHtml(label)}</span></div>
                    <div style="color:#8a93a3;font-size:0.8em;margin-top:2px;">${stat}</div>
                </div>
                <span style="color:#9fb2c8;font-size:0.85em;white-space:nowrap;">¥${SIDE_BET_STAKE.toLocaleString()} 複勝</span>
            </label>`;
        }).join('');

        const overlay = document.createElement('div');
        overlay.id = 'watchlist-sidebet-popup';
        overlay.className = 'modal-overlay';
        // outcome: 'add' | 'skip' | 'cancel'. Only 'add' writes anything; 'cancel' resolves false so the
        // caller aborts the ENTIRE day apply — no OrePro call happens for 'cancel'.
        const cleanup = (outcome) => {
            if (outcome === 'add') {
                overlay.querySelectorAll('input[type=checkbox]:checked').forEach(cb => {
                    const c = candidates[parseInt(cb.dataset.idx, 10)];
                    if (!c) return;
                    const cur = Array.isArray(globalRaceMeta[c.r_id]?.sideBets) ? globalRaceMeta[c.r_id].sideBets : [];
                    if (!cur.includes(c.hid)) {
                        globalRaceMeta[c.r_id] = { ...(globalRaceMeta[c.r_id] || {}), sideBets: [...cur, c.hid] };
                        try { touchRaceMeta(c.r_id); } catch (_) {}
                        try { renderSideBetStrip(c.r_id); } catch (_) {}
                    }
                });
                saveMarksToServer().catch(() => {});
            }
            try { overlay.remove(); } catch (_) {}
            resolve(outcome !== 'cancel');
        };
        overlay.onclick = (ev) => { if (ev.target === overlay) cleanup('cancel'); };
        overlay.innerHTML = `
            <div class="modal-content" style="max-width:560px;width:92%;display:flex;flex-direction:column;max-height:84vh;">
                <div class="modal-header">
                    <h3 class="modal-title">👁 Watchlist horses running ${escapeHtml(date)}</h3>
                    <div class="modal-header-actions"><button class="close-btn" id="wl-sidebet-x" title="Cancel — nothing will be sent to OrePro">✖</button></div>
                </div>
                <div style="padding:2px 16px 6px;color:#9fb2c8;font-size:13px;">
                    ${candidates.length} Watchlist horse(s) are running today, separate from the disciplined ◎ pick. Check any you want a ¥${SIDE_BET_STAKE.toLocaleString()} place bet on, just to have something on paper — unchecked ones get nothing. This never touches the ◎ spine bet.
                    <br><b>Nothing has been sent to OrePro yet</b> — pick an option below.
                </div>
                <div style="overflow:auto;padding:2px 16px;">${rows}</div>
                <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;padding:10px 16px;border-top:1px solid #243044;flex-wrap:wrap;">
                    <button id="wl-sidebet-cancel" style="padding:7px 14px;border-radius:6px;border:1px solid #6a3a3a;background:#2a1818;color:#ffc3c3;cursor:pointer;">✖ Cancel — place nothing</button>
                    <div style="display:flex;gap:8px;">
                        <button id="wl-sidebet-skip" style="padding:7px 14px;border-radius:6px;border:1px solid #3a4a60;background:#1b2230;color:#cdd9e8;cursor:pointer;">Skip — place ${eligibleRaceIds.length} race(s), no extras</button>
                        <button id="wl-sidebet-go" style="padding:7px 14px;border-radius:6px;border:1px solid #2f8f57;background:#176b3a;color:#eafff0;font-weight:700;cursor:pointer;">Add selected & place</button>
                    </div>
                </div>
            </div>`;
        document.body.appendChild(overlay);
        document.getElementById('wl-sidebet-go').onclick = () => cleanup('add');
        document.getElementById('wl-sidebet-skip').onclick = () => cleanup('skip');
        document.getElementById('wl-sidebet-cancel').onclick = () => cleanup('cancel');
        document.getElementById('wl-sidebet-x').onclick = () => cleanup('cancel');
    });
}

// The per-race "Bets" strip (shown at the top of an expanded race under Discipline): the green spine
// bet (◎ place ¥10k) plus a pink ♥ chip per Favorite in the field — active chips carry a ✕ to drop the
// side bet, removed ones a ＋ to re-add. Visually differentiated so a loyalty bet never looks like the
// disciplined spine. Empty under Manual / when side bets are off / on settled races (autopsy owns those).
function sideBetStripHtml(rid) {
    if (!sideBetsEnabled()) return '';
    try { if (raceIsSettledForAutopsy(rid)) return ''; } catch (_) {}
    const honmeiId = (getUnconditionalAutoBetRankingsForRace(rid).find(p => p.symbol === '◎') || {}).h_id;
    const nameById = {};
    (globalRaceEntries[rid] || []).forEach(row => { nameById[String(row?.Horse_ID ?? '').split('.')[0].trim()] = row.Horse; });
    const yk = DISCIPLINE_PLACE_STAKE / 1000;
    const spineName = honmeiId ? (nameById[honmeiId] || '◎') : '—';
    const chipBase = 'display:inline-flex;align-items:center;gap:5px;font-size:0.78em;font-weight:700;padding:2px 9px;border-radius:9px;white-space:nowrap;vertical-align:middle;';
    const spine = `<span style="${chipBase}background:#14361f;color:#b9f0c9;border:1px solid #2f8f57;" `
        + `title="The disciplined bet — a flat ¥${DISCIPLINE_PLACE_STAKE.toLocaleString()} place (複勝) on the cold engine's top pick.">🎯 ◎ place ¥${yk}k · ${escapeHtml(spineName)}</span>`;
    const favs = raceFavoriteHorseIds(rid);
    let favHtml = '';
    if (favs.length) {
        const active = new Set(activeSideBetHorseIds(rid));
        const activeN = favs.filter(h => active.has(h)).length;
        const typeLabel = activeN >= 2 ? `one ¥1k Wide (ワイド) on ${activeN}` : (activeN === 1 ? `¥1k place (複勝)` : 'none');
        favHtml = favs.map(h => {
            const on = active.has(h);
            const nm = escapeHtml(nameById[h] || h);
            const style = on
                ? `${chipBase}background:#3a1830;color:#ffc3e1;border:1px solid #8a3568;cursor:pointer;`
                : `${chipBase}background:transparent;color:#8a7686;border:1px dashed #6a4a60;cursor:pointer;`;
            const tip = on ? 'Loyalty side bet ON — click to remove (nothing places until you apply the race).'
                           : 'Click to add a ¥1k loyalty side bet on this Favorite.';
            return `<button type="button" style="${style}" title="${tip}" onclick="event.stopPropagation(); toggleSideBet('${rid}','${h}')">♥ ${nm} ${on ? '✕' : '＋'}</button>`;
        }).join('');
        favHtml += `<span style="font-size:0.72em;color:#9a8a96;margin-left:2px;">side → ${typeLabel}</span>`;
    } else {
        favHtml = `<span style="font-size:0.72em;color:#8a8a8a;">No Favorites in this race.</span>`;
    }
    const wrap = 'display:flex;align-items:center;gap:7px;flex-wrap:wrap;padding:7px 10px;margin:0 0 8px;'
        + 'border:1px solid #2a2f3a;border-radius:10px;background:#171a21;';
    return `<div style="${wrap}"><span style="font-size:0.72em;font-weight:700;color:#8a93a3;letter-spacing:0.04em;">BETS</span>${spine}${favHtml}</div>`;
}
function renderSideBetStrip(rid) {
    const el = document.getElementById(`bets-strip-${rid}`);
    if (el) el.innerHTML = sideBetStripHtml(rid);
    try { refreshSunkCostStat(); } catch (_) {}
}

// Phase 34 — per-preset COUNT BAND + selection TILT (design locked 2026-06-19).
// The active bet preset sets how MANY marks the engine suggests (a hard min–max band = a fence)
// and a small ranking lean for the SUPPORTING marks (the ◎ banker never moves):
//   tilt < 0 = consistency (chalk-leaning supporting marks); tilt > 0 = ceiling (overlay-leaning); 0 = neutral.
//   requireAxis = abstain unless there's a genuine ◎ standout (nagashi needs a real banker).
// Final count = clamp( min + round(risk·(max−min)) + shapeNudge, min, max ), then clamped to field size.
// See getEngineMarkPlanForRace for how the three inputs (preset band / risk position / shape nudge) stack.
const PRESET_PLANS = {
    // Win+Place is honmei-ONLY (both 単勝 and 複勝 bet just the ◎), so any 2nd mark is never bet —
    // pin the band to exactly 1 so the marks match the bet and no phantom ◯ appears. (tilt is moot at
    // count 1 — there are no supporting marks to lean — kept for documentation only.)
    win_place:     { id: 'win_place',     min: 1, max: 1, tilt: -15, requireAxis: false },
    balanced:      { id: 'balanced',      min: 3, max: 4, tilt:   0, requireAxis: false },
    quinella_wide: { id: 'quinella_wide', min: 3, max: 4, tilt:  -8, requireAxis: false },
    trio_chase:    { id: 'trio_chase',    min: 4, max: 5, tilt: +15, requireAxis: false },
    nagashi_chase: { id: 'nagashi_chase', min: 5, max: 6, tilt: +15, requireAxis: true  },
    wide_safe:     { id: 'wide_safe',     min: 2, max: 4, tilt:  -8, requireAxis: false },
    // Discipline mode — pure 複勝 on the ◎; pin to exactly 1 mark so no phantom 〇 appears (place only
    // ever bets the ◎). Not a BET_PRESETS bundle; keyed by the composition's presetId.
    discipline_place: { id: 'discipline_place', min: 1, max: 1, tilt: 0, requireAxis: false },
};
// A custom/edited composition (no exact preset match) → neutral band derived from its contract floor.
function presetPlanForComposition(comp) {
    // Special non-BET_PRESETS compositions (e.g. Discipline's pure-place) carry their plan id on presetId.
    if (comp && comp.presetId && PRESET_PLANS[comp.presetId]) return PRESET_PLANS[comp.presetId];
    const id = compositionPresetId(comp);
    if (PRESET_PLANS[id]) return PRESET_PLANS[id];
    let floor = 2;
    try { const f = compositionMarkFloor(comp); if (Number.isFinite(f) && f >= 1) floor = Math.max(2, f); } catch (_) {}
    return { id: 'custom', min: floor, max: Math.max(floor, 4), tilt: 0, requireAxis: false };
}
const PRESET_PLANS_DEFAULT = { id: 'custom', min: 2, max: 4, tilt: 0, requireAxis: false };

// A composition = { presetId, lines:[{type,yen}] }. presetId is for the label only ('custom'
// once the lines are edited away from the named preset). Deep-clones the preset's lines.
function compositionFromPreset(presetId) {
    const p = BET_PRESETS[presetId] || BET_PRESETS[DEFAULT_PRESET];
    return { presetId: BET_PRESETS[presetId] ? presetId : DEFAULT_PRESET, lines: p.lines.map(l => ({ type: l.type, yen: l.yen })) };
}
// Validate/normalize an arbitrary object into a composition (≤3 valid lines, positive ¥).
function normalizeComposition(c) {
    if (!c || typeof c !== 'object') return null;
    const lines = (Array.isArray(c.lines) ? c.lines : [])
        .filter(l => l && isValidLineType(l.type))
        .map(l => ({ type: l.type, yen: Math.max(0, parseInt(l.yen, 10) || 0) }))
        .slice(0, MAX_BET_LINES);
    if (!lines.length) return null;
    const presetId = typeof c.presetId === 'string' ? c.presetId : 'custom';
    return { presetId, lines };
}
// Does a composition's lines match a named preset exactly? → returns presetId or 'custom'.
function compositionPresetId(comp) {
    if (!comp || !Array.isArray(comp.lines)) return 'custom';
    for (const [id, p] of Object.entries(BET_PRESETS)) {
        if (p.lines.length !== comp.lines.length) continue;
        if (p.lines.every((l, i) => l.type === comp.lines[i].type && l.yen === comp.lines[i].yen)) return id;
    }
    return 'custom';
}
function compositionLabel(comp) {
    if (isAutoPerRaceComposition(comp)) return '🧪 Auto (per race)';
    if (isDisciplinePlaceComposition(comp)) return '🧊 Discipline (Place ◎)';
    if (isSmallFieldTokenComposition(comp)) return 'Small-field 2-bet (Q+Wide)';
    const id = compositionPresetId(comp);
    return id === 'custom' ? 'Custom' : (BET_PRESETS[id]?.label || 'Custom');
}
// The fewest marks at which a composition can place ANY line = min(minMarks) across its lines.
// A race with fewer marks than this floor can't form a single line of the preset → it won't place
// (the preset is a contract; no fallback to a bet type it doesn't contain). e.g. Trio chase
// (trio=3, wide=2) → floor 2; Balanced (win=1, q=2, t=3) → floor 1.
function compositionMarkFloor(comp) {
    const lines = (comp && Array.isArray(comp.lines)) ? comp.lines : [];
    let floor = Infinity;
    for (const l of lines) {
        const t = BET_LINE_TYPES[l && l.type];
        if (t && Number.isFinite(t.minMarks)) floor = Math.min(floor, t.minMarks);
    }
    return Number.isFinite(floor) ? floor : 1;
}

// Day-level composition cache: { 'YYYY-MM-DD': {presetId,lines} }. Synced with the server via
// /api/marks/day-bet/{date}. Temporary per race day (server ignores it once the day passes).
let globalDayBetCompositions = {};

async function loadDayBetComposition(cleanDate) {
    if (!cleanDate) return compositionFromPreset(DEFAULT_PRESET);
    try {
        const res = await fetch(`/api/marks/day-bet/${encodeURIComponent(cleanDate)}`, { cache: 'no-store' });
        if (res.ok) {
            const d = await res.json();
            const comp = normalizeComposition(d?.composition) || compositionFromPreset(DEFAULT_PRESET);
            globalDayBetCompositions[cleanDate] = comp;
            return comp;
        }
    } catch (_) {}
    return globalDayBetCompositions[cleanDate] || compositionFromPreset(DEFAULT_PRESET);
}
async function saveDayBetComposition(cleanDate, comp) {
    const norm = normalizeComposition(comp);
    if (!cleanDate || !norm) return;
    globalDayBetCompositions[cleanDate] = norm;
    try {
        await fetch(`/api/marks/day-bet/${encodeURIComponent(cleanDate)}`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ composition: norm }),
        });
    } catch (_) {}
}
function getDayBetComposition(cleanDate) {
    return globalDayBetCompositions[cleanDate] || compositionFromPreset(DEFAULT_PRESET);
}

// Per-race override (Voting tab). Stored on raceMeta.betComposition. Absent → use day setting.
function getRaceBetCompositionOverride(rid) {
    return normalizeComposition(globalRaceMeta[rid]?.betComposition);
}
// The composition that ACTUALLY applies to a race for live pricing/display:
//   per-race override → that race's day setting → default preset.
function resolveBetComposition(rid) {
    const override = getRaceBetCompositionOverride(rid);
    if (override) return override;
    // Discipline mode (cold engine) overrides the day preset with a single PLACE bet on the ◎ (or the
    // token on a tiny field). Everything downstream (pricing/preview/placement/freeze) flows through here.
    if (isDisciplineMode()) {
        return disciplineComposition(rid);
    }
    const dayComp = getDayBetComposition(globalRaceInfo[rid]?.clean_date || '');
    // Phase 35: the "Auto (per race)" day choice has no fixed lines — resolve it to the bet preset
    // the engine picks for THIS race's field shape (falls back to the default if there's no read yet,
    // e.g. pre-odds). Everything downstream (pricing, preview, placement, freeze) flows through here.
    if (isAutoPerRaceComposition(dayComp)) {
        const pid = autoBetTypePresetForRace(rid);
        return pid ? compositionForAutoPreset(pid) : compositionFromPreset(DEFAULT_PRESET);
    }
    return dayComp;
}

// ── Day bet composer (main toolbar) ──────────────────────────────────────────
// The toolbar shows a preset dropdown ("Balanced ▾ … Custom") + an "Edit lines…" toggle that
// reveals the line composer for the whole day. Per-race overrides reuse the same renderer.
function repriceDayRaces(date) {
    if (!date) return;
    Object.keys(globalRaceInfo).forEach(rid => {
        if ((globalRaceInfo[rid]?.clean_date || '') !== date) return;
        const tbody = document.getElementById(`tbody-${rid}`);
        if (tbody) tbody.innerHTML = buildTableBody(rid, globalRaceEntries[rid]);
        try { updateRaceActionButtons(rid); } catch (_) {}
    });
    try { updateQuickStats(); } catch (_) {}
    try { refreshSunkCostStat(); } catch (_) {}
    if (currentMainView === 'voting') { try { renderLiveViewPanel(); } catch (_) {} }
    // Rebuilding the rows above wiped the .auto-bet-preview highlight classes; re-apply them so the
    // engine's suggested marks survive a preset/line change (the suggestions themselves now depend
    // on the preset, so this MUST run after every reprice — not just on a full refresh).
    try { updateAutoBetHighlighting(); } catch (_) {}
}

// Populate the day preset <select> once (presets + a Custom sentinel).
function initDayBetStructureSelector() {
    const sel = document.getElementById('day-bet-structure');
    if (!sel || sel.dataset.populated === '1') return;
    sel.innerHTML = Object.entries(BET_PRESETS)
        .map(([k, v]) => `<option value="${k}">${escapeHtml(v.label)}</option>`).join('')
        + `<option value="${AUTO_PER_RACE_ID}">🧪 Auto — engine picks per race</option>`
        + `<option value="custom">Custom…</option>`;
    sel.dataset.populated = '1';
}

// Render the composer (used both for the day panel and per-race override). `comp` is the
// current composition; `onChangeFn` is the JS expression prefix used in inline handlers; the
// composer mutates a working copy via that handler. `markCount` previews 点数 / per-combo.
function renderBetComposer(comp, ctx) {
    // ctx = { scope:'day'|'race', rid?:string }. Handlers route to onDayComposer*/onRaceComposer*.
    const fn = ctx.scope === 'day' ? 'DayComposer' : 'RaceComposer';
    const arg = ctx.scope === 'race' ? `'${escapeHtml(ctx.rid)}', ` : '';
    const markCount = ctx.scope === 'race' ? collectRaceMarkedRunners(ctx.rid).length : 4; // day preview assumes 4
    const built = buildLinesFromComposition(comp, ctx.scope === 'race'
        ? collectRaceMarkedRunners(ctx.rid)
        : sampleRunnersForCount(4));
    const total = built.staked;
    const usedTypes = new Set(comp.lines.map(l => l.type));
    const isMain = (i) => Number(BET_PRESETS[comp.presetId]?.main) === i;
    let rows = comp.lines.map((line, i) => {
        const t = BET_LINE_TYPES[line.type];
        const opts = Object.entries(BET_LINE_TYPES)
            .map(([k, v]) => `<option value="${k}"${k === line.type ? ' selected' : ''}>${escapeHtml(v.label)}</option>`).join('');
        const b = built.lines.find(bl => bl._specIndex === i);
        const detail = b
            ? `${b.comboCount} combo${b.comboCount === 1 ? '' : 's'} × ¥${(b.stakePerCombo || 0).toLocaleString()} = ¥${(b.stakePerCombo * b.comboCount).toLocaleString()}`
            : `<span class="composer-line-na" title="Needs ≥${t?.minMarks} marks">needs ≥${t?.minMarks} marks (n/a at ${markCount})</span>`;
        const roleTag = isMain(i) ? `<span class="composer-line-role" title="The chase line">main</span>` : '';
        const winCond = t ? escapeHtml(t.describe()) : '';
        return `<div class="composer-line">
            <select class="composer-line-type" onchange="on${fn}LineType(${arg}${i}, this.value)">${opts}</select>
            ${roleTag}
            <span class="composer-line-yen">¥<input type="number" min="0" step="100" value="${line.yen}" class="composer-yen-input" onchange="on${fn}LineYen(${arg}${i}, this.value)"></span>
            <span class="composer-line-detail">${detail}</span>
            <a class="composer-line-del" title="Remove line" onclick="on${fn}RemoveLine(${arg}${i})">✕</a>
        </div>
        <div class="composer-line-desc">${winCond}</div>`;
    }).join('');
    const canAdd = comp.lines.length < MAX_BET_LINES;
    const addBtn = canAdd
        ? `<a class="composer-add" onclick="on${fn}AddLine(${arg.replace(/, $/, '')})">+ add line</a>`
        : `<span class="composer-add is-disabled">max ${MAX_BET_LINES} lines</span>`;
    const totalClass = total === getOreProDefaultStake() ? 'is-ok' : (total > getOreProDefaultStake() ? 'is-over' : 'is-under');
    return `<div class="bet-composer">
        ${rows}
        <div class="composer-foot">
            ${addBtn}
            <span class="composer-total ${totalClass}">Total ¥${total.toLocaleString()} <span class="composer-total-target">/ ¥${getOreProDefaultStake().toLocaleString()}</span></span>
        </div>
    </div>`;
}

// A throwaway runner list of N generic marks (◎〇▲△…) for previewing day-level 点数/¥ before a
// specific race is in scope. Post positions 1..N so combos compute.
function sampleRunnersForCount(n) {
    const seq = markSequenceForCount(n);
    return seq.map((symbol, i) => ({ symbol, horseId: `_sample${i}`, pp: i + 1 }));
}

// ── Day composer state + handlers ────────────────────────────────────────────
let dayComposerOpen = false;
function getActiveDayComposition() { return getDayBetComposition(currentActiveDate || ''); }

function syncDayBetStructureSelector() {
    initDayBetStructureSelector();
    const sel = document.getElementById('day-bet-structure');
    if (!sel) return;
    const comp = getActiveDayComposition();
    sel.value = isAutoPerRaceComposition(comp) ? AUTO_PER_RACE_ID : compositionPresetId(comp);
    renderDayComposerPanel();
}

// Render the day composer panel (below the toolbar). Shown when dayComposerOpen, or always
// renders the preset summary line.
function renderDayComposerPanel() {
    const el = document.getElementById('day-bet-structure-desc');
    if (!el) return;
    const comp = getActiveDayComposition();
    // Auto (per race) has no fixed lines to edit — explain what it does instead of the line composer.
    if (isAutoPerRaceComposition(comp)) {
        el.innerHTML = `<div class="day-bet-summary" style="line-height:1.4;">🧪 <b>Engine picks the bet type for each race</b> from its field shape — `
            + `lone favorite → Win+Place · two clear → Quinella+Wide · clear ◎ + open pack → Nagashi · `
            + `three/packed → Trio chase · flat → Wide-safe. Your hand-marked/locked races and any per-race override always win.</div>`;
        el.style.display = '';
        return;
    }
    const summary = comp.lines.map(l => `${BET_LINE_TYPES[l.type]?.short || l.type} ¥${l.yen.toLocaleString()}`).join('  +  ');
    const toggle = `<a class="composer-toggle" onclick="toggleDayComposer()">${dayComposerOpen ? '▾ hide lines' : '▸ edit lines'}</a>`;
    let html = `<div class="day-bet-summary">${escapeHtml(summary)} ${toggle}</div>`;
    if (dayComposerOpen) html += renderBetComposer(comp, { scope: 'day' });
    el.innerHTML = html;
    el.style.display = '';
}
function toggleDayComposer() { dayComposerOpen = !dayComposerOpen; renderDayComposerPanel(); }

// Preset dropdown changed: 'custom' keeps current lines but flags custom; a named preset
// replaces the day's lines with that bundle. Persist + re-price.
async function onDayBetStructureChange(presetId) {
    const date = currentActiveDate;
    if (!date) return;
    let comp;
    if (presetId === AUTO_PER_RACE_ID)   comp = autoPerRaceDayComposition();
    else if (presetId === 'custom')      comp = { ...getActiveDayComposition(), presetId: 'custom' };
    else                                 comp = compositionFromPreset(presetId);
    await saveDayBetComposition(date, comp);
    renderDayComposerPanel();
    repriceDayRaces(date);
}

// Day composer line edits — mutate the day composition, persist, re-price.
async function mutateDayComposition(mutator) {
    const date = currentActiveDate; if (!date) return;
    const comp = JSON.parse(JSON.stringify(getActiveDayComposition()));
    mutator(comp);
    comp.presetId = compositionPresetId(comp);
    await saveDayBetComposition(date, comp);
    syncDayBetStructureSelector();
    repriceDayRaces(date);
}
function onDayComposerLineType(i, type) { if (isValidLineType(type)) mutateDayComposition(c => { c.lines[i].type = type; }); }
function onDayComposerLineYen(i, yen)   { mutateDayComposition(c => { c.lines[i].yen = Math.max(0, parseInt(yen, 10) || 0); }); }
function onDayComposerRemoveLine(i)     { mutateDayComposition(c => { c.lines.splice(i, 1); if (!c.lines.length) c.lines.push({ type: 'wide_box', yen: getOreProDefaultStake() }); }); }
function onDayComposerAddLine()         { mutateDayComposition(c => { if (c.lines.length < MAX_BET_LINES) c.lines.push({ type: nextUnusedLineType(c), yen: 0 }); }); }

// Pick a sensible default type for a newly-added line (first one not already used).
function nextUnusedLineType(comp) {
    const used = new Set((comp.lines || []).map(l => l.type));
    return Object.keys(BET_LINE_TYPES).find(t => !used.has(t)) || 'wide_box';
}

// Load the active day's saved composition, reflect it, and re-price if it differs from the
// default the first paint assumed. Called on every day switch.
function refreshDayBetStructure() {
    syncDayBetStructureSelector();
    const date = currentActiveDate;
    if (!date) return;
    loadDayBetComposition(date).then(loaded => {
        syncDayBetStructureSelector();
        if (compositionPresetId(loaded) !== DEFAULT_PRESET || loaded.presetId === 'custom' || isAutoPerRaceComposition(loaded)) repriceDayRaces(date);
    });
}

// ── Per-race override composer (Voting tab) ──────────────────────────────────
// Collapsed by default ("only show when overridden"): follows the day setting → a "+ bet
// override" link. Once overriding (or revealed), shows the composer + a "use day default".
let raceBetOverrideUiOpen = new Set();

function buildRaceBetOverrideHtml(rid, timeline) {
    if (timeline === 'past') return ''; // settled races are bet/frozen — overriding is moot
    const override = getRaceBetCompositionOverride(rid);
    const safeRid = escapeHtml(rid);
    if (!override && !raceBetOverrideUiOpen.has(rid)) {
        const dayLabel = compositionLabel(getDayBetComposition(globalRaceInfo[rid]?.clean_date || ''));
        return `<div class="race-bet-override is-collapsed">`
             + `<a class="race-bet-override-link" onclick="openRaceBetOverride('${safeRid}')" title="Override the day's bet for this race only (currently: ${escapeHtml(dayLabel)})">+ bet override</a>`
             + `</div>`;
    }
    // Pre-fill an opened (not-yet-saved) override from the RESOLVED comp so an "Auto" day pre-fills
    // with the race's shape-chosen preset (real lines), not the auto sentinel.
    const comp = override || resolveBetComposition(rid);
    const badge = override ? `<span class="race-bet-override-badge">override</span>` : '';
    const reset = override
        ? `<a class="race-bet-override-clear" onclick="clearRaceBetOverride('${safeRid}')" title="Drop the override; follow the day default">✕ use day default</a>`
        : `<a class="race-bet-override-clear" onclick="cancelRaceBetOverride('${safeRid}')" title="Cancel">✕</a>`;
    return `<div class="race-bet-override">`
         + `<div class="race-bet-override-head"><span class="race-bet-override-label">💴 Bet for this race</span>${badge}${reset}</div>`
         + renderBetComposer(comp, { scope: 'race', rid })
         + `</div>`;
}

function openRaceBetOverride(rid) {
    // Reveal seeded from the day composition so edits start from what would otherwise apply.
    if (!getRaceBetCompositionOverride(rid)) {
        const seed = JSON.parse(JSON.stringify(getDayBetComposition(globalRaceInfo[rid]?.clean_date || '')));
        globalRaceMeta[rid] = { ...(globalRaceMeta[rid] || {}), betComposition: seed };
    }
    raceBetOverrideUiOpen.add(rid);
    if (currentMainView === 'voting') renderLiveViewPanel();
}
function cancelRaceBetOverride(rid) {
    // Only reachable before any real edit committed an override beyond the seed → drop the seed.
    raceBetOverrideUiOpen.delete(rid);
    if (globalRaceMeta[rid]) delete globalRaceMeta[rid].betComposition;
    if (currentMainView === 'voting') renderLiveViewPanel();
}

async function mutateRaceComposition(rid, mutator) {
    const base = getRaceBetCompositionOverride(rid)
        || JSON.parse(JSON.stringify(getDayBetComposition(globalRaceInfo[rid]?.clean_date || '')));
    mutator(base);
    base.presetId = compositionPresetId(base);
    globalRaceMeta[rid] = { ...(globalRaceMeta[rid] || {}), betComposition: base };
    touchRaceMeta(rid); // spreads existing → preserves betComposition + re-snapshots lock state
    try { await saveMarksToServer(); } catch (_) {}
    const tbody = document.getElementById(`tbody-${rid}`);
    if (tbody) tbody.innerHTML = buildTableBody(rid, globalRaceEntries[rid]);
    try { updateQuickStats(); } catch (_) {}
    try { refreshSunkCostStat(); } catch (_) {}
    if (currentMainView === 'voting') renderLiveViewPanel();
}
function onRaceComposerLineType(rid, i, type) { if (isValidLineType(type)) mutateRaceComposition(rid, c => { c.lines[i].type = type; }); }
function onRaceComposerLineYen(rid, i, yen)   { mutateRaceComposition(rid, c => { c.lines[i].yen = Math.max(0, parseInt(yen, 10) || 0); }); }
function onRaceComposerRemoveLine(rid, i)     { mutateRaceComposition(rid, c => { c.lines.splice(i, 1); if (!c.lines.length) c.lines.push({ type: 'wide_box', yen: getOreProDefaultStake() }); }); }
function onRaceComposerAddLine(rid)           { mutateRaceComposition(rid, c => { if (c.lines.length < MAX_BET_LINES) c.lines.push({ type: nextUnusedLineType(c), yen: 0 }); }); }

async function clearRaceBetOverride(rid) {
    raceBetOverrideUiOpen.delete(rid);
    if (globalRaceMeta[rid]) { delete globalRaceMeta[rid].betComposition; delete globalRaceMeta[rid].betCompositionAutoBackup; }
    touchRaceMeta(rid);
    try { await saveMarksToServer(); } catch (_) { /* state still live in-session */ }
    const tbody = document.getElementById(`tbody-${rid}`);
    if (tbody) tbody.innerHTML = buildTableBody(rid, globalRaceEntries[rid]);
    try { updateQuickStats(); } catch (_) {}
    try { refreshSunkCostStat(); } catch (_) {}
    if (currentMainView === 'voting') renderLiveViewPanel();
}

// ── Phase 34: Auto Bet Day "abstain backup preset" ───────────────────────────
// When the day's preset abstains on a race because the BET TYPE doesn't fit it (e.g. Nagashi with
// no clear axis), Auto Bet Day can re-bet that race with a backup preset instead. The rescue sets a
// per-race composition override TAGGED as auto-created (betCompositionAutoBackup) so a later sweep
// can tell it apart from the operator's own overrides and re-decide it fresh (self-correcting).
function getAbstainBackupPreset() {
    const v = appConfig.ui?.abstainBackupPreset;
    return (v && BET_PRESETS[v]) ? v : 'none';
}
function isAutoBackupOverride(rid) { return globalRaceMeta[rid]?.betCompositionAutoBackup === true; }
function setAutoBackupOverride(rid, presetId) {
    globalRaceMeta[rid] = { ...(globalRaceMeta[rid] || {}), betComposition: compositionForAutoPreset(presetId), betCompositionAutoBackup: true };
}
function clearAutoBackupOverride(rid) {
    const m = globalRaceMeta[rid];
    if (m && m.betCompositionAutoBackup) { delete m.betComposition; delete m.betCompositionAutoBackup; }
}

// Per-race bet record — what was ACTUALLY bet on a race, frozen at APPLY so later day/stake
// changes don't rewrite history. Stored in the marks blob's raceMeta.betProfile =
// { compositionLabel?, stake, betLines }. Null = price by the resolved composition live.
// The frozen betLines ARE the truth (C# scores them verbatim); compositionLabel is display-only.
function normalizeBetProfile(bp) {
    if (!bp || typeof bp !== 'object' || Array.isArray(bp)) return null;
    const stake  = parseInt(bp.stake, 10);
    const aStake = parseInt(bp.actualStaked, 10);  // imported: real ¥ staked (OrePro truth)
    const aWon   = parseInt(bp.actualWon, 10);      // imported: real ¥ returned
    const out = {};
    if (typeof bp.compositionLabel === 'string' && bp.compositionLabel) out.compositionLabel = bp.compositionLabel;
    if (Number.isFinite(stake)  && stake  > 0) out.stake = stake;
    if (Number.isFinite(aStake) && aStake >= 0) out.actualStaked = aStake;
    if (Number.isFinite(aWon)   && aWon   >= 0) out.actualWon = aWon;
    if (bp.source) out.source = String(bp.source);
    // betLines: the frozen per-line bet breakdown (set at placement / future custom tickets).
    // Each line self-describing: { ticket, method, label, horses:[{pp}], axisPp?, comboCount, stakePerCombo }.
    if (Array.isArray(bp.betLines) && bp.betLines.length) {
        const lines = bp.betLines.map(l => {
            if (!l || typeof l !== 'object') return null;
            const horses = Array.isArray(l.horses) ? l.horses.map(h => {
                const pp = parseInt(h?.pp, 10);
                return Number.isFinite(pp) && pp > 0 ? { pp } : null;
            }).filter(Boolean) : [];
            const comboCount = parseInt(l.comboCount, 10);
            const stakePerCombo = Number(l.stakePerCombo);
            const line = {
                ticket: String(l.ticket || ''),
                method: String(l.method || ''),
                label: l.label ? String(l.label) : '',
                horses,
                comboCount: Number.isFinite(comboCount) && comboCount > 0 ? comboCount : 0,
                stakePerCombo: Number.isFinite(stakePerCombo) && stakePerCombo >= 0 ? stakePerCombo : 0
            };
            const axisPp = parseInt(l.axisPp, 10);
            if (Number.isFinite(axisPp) && axisPp > 0) line.axisPp = axisPp;
            return line.ticket ? line : null;
        }).filter(Boolean);
        if (lines.length) out.betLines = lines;
    }
    if (bp.extrapolated === true) out.extrapolated = true;
    return Object.keys(out).length ? out : null;
}
function getRaceBetProfile(r_id) {
    return normalizeBetProfile(globalRaceMeta[r_id]?.betProfile);
}

// Slider 0-100 → SAFE (<40), CHAOS (>60), BLEND (40-60).
// BLEND defers to BOX_OPTIMIZATION for divergent role assignments.
function riskZone(riskValue) {
    if (riskValue < 40) return 'SAFE';
    if (riskValue > 60) return 'CHAOS';
    return 'BLEND';
}

// Canonical mark sequence for a target count (Phase 29 v2). ◎〇▲ are singletons;
// every mark beyond the 3rd is a △ (mirrors OrePro marks 4..18 = △). So:
//   1→◎  2→◎〇  3→◎〇▲  4→◎〇▲△  5→◎〇▲△△  6→◎〇▲△△△
// The mark COUNT is what auto-selects the OrePro template downstream.
function markSequenceForCount(count) {
    const n = Math.max(0, Math.min(6, Number(count) || 0));
    const base = ['◎', '〇', '▲'];
    const seq = [];
    for (let i = 0; i < n; i++) seq.push(i < 3 ? base[i] : '△');
    return seq;
}

// Parse Last3 string ("2⑧-1③-—") into structured runs, most-recent first.
function parseLast3Runs(last3Str) {
    if (!last3Str || last3Str === '—-—-—') return [];
    return String(last3Str).split('-').map(p => {
        const m = p.match(/^(\d+)([①-⑱])?$/);
        if (!m) return null;
        const fin = parseInt(m[1], 10);
        const favRank = m[2] ? (m[2].codePointAt(0) - 0x245F) : null;
        const delta = (favRank !== null && Number.isFinite(fin)) ? (favRank - fin) : null;
        return { fin, favRank, delta };
    }).filter(Boolean);
}

// Max positive NinkiFinishDelta across last-3 runs (0 if none qualify).
function ninkiDeltaMaxPositive(runs) {
    let max = 0;
    for (const r of runs) if (r.delta !== null && r.delta > max) max = r.delta;
    return max;
}

// If most-recent run was a "burned favorite" (favRank ≤ 5 AND delta < 0), return that delta; else null.
function mostRecentBurnedDelta(runs) {
    if (runs.length === 0) return null;
    const r = runs[0];
    if (r.delta === null || r.favRank === null) return null;
    if (r.favRank <= 5 && r.delta < 0) return r.delta;
    return null;
}

function raceStatusEmoji(race) {
    if (raceHasHistoryData(race)) return '🏁';
    const sortTime = parseRaceSortTime(race?.info?.sort_time_iso || race?.info?.sort_time, race?.info);
    if (sortTime && sortTime.getTime() < Date.now()) return '⌛';
    return '🕒';
}

// Phase 43: surface + distance chip for the race header (e.g. "🌱 Turf 1600m").
// distance/surface now come straight from /api/races info (races-v9). Returns '' when
// the race has neither yet (pre-ingest), so the header simply omits it. Inline-styled
// to avoid a style.css version bump.
function raceSurfaceDistChip(info) {
    if (!info) return '';
    const dist = (info.distance && Number(info.distance) > 0) ? `${info.distance}m` : '';
    const surfRaw = String(info.surface || '').toLowerCase();
    let icon = '', label = '', color = '';
    if (surfRaw === 'turf') { icon = '🌱'; label = 'Turf'; color = '#3fae5a'; }
    else if (surfRaw === 'dirt') { icon = '🟤'; label = 'Dirt'; color = '#c08a4a'; }
    else if (surfRaw === 'jump') { icon = '🚧'; label = 'Jump'; color = '#b06ad0'; } // 障害 — a separate discipline
    const text = [label, dist].filter(Boolean).join(' ');
    if (!text) return '';
    return ` <span class="race-sd-chip" style="display:inline-block;font-size:0.78em;font-weight:600;padding:1px 7px;margin:0 4px;border-radius:10px;background:${color}22;color:${color};border:1px solid ${color}66;vertical-align:middle;">${icon} ${text}</span>`;
}

function raceHasHistoryData(race) {
    if (!race) return false;
    if (race.info?.history_refreshed) return true;
    return Array.isArray(race.entries) && race.entries.some(row => String(row.Finish || '').trim() !== '');
}

// --- SECURITY: HTML Escaping ---
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function parseRaceSortTime(sortTimeStr, raceInfo = null) {
    const raw = String(sortTimeStr || '').trim();
    if (!raw) return null;

    const normalized = raw.replace(' ', 'T');
    if (/([zZ]|[+-]\d{2}:\d{2})$/.test(normalized)) {
        const explicit = new Date(normalized);
        return Number.isNaN(explicit.getTime()) ? null : explicit;
    }

    const cleanDate = String(raceInfo?.clean_date || '').trim();
    const displayTime = String(raceInfo?.time || '').trim();
    const sortDate = normalized.slice(0, 10);
    const looksCtLocal = /\b(?:AM|PM)\b/i.test(displayTime) || (!!cleanDate && cleanDate !== sortDate);

    // NK cache currently stores `sort_time` in local/CT wall-clock form for display,
    // while JV-style timestamps are closer to JST. Detect the NK pattern first so we
    // do not accidentally treat same-day upcoming races as already finished.
    const dt = new Date(looksCtLocal ? normalized : `${normalized}+09:00`);
    return Number.isNaN(dt.getTime()) ? null : dt;
}

// --- CLOCK & COUNTDOWN ---
function updateClock() {
    // JST/CT show HH:MM (no seconds) — narrower + less noise; the countdown below keeps its seconds.
    const jstOpts = {hour: '2-digit', minute:'2-digit', hour12: false, timeZone: 'Asia/Tokyo'};
    const cstOpts = {hour: '2-digit', minute:'2-digit', hour12: true, timeZone: 'America/Chicago'};
    const now = new Date();
    
    document.getElementById('jst').innerText = now.toLocaleTimeString('en-US', jstOpts);
    document.getElementById('cst').innerText = now.toLocaleTimeString('en-US', cstOpts);

    // Countdown Logic
    const cdContainer = document.getElementById('countdown-container');
    if (upcomingRaces.length > 0) {
        // Find the absolute closest race that is still in the future
        const nextRace = upcomingRaces.find(r => r.time > now);
        
        if (nextRace) {
            cdContainer.style.display = "block";
            const diff = nextRace.time - now;
            
            // NEW: Added the Days (d) calculation!
            const d = Math.floor(diff / (1000 * 60 * 60 * 24));
            const h = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
            const m = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
            const s = Math.floor((diff % (1000 * 60)) / 1000);
            
            // Format to always show two digits (e.g., 05:09:02)
            let timeStr = `${h.toString().padStart(2, '0')}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
            
            // If the race is a day or more away, stick the Days onto the front
            if (d > 0) {
                timeStr = `${d}d ${timeStr}`;
            }
            
            document.getElementById('countdown-time').innerText = timeStr;
            document.getElementById('countdown-race').innerText = nextRace.name;
        } else {
            cdContainer.style.display = "none"; // All races are done!
        }
    }
}

setInterval(updateClock, 1000); updateClock();

// --- PHASE BADGE ---
async function refreshPhaseBadge() {
    try {
        const res = await fetch('/api/orchestrator/status');
        if (!res.ok) return;
        const data = await res.json();
        const badge = document.getElementById('phase-badge');
        const sub   = document.getElementById('phase-badge-sub');
        if (!badge || !sub) return;

        const phase = data.phase || 'WAITING_FOR_RACES';
        const inMaintenance = !!data.maintenance;
        badge.classList.remove('phase-waiting', 'phase-posts', 'phase-upcoming', 'phase-live', 'phase-maintenance');
        badge.style.removeProperty('background');
        const labelEl = badge.querySelector('.phase-badge-label');

        if (inMaintenance) {
            // JRA-VAN server is under maintenance (rc=-504) — the orchestrator is backed off.
            // Amber inline so it stands out regardless of phase; no style.css dependency.
            badge.classList.add('phase-maintenance');
            badge.style.background = '#8a5a00';
            labelEl.textContent = '🛠 JRA-VAN maintenance';
        } else if (phase === 'LIVE_OPERATIONS') {
            badge.classList.add('phase-live');
            labelEl.textContent = '🔴 LIVE';
        } else if (phase === 'AWAITING_POSTS') {
            badge.classList.add('phase-posts');
            labelEl.textContent = '🎫 Awaiting posts';
        } else if (phase === 'AWAITING_ODDS') {
            badge.classList.add('phase-upcoming');
            labelEl.textContent = '⏳ Awaiting odds';
        } else if (phase === 'RACES_POPULATED') {
            badge.classList.add('phase-upcoming');
            labelEl.textContent = '📅 Upcoming';
        } else {
            badge.classList.add('phase-waiting');
            labelEl.textContent = '⏸ Waiting';
        }

        _phaseBadgeState.eta = data.next_tick_eta_utc ? new Date(data.next_tick_eta_utc) : null;
        _phaseBadgeState.phase = phase;
        _phaseBadgeState.maintenance = inMaintenance;
        _updateTickCountdown();
        renderPipelineHealth(data.pipeline_health);
    } catch { /* silently ignore network errors */ }
}

// Pipeline-health dot (T1-1). A subtle indicator beside the sidebar "UMAnager" title.
// Consumes /api/orchestrator/status → pipeline_health, a dict keyed by step name ('parse',
// 'orchestrator-tick', 'streaming-watchdog') with each step's { consecutive_failures,
// seconds_since_success, last_error, healthy }. The dot COLOUR is the only signal — green all-ok,
// amber a step failing 1–2× (degraded), red a step failed ≥3× (the same threshold that fires the
// Discord alert). Full per-step detail lives in the hover tooltip. Born from the 2026-06-13
// silent-results-outage (see TECH_DEBT.md T1-1).
function renderPipelineHealth(health) {
    const dot = document.getElementById('health-dot');
    if (!dot) return;
    const steps = (health && typeof health === 'object') ? Object.entries(health) : [];
    const fmtAgo = s => (s == null) ? null : (s < 90 ? `${Math.round(s)}s` : `${Math.round(s / 60)}m`);

    if (!steps.length) {
        dot.className = 'health-dot health-unknown';
        dot.title = 'Pipeline health — no steps have reported since the last Nexus restart.';
        return;
    }

    // Worst state across steps: ≥3 consecutive failures = down, 1–2 = degraded, 0 = ok.
    let worst = 'ok';
    for (const [, s] of steps) {
        const cf = Number(s?.consecutive_failures || 0);
        if (cf >= 3)     { worst = 'down'; }
        else if (cf > 0) { if (worst !== 'down') worst = 'degraded'; }
    }
    dot.className = 'health-dot ' + (worst === 'down' ? 'health-down' : worst === 'degraded' ? 'health-degraded' : 'health-ok');

    const headline = worst === 'down' ? 'Pipeline: a step is DOWN' : worst === 'degraded' ? 'Pipeline: a step is degraded' : 'Pipeline: healthy';
    dot.title = headline + '\n' + steps.map(([name, s]) => {
        const cf = Number(s?.consecutive_failures || 0);
        const fresh = fmtAgo(s?.seconds_since_success);
        const state = cf >= 3 ? `DOWN (${cf}× in a row)` : cf > 0 ? `degraded (${cf}×)` : 'ok';
        const err = (cf > 0 && s?.last_error) ? ` — ${String(s.last_error).slice(0, 90)}` : '';
        return `• ${name}: ${state}; last ok ${fresh != null ? fresh + ' ago' : 'never'}${err}`;
    }).join('\n');
}

const _phaseBadgeState = { eta: null, phase: '', maintenance: false };
const _tickActionLabel = {
    LIVE_OPERATIONS:   'odds refresh',
    RACES_POPULATED:   'odds refresh',
    AWAITING_ODDS:     'odds check',
    AWAITING_POSTS:    'post draw check',
    WAITING_FOR_RACES: 'race plan poll',
};
function _updateTickCountdown() {
    const sub = document.getElementById('phase-badge-sub');
    if (!sub) return;
    const { eta, phase, maintenance } = _phaseBadgeState;
    let text;
    if (maintenance && !eta) {
        text = 'Server down · retrying';
    } else if (!eta) {
        text = phase === 'WAITING_FOR_RACES' ? 'No upcoming races'
             : phase === 'AWAITING_POSTS'   ? 'Draw pending'
             : '';
    } else {
        const diffMs = eta - Date.now();
        const action = maintenance ? 'maintenance retry' : (_tickActionLabel[phase] || 'tick');
        if (diffMs <= 0) {
            text = `${action} imminent`;
        } else {
            const mins = Math.floor(diffMs / 60000);
            const secs = Math.floor((diffMs % 60000) / 1000);
            const timeStr = mins > 0 ? `${mins}m ${String(secs).padStart(2, '0')}s` : `${secs}s`;
            text = `Next ${action}: ${timeStr}`;
        }
    }
    sub.textContent = text;
    // The phase pill (sidebar) hides this sub line via CSS and surfaces it as the hover tooltip,
    // alongside the phase label — so the pill stays compact but the countdown is one hover away.
    const badge = document.getElementById('phase-badge');
    if (badge) {
        const label = (badge.querySelector('.phase-badge-label')?.textContent || '').trim();
        badge.title = text ? `${label} · ${text}` : label;
    }
}

refreshPhaseBadge();
setInterval(refreshPhaseBadge, 30000);
setInterval(_updateTickCountdown, 1000);

function applyRiskSliderValue(value) {
    const slider = document.getElementById('risk-slider');
    if (!slider) return;
    slider.value = String(value);
    slider.dispatchEvent(new Event('input', { bubbles: true }));
    slider.dispatchEvent(new Event('change', { bubbles: true }));
}

// Per-race detail behind the Day Net quick-stat, refreshed on every updateQuickStats() call so the
// hover panel below always matches what's on screen.
let dayNetBreakdownCache = [];

// Simple hover panel for Day Net — total gained across winning races vs total lost across losing
// races (a race with any hit that still nets negative, e.g. a partial box, counts toward "lost", not
// "gained" — bucketed by NET, not by whether anything hit at all). A native title="" can't show two
// separate figures cleanly, so this builds a tiny panel on mouseenter (over #qs-pl-block in
// index.html) and tears it down on mouseleave. Positioned via getBoundingClientRect so it never
// depends on a parent's overflow/z-index.
function showDayNetBreakdown(ev) {
    hideDayNetBreakdown(); // guard against a stray double-fire leaving two panels
    const settled = dayNetBreakdownCache.filter(r => r.settled);
    const pending = dayNetBreakdownCache.filter(r => !r.settled);
    if (!settled.length && !pending.length) return;

    let gained = 0, lost = 0, winCount = 0, lossCount = 0;
    settled.forEach(r => {
        const net = r.won - r.staked;
        if (net > 0) { gained += net; winCount++; }
        else if (net < 0) { lost += -net; lossCount++; }
    });

    const pendingNote = pending.length
        ? `<div style="margin-top:6px;padding-top:6px;border-top:1px solid #243044;color:#8a93a3;font-size:0.82em;">`
          + `${pending.length} still running (¥${pending.reduce((s, r) => s + r.staked, 0).toLocaleString()} staked, not counted yet)</div>`
        : '';
    const body = settled.length
        ? `<div style="display:flex;flex-direction:column;gap:4px;">
             <div style="display:flex;justify-content:space-between;gap:16px;"><span style="color:#9fb2c8;">🟢 Gained</span><span style="font-weight:700;color:#7fe0a0;">+¥${gained.toLocaleString()}${winCount ? ` (${winCount})` : ''}</span></div>
             <div style="display:flex;justify-content:space-between;gap:16px;"><span style="color:#9fb2c8;">🔴 Lost</span><span style="font-weight:700;color:#ff9a9a;">−¥${lost.toLocaleString()}${lossCount ? ` (${lossCount})` : ''}</span></div>
           </div>${pendingNote}`
        : `<div style="color:#8a93a3;">No races settled yet.</div>${pendingNote}`;

    const panel = document.createElement('div');
    panel.id = 'day-net-breakdown';
    panel.style.cssText = 'position:fixed;z-index:9999;background:#141a24;border:1px solid #2a3040;'
        + 'border-radius:10px;box-shadow:0 8px 24px rgba(0,0,0,0.5);padding:10px 12px;font-size:0.85em;'
        + 'max-width:260px;pointer-events:none;';
    panel.innerHTML = `<div style="color:#8a93a3;font-size:0.78em;margin-bottom:6px;">Settled races only — Day Net starts at ¥0 each day</div>${body}`;
    document.body.appendChild(panel);

    const targetEl = document.getElementById('qs-pl-block') || ev.currentTarget;
    const r = targetEl.getBoundingClientRect();
    const pr = panel.getBoundingClientRect();
    let top = r.bottom + 6;
    let left = Math.min(r.left, window.innerWidth - pr.width - 8);
    if (left < 8) left = 8;
    if (top + pr.height > window.innerHeight - 8) top = r.top - pr.height - 6;
    panel.style.top = `${top}px`;
    panel.style.left = `${left}px`;
}
function hideDayNetBreakdown() {
    const el = document.getElementById('day-net-breakdown');
    if (el) el.remove();
}

function updateQuickStats() {
    const qsMarks = document.getElementById('qs-marks');
    const qsMarksDetail = document.getElementById('qs-marks-detail');
    const qsAgreement = document.getElementById('qs-agreement');
    const qsAgreementSub = document.getElementById('qs-agreement-sub');
    const qsPL = document.getElementById('qs-pl');
    const qsPLSub = document.getElementById('qs-pl-sub');
    updateLockAllBetsButton(); // keep Lock All / Unlock All label in sync with lock state
    if (!qsMarks) return;

    const activeDate = currentActiveDate;
    if (!activeDate) {
        qsMarks.textContent = '—';
        qsMarksDetail.textContent = 'no day';
        qsAgreement.textContent = '—';
        qsAgreementSub.textContent = 'engine vs you';
        qsPL.textContent = '—';
        qsPLSub.textContent = 'stake / net';
        return;
    }

    const dateRaceIds = Object.keys(globalRaceInfo).filter(
        r_id => globalRaceInfo[r_id]?.clean_date === activeDate
    );

    // Marks count: sum of all non-X marks across the day's races.
    // collectRaceMainMarks returns { symbol → horse_id }, so iterate keys.
    const markSymbolCounts = { '◎': 0, '〇': 0, '▲': 0, '△': 0 };
    let totalMarks = 0;
    dateRaceIds.forEach(r_id => {
        const marks = collectRaceMainMarks(r_id) || {};
        Object.keys(marks).forEach(sym => {
            if (markSymbolCounts.hasOwnProperty(sym)) {
                markSymbolCounts[sym]++;
                totalMarks++;
            }
        });
    });
    qsMarks.textContent = totalMarks.toString();
    qsMarksDetail.textContent = `◎${markSymbolCounts['◎']} 〇${markSymbolCounts['〇']} ▲${markSymbolCounts['▲']} △${markSymbolCounts['△']}`;

    // Agreement %: across races where the user marked anything, what fraction of
    // their marked horses are in the engine's top-4 unconditional ranking.
    // Build per-race {userHorseIds, allEntries} so we can sweep risk values cheaply.
    const markedRaces = [];
    dateRaceIds.forEach(r_id => {
        const marks = collectRaceMainMarks(r_id) || {};
        const userHorseIds = Object.values(marks).map(h => String(h).split('.')[0]);
        if (!userHorseIds.length) return;
        const entries = globalRaceEntries[r_id] || [];
        if (!entries.length) return;
        markedRaces.push({ userHorseIds, entries });
    });

    const computeAgreementAtRisk = (riskVal) => {
        let total = 0;
        let hits = 0;
        markedRaces.forEach(({ userHorseIds, entries }) => {
            const top4 = entries
                .map(row => ({ h_id: String(row.Horse_ID).split('.')[0], power: calculatePowerScore(row, riskVal) }))
                .sort((a, b) => b.power - a.power)
                .slice(0, 4)
                .map(e => e.h_id);
            const engineSet = new Set(top4);
            userHorseIds.forEach(h_id => {
                total++;
                if (engineSet.has(h_id)) hits++;
            });
        });
        return { total, hits };
    };

    const currentRisk = getCurrentAutoPickRisk();
    const current = computeAgreementAtRisk(currentRisk);

    if (current.total === 0) {
        qsAgreement.textContent = '—';
        qsAgreementSub.textContent = 'no marks yet';
        document.getElementById('qs-agreement-best').textContent = '';
    } else {
        const pct = Math.round((current.hits / current.total) * 100);
        qsAgreement.textContent = `${pct}%`;
        qsAgreementSub.textContent = `${current.hits}/${current.total} on engine top-4`;

        // Sweep risk 0..100 in steps of 5; report the slider value that maximises hits.
        // Tie-break by picking the value closest to the current slider position (least
        // disruptive suggestion).
        let bestHits = -1;
        let bestRisk = currentRisk;
        for (let r = 0; r <= 100; r += 5) {
            const { hits } = computeAgreementAtRisk(r);
            if (hits > bestHits || (hits === bestHits && Math.abs(r - currentRisk) < Math.abs(bestRisk - currentRisk))) {
                bestHits = hits;
                bestRisk = r;
            }
        }
        const bestEl = document.getElementById('qs-agreement-best');
        if (bestHits === current.hits) {
            bestEl.textContent = `best: ${bestRisk} (current)`;
            bestEl.onclick = null;
            bestEl.classList.remove('quick-stat-best-clickable');
        } else {
            const bestPct = Math.round((bestHits / current.total) * 100);
            bestEl.textContent = `best: ${bestRisk} → ${bestPct}% ↗`;
            bestEl.title = `Click to set risk slider to ${bestRisk}`;
            bestEl.onclick = () => applyRiskSliderValue(bestRisk);
            bestEl.classList.add('quick-stat-best-clickable');
        }
    }

    // Day Net (SETTLED basis, s60+): only races that have actually FINISHED move this number — a
    // placed-but-still-running bet no longer drags it into the red the moment it's locked. Starts the
    // day at ¥0 and moves by each race's own net exactly once it settles, same framing as the Discord
    // win-ping's "Day net" line. (Previously counted the full stake the instant a race was locked,
    // before it even ran — the "assume you've lost it, hope to claw back big" framing the operator
    // explicitly asked to move away from now that Discipline mode is the norm, not wild bets.)
    // Computed from the ACTUAL template bets (place/wide/trio per the ladder), not the obsolete
    // Win/Q/T model.
    let wonTotal = 0;
    let spentTotal = 0;
    let placedCount = 0;
    let settledCount = 0;
    const breakdown = []; // per-race staked/won for the hover panel's gained/lost totals — see showDayNetBreakdown
    dateRaceIds.forEach(r_id => {
        if (!isRaceLocked(r_id)) return;
        const race = { info: globalRaceInfo[r_id], entries: globalRaceEntries[r_id] || [] };
        const out = evaluateTemplateOutcome(race);
        if (out.markCount === 0) return; // locked but no marks → no bet
        placedCount++;
        if (!out.hasResults) { breakdown.push({ staked: out.staked, won: 0, settled: false }); return; }
        settledCount++;
        spentTotal += out.staked;
        wonTotal   += out.won;
        breakdown.push({ staked: out.staked, won: out.won, settled: true });
    });
    dayNetBreakdownCache = breakdown;
    if (placedCount === 0) {
        qsPL.textContent = '—';
        qsPLSub.textContent = 'no bets placed';
        qsPL.classList.remove('quick-stat-pos', 'quick-stat-neg');
    } else if (settledCount === 0) {
        qsPL.textContent = '¥0';
        qsPLSub.textContent = `${placedCount} placed · none settled yet`;
        qsPL.classList.remove('quick-stat-pos', 'quick-stat-neg');
    } else {
        const net = Math.round(wonTotal - spentTotal);
        const sign = net >= 0 ? '+' : '−';
        qsPL.textContent = `${sign}¥${Math.abs(net).toLocaleString()}`;
        qsPLSub.textContent = `${settledCount}/${placedCount} settled · ¥${Math.round(spentTotal).toLocaleString()} staked · ¥${Math.round(wonTotal).toLocaleString()} won`;
        qsPL.classList.toggle('quick-stat-pos', net >= 0);
        qsPL.classList.toggle('quick-stat-neg', net < 0);
    }
    updatePreOddsNotice();
}

// All-time sunk-cost tally — lives in the Voting tab (server-derived from locked = placed
// bets). Rises as you lock bets, credits back as results land. Survives restart + identical
// across devices. API keys are PascalCase (Program.cs pins PropertyNamingPolicy = null).
async function refreshSunkCostStat() {
    const netEl = document.getElementById('voting-sunk-net');
    if (!netEl) return; // panel only exists on the Voting tab
    const stakedEl = document.getElementById('voting-sunk-staked');
    const wonEl    = document.getElementById('voting-sunk-won');
    const betsEl   = document.getElementById('voting-sunk-bets');
    const scopeEl  = document.getElementById('voting-sunk-scope');
    try {
        const res = await fetch('/api/sunk-cost', { cache: 'no-store' });
        if (!res.ok) return;
        const d = await res.json();
        const net     = Number(d.NetYen)         || 0; // won − staked (negative = in the hole)
        const staked  = Number(d.TotalStakedYen) || 0;
        const won     = Number(d.TotalWonYen)    || 0;
        const placed  = Number(d.PlacedRaces)    || 0;
        const pending = Number(d.PendingRaces)   || 0;
        const sign = net >= 0 ? '+' : '−';
        netEl.textContent = `${sign}¥${Math.abs(net).toLocaleString()}`;
        netEl.classList.toggle('quick-stat-pos', net >= 0);
        netEl.classList.toggle('quick-stat-neg', net < 0);
        if (stakedEl) stakedEl.textContent = `¥${staked.toLocaleString()}`;
        if (wonEl)    wonEl.textContent    = `¥${won.toLocaleString()}`;
        if (betsEl)   betsEl.textContent   = pending ? `${placed} (${pending} live)` : `${placed}`;
        if (scopeEl)  scopeEl.textContent  = d.ResetAt ? `since ${d.ResetAt}` : 'all-time';
        // Side (loyalty) bets are tracked APART so the Net/recovery above stays pure Discipline. Show
        // their running P/L on its own line — hide the row entirely until any side bet exists.
        const sideWrap = document.getElementById('voting-sunk-side-wrap');
        const sideEl   = document.getElementById('voting-sunk-side');
        const sideStaked = Number(d.SideStakedYen) || 0;
        const sideNet    = Number(d.SideNetYen)    || 0;
        if (sideWrap) sideWrap.style.display = sideStaked > 0 ? '' : 'none';
        if (sideEl && sideStaked > 0) {
            const ss = sideNet >= 0 ? '+' : '−';
            sideEl.textContent = `${ss}¥${Math.abs(sideNet).toLocaleString()}`;
            sideEl.classList.toggle('quick-stat-pos', sideNet >= 0);
            sideEl.classList.toggle('quick-stat-neg', sideNet < 0);
        }
    } catch (_) { /* leave prior values */ }
}

async function resetSunkCost() {
    if (!confirm('Reset the sunk-cost tally? It will then count only races from today (JST) forward.')) return;
    try {
        await fetch('/api/sunk-cost/reset', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({})
        });
    } catch (_) { /* ignore */ }
    refreshSunkCostStat();
}

// Retroactively record every locked (placed) race as Default-OrePro 4-horse @ ¥10k — your
// historical bet style — so the all-time total prices history correctly. Server-side +
// re-runnable. Each race's record is then frozen against future global Bet-Mode changes.
async function backfillHistoricalBets() {
    if (!confirm('Record all locked (placed) races as Default OrePro 4-horse @ ¥10,000?\n\n'
        + 'This sets what each past race counts as for total sunk cost. Re-runnable; overwrites any existing record.')) return;
    try {
        const res = await fetch('/api/sunk-cost/backfill', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode: 'orepro_default', stake: 10000 })
        });
        const d = await res.json();
        // reload marks so globalRaceMeta picks up the new betProfiles (keeps frontend Day Net in sync)
        try {
            const mr = await fetch('/api/marks');
            const mp = normalizeMarksPayload(await mr.json());
            globalMarks = mp.marks; globalRaceMeta = mp.raceMeta; globalMarksVersion = mp.version;
        } catch (_) {}
        refreshSunkCostStat();
        updateQuickStats();
        alert(`Recorded ${d?.stamped ?? 0} race(s) as Default OrePro @ ¥10,000.`
            + (d?.summary ? `\n\nTotal: net ¥${Number(d.summary.NetYen||0).toLocaleString()} `
                + `(¥${Number(d.summary.TotalStakedYen||0).toLocaleString()} staked, `
                + `¥${Number(d.summary.TotalWonYen||0).toLocaleString()} won, `
                + `${d.summary.PlacedRaces||0} bets).` : ''));
    } catch (err) {
        alert('Backfill failed: ' + (err?.message || err));
    }
}

// Parse a pasted 俺プロフ day (the plain-text export) into import records. Mirrors the
// server-side resolver's expected shape: { track, num, purchase, payout, marks:[{sym,horse}] }.
function parseOreProDayText(text) {
    const TRACKS = new Set(['中山','阪神','中京','東京','京都','福島','新潟','小倉','札幌','函館']);
    const lines = String(text || '').split(/\r?\n/);
    const recs = [];
    let cur = null;
    for (let i = 0; i < lines.length; i++) {
        const l = lines[i].trim();
        const m = l.match(/^(\S+)\s+(\d+)R\b/);            // "中山 12R 4歳以上2勝クラス"
        if (m && TRACKS.has(m[1])) {
            if (cur) recs.push(cur);
            cur = { track: m[1], num: parseInt(m[2], 10), purchase: 0, payout: 0, marks: [] };
        } else if (cur) {
            if (l.startsWith('払戻金')) {
                const mm = l.match(/([\d,]+)円/);            // payout on same line after a tab
                cur.payout = mm ? parseInt(mm[1].replace(/,/g, ''), 10) : 0;
            } else if (l && '◎◯〇▲△'.indexOf(l[0]) >= 0) {
                cur.marks.push({ sym: l[0], horse: l.slice(1).trim() });
            } else if (l.startsWith('購入金額')) {
                const next = (lines[i + 1] || '').match(/([\d,]+)円/); // amount on the NEXT line
                if (next) cur.purchase = parseInt(next[1].replace(/,/g, ''), 10);
            }
        }
    }
    if (cur) recs.push(cur);
    return recs;
}

// Dev tool: parse the pasted OrePro day text → POST to the import resolver (sets real marks
// on the matched races + actual ¥) → reload marks so they show on the cards.
async function importOreProPastedText() {
    const ta = document.getElementById('orepro-import-text');
    const status = document.getElementById('orepro-import-status');
    const setStatus = (t) => { if (status) status.textContent = t; };
    const recs = parseOreProDayText(ta ? ta.value : '');
    if (!recs.length) { setStatus('No races found in the pasted text.'); return; }
    const staked = recs.reduce((s, r) => s + (r.purchase || 0), 0);
    const payout = recs.reduce((s, r) => s + (r.payout || 0), 0);
    setStatus(`Parsed ${recs.length} races (staked ¥${staked.toLocaleString()}, returned ¥${payout.toLocaleString()}). Importing…`);
    try {
        const res = await fetch('/api/sunk-cost/import', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ records: recs })
        });
        const d = await res.json();
        const matched = d.RacesMatched ?? d.racesMatched ?? 0;
        const written = d.MarksWritten ?? d.marksWritten ?? 0;
        const unresolved = d.Unresolved ?? d.unresolved ?? [];
        const sum = d.summary ?? d.Summary ?? {};
        // Reload marks so globalRaceMeta/globalMarks pick up the freshly-written marks.
        try {
            const mr = await fetch('/api/marks');
            const mp = normalizeMarksPayload(await mr.json());
            globalMarks = mp.marks; globalRaceMeta = mp.raceMeta; globalMarksVersion = mp.version;
        } catch (_) {}
        refreshSunkCostStat(); updateQuickStats(); updateAutoBetHighlighting();
        let line = `Imported ${matched} race(s), ${written} marks${unresolved.length ? `, ${unresolved.length} unresolved` : ''}.`;
        if (sum && sum.NetYen != null) line += ` All-time: net ¥${Number(sum.NetYen).toLocaleString()} over ${sum.PlacedRaces} bets.`;
        setStatus(line);
        if (unresolved.length) {
            alert(`${matched} races imported.\n\n${unresolved.length} could not be matched to our DB:\n- ` + unresolved.slice(0, 30).join('\n- '));
        }
    } catch (err) {
        setStatus('Import failed: ' + (err?.message || err));
    }
}

function renderEnginePicks() {
    const container = document.getElementById('sidebar-engine-picks');
    if (!container) return;

    const activeDate = currentActiveDate;
    if (!activeDate) {
        container.innerHTML = '<div class="ww-empty">No date selected.</div>';
        return;
    }

    const risk = getCurrentAutoPickRisk();
    const cands = [];
    Object.keys(globalRaceInfo).forEach(r_id => {
        const info = globalRaceInfo[r_id];
        if (info.clean_date !== activeDate) return;
        const timeline = info._timeline || 'upcoming';
        (globalRaceEntries[r_id] || []).forEach(entry => {
            const score = calculatePowerScore(entry, risk);
            if (!Number.isFinite(score)) return;
            cands.push({
                r_id,
                date: info.clean_date,
                sortTime: info.sort_time || '',
                label: `${trackName(info.place)} R${info.race_number}`,
                entry,
                score,
                timeline,
            });
        });
    });

    if (cands.length === 0) {
        container.innerHTML = `<div class="ww-empty">No races on ${activeDate}.</div>`;
        return;
    }

    cands.sort((a, b) => b.score - a.score);
    const top = cands.slice(0, 5);
    const isPastDate = top[0].timeline === 'past';

    const RANK_BADGES = ['①', '②', '③', '④', '⑤'];
    container.innerHTML = top.map((m, idx) => {
        const odds = m.entry.Odds ? `×${parseFloat(m.entry.Odds).toFixed(1)}` : '—';
        const pp = m.entry.PP ? ` #${m.entry.PP}` : '';
        const finish = Number(m.entry.Finish);
        const finishTag = (isPastDate && Number.isFinite(finish) && finish > 0)
            ? ` · fin ${finish}`
            : '';
        const callClass = (isPastDate && Number.isFinite(finish))
            ? (finish === 1 ? ' ww-call-win' : finish <= 3 ? ' ww-call-place' : '')
            : '';
        return `<div class="ww-item ww-engine-pick${callClass}" onclick="jumpToHorse('${m.date}', '${m.r_id}', '${m.entry.Horse_ID}', '${m.timeline}')" title="Engine score: ${m.score.toFixed(2)}">
            <span class="ww-badge">${RANK_BADGES[idx]}</span>
            <span class="ww-name">${m.entry.Horse || m.entry.Horse_ID}</span>
            <span class="ww-meta">${m.label}${pp} · ${odds}${finishTag}</span>
        </div>`;
    }).join('');
}

function renderWeekendWatchlist() {
    const container = document.getElementById('sidebar-weekend-watchlist');
    if (!container) return;

    const tracked = getTrackedSets();
    // Only the Watchlist drives this panel: Bloodlines are breeding horses (sires/dams/BMS)
    // and won't normally appear as runners. Pedigree-tracked highlighting handles that
    // case at the row-coloring layer.
    const watched = tracked.watchlist;

    if (watched.size === 0) {
        container.innerHTML = '<div class="ww-empty">Add horses to your Favorites to see them here.</div>';
        return;
    }

    const activeDate = currentActiveDate;
    if (!activeDate) {
        container.innerHTML = '<div class="ww-empty">No date selected.</div>';
        return;
    }

    const matches = [];
    Object.keys(globalRaceInfo).forEach(r_id => {
        const info = globalRaceInfo[r_id];
        if (info.clean_date !== activeDate) return;
        (globalRaceEntries[r_id] || []).forEach(entry => {
            const id = String(entry.Horse_ID || '').split('.')[0].trim();
            if (!watched.has(id)) return;
            matches.push({
                r_id,
                date: info.clean_date,
                sortTime: info.sort_time || '',
                label: `${trackName(info.place)} R${info.race_number}`,
                entry,
                timeline: info._timeline || 'upcoming',
            });
        });
    });

    matches.sort((a, b) => a.sortTime < b.sortTime ? -1 : a.sortTime > b.sortTime ? 1 : 0);

    if (matches.length === 0) {
        container.innerHTML = `<div class="ww-empty">No Favorites running on ${activeDate}.</div>`;
        return;
    }

    container.innerHTML = matches.map(m => {
        const odds = m.entry.Odds ? `×${parseFloat(m.entry.Odds).toFixed(1)}` : '—';
        const pp = m.entry.PP ? ` #${m.entry.PP}` : '';
        const finish = Number(m.entry.Finish);
        const finishTag = (m.timeline === 'past' && Number.isFinite(finish) && finish > 0)
            ? ` · fin ${finish}`
            : '';
        return `<div class="ww-item${m.timeline === 'past' ? ' ww-past' : ''}" onclick="jumpToHorse('${m.date}', '${m.r_id}', '${m.entry.Horse_ID}', '${m.timeline}')">
            <span class="ww-badge">👁</span>
            <span class="ww-name">${m.entry.Horse || m.entry.Horse_ID}</span>
            <span class="ww-meta">${m.label}${pp} · ${odds}${finishTag}</span>
        </div>`;
    }).join('');
}

function toggleWeekendWatchlist() {
    const body = document.getElementById('weekend-watchlist-body');
    const chevron = document.getElementById('weekend-watchlist-chevron');
    if (!body) return;
    const collapsed = body.style.display === 'none';
    body.style.display = collapsed ? '' : 'none';
    if (chevron) chevron.textContent = collapsed ? '▲' : '▼';
}

// ── Phase 30: Vote History ──────────────────────────────────────────────────

// "Voted N×" badge count for a horse on a given race card — counts only races OTHER
// than the one being viewed, so a horse you've only voted for in THIS race shows nothing.
// The badge means "you've backed this horse before (in another race)."
function votedCountExcludingRace(h_id, r_id) {
    const races = globalVoteHistoryRaces[h_id];
    if (!races || !races.length) return 0;
    return races.filter(rid => rid !== r_id).length;
}

async function loadVoteHistory() {
    try {
        const res = await fetch('/api/votes/history');
        if (!res.ok) return;
        const data = await res.json();
        globalVoteHistory = data.Counts || data.counts || {};
        globalVoteHistoryRaces = data.Races || data.races || {};
        globalVoteHistoryRecent = data.Recent || data.recent || [];
    } catch (e) {
        // Non-fatal — badges just won't show
    }
}

function renderVoteHistory() {
    const container = document.getElementById('sidebar-vote-history');
    if (!container) return;

    if (!globalVoteHistoryRecent.length) {
        container.innerHTML = '<div class="ww-empty">No votes recorded yet. Apply Votes to OrePro to start tracking.</div>';
        return;
    }

    // Dedupe by horse+race, most recent first (already sorted by backend).
    container.innerHTML = globalVoteHistoryRecent.map(v => {
        const count = globalVoteHistory[v.HorseId || v.horseId] || 1;
        const name  = v.Name || v.name || v.HorseId || v.horseId || '?';
        const mark  = v.Mark || v.mark || '';
        const raceId = v.RaceId || v.raceId || '';
        // Parse race_id for a short label: YYYYMMDD PP NNNN RR → track code at bytes 8–9
        const raceLabel = raceId.length === 16 ? `${raceId.slice(0,8)} R${parseInt(raceId.slice(14,16),10)}` : raceId;
        const countTag = count > 1 ? `<span class="voted-badge voted-badge-mini">×${count}</span>` : '';
        return `<div class="ww-item ww-voted-item">
            <span class="ww-badge">${mark}</span>
            <span class="ww-name">${escapeHtml(name)}${countTag}</span>
            <span class="ww-meta">${escapeHtml(raceLabel)}</span>
        </div>`;
    }).join('');
}

// ==========================================
// --- LIST MANAGEMENT & UI REFRESH SUITE ---
// ==========================================

async function refreshDataAndUI() {
    // 1. Save scroll position so the screen doesn't jump
    const scrollY = window.scrollY;

    // 2. Refresh the Grid & Weekend Watchlist (must load races FIRST to populate searchableHorses)
    await loadCalendarSkeleton(); // Phase 38: keep calendar day coverage fresh
    bootMark('calendarSkeletonDone');
    await loadRaces();
    bootMark('loadRacesDone');
    
    // 3. Refresh the Sidebar Lists (needs searchableHorses populated)
    const listRes = await fetch('/api/lists');
    listsData = await listRes.json();
    renderLists();
    // loadRaces() called these before listsData was populated, so re-render now.
    renderWeekendWatchlist();
    renderVoteHistory();
    renderEnginePicks();
    updateQuickStats();
    updateRaceHighlighting();
    
    // 4. Restore scroll position seamlessly
    window.scrollTo(0, scrollY);
}

async function refreshListsOnly() {
    // Lightweight refresh that just updates sidebar lists without reloading races
    const listRes = await fetch('/api/lists');
    listsData = await listRes.json();
    renderLists();
    renderWeekendWatchlist();
    renderVoteHistory();
    renderEnginePicks();
    updateQuickStats();

    // Recalculate highlighting and scores based on new listsData
    updateRaceHighlighting();
    
    // Sync all hover buttons across the page to reflect current list membership
    updateAllHoverButtons();
}

function parseListIds(text) {
    /**Extract all horse IDs from a list string (format: "ID # Name")*/
    if (!text || typeof text !== 'string') return new Set();
    const ids = new Set();
    text.split('\n').forEach(line => {
        const clean = line.split('#')[0].trim();
        if (clean && clean.length === 10) ids.add(clean);
    });
    return ids;
}

function getTrackedSets() {
    return {
        bloodlines: parseListIds(listsData?.bloodlines || ""),
        watchlist: parseListIds(listsData?.watchlist || "")
    };
}

function getTrackedStatus(horseId, trackedSets = null) {
    /**Check if a horse is tracked and on which lists. Returns {bld: bool, watch: bool}*/
    const sets = trackedSets || getTrackedSets();
    const cleanId = String(horseId).split('.')[0].trim();
    return {
        bld: sets.bloodlines.has(cleanId),
        watch: sets.watchlist.has(cleanId)
    };
}

function calculateWeightedIntensity(horse, sire, dam, bms) {
    // Keep frontend highlight logic aligned with backend scoring rules.
    let bld_weight = horse.bld ? SCORE_TRACKED_HORSE : (sire.bld ? SCORE_TRACKED_SIRE : 0.0);
    bld_weight += (dam.bld ? SCORE_TRACKED_DAM : 0.0) + (bms.bld ? SCORE_TRACKED_BMS : 0.0);

    let watch_weight = horse.watch ? SCORE_WATCHLIST_HORSE : (sire.watch ? SCORE_WATCHLIST_SIRE : 0.0);
    watch_weight += (dam.watch ? SCORE_WATCHLIST_DAM : 0.0) + (bms.watch ? SCORE_WATCHLIST_BMS : 0.0);

    bld_weight = Math.min(bld_weight, SCORE_MAX);
    watch_weight = Math.min(watch_weight, SCORE_MAX);

    return { bld_weight, watch_weight, max: Math.max(bld_weight, watch_weight) };
}

function calculateFamilyTracking(horse_id, sire_id, dam_id, bms_id, trackedSets = null) {
    /**Calculate which family members are tracked and weighted intensity level. Returns {horse, sire, dam, bms, intensity, isMixed, weights}*/
    const horse = getTrackedStatus(horse_id, trackedSets);
    const sire = getTrackedStatus(sire_id, trackedSets);
    const dam = getTrackedStatus(dam_id, trackedSets);
    const bms = getTrackedStatus(bms_id, trackedSets);
    
    const weights = calculateWeightedIntensity(horse, sire, dam, bms);
    
    // Determine intensity level from weighted value.
    let intensity = 0;
    const maxWeight = weights.max;
    if (maxWeight > 0) {
        if (maxWeight <= 0.25) intensity = 0.25;
        else if (maxWeight <= 0.50) intensity = 0.50;
        else if (maxWeight <= 0.75) intensity = 0.66;
        else intensity = 0.80;
    }
    
    // Check if mixed (both fav and watch)
    const isMixed = (weights.bld_weight > 0 && weights.watch_weight > 0);
    
    return {horse, sire, dam, bms, intensity, isMixed, weights};
}

function updateRaceHighlighting() {
    /**Recalculate race scores and icons based on current listsData*/
    const trackedSets = getTrackedSets();
    
    // Update each race's highlighting and icons
    Object.keys(globalRaceEntries).forEach(r_id => {
        const entries = globalRaceEntries[r_id];
        let hasTracked = false;
        let hasWatchlist = false;
        let hasMixed = false;
        let maxIntensity = 0;
        let maxIntensityStatus = "";
        
        // Recalculate scores for all entries in this race
        entries.forEach(row => {
            // Calculate family tracking with weighted importance (always recalculate)
            row.familyTracking = calculateFamilyTracking(row.Horse_ID, row.Sire_ID, row.Dam_ID, row.BMS_ID, trackedSets);
            const tracking = row.familyTracking;
            const weights = tracking.weights;
            
            // Use the weighted values to determine icon and status
            const b_weight = weights.bld_weight;
            const w_weight = weights.watch_weight;
            
            // Update row data
            let icon = "";
            let score = 0;
            let status = "";
            
            if (b_weight > 0) {
                score = Math.min(b_weight, 1.0);
                status = "BLD";
                icon = b_weight >= ICON_THRESHOLD_3STAR ? "⭐⭐⭐" : (b_weight >= ICON_THRESHOLD_2STAR ? "⭐⭐" : "⭐");
                hasTracked = true;
            } else if (w_weight > 0) {
                score = Math.min(w_weight, 1.0);
                status = "WATCH";
                icon = w_weight >= ICON_THRESHOLD_3STAR ? "👁️👁️" : "👁️";
                hasWatchlist = true;
            }
            
            row.Match = icon;
            row.Score = score;
            row.Status = status;
            
            // Check if this row is mixed
            if (tracking.isMixed) {
                hasMixed = true;
            }
            
            // Track the max intensity in this race for header highlighting
            if (tracking.intensity > maxIntensity) {
                maxIntensity = tracking.intensity;
                maxIntensityStatus = tracking.isMixed ? "MIXED" : status;
            }
        });
        
        // Rebuild the table body with updated scores
        const tbody = document.getElementById(`tbody-${r_id}`);
        if (tbody) {
            tbody.innerHTML = buildTableBody(r_id, entries);
        }
        
        // Update race header highlighting with max intensity found in the race
        const header = document.getElementById(`header-${r_id}`);
        if (header) {
            // Remove all intensity and status classes first
            header.classList.remove('has-bld', 'has-watch', 'row-mixed', 'intensity-light', 'intensity-medium', 'intensity-strong', 'intensity-very-strong');
            
            // Apply appropriate status class - WATCHLIST COLOR TAKES PRIORITY OVER BLOODLINES
            if (hasWatchlist) {
                header.classList.add('has-watch');
            } else if (hasMixed) {
                header.classList.add('row-mixed');
            } else if (hasTracked) {
                header.classList.add('has-bld');
            }
            
            // Apply max intensity class to header
            if (maxIntensity > 0) {
                if (maxIntensity <= 0.25) header.classList.add('intensity-light');
                else if (maxIntensity <= 0.33) header.classList.add('intensity-light');
                else if (maxIntensity <= 0.50) header.classList.add('intensity-medium');
                else if (maxIntensity <= 0.66) header.classList.add('intensity-strong');
                else header.classList.add('intensity-very-strong');
            }
        }
    });
    
    // Update all hover buttons to reflect current list status
    updateAllHoverButtons();
    updateAutoBetHighlighting();
}

function updateAllHoverButtons() {
    /**Update all hover buttons to show Add or Remove based on current lists*/
    const tracked_ids = parseListIds(listsData.bloodlines);
    const watchlist_ids = parseListIds(listsData.watchlist);
    
    document.querySelectorAll('.hover-action-btn').forEach(btn => {
        const horseId = btn.getAttribute('data-horse-id');
        const listType = btn.getAttribute('data-list-type');
        
        if (!horseId || !listType) return;
        
        const isTracked = (listType === 'bloodlines' && tracked_ids.has(horseId)) ||
                         (listType === 'watchlist' && watchlist_ids.has(horseId));
        
        if (isTracked) {
            btn.className = "hover-action-btn remove-btn";
            btn.textContent = "➖ Remove";
            btn.onclick = () => removeHorseFromHover(horseId, listType);
        } else {
            btn.className = "hover-action-btn add-btn";
            btn.textContent = "➕ Add";
            btn.onclick = () => quickAddFromHover(horseId, listType);
        }
    });
}

// --- INITIALIZATION ---
async function init() {
    bootMark('initStart');
    // Over a high-latency link (Tailscale / Cloudflare tunnel), awaiting each startup fetch one-by-one
    // stacks the round-trips serially — a big chunk of the wall-clock load. Fire the independent ones
    // CONCURRENTLY instead. (Was ~7 serial requests; now two small parallel groups.)
    const [marksRes, configRes, dictRes] = await Promise.all([
        fetch('/api/marks'),
        fetch('/api/config'),
        fetch('/static/race_name_dict.json').catch(() => null),
    ]);
    bootMark('coreFetchHeadersDone'); // marks/config/dict responses landed (bodies not yet parsed)

    const marksPayload = normalizeMarksPayload(await marksRes.json());
    globalMarks = marksPayload.marks;
    globalRaceMeta = marksPayload.raceMeta;
    globalMarksVersion = marksPayload.version;

    appConfig = await configRes.json();
    // Migrate jockeyWeight from old default 40 → 20 (A/E shrinkage fix).
    // Only fires if the user never manually changed it away from the old default.
    if (appConfig.ui?.formulaWeights?.jockeyWeight === 40) {
        appConfig.ui.formulaWeights.jockeyWeight = 20;
    }
    applyDevModeBodyClass();
    // Restore UMM theme from localStorage (theme is client-side; no round-trip needed).
    applyUmmTheme(localStorage.getItem(UMM_STORAGE_KEY) === '1');
    // Preload the Uma headshot map so icons are ready when race cards first render.
    if (localStorage.getItem(UMM_STORAGE_KEY) === '1') { await loadUmmIconMap(); }
    relocateSearchBar();

    // Phase 21: race name translation dictionary (fetched above, in parallel).
    try { raceNameDict = dictRes ? await dictRes.json() : { stakes: {}, classNames: {} }; }
    catch (e) { console.warn('Failed to load race_name_dict.json:', e); raceNameDict = { stakes: {}, classNames: {} }; }
    bootMark('coreFetchBodiesDone'); // marks/config/dict JSON parsed, ready to use

    // Vote history (Voted N× badges) + OrePro apply state + settings are independent — load them
    // concurrently. The calendar skeleton is loaded by refreshDataAndUI below, so it's not fetched here.
    await Promise.all([
        loadVoteHistory().catch(() => {}),
        loadOreProApplyState().catch(() => {}),
        loadOreProSettingsLite().catch(() => {}),
    ]);
    bootMark('secondaryFetchDone'); // vote history + OrePro apply state + OrePro settings

    // Restore persisted race-level estimate cache to avoid recomputing every view switch.
    raceBetEstimateCache = loadStoredBetEstimateCache();
    
    // NEW: Save slider state to config periodically
    document.getElementById('risk-slider').addEventListener('change', saveConfigToServer);
    document.getElementById('risk-slider').addEventListener('input', updateAllRiskBadges);
    document.getElementById('risk-slider').addEventListener('input', updateAutoBetHighlighting);
    document.getElementById('risk-slider').addEventListener('input', refreshScoreExplainIfOpen);
    document.getElementById('risk-slider').addEventListener('input', renderEnginePicks);
    document.getElementById('risk-slider').addEventListener('input', updateQuickStats);
    
    // NEW: Load saved slider state from config
    const savedRisk = appConfig.ui?.riskSlider || 50;
    document.getElementById('risk-slider').value = savedRisk;
    updateRiskLabel(savedRisk);
    // Restore Discipline mode (the slider override). Reflects into the toggle + disables the slider.
    updateDisciplineUi();
    
    // NEW: Apply sidebar settings
    applySidebarSettings();
    
    await refreshDataAndUI();
    bootMark('initComplete');
    if (isDevModeEnabled()) printBootBreakdown();
    switchMainView('races');

    if (document.getElementById('jvlink-test-status')) {
        setJvlinkPanelStatus('Ready. Click Status to verify bridge wiring.', false);
        setJvlinkPanelOutput({
            status: 'ready',
            hint: 'Use Status, then Probe Open, then Stream Sample.'
        });
    }
}

// --- HORSE LIST UI LOGIC ---
function renderLists() {
    document.getElementById('list-fav').innerHTML = buildListHTML(listsData.bloodlines, 'bloodlines');
    document.getElementById('list-watch').innerHTML = buildListHTML(listsData.watchlist, 'watchlist');
}

function buildListHTML(rawText, listType) {
    if (!rawText || !rawText.trim()) return "<div style='color:#888; font-size:12px; text-align:center; margin-top:10px;'>No horses tracked yet.</div>";
    
    let html = "";
    const lines = rawText.split('\n');
    lines.forEach(line => {
        const cleanLine = (line || '').trim();
        if (!cleanLine) return;

        const parts = cleanLine.split('#');
        const id = (parts[0] || '').trim();
        if (!id) return;

        // Prefer an upcoming entry over a past one — a horse may appear in both
        // (ran last week AND drawn for this weekend). The chip should reflect
        // "currently entered", not "ran most recently".
        const entries = searchableHorses.filter(h => h.h_id === id);
        const horseData = entries.find(h => h.timeline === 'upcoming') || entries[0];
        const parsedName = parts.length >= 2 ? (parts.slice(1).join('#') || '').trim() : '';
        const name = parsedName || (horseData ? horseData.name : '') || id;

        const escapedName = escapeHtml(name);
        const escapedId = escapeHtml(id);

        // Bloodlines = breeding horses (sires/dams) — they're not expected to
        // appear in race entries, so the upcoming/past/idle timeline styling
        // doesn't apply. Render them as plain chips. Watchlist = active runners,
        // gets the three-tier fade.
        const isBloodlines = listType === 'bloodlines';

        if (horseData) {
            const isPast = !isBloodlines && horseData.timeline === 'past';
            const cls = isPast ? 'horse-item horse-item-past' : 'horse-item';
            const title = isPast ? 'Last race — click to view' : 'Click to view in race';
            html += `
                <div class="${cls}">
                    <span class="horse-item-name" style="cursor: pointer;" onclick="jumpToHorse('${horseData.date}', '${horseData.r_id}', '${horseData.h_id}', '${horseData.timeline || "upcoming"}')" title="${title}">${escapedName}</span>
                    <button class="btn-delete" title="Remove ${escapedName}" onclick="removeHorse('${escapeHtml(listType)}', '${escapedId}')">✖</button>
                </div>`;
        } else if (isBloodlines) {
            // Breeding horse not in any loaded race — expected; plain chip.
            html += `
                <div class="horse-item">
                    <span class="horse-item-name">${escapedName}</span>
                    <button class="btn-delete" title="Remove ${escapedName}" onclick="removeHorse('${escapeHtml(listType)}', '${escapedId}')">✖</button>
                </div>`;
        } else {
            // Watchlist horse not in any loaded race — most faded state.
            html += `
                <div class="horse-item horse-item-idle" title="Not entered in any loaded race">
                    <span class="horse-item-name">${escapedName}</span>
                    <button class="btn-delete" title="Remove ${escapedName}" onclick="removeHorse('${escapeHtml(listType)}', '${escapedId}')">✖</button>
                </div>`;
        }
    });
    return html;
}

function navigateToHorse(horseId) {
    /**Find which race contains this horse and navigate to it*/
    let foundRaceId = null;
    
    // Search through all races to find this horse
    for (const [r_id, entries] of Object.entries(globalRaceEntries)) {
        for (const row of entries) {
            if (String(row.Horse_ID).split('.')[0] === horseId) {
                foundRaceId = r_id;
                break;
            }
        }
        if (foundRaceId) break;
    }
    
    if (!foundRaceId) {
        alert('Horse not found in any race');
        return;
    }
    
    // Get the date from globalRaceInfo for tab switching
    const raceInfo = globalRaceInfo[foundRaceId];
    const foundDate = raceInfo ? raceInfo.clean_date : null;
    
    if (!foundDate) {
        alert('Race information not found');
        return;
    }
    
    // Switch to the correct day in the calendar-backed schedule.
    switchMainTab(foundDate);
    
    // Expand the specific race if it is collapsed then scroll to it
    setTimeout(() => {
        const content = document.getElementById(`content-${foundRaceId}`);
        const header = document.getElementById(`header-${foundRaceId}`);
        const arrow = document.getElementById(`arrow-${foundRaceId}`);
        
        if (content && content.classList.contains('collapsed')) {
            content.classList.remove('collapsed');
            if (header) header.classList.remove('collapsed');
            if (arrow) arrow.innerText = '▼';
        }
        
        // Use anchor link to scroll and ensure visibility
        window.location.hash = `race-${foundRaceId}`;
        const raceHeader = document.getElementById(`header-${foundRaceId}`);
        if (raceHeader) {
            raceHeader.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
    }, 100);
}

// --- ADD / REMOVE LIST ACTIONS ---

async function quickAddFromHover(id, listType, nameEncoded) {
    // Symmetric with removeHorseFromHover: edit listsData locally + POST /api/lists.
    // Persists as "<id>#<name>" so buildListHTML renders names even for breeding
    // horses (sires/dams) that don't appear in searchableHorses.
    const cleanId = String(id || '').trim();
    if (!cleanId) return;
    if (!listsData[listType]) listsData[listType] = '';

    let name = '';
    try { name = decodeURIComponent(String(nameEncoded || '')); } catch (_) { name = String(nameEncoded || ''); }
    name = name.trim();
    const line = name ? `${cleanId}#${name}` : cleanId;

    const existing = listsData[listType].split('\n').map(l => l.trim()).filter(Boolean);
    const matchIdx = existing.findIndex(l => l === cleanId || l.startsWith(cleanId + '#') || l.startsWith(cleanId + ' ') || l.startsWith(cleanId + '\t'));
    if (matchIdx >= 0) {
        // Upgrade an existing bare-ID line to include the name if we have one now.
        if (name && existing[matchIdx] === cleanId) {
            existing[matchIdx] = line;
            listsData[listType] = existing.join('\n') + '\n';
        } else {
            await refreshListsOnly();
            return;
        }
    } else {
        existing.push(line);
        listsData[listType] = existing.join('\n') + '\n';
    }

    try {
        const resp = await fetch('/api/lists', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                bloodlines: listsData.bloodlines,
                watchlist: listsData.watchlist
            })
        });
        if (!resp.ok) {
            const msg = await resp.text().catch(() => '');
            alert(`Add failed: ${msg || resp.status}`);
            return;
        }
    } catch (err) {
        alert(`Add failed: ${err.message || 'network error'}`);
        return;
    }

    await refreshListsOnly();
}

async function removeHorseFromHover(id, listType) {
    const lines = listsData[listType].split('\n');
    const newLines = lines.filter(line => {
        const cleanLine = line.trim();
        return cleanLine !== "" && !cleanLine.startsWith(id);
    });
    
    listsData[listType] = newLines.join('\n') + (newLines.length > 0 ? '\n' : '');
    
    // Save to Python
    await fetch('/api/lists', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            bloodlines: listsData.bloodlines,
            watchlist: listsData.watchlist
        })
    });
    
    // Refresh lists and update highlighting/buttons
    await refreshListsOnly();
}

async function removeHorse(listType, idToRemove) {
    const lines = listsData[listType].split('\n');
    const newLines = lines.filter(line => {
        const cleanLine = line.trim();
        return cleanLine !== "" && !cleanLine.startsWith(idToRemove);
    });
    
    listsData[listType] = newLines.join('\n') + (newLines.length > 0 ? '\n' : '');
    
    // Save to Python
    await fetch('/api/lists', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            bloodlines: listsData.bloodlines,
            watchlist: listsData.watchlist
        })
    });
    
    // Refresh only the sidebar lists (keep scroll position)
    await refreshListsOnly();
}

let listAddDebounceTimer = null;
let currentListAddSelection = -1;

function handleListAddSearch(e) {
    if (listAddDebounceTimer) clearTimeout(listAddDebounceTimer);
    listAddDebounceTimer = setTimeout(performListAddSearch, 150);
}

async function performListAddSearch() {
    const val = document.getElementById('list-add-search').value.toLowerCase().trim();
    const box = document.getElementById('list-add-suggestions');
    currentListAddSelection = -1;

    if (!val) { box.style.display = 'none'; return; }

    // Active tab determines which ID space we're populating:
    //   Bloodlines tab (side-tab-fav active) → breeding_horses (HansyokuNum)
    //   Watchlist tab  (side-tab-watch active) → horses (KettoNum / active runners)
    const watchActive = document.getElementById('side-tab-watch')?.classList.contains('active');
    const searchType = watchActive ? 'racing' : 'breeding';

    const seen = new Set();
    const matches = [];

    // 1) Local matches from race-card entries — only relevant on Watchlist tab
    //    (searchableHorses are active runners, not breeding entries).
    if (watchActive) {
        for (const h of searchableHorses) {
            if (seen.has(h.h_id)) continue;
            if (h.name.toLowerCase().includes(val)) {
                seen.add(h.h_id);
                matches.push({ h_id: h.h_id, name: h.name, badge: '' });
            }
        }
    }

    // Render local hits immediately so the dropdown stays snappy.
    renderListAddSuggestions(matches, box);

    // 2) Backend search filtered to the correct ID space.
    if (val.length < 2) return;
    try {
        const resp = await fetch(`/api/horses/search?q=${encodeURIComponent(val)}&type=${searchType}`);
        if (!resp.ok) return;
        const data = await resp.json();
        const results = (data && data.results) || [];
        for (const r of results) {
            if (!r.id || seen.has(r.id)) continue;
            seen.add(r.id);
            // Badge only for Watchlist results not on a current race card.
            const badge = watchActive && r.source === 'horse' ? 'not racing this week' : '';
            matches.push({ h_id: r.id, name: r.name || r.id, badge });
        }
        renderListAddSuggestions(matches, box);
    } catch (_) { /* ignore — local results already rendered */ }
}

function renderListAddSuggestions(matches, box) {
    if (matches.length === 0) {
        box.innerHTML = '<div class="suggestion-item" style="color:#888;">No matches</div>';
        box.style.display = 'block';
        return;
    }
    let html = '';
    matches.slice(0, 12).forEach((m, idx) => {
        // encodeURIComponent leaves ' unencoded; replace manually so the onclick
        // attribute string isn't broken by names like "I'm Indy".
        const nameEnc = encodeURIComponent(m.name || '').replace(/'/g, '%27');
        const badgeHtml = m.badge
            ? ` <span style="color:#888; font-size:11px;">(${escapeHtml(m.badge)})</span>`
            : '';
        html += `<div class="suggestion-item" id="list-sugg-${idx}"
                      onclick="selectListAddHorse('${m.h_id}', '${nameEnc}')">
            <strong>${escapeHtml(m.name)}</strong>${badgeHtml}
        </div>`;
    });
    box.innerHTML = html;
    box.style.display = 'block';
}

function handleListAddKey(e) {
    const box = document.getElementById('list-add-suggestions');
    if (box.style.display === 'none') return;
    const items = box.querySelectorAll('.suggestion-item');
    if (items.length === 0 || items[0].innerText.includes('No matches')) return;

    if (e.key === 'ArrowDown') {
        e.preventDefault();
        currentListAddSelection = (currentListAddSelection + 1) % items.length;
        updateListAddSelection(items);
    } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        currentListAddSelection = (currentListAddSelection - 1 + items.length) % items.length;
        updateListAddSelection(items);
    } else if (e.key === 'Enter') {
        e.preventDefault();
        const idx = currentListAddSelection > -1 ? currentListAddSelection : 0;
        items[idx].click();
    }
}

function updateListAddSelection(items) {
    items.forEach((item, idx) => {
        if (idx === currentListAddSelection) item.classList.add('active');
        else item.classList.remove('active');
    });
}

async function selectListAddHorse(h_id, nameEnc) {
    document.getElementById('list-add-suggestions').style.display = 'none';
    document.getElementById('list-add-search').value = '';
    currentListAddSelection = -1;
    // Active sidebar tab decides the destination list — #side-tab-watch has
    // the `active` class when Watchlist is open, otherwise Bloodlines.
    const watchActive = document.getElementById('side-tab-watch')?.classList.contains('active');
    const listType = watchActive ? 'watchlist' : 'bloodlines';
    await quickAddFromHover(h_id, listType, nameEnc);
}

// Creates a wrapper with a 500ms delay hover menu
function buildNameWithHover(id, name, listType, trackedStatus, intensity, isMixed) {
    if (!id || id === 'nan' || id === '---') return escapeHtml(name || "");
    const cleanId = String(id).split('.')[0].trim();
    if (!cleanId) return escapeHtml(name || "");
    const displayName = name || `(${cleanId})`;
    
    // Safety check just in case listsData isn't fully loaded yet
    const isTracked = listsData[listType] && listsData[listType].includes(cleanId);
    
    const escapedId = escapeHtml(cleanId);
    const escapedListType = escapeHtml(listType);
    const escapedName = escapeHtml(displayName);

    // Encode the name for inline onclick; also replace ' so apostrophes in
    // horse names (e.g. "I'm Indy") don't break the JS string delimiter.
    const nameEnc = encodeURIComponent(displayName).replace(/'/g, '%27');
    let btnHtml = "";
    if (isTracked) {
        btnHtml = `<button class="hover-action-btn remove-btn" data-horse-id="${cleanId}" data-list-type="${listType}" onclick="removeHorseFromHover('${cleanId}', '${listType}')">➖ Remove</button>`;
    } else {
        btnHtml = `<button class="hover-action-btn add-btn" data-horse-id="${cleanId}" data-list-type="${listType}" onclick="quickAddFromHover('${cleanId}', '${listType}', '${nameEnc}')">➕ Add</button>`;
    }
    
    // 🔍 Profile button → opens the Horse Deep-Dive tab for this horse
    const profileHtml = `<button class="hover-action-btn profile-btn" onclick="viewHorse('${cleanId}')">🔍 Profile</button>`;

    // Generate the link to the English Netkeiba Database!
    const linkHtml = `<a href="https://en.netkeiba.com/db/horse/${escapedId}/" target="_blank" class="hover-link-btn" title="View on Netkeiba DB">🔗 DB</a>`;
    
    // Apply tracking formatting if this horse is tracked
    let nameClass = "name-text";
    if (trackedStatus && (trackedStatus.bld || trackedStatus.watch)) {
        // Determine color based on which list(s) the family member is on
        let colorClass = "";
        if (trackedStatus.bld && trackedStatus.watch) {
            colorClass = "tracked-mixed";
        } else if (trackedStatus.bld) {
            colorClass = "tracked-bld";
        } else { // watch
            colorClass = "tracked-watch";
        }
        
        nameClass = `name-text ${colorClass}`;
    }
    
    // 🎤 Uma Musume headshot — show next to any character horse (sire/dam/BMS or
    // runner) when UMM theme is on. Map is name-keyed; render-time injection
    // covers normal renders, refreshUmmIcons() covers toggle/late-load.
    let ummIcon = "";
    if (document.body.classList.contains('umm-mode')) {
        const iconUrl = ummIconFor(displayName);
        if (iconUrl) ummIcon = ummIconImg(iconUrl);
    }

    return `
    <div class="name-container">
        ${ummIcon}<span class="${nameClass}">${escapedName}</span>
        <div class="hover-menu">
            ${btnHtml}
            ${profileHtml}
            ${linkHtml}
        </div>
    </div>`;
}

// --- TOGGLE RACE VISIBILITY ---
function toggleRace(r_id) {
    const content = document.getElementById(`content-${r_id}`);
    const header = document.getElementById(`header-${r_id}`); // NEW: Grab the header too
    const arrow = document.getElementById(`arrow-${r_id}`);
    
    if (content.classList.contains('collapsed')) {
        content.classList.remove('collapsed');
        if (header) header.classList.remove('collapsed');
        arrow.innerText = '▼';
    } else {
        content.classList.add('collapsed');
        if (header) header.classList.add('collapsed');
        arrow.innerText = '▶';
    }
}

let allCollapsedState = false;
function toggleAllRaces() {
    allCollapsedState = !allCollapsedState;
    document.querySelectorAll('.race-content').forEach(el => {
        allCollapsedState ? el.classList.add('collapsed') : el.classList.remove('collapsed');
    });
    // Toggle the header class so the colors turn on/off
    document.querySelectorAll('.race-header').forEach(el => {
        allCollapsedState ? el.classList.add('collapsed') : el.classList.remove('collapsed');
    });
    document.querySelectorAll('.collapse-arrow').forEach(el => {
        el.innerText = allCollapsedState ? '▶' : '▼';
    });
}

// --- COLLAPSE COMPLETED RACES ---
function collapseVotedRaces() {
    const mainSymbols = ["◎", "〇", "▲", "△"];
    const allRaceIds = Object.keys(globalRaceEntries);
    let firstUnvotedRaceId = null;
    
    allRaceIds.forEach(r_id => {
        let usedCount = 0;
        
        // Tally up how many main votes exist for this specific race
        for (const [k, v] of Object.entries(globalMarks)) {
            if (k.startsWith(`${r_id}_`) && mainSymbols.includes(v)) {
                usedCount++;
            }
        }
        
        // A race counts as "voted" if it carries AT LEAST ONE main mark — the engine now
        // produces partial (2-3 mark) plans, and those are real placed bets, so they should
        // collapse too. Only fully-abstained races (0 marks) stay open as "unvoted".
        if (usedCount >= 1) {
            const content = document.getElementById(`content-${r_id}`);
            const header = document.getElementById(`header-${r_id}`);
            const arrow = document.getElementById(`arrow-${r_id}`);

            if (content && !content.classList.contains('collapsed')) {
                content.classList.add('collapsed');
                if (header) header.classList.add('collapsed');
                if (arrow) arrow.innerText = '▶';
            }
        } else if (!firstUnvotedRaceId) {
            // Track the first unvoted (0-mark) race
            firstUnvotedRaceId = r_id;
        }
    });
    
    // Scroll to the first unvoted race
    if (firstUnvotedRaceId) {
        setTimeout(() => {
            const header = document.getElementById(`header-${firstUnvotedRaceId}`);
            if (header) {
                header.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
        }, 100);
    }
}

// --- SORTING LOGIC ---
function comparePrimitiveValues(a, b, asc = true) {
    if (a === b) return 0;
    if (a === null || a === undefined) return 1;
    if (b === null || b === undefined) return -1;
    if (a < b) return asc ? -1 : 1;
    return asc ? 1 : -1;
}

function parseRaceNumber(value) {
    const parsed = parseFloat(String(value ?? "").trim());
    return Number.isFinite(parsed) ? parsed : null;
}

function normalizeRaceText(value) {
    const text = String(value ?? "").trim().toLowerCase();
    return text || null;
}

function parseRecordSortValue(record) {
    const text = String(record ?? "").trim();
    const parts = text.match(/(\d+)\s*[\/\\-]\s*(\d+)/);
    if (!parts) {
        const nums = text.match(/\d+/g) || [];
        if (nums.length >= 2) {
            const wins = parseInt(nums[0], 10) || 0;
            const starts = parseInt(nums[1], 10) || 0;
            return { wins, starts, rate: starts > 0 ? wins / starts : -1 };
        }
        return { wins: -1, starts: -1, rate: -1 };
    }

    const wins = parseInt(parts[1], 10) || 0;
    const starts = parseInt(parts[2], 10) || 0;
    return { wins, starts, rate: starts > 0 ? wins / starts : -1 };
}

function compareRecordValues(a, b, asc = true) {
    const recordA = parseRecordSortValue(a.Record);
    const recordB = parseRecordSortValue(b.Record);

    if (recordA.rate !== recordB.rate) {
        return asc
            ? comparePrimitiveValues(recordB.rate, recordA.rate, true)
            : comparePrimitiveValues(recordA.rate, recordB.rate, true);
    }

    if (recordA.wins === 0 && recordB.wins === 0 && recordA.starts !== recordB.starts) {
        return asc
            ? comparePrimitiveValues(recordA.starts, recordB.starts, true)
            : comparePrimitiveValues(recordB.starts, recordA.starts, true);
    }

    if (recordA.wins !== recordB.wins) {
        return asc
            ? comparePrimitiveValues(recordB.wins, recordA.wins, true)
            : comparePrimitiveValues(recordA.wins, recordB.wins, true);
    }

    if (recordA.starts !== recordB.starts) {
        return asc
            ? comparePrimitiveValues(recordA.starts, recordB.starts, true)
            : comparePrimitiveValues(recordB.starts, recordA.starts, true);
    }

    return 0;
}

function applySortLogic(r_id, col, asc) {
    const entries = globalRaceEntries[r_id];
    const sMap = {"◎": 1, "〇": 2, "▲": 3, "△": 4, "☆": 5, "消": 6, "X": 99};
    applySortLogic._autoCache = null;

    entries.sort((a, b) => {
        // Our Custom Default (Votes at top, unmarked middle, X at bottom)
        if (col === 'Default') {
            const keyA = `${r_id}_${String(a.Horse_ID).split('.')[0]}`;
            const keyB = `${r_id}_${String(b.Horse_ID).split('.')[0]}`;
            const voteSortingEnabled = isVoteSortingEnabled();

            if (voteSortingEnabled) {
                const valA = sMap[globalMarks[keyA]] || 50;
                const valB = sMap[globalMarks[keyB]] || 50;

                if (valA !== valB) return valA < valB ? -1 : 1;
            }

            // Past races: finish position ascending. Upcoming: post position ascending.
            if (globalRaceInfo[r_id]?._timeline === 'past') {
                const finishCmp = comparePrimitiveValues(parseRaceNumber(a.Finish), parseRaceNumber(b.Finish), true);
                if (finishCmp !== 0) return finishCmp;
            } else {
                const ppCmp = comparePrimitiveValues(parseRaceNumber(a.PP), parseRaceNumber(b.PP), true);
                if (ppCmp !== 0) return ppCmp;
            }
            return a.original_index - b.original_index;
        }

        let comparison = 0;
        if (col === 'Shirushi') {
            const hIdA = String(a.Horse_ID).split('.')[0];
            const hIdB = String(b.Horse_ID).split('.')[0];
            const markA = globalMarks[`${r_id}_${hIdA}`];
            const markB = globalMarks[`${r_id}_${hIdB}`];
            let valA = sMap[markA] || 50;
            let valB = sMap[markB] || 50;
            if (valA === 50 || valB === 50) {
                if (!applySortLogic._autoCache) applySortLogic._autoCache = {};
                const cacheKey = r_id;
                if (!applySortLogic._autoCache[cacheKey]) {
                    const mode = typeof getVotingMarkMode === 'function' ? getVotingMarkMode() : 'BOX_OPTIMIZATION';
                    const auto = mode === 'TRADITIONAL_ROLES'
                        ? getTraditionalRoleAssignments(r_id)
                        : getMarkAwareAutoBetRankingsForRace(r_id);
                    const m = {};
                    auto.forEach(a => { m[a.h_id] = sMap[a.symbol] || 50; });
                    applySortLogic._autoCache[cacheKey] = m;
                }
                const ac = applySortLogic._autoCache[cacheKey];
                if (valA === 50 && ac[hIdA]) valA = ac[hIdA] + 0.5;
                if (valB === 50 && ac[hIdB]) valB = ac[hIdB] + 0.5;
            }
            comparison = comparePrimitiveValues(valA, valB, asc);
            if (comparison === 0) comparison = comparePrimitiveValues(parseRaceNumber(a.Fav), parseRaceNumber(b.Fav), true);
        } else if (col === 'BK') {
            comparison = comparePrimitiveValues(parseRaceNumber(a.BK), parseRaceNumber(b.BK), asc);
        } else if (col === 'PP') {
            comparison = comparePrimitiveValues(parseRaceNumber(a.PP), parseRaceNumber(b.PP), asc);
        } else if (col === 'Horse') {
            comparison = comparePrimitiveValues(normalizeRaceText(a.Horse), normalizeRaceText(b.Horse), asc);
        } else if (col === 'Record') {
            comparison = compareRecordValues(a, b, asc);
        } else if (col === 'Last3') {
            // Sort by recency-weighted form_score (server-computed); higher = better.
            comparison = comparePrimitiveValues(parseFloat(a.Form_Score) || 0, parseFloat(b.Form_Score) || 0, asc);
        } else if (col === 'Sire') {
            comparison = comparePrimitiveValues(normalizeRaceText(a.Sire), normalizeRaceText(b.Sire), asc);
        } else if (col === 'SF') {
            // Phase 9: sire-fit % for THIS race's (surface, bucket). Null entries
            // (below MinSireStarts sample size) sort to the bottom regardless of asc/desc.
            const av = (a.Sire_Fit === null || a.Sire_Fit === undefined) ? null : parseFloat(a.Sire_Fit);
            const bv = (b.Sire_Fit === null || b.Sire_Fit === undefined) ? null : parseFloat(b.Sire_Fit);
            if (av === null && bv === null) comparison = 0;
            else if (av === null) comparison = 1;
            else if (bv === null) comparison = -1;
            else comparison = comparePrimitiveValues(av, bv, asc);
        } else if (col === 'J%' || col === 'T%') {
            // Phase 8: jockey/trainer Win% (rolling 90d / 180d). Null below min-sample
            // sorts to the bottom regardless of direction.
            const field = col === 'J%' ? 'Jockey_Win_Pct' : 'Trainer_Win_Pct';
            const av = (a[field] === null || a[field] === undefined) ? null : parseFloat(a[field]);
            const bv = (b[field] === null || b[field] === undefined) ? null : parseFloat(b[field]);
            if (av === null && bv === null) comparison = 0;
            else if (av === null) comparison = 1;
            else if (bv === null) comparison = -1;
            else comparison = comparePrimitiveValues(av, bv, asc);
        } else if (col === 'Dam') {
            comparison = comparePrimitiveValues(normalizeRaceText(a.Dam), normalizeRaceText(b.Dam), asc);
        } else if (col === 'BMS') {
            comparison = comparePrimitiveValues(normalizeRaceText(a.BMS), normalizeRaceText(b.BMS), asc);
        } else if (col === 'Odds') {
            comparison = comparePrimitiveValues(parseRaceNumber(a.Odds), parseRaceNumber(b.Odds), asc);
        } else if (col === 'Fav') {
            comparison = comparePrimitiveValues(parseRaceNumber(a.Fav), parseRaceNumber(b.Fav), asc);
        } else if (col === 'Finish') {
            comparison = comparePrimitiveValues(parseRaceNumber(a.Finish), parseRaceNumber(b.Finish), asc);
        }

        if (comparison !== 0) return comparison;
        return a.original_index - b.original_index;
    });
}

function getSortIcon(r_id, col) {
    if (!raceSorts[r_id] || raceSorts[r_id].col !== col) return '<span class="sort-icon">↕</span>';
    return raceSorts[r_id].asc ? '<span class="sort-icon" style="color:#ff4b4b;">▲</span>' : '<span class="sort-icon" style="color:#ff4b4b;">▼</span>';
}

function buildTableHeaderRow(r_id) {
    const cols = getVisibleRaceColumns();
    let html = "<tr>";
    // The Shirushi <th>'s width (style.css) governs the whole column regardless of individual cell
    // content — so under Discipline on a settled race, where every cell in that column renders empty
    // (see the Shirushi cellHtmlByCol branch), the header still needs its OWN narrow-state class or
    // the column stays full-width for a column with nothing in it (s60 fix).
    const shirushiEmpty = (() => {
        try { return isDisciplineMode() && raceIsSettledForAutopsy(r_id); } catch (_) { return false; }
    })();

    cols.forEach(col => {
        const meta = RACE_COLUMN_META[col];
        if (!meta) return;
        const emptyCls = (col === 'Shirushi' && shirushiEmpty) ? ' shirushi-empty' : '';

        if (meta.sortable) {
            const sortKey = meta.sortKey;
            html += `<th class="sortable${emptyCls}" data-col="${col}" id="th-${r_id}-${sortKey}" onclick="setSort('${r_id}', '${sortKey}')">${meta.label} ${getSortIcon(r_id, sortKey)}</th>`;
        } else {
            html += `<th data-col="${col}"${emptyCls ? ` class="${emptyCls.trim()}"` : ''}>${meta.label}</th>`;
        }
    });

    html += "</tr>";
    return html;
}

function refreshRaceHeaderSortLabels(r_id) {
    const sortableCols = Object.entries(RACE_COLUMN_META).filter(([, col]) => col.sortable);

    sortableCols.forEach(([key, col]) => {
        const el = document.getElementById(`th-${r_id}-${key}`);
        if (el) el.innerHTML = `${col.label} ${getSortIcon(r_id, key)}`;
    });
}

function setSort(r_id, col) {
    const meta = RACE_COLUMN_META[col];
    const initialAsc = meta?.initialAsc ?? true;

    // Direction is computed against the global state so every race toggles in lockstep.
    const newAsc = (globalSort.col === col) ? !globalSort.asc : initialAsc;
    globalSort = { col, asc: newAsc };

    // Propagate to every loaded race card in one pass.
    Object.keys(globalRaceEntries).forEach(rid => {
        raceSorts[rid] = { col, asc: newAsc };
        applySortLogic(rid, col, newAsc);
        const tbody = document.getElementById(`tbody-${rid}`);
        if (tbody) tbody.innerHTML = buildTableBody(rid, globalRaceEntries[rid]);
        refreshRaceHeaderSortLabels(rid);
    });
    updateAutoBetHighlighting();
}

// SE 性別コード → small sex symbol shown next to the horse name. 1=♂ colt (blue),
// 2=♀ filly/mare (pink), 3=⚥ gelding (gray). 0/null/unknown → nothing. We have sex, not
// age, so the hover says "Mare / Filly" rather than guessing. Mirrors the TV-mode sign.
function sexSign(sex) {
    const map = { 1: ['♂', '#4a9eff', 'Colt'], 2: ['♀', '#ff6b9d', 'Mare / Filly'], 3: ['⚥', '#9aa0a6', 'Gelding'] };
    const s = map[parseInt(sex, 10)];
    return s ? `<span class="horse-sex" style="color:${s[1]};font-weight:bold;margin-left:4px;" title="${s[2]}">${s[0]}</span>` : '';
}

// Generates the inner rows (Pulled out of loadRaces to be reusable)
function buildTableBody(r_id, entries) {
    let rowsHtml = "";
    // Engine-disagreement annotation: ONLY on races YOU hand-marked (raceHasUserMarks) so an
    // auto-picked race never shows "engine disagrees with itself". Compares your marks against the
    // engine's mark-blind top-4 power ranking (◎〇▲△); a small ⚙ chip flags each row that differs.
    let showEngineDiff = false;
    const engineMarkByHorse = {};
    const discMode = isDisciplineMode();
    const discSettled = (() => { try { return raceIsSettledForAutopsy(r_id); } catch (_) { return false; } })();
    try {
        // The engine's mark-blind top-4 ranking feeds the ⚙ disagreement chip (manual, hand-marked races)
        // and the Discipline "engine read" column — both PRE-race only. A settled race shows the actual
        // FINISH in that column instead (cheap, review-useful) — and its pre-race handicapping data is
        // intentionally trimmed from the payload, so an engine recompute here would be wrong anyway.
        const wantManualDiff = (appConfig.ui?.showEngineDisagreement ?? true) && raceHasUserMarks(r_id) && !discSettled;
        const wantDisciplineRead = discMode && !discSettled;
        if (wantManualDiff || wantDisciplineRead) {
            getUnconditionalAutoBetRankingsForRace(r_id).forEach(p => { engineMarkByHorse[p.h_id] = p.symbol; });
            showEngineDiff = wantManualDiff;
        }
    } catch (_) { showEngineDiff = false; }
    // s60 fix — a real live discrepancy: once a Discipline bet is LOCKED (frozen at Apply), the grid
    // must show the horse that was ACTUALLY bet, not a live re-recompute of the engine's CURRENT pick.
    // The market can move after a bet locks in (◎ favorite-drift, tuning_hypotheses.md H16 — ~26% of
    // bets drift), so engineMarkByHorse (recomputed fresh on every render) can silently point at a
    // DIFFERENT horse than the one frozen in betProfile.betLines. Found live: Kokura R1 showed PP10
    // (the current/live favorite) as the ◎ pick; PP10 WON; the grid still (correctly) scored a loss,
    // because the actual frozen bet was on PP15, which ran 3rd — the display and the money had quietly
    // disagreed the whole time. When a frozen single-horse place bet exists, it now overrides the live
    // read entirely (leans suppressed too — showing a "live lean" next to an already-placed real bet is
    // the same kind of misleading mix that caused this).
    let frozenDisciplinePp = null;
    if (discMode && !discSettled) {
        try {
            const frozenLines = getRaceBetProfile(r_id)?.betLines;
            if (Array.isArray(frozenLines) && frozenLines.length === 1 && frozenLines[0]?.horses?.length === 1) {
                frozenDisciplinePp = frozenLines[0].horses[0].pp;
            }
        } catch (_) { frozenDisciplinePp = null; }
    }
    entries.forEach(row => {
        const h_id = String(row.Horse_ID).split('.')[0];
        const key = `${r_id}_${h_id}`;
        
        // Ensure tracking data exists; calculate if missing
        if (!row.familyTracking) {
            row.familyTracking = calculateFamilyTracking(row.Horse_ID, row.Sire_ID, row.Dam_ID, row.BMS_ID);
        }
        const tracking = row.familyTracking;
        const weights = tracking?.weights || { bld_weight: 0, watch_weight: 0 };
        
        // Determine base status class: mixed takes priority, then FAV/WATCH
        let rowStatusClass = "";
        if (tracking.isMixed) {
            rowStatusClass = "row-mixed";
        } else if (weights.bld_weight > 0) {
            rowStatusClass = "row-bld";
        } else if (weights.watch_weight > 0) {
            rowStatusClass = "row-watch";
        }
        
        // Determine intensity class for the row
        let intensityClass = "";
        if (tracking && tracking.intensity > 0) {
            if (tracking.intensity <= 0.33) intensityClass = "intensity-light";
            else if (tracking.intensity <= 0.50) intensityClass = "intensity-medium";
            else if (tracking.intensity <= 0.66) intensityClass = "intensity-strong";
            else intensityClass = "intensity-very-strong";
        }
        
        // Build final class string
        let finalClasses = [];
        if (rowStatusClass) finalClasses.push(rowStatusClass);
        if (intensityClass) finalClasses.push(intensityClass);
        const trClass = finalClasses.join(" ");
        
        const horseStr = buildNameWithHover(row.Horse_ID, row.Horse, 'watchlist', tracking.horse, tracking.intensity, tracking.isMixed);
        const sireStr = buildNameWithHover(row.Sire_ID, row.Sire, 'bloodlines', tracking.sire, tracking.intensity, tracking.isMixed);
        const damStr = buildNameWithHover(row.Dam_ID, row.Dam, 'bloodlines', tracking.dam, tracking.intensity, tracking.isMixed);
        const bmsStr = buildNameWithHover(row.BMS_ID, row.BMS, 'bloodlines', tracking.bms, tracking.intensity, tracking.isMixed);

        const fallbackSources = (row && typeof row._fallbackSources === 'object' && row._fallbackSources)
            ? row._fallbackSources
            : {};
        const fallbackHighlightEnabled = false;
        const fallbackCellAttrs = (field) => {
            const source = String(fallbackSources[field] || '').trim();
            if (!fallbackHighlightEnabled || !source) return '';
            return ` class="fallback-field-cell" title="Fallback source: ${escapeHtml(source)}"`;
        };
        // Show a dash for missing/zero values (posts/brackets blank until Fri JST,
        // odds/fav blank until publish). Treats null, undefined, '', '0', and 0
        // as missing — JV-Link zero-pads unset numeric fields.
        const dispNum = (v) => {
            if (v === null || v === undefined) return '—';
            const s = String(v).trim();
            if (s === '' || s === '0') return '—';
            const n = parseFloat(s);
            if (Number.isFinite(n) && n === 0) return '—';
            return s;
        };
        
        // NEW: Added id="row-${r_id}-${h_id}" to the <tr>
        const cellHtmlByCol = {
            Shirushi: (() => {
                // Discipline mode: marks are analysis-only and live in the ⓘ popover, so the grid's first
                // column becomes a read-only ENGINE READ — the cold engine's ◎ (the actual ¥10k place
                // target) badged, and its 〇▲ leans as quiet notes. No buttons here = nothing on the grid
                // can place a bet (the safety valve).
                //
                // s60 fix: a LOCKED race shows the FROZEN bet's actual horse (frozenDisciplinePp, computed
                // above), not a live re-recompute of the engine's current ranking — those can disagree
                // once the market drifts after a bet locks in (H16). Only an UNLOCKED race (nothing frozen
                // yet — this IS still a live preview of what Apply would bet right now) falls back to
                // engineMarkByHorse, and only then do the 〇▲△ "leans" make sense to show at all — once a
                // real bet is frozen, a live-recomputed lean sitting next to it is the same kind of
                // misleading mix that caused the original bug, so it's suppressed too.
                if (discMode) {
                    // Settled race: nothing to show here — the bet/engine read is pre-race, and the actual
                    // finish already has its own column. Leave the first column empty on review. Distinct
                    // "shirushi-empty" class (not just shirushi-discipline, which the pill-bearing pre-race
                    // state below also uses) so CSS can collapse ONLY the truly-empty cell's width — it was
                    // reserving the same 170px as when it held mark buttons/the engine-read pill (s60 fix).
                    if (discSettled) return `<td class="shirushi-cell shirushi-discipline shirushi-empty"></td>`;
                    let eng;
                    if (frozenDisciplinePp !== null) {
                        const pp = parseInt(row?.PP, 10);
                        eng = (Number.isFinite(pp) && pp === frozenDisciplinePp) ? '◎' : '';
                    } else {
                        eng = engineMarkByHorse[h_id] || '';
                    }
                    let inner = '';
                    if (eng === '◎') {
                        const yk = DISCIPLINE_PLACE_STAKE / 1000;
                        const betNote = frozenDisciplinePp !== null ? ' — the bet actually placed, locked at Apply' : ' (not placed yet — applies at the next Apply Day Votes)';
                        inner = `<span title="The disciplined bet${betNote}. A flat ¥${DISCIPLINE_PLACE_STAKE.toLocaleString()} place (複勝). Cashes if it finishes in the top 3." `
                            + `style="display:inline-flex;align-items:center;gap:4px;font-size:0.78em;font-weight:700;padding:2px 8px;border-radius:8px;`
                            + `background:#14361f;color:#b9f0c9;border:1px solid #2f8f57;white-space:nowrap;">◎ place ¥${yk}k</span>`;
                    } else if (eng) {
                        const rankTxt = { '〇': '2nd', '▲': '3rd', '△': '4th' }[eng] || '';
                        inner = `<span title="The cold engine's ${rankTxt || 'next'} lean — analysis only, bets nothing." `
                            + `style="display:inline-flex;align-items:center;gap:3px;font-size:0.72em;font-weight:600;padding:1px 6px;border-radius:6px;`
                            + `color:#9fb0c0;border:1px solid #3a4654;white-space:nowrap;">${eng}<span style="opacity:0.7;">${rankTxt}</span></span>`;
                    }
                    return `<td class="shirushi-cell shirushi-discipline">${inner}</td>`;
                }
                const btns = `${createMarkBtn(r_id, h_id, '◎', key)}
                ${createMarkBtn(r_id, h_id, '〇', key)}
                ${createMarkBtn(r_id, h_id, '▲', key)}
                ${createMarkBtn(r_id, h_id, '△', key)}
                ${createMarkBtn(r_id, h_id, 'X', key)}`;
                let diffChip = '';
                if (showEngineDiff && globalMarks[key] !== 'X') {
                    const you = globalMarks[key] || '';
                    const eng = engineMarkByHorse[h_id] || '';
                    if (you !== eng && (you || eng)) {
                        const engTxt = eng || '–';
                        const title = (you && eng)
                            ? `You marked ${you} here; the engine would mark this horse ${eng}.`
                            : (eng
                                ? `The engine would mark this horse ${eng} — you haven't marked it.`
                                : `You marked ${you} here; the engine wouldn't mark this horse.`);
                        diffChip = ` <span class="engine-diff-chip" title="${escapeHtml(title)}" `
                            + `style="display:inline-block;font-size:0.7em;font-weight:700;margin-left:4px;padding:0 5px;border-radius:8px;`
                            + `background:#3a3320;color:#ffe9b0;border:1px solid #8a7430;vertical-align:middle;white-space:nowrap;">⚙ ${escapeHtml(engTxt)}</span>`;
                    }
                }
                return `<td class="shirushi-cell">${btns}${diffChip}</td>`;
            })(),
            BK: (() => {
                const bkNum = parseInt(row.BK, 10);
                const bkCls = (Number.isFinite(bkNum) && bkNum >= 1 && bkNum <= 8) ? `bk-color-${bkNum}` : '';
                const fb = fallbackCellAttrs('BK');
                const txt = dispNum(row.BK);
                if (fb && bkCls) {
                    return `<td${fb.replace('class="', `class="${bkCls} `)}>${txt}</td>`;
                }
                const cls = bkCls ? ` class="${bkCls}"` : '';
                return `<td${cls}${fb}>${txt}</td>`;
            })(),
            PP: (() => {
                const bkNum = parseInt(row.BK, 10);
                const bkCls = (Number.isFinite(bkNum) && bkNum >= 1 && bkNum <= 8) ? `bk-color-${bkNum}` : '';
                const fb = fallbackCellAttrs('PP');
                const txt = dispNum(row.PP);
                if (fb && bkCls) {
                    return `<td${fb.replace('class="', `class="${bkCls} `)}>${txt}</td>`;
                }
                const cls = bkCls ? ` class="${bkCls}"` : '';
                return `<td${cls}${fb}>${txt}</td>`;
            })(),
            Horse: (() => {
                const voteCount = votedCountExcludingRace(h_id, r_id);
                const votedBadge = voteCount > 0 ? `<span class="voted-badge" title="You backed this horse in ${voteCount} earlier race${voteCount !== 1 ? 's' : ''} this season (not counting this one)">Voted ${voteCount}×</span>` : '';
                const coldPill = coldValuePillForRow(r_id, row);
                // Predicted run-style is a PRE-race read → only on unfinished rows (a settled row's
                // "last start" would be a stale prior race; today's actual run is the autopsy's job).
                const stylePill = String(row.Finish || '').trim() ? '' : predictedStylePill(r_id, row);
                // Name + ⓘ stay on the top line; any chips (style, cold-value, voted) drop to a line underneath.
                const chips = `${stylePill}${coldPill}${votedBadge}`.trim();
                const chipsHtml = chips ? `<div class="horse-chips">${chips}</div>` : '';
                return `<td class="horse-cell" style="font-weight: bold;"><span class="horse-name-line">${horseStr}${sexSign(row.Sex)} <button class="score-explain-trigger" title="Explain auto-pick score" onclick="openScoreExplain(event, '${r_id}', '${h_id}')">ⓘ</button></span>${chipsHtml}</td>`;
            })(),
            Record: `<td>${row.Record || ""}</td>`,
            Last3: (() => {
                const raw = String(row.Last3 || "—-—-—");
                const parts = raw.split('-');
                const cells = parts.map(p => {
                    const m = p.match(/^(\d+)([①-⑱])?$/);
                    const n = m ? parseInt(m[1], 10) : NaN;
                    const favRank = m?.[2] ? (m[2].codePointAt(0) - 0x245F) : null;
                    let cls = 'last3-cell';
                    if (n === 1) cls += ' last3-win';
                    else if (n === 2 || n === 3) cls += ' last3-place';
                    else if (Number.isFinite(n) && n >= 4 && n <= 5) cls += ' last3-show';
                    else if (Number.isFinite(n)) cls += ' last3-out';
                    else cls += ' last3-none';
                    const rankHtml = favRank !== null ? `<span class="last3-rank">${favRank}</span>` : '';
                    return `<span class="${cls}">${Number.isFinite(n) ? m[1] : p}${rankHtml}</span>`;
                }).join('');
                return `<td class="last3-strip" title="Form score: ${(parseFloat(row.Form_Score) || 0).toFixed(3)}">${cells}</td>`;
            })(),
            "J%": (() => {
                const wp = (row.Jockey_Win_Pct === null || row.Jockey_Win_Pct === undefined) ? null : parseFloat(row.Jockey_Win_Pct);
                const ae = parseFloat(row.Jockey_AE);
                const name = row.Jockey || row.Jockey_Code || '';
                const starts = row.Jockey_Starts;
                if (wp === null || !Number.isFinite(wp)) {
                    const tip = starts ? `${name} (${starts} starts in 90d — below 30 min)` : `${name} (no recent rides)`;
                    return `<td class="jt-rate jt-rate-none" title="${escapeHtml(tip)}">—</td>`;
                }
                const pct = wp * 100;
                let cls = 'jt-rate';
                if (pct >= 18) cls += ' jt-rate-strong';
                else if (pct >= 11) cls += ' jt-rate-mid';
                else cls += ' jt-rate-weak';
                const aeStr = Number.isFinite(ae) ? ` · A/E ${ae.toFixed(2)}` : '';
                const tip = `${name} — 90d Win% ${pct.toFixed(0)}%${aeStr} (${starts || 0} starts)`;
                return `<td class="${cls}" title="${escapeHtml(tip)}">${pct.toFixed(0)}%</td>`;
            })(),
            "T%": (() => {
                const wp = (row.Trainer_Win_Pct === null || row.Trainer_Win_Pct === undefined) ? null : parseFloat(row.Trainer_Win_Pct);
                const ae = parseFloat(row.Trainer_AE);
                const name = row.Trainer || row.Trainer_Code || '';
                const starts = row.Trainer_Starts;
                if (wp === null || !Number.isFinite(wp)) {
                    const tip = starts ? `${name} (${starts} starts in 180d — below 20 min)` : `${name} (no recent runners)`;
                    return `<td class="jt-rate jt-rate-none" title="${escapeHtml(tip)}">—</td>`;
                }
                const pct = wp * 100;
                let cls = 'jt-rate';
                if (pct >= 15) cls += ' jt-rate-strong';
                else if (pct >= 9) cls += ' jt-rate-mid';
                else cls += ' jt-rate-weak';
                const aeStr = Number.isFinite(ae) ? ` · A/E ${ae.toFixed(2)}` : '';
                const tip = `${name} — 180d Win% ${pct.toFixed(0)}%${aeStr} (${starts || 0} starts)`;
                return `<td class="${cls}" title="${escapeHtml(tip)}">${pct.toFixed(0)}%</td>`;
            })(),
            Sire: `<td>${sireStr}</td>`,
            SF: (() => {
                const sf = (row.Sire_Fit === null || row.Sire_Fit === undefined) ? null : parseFloat(row.Sire_Fit);
                if (sf === null || !Number.isFinite(sf)) return `<td class="sire-fit sire-fit-none" title="No sample (need ≥10 progeny starts in this surface/distance bucket)">—</td>`;
                let cls = 'sire-fit';
                if (sf >= 15) cls += ' sire-fit-strong';
                else if (sf >= 9) cls += ' sire-fit-mid';
                else cls += ' sire-fit-weak';
                const pf = (row.Sire_Place_Fit === null || row.Sire_Place_Fit === undefined) ? null : parseFloat(row.Sire_Place_Fit);
                const pfLabel = pf !== null && Number.isFinite(pf) ? ` / ${pf.toFixed(0)}% place` : '';
                const nLabel = row.Sire_Starts ? ` (n=${row.Sire_Starts})` : '';
                const tip = `Sire win%: ${sf.toFixed(1)}%${pfLabel}${nLabel}`;
                return `<td class="${cls}" title="${tip}">${sf.toFixed(0)}%</td>`;
            })(),
            Dam: `<td>${damStr}</td>`,
            BMS: `<td>${bmsStr}</td>`,
            Odds: (() => {
                const finishN = Number(row.Finish);
                const favN = Number(row.Fav);
                let upsetCls = '';
                if (Number.isFinite(finishN) && finishN > 0 && Number.isFinite(favN) && favN > 0) {
                    const delta = finishN - favN;  // negative = beat market (upset by winning above rank); positive = underperformed
                    if (delta <= -5) upsetCls = 'upset-strong';
                    else if (delta <= -2) upsetCls = 'upset-mild';
                    else if (delta >= 5) upsetCls = 'chalk-fail-strong';
                    else if (delta >= 2) upsetCls = 'chalk-fail-mild';
                }
                const fb = fallbackCellAttrs('Odds');
                let openTag;
                if (fb && upsetCls) {
                    openTag = `<td data-cell="odds"${fb.replace('class="', `class="${upsetCls} `)}`;
                } else if (upsetCls) {
                    openTag = `<td data-cell="odds" class="${upsetCls}"`;
                } else {
                    openTag = `<td data-cell="odds"${fb}`;
                }
                const tip = upsetCls ? ` title="Finish ${finishN} vs market rank ${favN} (Δ${(finishN - favN > 0 ? '+' : '')}${finishN - favN})"` : '';
                const curOdds = parseFloat(row.Odds), prevOdds = parseFloat(row.Prev_Odds);
                let oddsDelta = '';
                if (!isNaN(prevOdds) && prevOdds > 0 && !isNaN(curOdds) && curOdds > 0 && Math.abs(curOdds - prevOdds) >= 0.2) {
                    // Arrow direction matches the trend graph: up = good (odds shortening),
                    // down = bad (drifting). Colour class carries the meaning; glyph follows the graph.
                    oddsDelta = curOdds < prevOdds
                        ? `<span class="odds-short" title="Shortened from ${prevOdds.toFixed(1)}">↑</span>`
                        : `<span class="odds-drift" title="Drifted from ${prevOdds.toFixed(1)}">↓</span>`;
                }
                // Phase 37: clickable odds → single-horse trend graph (upcoming/live only).
                const ohClickable = (globalRaceInfo[r_id]?._timeline !== 'past');
                const ohAttr = ohClickable
                    ? ` onclick="event.stopPropagation(); showOddsHistory('${r_id}','${h_id}')" style="cursor:pointer" title="Click for odds trend"`
                    : '';
                return `${openTag}${tip}${ohAttr}>${dispNum(row.Odds)}${oddsDelta}</td>`;
            })(),
            Fav: `<td data-cell="fav"${fallbackCellAttrs('Fav')}>${dispNum(row.Fav)}</td>`,
            Finish: (() => { const f = Number(row.Finish); const shown = (Number.isFinite(f) && f > 0) ? f : ''; return `<td data-cell="finish" class="finish-pos finish-pos-${shown}">${shown}</td>`; })()
        };

        const orderedCells = getVisibleRaceColumns().map(col => {
            const cellHtml = cellHtmlByCol[col] || "";
            if (!cellHtml) return "";
            // Inject data-col on the opening <td> so the mobile CSS can hide
            // pedigree/form columns by key without depending on column order.
            return cellHtml.replace(/^<td/, `<td data-col="${col}"`);
        }).join("");
        const finishNum = Number(row.Finish);
        const finishAttr = (Number.isFinite(finishNum) && finishNum >= 1 && finishNum <= 3) ? ` data-finish="${finishNum}"` : '';
        rowsHtml += `<tr id="row-${r_id}-${h_id}" class="${trClass}"${finishAttr}>${orderedCells}</tr>`;
    });
    return rowsHtml;
}


// --- STRATEGY SLIDER LOGIC ---
function hexToRgbTuple(hex) {
    if (!hex || typeof hex !== 'string') return null;
    const normalized = hex.replace('#', '').trim();
    if (!/^[0-9a-fA-F]{6}$/.test(normalized)) return null;
    return {
        r: parseInt(normalized.slice(0, 2), 16),
        g: parseInt(normalized.slice(2, 4), 16),
        b: parseInt(normalized.slice(4, 6), 16)
    };
}

function syncAutoBetPreviewColor(riskVal) {
    const colorHex = getRiskColor(Number(riskVal));
    const rgb = hexToRgbTuple(colorHex);
    if (!rgb) return;
    document.documentElement.style.setProperty('--auto-bet-preview-rgb', `${rgb.r}, ${rgb.g}, ${rgb.b}`);
}

function updateRiskLabel(val) {
    const label = document.getElementById('risk-label');
    const slider = document.getElementById('risk-slider');
    let text = "Balanced";
    let color = "#ff9f43"; // Orange

    if (val <= 20) { text = "Ultra Safe"; color = "#0abde3"; }
    else if (val <= 40) { text = "Chalky"; color = "#1dd1a1"; }
    else if (val <= 60) { text = "Balanced"; color = "#ff9f43"; }
    else if (val <= 85) { text = "Value Hunter"; color = "#ff4b4b"; }
    else { text = "Maximum Chaos"; color = "#ff0000"; }

    label.innerText = `${text} (${val})`;
    label.style.color = color;
    if (slider) slider.style.color = color;
    syncAutoBetPreviewColor(val);
    updatePreOddsNotice();
}

// ── Discipline-mode toggle (the slider override) ─────────────────────────────
// Reflects state into the slider (disabled) + the MODE label, and refreshes everything a slider
// move would (badges, highlight overlay, engine picks, quick stats), then persists to config.
function updateDisciplineUi() {
    const on = isDisciplineMode();
    const toggle = document.getElementById('discipline-toggle');
    const slider = document.getElementById('risk-slider');
    const label = document.getElementById('risk-label');
    if (toggle) toggle.checked = on;
    if (slider) { slider.disabled = on; slider.style.opacity = on ? '0.4' : ''; }
    if (on) {
        if (label) { label.innerText = '🧊 Discipline (trust market · place-biased)'; label.style.color = '#0abde3'; }
        try { syncAutoBetPreviewColor(DISCIPLINE_RISK); } catch (_) {}
        try { updatePreOddsNotice(); } catch (_) {}
    } else {
        updateRiskLabel(slider ? slider.value : 50);
    }
    // The day-preset selector + line editor only drive MANUAL bets — they're moot under Discipline
    // (the bet is a fixed ¥10k place on the engine ◎). Hide them so the Bets tab can't mislead.
    try {
        document.querySelectorAll('.toolbar-betsel').forEach(el => { el.style.display = on ? 'none' : ''; });
        const desc = document.getElementById('day-bet-structure-desc');
        if (desc && on) desc.style.display = 'none';
    } catch (_) {}
    try { renderBetsDashMode(); } catch (_) {}
}

// The Bets-tab sidebar "mode" tile: what betting mode is active + the one-line bet readout.
function renderBetsDashMode() {
    const el = document.getElementById('voting-dash-mode');
    if (!el) return;
    const on = isDisciplineMode();
    const yk = DISCIPLINE_PLACE_STAKE / 1000;
    const name = on ? '🧊 Discipline' : '🎚 Manual';
    const sub  = on ? `¥${yk}k place on the engine ◎` : 'marks drive bets · risk slider';
    el.innerHTML = `<div class="dash-mode-label">Mode</div>`
        + `<div class="dash-mode-name" style="color:${on ? '#0abde3' : '#f3f6fb'};">${name}</div>`
        + `<div class="dash-mode-sub">${sub}</div>`;
}
function toggleDisciplineMode(on) {
    if (typeof appConfig !== 'object' || !appConfig) return;
    appConfig.ui = appConfig.ui || {};
    appConfig.ui.disciplineMode = !!on;
    updateDisciplineUi();
    try { saveConfigToServer(); } catch (_) {}
    // Unlike a risk-slider move, this flips the Shirushi column's HTML itself (engine-read badges vs.
    // clickable mark buttons) — rebuild every visible race's rows, or the grid keeps showing the other
    // mode's markup until something else happens to force a re-render.
    try {
        Object.keys(globalRaceEntries).forEach(rid => {
            const tbody = document.getElementById(`tbody-${rid}`);
            if (tbody) tbody.innerHTML = buildTableBody(rid, globalRaceEntries[rid]);
        });
    } catch (_) {}
    // Re-run the same refreshers the slider's 'input' listeners fire, so picks/preview/stats update live.
    try { updateAllRiskBadges(); } catch (_) {}
    try { updateAutoBetHighlighting(); } catch (_) {}
    try { refreshScoreExplainIfOpen(); } catch (_) {}
    try { refreshRaceAutopsyIfOpen(); } catch (_) {}
    try { renderEnginePicks(); } catch (_) {}
    try { updateQuickStats(); } catch (_) {}
}

function updatePreOddsNotice() {
    const el = document.getElementById('pre-odds-notice');
    if (!el) return;
    const hasAnyOdds = Object.keys(globalRaceEntries).some(r_id => {
        const info = globalRaceInfo[r_id];
        if (!info || info._timeline !== 'upcoming') return false;
        return (globalRaceEntries[r_id] || []).some(row => {
            const o = parseFloat(row.Odds);
            return Number.isFinite(o) && o > 0;
        });
    });
    if (hasAnyOdds) { el.style.display = 'none'; return; }
    const risk = getCurrentAutoPickRisk();
    const zone = risk <= ENGINE_TUNING.SAFE_MAX ? 'SAFE' : risk >= ENGINE_TUNING.CHAOS_MIN ? 'CHAOS' : 'BLEND';
    el.style.display = 'block';
    el.textContent = zone === 'CHAOS'
        ? '⚡ Pre-odds · Showing form picks (CHAOS only)'
        : '⚡ Pre-odds · SAFE/BLEND picks need market prices';
}

function getRiskColor(val) {
    if (val <= 20) return "#0abde3";
    if (val <= 40) return "#1dd1a1";
    if (val <= 60) return "#ff9f43";
    if (val <= 85) return "#ff4b4b";
    return "#ff0000";
}

function getRiskLabel(val) {
    if (val <= 20) return "Ultra Safe";
    if (val <= 40) return "Chalky";
    if (val <= 60) return "Balanced";
    if (val <= 85) return "Value Hunter";
    return "Max Chaos";
}

function normalizeMarksPayload(payload) {
    const normalized = {
        version: 2,
        marks: {},
        raceMeta: {}
    };

    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
        return normalized;
    }

    const isVersioned = Object.prototype.hasOwnProperty.call(payload, 'marks')
        || Object.prototype.hasOwnProperty.call(payload, 'raceMeta')
        || Object.prototype.hasOwnProperty.call(payload, 'version');

    const rawMarks = isVersioned ? payload.marks : payload;
    if (rawMarks && typeof rawMarks === 'object' && !Array.isArray(rawMarks)) {
        Object.entries(rawMarks).forEach(([key, value]) => {
            const cleanKey = String(key || '').trim();
            const cleanValue = typeof value === 'string' ? value.trim() : '';
            if (cleanKey && cleanValue) {
                normalized.marks[cleanKey] = cleanValue;
            }
        });
    }

    const rawRaceMeta = isVersioned ? payload.raceMeta : null;
    if (rawRaceMeta && typeof rawRaceMeta === 'object' && !Array.isArray(rawRaceMeta)) {
        Object.entries(rawRaceMeta).forEach(([raceId, meta]) => {
            if (!raceId || !meta || typeof meta !== 'object' || Array.isArray(meta)) return;

            const strategySnapshot = meta.strategySnapshot && typeof meta.strategySnapshot === 'object' && !Array.isArray(meta.strategySnapshot)
                ? meta.strategySnapshot
                : {};

            normalized.raceMeta[raceId] = {
                savedAt: meta.savedAt || null,
                updatedAt: meta.updatedAt || null,
                markSource: meta.markSource || null,
                strategySnapshot: {
                    riskSlider: Number.isFinite(Number(strategySnapshot.riskSlider)) ? Number(strategySnapshot.riskSlider) : null,
                    riskLabel: strategySnapshot.riskLabel || null,
                    formulaWeights: strategySnapshot.formulaWeights && typeof strategySnapshot.formulaWeights === 'object' && !Array.isArray(strategySnapshot.formulaWeights)
                        ? strategySnapshot.formulaWeights
                        : {},
                    engineShape: typeof strategySnapshot.engineShape === 'string' ? strategySnapshot.engineShape : null,
                    engineCount: Number.isFinite(Number(strategySnapshot.engineCount)) ? Number(strategySnapshot.engineCount) : null
                },
                manualAdjustments: Number.isFinite(Number(meta.manualAdjustments)) ? Number(meta.manualAdjustments) : 0,
                lockStateAtSave: typeof meta.lockStateAtSave === 'boolean' ? meta.lockStateAtSave : null,
                activeSymbols: Array.isArray(meta.activeSymbols)
                    ? meta.activeSymbols.map(symbol => String(symbol || '').trim()).filter(Boolean)
                    : [],
                // Per-race side-bet horse ids (Watchlist ＋ chip / Apply-time popup). Missing from
                // this whitelist until 2026-07-18 — every full marks reload (page load included)
                // silently wiped configured side bets from memory even though the server still had
                // them saved, so a side bet placed moments before a reload would vanish from the
                // NEXT apply with zero error. This is what caused two real live drops.
                sideBets: Array.isArray(meta.sideBets)
                    ? meta.sideBets.map(hid => String(hid || '').trim()).filter(Boolean)
                    : [],
                betProfile: normalizeBetProfile(meta.betProfile),
                // Per-race bet COMPOSITION override (Voting tab). Preserve only valid shapes.
                ...(normalizeComposition(meta.betComposition) ? { betComposition: normalizeComposition(meta.betComposition) } : {}),
                // Phase 34: tag marking an override as auto-created by Auto Bet Day's backup-preset rescue.
                ...(meta.betCompositionAutoBackup === true ? { betCompositionAutoBackup: true } : {})
            };
        });
    }

    const version = Number(isVersioned ? payload.version : 2);
    normalized.version = Number.isFinite(version) && version > 0 ? version : 2;
    return normalized;
}

function getCurrentRiskValue() {
    if (isDisciplineMode()) return DISCIPLINE_RISK;   // recorded snapshots reflect the pinned risk
    const slider = document.getElementById('risk-slider');
    const parsed = Number.parseInt(slider?.value ?? '50', 10);
    return Number.isFinite(parsed) ? parsed : 50;
}

function getFormulaWeightsSnapshot() {
    return { ...getFormulaWeights() };
}

function getRaceActiveSymbols(r_id) {
    const activeSymbols = [];
    const seen = new Set();

    Object.entries(globalMarks).forEach(([key, value]) => {
        if (!key.startsWith(`${r_id}_`) || !value) return;
        if (!seen.has(value)) {
            seen.add(value);
            activeSymbols.push(value);
        }
    });

    return activeSymbols;
}

function mergeMarkSource(existingSource, incomingSource) {
    const current = String(existingSource || '').trim();
    const incoming = String(incomingSource || '').trim();

    if (!incoming) return current || 'manual';
    if (!current || current === incoming) return incoming;
    if (current === 'mixed' || incoming === 'mixed') return 'mixed';
    return 'mixed';
}

function touchRaceMeta(r_id, options = {}) {
    const existing = globalRaceMeta[r_id] && typeof globalRaceMeta[r_id] === 'object'
        ? globalRaceMeta[r_id]
        : {};
    const now = new Date().toISOString();
    const riskSlider = Number.isFinite(Number(options.riskSlider)) ? Number(options.riskSlider) : getCurrentRiskValue();
    const manualAdjustmentsDelta = Number.isFinite(Number(options.manualAdjustmentsDelta))
        ? Number(options.manualAdjustmentsDelta)
        : 0;
    const currentManualAdjustments = Number.isFinite(Number(existing.manualAdjustments))
        ? Number(existing.manualAdjustments)
        : 0;

    globalRaceMeta[r_id] = {
        ...existing,
        savedAt: existing.savedAt || now,
        updatedAt: now,
        markSource: mergeMarkSource(existing.markSource, options.markSource || existing.markSource || 'manual'),
        strategySnapshot: {
            riskSlider: riskSlider,
            riskLabel: getRiskLabel(riskSlider),
            formulaWeights: getFormulaWeightsSnapshot(),
            // Phase 29 v2 field SHAPE + mark count the engine chose for this race (for week-over-week
            // tuning — lets the recap break P&L down by shape, not just bet type). Preserve a prior
            // auto-pick value when a later manual touch doesn't re-supply it.
            engineShape: (typeof options.engineShape === 'string' && options.engineShape)
                ? options.engineShape
                : (existing.strategySnapshot?.engineShape || null),
            engineCount: Number.isFinite(Number(options.engineCount))
                ? Number(options.engineCount)
                : (Number.isFinite(Number(existing.strategySnapshot?.engineCount)) ? Number(existing.strategySnapshot.engineCount) : null)
        },
        manualAdjustments: Math.max(0, currentManualAdjustments + manualAdjustmentsDelta),
        lockStateAtSave: isRaceLocked(r_id),
        activeSymbols: getRaceActiveSymbols(r_id)
    };

    return globalRaceMeta[r_id];
}

// NEW: Save config to server when slider changes
async function saveConfigToServer() {
    const riskVal = document.getElementById('risk-slider').value;
    appConfig.ui.riskSlider = parseInt(riskVal);
    
    await fetch('/api/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(appConfig)
    });
}

// --- DYNAMIC MATH ENGINE ---
function getFormulaWeights() {
    const fw = appConfig.ui?.formulaWeights ?? {};
    const parseFW = (val, def) => { const n = parseFloat(val); return isNaN(n) ? def : n; };
    return {
        oddsCap:              parseFW(fw.oddsCap,              100),
        formMultiplier:       parseFW(fw.formMultiplier,       100),
        freshnessBonus:       parseFW(fw.freshnessBonus,         3),
        freshnessBreakeven:   parseFW(fw.freshnessBreakeven,    10),
        pedigreeMultiplier:   parseFW(fw.pedigreeMultiplier,    30),
        formWeight:           parseFW(fw.formWeight,            80),
        sireFitWeight:        parseFW(fw.sireFitWeight,         10),
        surfaceFitWeight:     parseFW(fw.surfaceFitWeight,       8),
        distanceFitWeight:    parseFW(fw.distanceFitWeight,      8),
        jockeyWeight:         parseFW(fw.jockeyWeight,          20),
        trainerWeight:        parseFW(fw.trainerWeight,         20),
    };
}

function calculatePowerScore(row, riskVal, raceClass) {
    const fw = getFormulaWeights();
    const cls = raceClass || globalRaceClass[row._raceId] || { isDebut: false, isMaiden: false };
    // Maiden/debut races: Career W/S is uninformative (0 wins for everyone in maidens,
    // 0 starts for everyone in debuts). Skip those terms and rely more heavily on
    // Sire Fit, which is the strongest signal we have for unraced/winless horses.
    const isMaidenLike = cls.isDebut || cls.isMaiden;
    const sireFitBoost = cls.isDebut ? 5 : (cls.isMaiden ? 3 : 1);
    // Ensures risk is always exactly between 0.0 and 1.0
    const risk = Math.max(0, Math.min(100, riskVal)) / 100;

    // 1. Base Odds Score
    let baseOddsScore = 0;
    const odds = parseFloat(row.Odds);
    if (!isNaN(odds) && odds > 0) {
        baseOddsScore = fw.oddsCap / Math.max(1.0, odds); // Caps max at oddsCap to prevent infinity
    }

    // Synthetic odds: when real odds aren't published yet, estimate from career record
    // (Laplace-smoothed so a 2/4 horse → ~2.0, 0/8 → ~10.0, 1/8 → ~5.0). Feeds the
    // VALUE term so the CHAOS slider differentiates longshot-profile horses from form
    // horses even before draw. Real odds replace this the moment JRA publishes them.
    // Skipped for maiden/debut races where career records are near-zero for everyone.
    let syntheticOdds = null;
    if ((isNaN(odds) || odds <= 0) && !isMaidenLike && row.Record) {
        const synNums = String(row.Record).match(/\d+/g);
        if (synNums && synNums.length > 1) {
            const sw = parseInt(synNums[0]) || 0;
            const ss = parseInt(synNums[1]) || 0;
            if (ss > 0) syntheticOdds = (ss + 2) / (sw + 1);
        }
    }

    // 2. Base Form Score
    let baseFormScore = 0;
    if (row.Record && !isMaidenLike) {
        const nums = String(row.Record).match(/\d+/g);
        if (nums && nums.length > 0) {
            const wins = parseInt(nums[0]) || 0;
            // FIX: Reads "Starts" correctly from the "W/S" format (e.g., 2/10 -> Starts = 10)
            const starts = nums.length > 1 ? parseInt(nums[1]) : wins;

            if (starts > 0) {
                baseFormScore += (wins / starts) * fw.formMultiplier;
            }
            // Freshness bonus: rewards lightly raced horses, penalizes over-raced veterans
            baseFormScore += (fw.freshnessBreakeven - starts) * fw.freshnessBonus;
        }
    }

    // Phase 7: recency-weighted form score (server-computed, weights [0.5, 0.3, 0.2] · 1/pos top-5).
    // Range roughly [0, 1]; scale into the same magnitude as the win-rate term.
    // Skipped for debut races (no prior runs → always 0 for everyone anyway).
    if (!cls.isDebut) {
        const formScoreVal = parseFloat(row.Form_Score) || 0;
        baseFormScore += formScoreVal * fw.formWeight;
    }

    // Phase 8: jockey + trainer A/E (Actual/Expected wins, market-bias-corrected).
    // ~1.0 = market-neutral; we center on 1.0 and weight the deviation. Null when
    // the rolling-window sample is below min-starts (server already gated).
    // Shrinkage: ae_eff = 1 + (min(ae,2.0)−1)·n/(n+25). At n=10 → 29% credibility;
    // at n=100 → 80%. Hard cap at 2.0 prevents low-sample outliers from dominating.
    const jAE = parseFloat(row.Jockey_AE);
    if (Number.isFinite(jAE)) {
        const jN = parseFloat(row.Jockey_Starts) || 0;
        const jEff = 1 + (Math.min(jAE, 2.0) - 1) * jN / (jN + 25);
        baseFormScore += (jEff - 1.0) * fw.jockeyWeight;
    }
    const tAE = parseFloat(row.Trainer_AE);
    if (Number.isFinite(tAE)) {
        const tN = parseFloat(row.Trainer_Starts) || 0;
        const tEff = 1 + (Math.min(tAE, 2.0) - 1) * tN / (tN + 25);
        baseFormScore += (tEff - 1.0) * fw.trainerWeight;
    }

    // 3. Base Pedigree Score (from Tracked Bloodlines)
    let basePedScore = (parseFloat(row.Score) || 0) * fw.pedigreeMultiplier;

    // Phase 9: sire-fit tiebreaker — sire's historical win% on THIS race's
    // (surface × distance-bucket). Normalize to 0..1 (divide by 100) so a 20%
    // sire at weight 10 contributes 2 to basePedScore — low-weight tiebreaker
    // per the CLAUDE.md spec. Null sire-fit (below MinSireStarts) contributes 0.
    // In maiden/debut races we multiply the weight (5× debut, 3× maiden) because
    // it's the most reliable ability proxy when career stats are uninformative.
    const sireFitVal = (row.Sire_Fit === null || row.Sire_Fit === undefined) ? null : parseFloat(row.Sire_Fit);
    if (sireFitVal !== null && Number.isFinite(sireFitVal)) {
        basePedScore += (sireFitVal / 100) * fw.sireFitWeight * sireFitBoost;
    }

    // Phase 43: the horse's OWN surface- and distance-fit — a low-weight tiebreaker that
    // nudges horses proven on THIS race's surface and distance bucket. The server gates
    // these to null below 3 starts in the split, so lightly-raced horses contribute 0.
    // Win% based, so it rewards proven winners at the trip (a pure tiebreaker, not a boost):
    // unlike Sire_Fit it is NOT amplified in maiden/debut races — own-record is least
    // available exactly there, so it stays at face weight. Rides the merit weight (risk-scaled).
    const surfaceFitVal = (row.Surface_Win_Pct === null || row.Surface_Win_Pct === undefined) ? null : parseFloat(row.Surface_Win_Pct);
    if (surfaceFitVal !== null && Number.isFinite(surfaceFitVal)) {
        basePedScore += (surfaceFitVal / 100) * fw.surfaceFitWeight;
    }
    const distFitVal = (row.Dist_Win_Pct === null || row.Dist_Win_Pct === undefined) ? null : parseFloat(row.Dist_Win_Pct);
    if (distFitVal !== null && Number.isFinite(distFitVal)) {
        basePedScore += (distFitVal / 100) * fw.distanceFitWeight;
    }

    // 4. THE SLIDER MIXER (rebuilt 2026-05-31 — see SLIDER_TUNING.md)
    // Old model: odds·(1−risk) + (form+ped)·risk. Problem: "chaos" only DIMMED favorites,
    // it never REWARDED longshots, so on stat-heavy fields picks plateaued from ~50→100
    // (a horse leading on form+ped just kept leading). Fix: add a VALUE term that actively
    // rewards long odds, with weight growing like risk² so it's dormant in the safe half
    // and dominant near chaos — giving the slider a smooth, even spread across 1–99.
    // Endpoints are intentionally degenerate (mirror images):
    //   risk 0   = PURE CHALK  → top-4 strictly by favouritism (odds only)
    //   risk 100 = PURE YOLO   → top-4 strictly by longest odds among horses with any merit
    //   1–99     = graded, defensible blend
    const favRank = parseFloat(row.Fav) || 999;

    // --- Degenerate endpoint: 100 = "revealed in a dream" longshot sort ---
    if (riskVal >= 100) {
        // Rank by longest odds, but require a shred of merit so we don't crown a
        // no-hoper with zero signal. meritGate nudges horses with any form/ped above
        // pure price. Unposted odds sink to the bottom.
        const meritGate = (baseFormScore + basePedScore) > 0 ? 1 : 0;
        const yoloOdds = (!isNaN(odds) && odds > 0) ? odds : (syntheticOdds || 0);
        return yoloOdds * 1000 + meritGate - (favRank * 0.0001);
    }

    // --- Value term: rewards long odds via odds^exponent, where the EXPONENT grows
    // with risk (SLIDER_TUNING.md, 2nd iteration). The earlier log(odds)·weight only
    // SCALED a fixed odds-ordering, so once value dominated (~75) the ranking froze =
    // plateau. A growing exponent instead keeps re-stretching the gaps between longshots:
    // at moderate risk the gaps are small enough that MERIT still decides order among
    // longshots; as the exponent climbs toward 1.0 the odds gaps widen and merit's
    // influence recedes, so progressively-longer-odds horses overtake SMOOTHLY across
    // 75→99 instead of locking. SV_* constants are tunable from live sweeps.
    // RISING-CEILING value: reward odds only UP TO a ceiling that climbs with risk, so the
    // slider targets a moving "value sweet spot" instead of always preferring the longest
    // price. Horses beyond the ceiling are clamped (no extra reward for being even longer),
    // which (a) keeps 99 GROUNDED — the ceiling is ~120 at risk 0.99 so true 160-200:1 bombs
    // only win at the degenerate risk-100 endpoint, and (b) lets MERIT break ties among the
    // out-of-band longshots so the order keeps shifting (no plateau). SV_* are tunable.
    const SV_CEIL_BASE = 10, SV_CEIL_SPAN = 115; // odds ceiling 10 → 125 as risk 0 → 1
    const SV_COEF = 2.2;
    let baseValueScore = 0;
    const valueOdds = (!isNaN(odds) && odds > 1) ? odds : (syntheticOdds != null && syntheticOdds > 1 ? syntheticOdds : null);
    if (valueOdds !== null) {
        const ceil = SV_CEIL_BASE + SV_CEIL_SPAN * risk * risk; // quadratic: stays low early
        baseValueScore = Math.min(valueOdds, ceil) * SV_COEF;
    }

    // Maiden/debut career-exposure penalty (soft). A winless horse with MANY starts is
    // "exposed form" (known bad); a winless horse with FEW starts has "hidden upside".
    // So in maiden-like races we scale the value term DOWN for heavily-raced winless
    // horses, sparing lightly-raced ones — stops the engine chasing 0-for-8 bombs on
    // sire-fit alone, per the external maiden review. ~1.0 at 0–1 starts → ~0.45 at 10+.
    if (isMaidenLike && baseValueScore > 0 && row.Record) {
        const recNums = String(row.Record).match(/\d+/g);
        const wins0 = recNums ? (parseInt(recNums[0]) || 0) : 0;
        const starts0 = recNums && recNums.length > 1 ? (parseInt(recNums[1]) || 0) : wins0;
        if (wins0 === 0 && starts0 > 1) {
            const exposure = Math.min(starts0, 10) / 10;       // 0..1
            baseValueScore *= (1.0 - 0.55 * exposure);          // up to −55% at 10+ starts
        }
    }

    // Weights. Odds (favourite reward) fades with risk but on a gentler curve (sqrt) so a
    // strong favorite clings to a low mark into the 70s. Merit peaks mid, eases near top.
    // Value grows like risk³ — stays dormant longer (favorite survives), then ramps hard.
    const oddsWeight  = Math.pow(1.0 - risk, 0.7);           // gentler-than-linear favorite decay
    const meritWeight = risk * (1.0 - 0.35 * risk);
    const valueWeight = risk * risk * risk;                   // risk³: later, sharper value onset

    let totalScore = (baseOddsScore * oddsWeight)
                   + ((baseFormScore + basePedScore) * meritWeight)
                   + (baseValueScore * valueWeight);

    // Pre-odds CHAOS bonus: a separate additive term (not multiplied by any weight) that
    // gives the slider visible differentiation within CHAOS (risk 65→99) before JRA publishes
    // odds. The value term above can't bridge the merit gap (formMultiplier=100 → merit scores
    // in the 50–90 range; synthetic odds max ~11 × SV_COEF × valueWeight ≈ 23). This bonus
    // bypasses the weight dance and grows linearly from 0 at the CHAOS floor (65) to its max
    // at 99. Scale factor (3.0) tunable from SLIDER_TUNING.md.
    // ▲/△ start shifting around risk=88–90; ◎ may flip by risk=95+.
    if (syntheticOdds !== null && risk > ENGINE_TUNING.CHAOS_MIN / 100) {
        const chaosFloor = ENGINE_TUNING.CHAOS_MIN / 100;
        const chaosDepth = Math.min(1, (risk - chaosFloor) / (0.99 - chaosFloor));
        totalScore += Math.max(0, (syntheticOdds - 1.5) * chaosDepth * 3.0);
    }

    // 5. Ultimate Tie-Breaker — true Fav wins ties by a fraction (esp. at Risk 0 pre-odds).
    totalScore -= (favRank * 0.0001);

    return totalScore;
}

// Phase 18: same math as calculatePowerScore, but returns a structured breakdown
// for the hover popover. Keeping it as a parallel function instead of folding
// {total, breakdown} into calculatePowerScore so the 4 hot-path call sites stay
// allocation-free during auto-pick sorts over hundreds of entries.
function explainPowerScore(row, riskVal) {
    const fw = getFormulaWeights();
    const cls = globalRaceClass[row._raceId] || { isDebut: false, isMaiden: false };
    const isMaidenLike = cls.isDebut || cls.isMaiden;
    const sireFitBoost = cls.isDebut ? 5 : (cls.isMaiden ? 3 : 1);
    const risk = Math.max(0, Math.min(100, riskVal)) / 100;
    // Mirror the rebuilt mixer in calculatePowerScore (SLIDER_TUNING.md): odds fades with
    // risk, merit (form+ped) peaks mid and eases near the top, value (long-odds reward)
    // grows like risk². Endpoints 0/100 are degenerate in calculatePowerScore; the popover
    // shows the graded blend (it's only opened on real rows, not the endpoint sorts).
    const oddsMix = Math.pow(1.0 - risk, 0.7);
    const formMix = risk * (1.0 - 0.35 * risk);
    const pedMix  = risk * (1.0 - 0.35 * risk);
    const valueMix = risk * risk * risk;

    // ODDS branch
    let baseOddsScore = 0;
    const odds = parseFloat(row.Odds);
    const oddsLines = [];
    if (!isNaN(odds) && odds > 0) {
        baseOddsScore = fw.oddsCap / Math.max(1.0, odds);
        oddsLines.push({ label: `Odds ${odds.toFixed(1)} → ${fw.oddsCap}/${odds.toFixed(1)}`, value: baseOddsScore });
    } else {
        oddsLines.push({ label: 'Odds not posted', value: 0 });
    }

    // FORM branch
    let baseFormScore = 0;
    const formLines = [];
    if (row.Record && !isMaidenLike) {
        const nums = String(row.Record).match(/\d+/g);
        if (nums && nums.length > 0) {
            const wins = parseInt(nums[0]) || 0;
            const starts = nums.length > 1 ? parseInt(nums[1]) : wins;
            if (starts > 0) {
                const wr = (wins / starts) * fw.formMultiplier;
                baseFormScore += wr;
                const pct = ((wins / starts) * 100).toFixed(1);
                formLines.push({ label: `Career ${wins}/${starts} (${pct}%) × ${fw.formMultiplier}`, value: wr });
            }
            const fresh = (fw.freshnessBreakeven - starts) * fw.freshnessBonus;
            baseFormScore += fresh;
            formLines.push({ label: `Freshness (${fw.freshnessBreakeven}−${starts}) × ${fw.freshnessBonus}`, value: fresh });
        }
    } else if (isMaidenLike) {
        formLines.push({ label: `Career W/S skipped (${cls.isDebut ? 'debut race' : 'maiden race'})`, value: 0 });
    }
    if (!cls.isDebut) {
        const formScoreVal = parseFloat(row.Form_Score) || 0;
        const last3Contrib = formScoreVal * fw.formWeight;
        baseFormScore += last3Contrib;
        formLines.push({ label: `Form (Ninki-Δ, field-wtd) ${formScoreVal.toFixed(3)} × ${fw.formWeight}`, value: last3Contrib });
    } else {
        formLines.push({ label: 'Last-3 skipped (debut race)', value: 0 });
    }

    // Phase 8: jockey/trainer A/E contributions (with shrinkage, same math as calculatePowerScore).
    const jAEv = parseFloat(row.Jockey_AE);
    if (Number.isFinite(jAEv)) {
        const jNv = parseFloat(row.Jockey_Starts) || 0;
        const jEffv = 1 + (Math.min(jAEv, 2.0) - 1) * jNv / (jNv + 25);
        const jc = (jEffv - 1.0) * fw.jockeyWeight;
        baseFormScore += jc;
        const jName = row.Jockey || row.Jockey_Code || 'jockey';
        const jShrinkNote = jNv > 0 ? ` → ${jEffv.toFixed(2)} (n=${jNv})` : '';
        formLines.push({ label: `${jName} A/E ${jAEv.toFixed(2)}${jShrinkNote} × ${fw.jockeyWeight}`, value: jc });
    } else if (row.Jockey_Code) {
        formLines.push({ label: `${row.Jockey || row.Jockey_Code} A/E — (low sample)`, value: 0 });
    }
    const tAEv = parseFloat(row.Trainer_AE);
    if (Number.isFinite(tAEv)) {
        const tNv = parseFloat(row.Trainer_Starts) || 0;
        const tEffv = 1 + (Math.min(tAEv, 2.0) - 1) * tNv / (tNv + 25);
        const tc = (tEffv - 1.0) * fw.trainerWeight;
        baseFormScore += tc;
        const tName = row.Trainer || row.Trainer_Code || 'trainer';
        const tShrinkNote = tNv > 0 ? ` → ${tEffv.toFixed(2)} (n=${tNv})` : '';
        formLines.push({ label: `${tName} A/E ${tAEv.toFixed(2)}${tShrinkNote} × ${fw.trainerWeight}`, value: tc });
    } else if (row.Trainer_Code) {
        formLines.push({ label: `${row.Trainer || row.Trainer_Code} A/E — (low sample)`, value: 0 });
    }

    // PEDIGREE branch
    const pedLines = [];
    const tracked = parseFloat(row.Score) || 0;
    let basePedScore = tracked * fw.pedigreeMultiplier;
    pedLines.push({ label: `Tracked bloodlines ${tracked.toFixed(2)} × ${fw.pedigreeMultiplier}`, value: tracked * fw.pedigreeMultiplier });

    const sireFitVal = (row.Sire_Fit === null || row.Sire_Fit === undefined) ? null : parseFloat(row.Sire_Fit);
    if (sireFitVal !== null && Number.isFinite(sireFitVal)) {
        const sf = (sireFitVal / 100) * fw.sireFitWeight * sireFitBoost;
        basePedScore += sf;
        const boostLabel = sireFitBoost > 1 ? ` (×${sireFitBoost} ${cls.isDebut ? 'debut' : 'maiden'})` : '';
        pedLines.push({ label: `Sire Fit ${sireFitVal.toFixed(1)}% × ${fw.sireFitWeight}${boostLabel}`, value: sf });
    } else {
        pedLines.push({ label: 'Sire Fit — (no sample)', value: 0 });
    }

    // Phase 43: horse's OWN surface/distance fit (mirror of calculatePowerScore — keep in sync).
    const surfaceFitVal = (row.Surface_Win_Pct === null || row.Surface_Win_Pct === undefined) ? null : parseFloat(row.Surface_Win_Pct);
    if (surfaceFitVal !== null && Number.isFinite(surfaceFitVal)) {
        const sfc = (surfaceFitVal / 100) * fw.surfaceFitWeight;
        basePedScore += sfc;
        pedLines.push({ label: `Surface fit ${surfaceFitVal.toFixed(1)}% (${row.Surface_Starts || '?'} st) × ${fw.surfaceFitWeight}`, value: sfc });
    }
    const distFitVal = (row.Dist_Win_Pct === null || row.Dist_Win_Pct === undefined) ? null : parseFloat(row.Dist_Win_Pct);
    if (distFitVal !== null && Number.isFinite(distFitVal)) {
        const dfc = (distFitVal / 100) * fw.distanceFitWeight;
        basePedScore += dfc;
        pedLines.push({ label: `Distance fit ${distFitVal.toFixed(1)}% (${row.Dist_Starts || '?'} st) × ${fw.distanceFitWeight}`, value: dfc });
    }

    // VALUE branch (long-odds reward — the chaos dimension). Mirrors calculatePowerScore:
    // min(odds, 10+115·risk²)·2.2 (rising ceiling), with a soft maiden career-exposure penalty.
    const valueLines = [];
    let baseValueScore = 0;
    if (!isNaN(odds) && odds > 1) {
        const ceil = 10 + 115 * risk * risk;
        baseValueScore = Math.min(odds, ceil) * 2.2;
        let expoNote = '';
        if (isMaidenLike && row.Record) {
            const rn = String(row.Record).match(/\d+/g);
            const w0 = rn ? (parseInt(rn[0]) || 0) : 0;
            const s0 = rn && rn.length > 1 ? (parseInt(rn[1]) || 0) : w0;
            if (w0 === 0 && s0 > 1) {
                const mult = 1.0 - 0.55 * (Math.min(s0, 10) / 10);
                baseValueScore *= mult;
                expoNote = ` ×${mult.toFixed(2)} (0-for-${s0} exposed)`;
            }
        }
        const capNote = odds > ceil ? ` (capped at ${ceil.toFixed(0)})` : '';
        valueLines.push({ label: `Value min(${odds.toFixed(1)},${ceil.toFixed(0)}) × 2.2${capNote}${expoNote}`, value: baseValueScore });
    } else {
        valueLines.push({ label: 'Value — (odds ≤ 1 or unposted)', value: 0 });
    }

    // MIX
    const oddsMixed = baseOddsScore * oddsMix;
    const formMixed = baseFormScore * formMix;
    const pedMixed  = basePedScore  * pedMix;
    const valueMixed = baseValueScore * valueMix;

    // TIEBREAKER
    const favRank = parseFloat(row.Fav) || 999;
    const tiebreaker = -(favRank * 0.0001);

    const total = oddsMixed + formMixed + pedMixed + valueMixed + tiebreaker;

    return {
        total,
        risk: Math.round(risk * 100),
        raceClass: cls,
        mix: { odds: oddsMix, form: formMix, ped: pedMix, value: valueMix },
        odds:    { lines: oddsLines, subtotal: baseOddsScore, mixed: oddsMixed },
        form:    { lines: formLines, subtotal: baseFormScore, mixed: formMixed },
        pedigree:{ lines: pedLines,  subtotal: basePedScore,  mixed: pedMixed  },
        value:   { lines: valueLines, subtotal: baseValueScore, mixed: valueMixed },
        tiebreaker: { favRank, value: tiebreaker }
    };
}

// Race class flag is now computed server-side from prior-career history strictly
// BEFORE each race's date (RacesController.GetRaces). Stable historically. The
// frontend just reads race.info.race_class — values: "debut" | "maiden" | "normal".
function raceClassFlags(rc) {
    const v = String(rc || '').toLowerCase();
    return { isDebut: v === 'debut', isMaiden: v === 'maiden' };
}

// Phase 18: popover state. Single global popover element; we re-render it each
// time the operator opens a new row or the risk slider moves while it's open.
let scoreExplainState = { raceId: null, horseId: null };

function getRiskLabel(risk) {
    if (risk <= 20) return 'Safe';
    if (risk <= 40) return 'Chalky';
    if (risk <= 60) return 'Balanced';
    if (risk <= 80) return 'Lucky';
    return 'Wild';
}

function ensureScoreExplainPopover() {
    let pop = document.getElementById('score-explain-popover');
    if (pop) return pop;
    pop = document.createElement('div');
    pop.id = 'score-explain-popover';
    pop.className = 'score-explain-popover';
    pop.style.display = 'none';
    pop.addEventListener('click', (e) => e.stopPropagation());
    document.body.appendChild(pop);
    document.addEventListener('click', (e) => {
        if (pop.style.display === 'none') return;
        if (e.target.closest('.score-explain-trigger')) return;
        closeScoreExplain();
    });
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeScoreExplain(); });
    return pop;
}

function closeScoreExplain() {
    const pop = document.getElementById('score-explain-popover');
    if (pop) pop.style.display = 'none';
    scoreExplainState = { raceId: null, horseId: null };
}

function openScoreExplain(event, raceId, horseId) {
    event.stopPropagation();
    const entries = globalRaceEntries[raceId] || [];
    const row = entries.find(e => String(e.Horse_ID).split('.')[0] === String(horseId));
    if (!row) return;
    scoreExplainState = { raceId, horseId };
    renderScoreExplain(row, event.currentTarget);
}

function renderScoreExplain(row, anchor) {
    const pop = ensureScoreExplainPopover();
    const risk = getCurrentAutoPickRisk();
    const b = explainPowerScore(row, risk);
    const horseName = row.Horse || row.Horse_ID || '';
    const raceId = scoreExplainState.raceId;
    const horseId = scoreExplainState.horseId;

    const allEntries = globalRaceEntries[raceId] || [];
    const total = allEntries.length || 1;

    // Derive the mark the Phase 29 v2 engine would assign — from its full race plan
    // (mark-blind), not globalMarks. The user may have overridden marks; the popover
    // always explains the engine's view, including its chosen COUNT and field SHAPE.
    const enginePlan = getEngineMarkPlanForRace(raceId);
    const mark = (enginePlan.assignments.find(a => a.h_id === String(horseId)) || {}).symbol || null;
    const ENGINE_SHAPE_LABELS = {
        'lone-favorite': 'lone favorite', 'two-clear': 'two clear', 'tight-top3': 'tight top-3',
        'tight-pack': 'tight pack', 'standout+pack': 'standout + open pack', 'wide-open': 'wide open',
        'small-field': 'field too small', 'no-safe-anchor': 'no safe anchor', 'no-odds': 'pre-odds (no market prices)',
        'chalk-no-overlay': 'chalk, no value', 'underpriced-fav': 'underpriced favorite',
        'shape-risk-mismatch': 'shape/risk mismatch', 'empty': 'no field',
        'nagashi-no-axis': 'nagashi — no axis horse', 'below-preset-floor': 'too few marks for this bet',
    };
    const engineShapeText = ENGINE_SHAPE_LABELS[enginePlan.shape] || enginePlan.shape || '';
    const enginePlanNote = enginePlan.count > 0
        ? `⚙ Engine: ${enginePlan.count} mark${enginePlan.count === 1 ? '' : 's'} · ${engineShapeText}`
        : `⚙ Engine: abstain · ${engineShapeText}`;

    // Rank this horse among the field for a given metric (1 = best).
    function fieldRank(extractor, higherIsBetter) {
        const thisVal = extractor(row);
        if (thisVal === null || !Number.isFinite(thisVal)) return null;
        const vals = allEntries.map(extractor).filter(v => v !== null && Number.isFinite(v));
        vals.sort((a, c) => higherIsBetter ? c - a : a - c);
        const idx = vals.findIndex(v => Math.abs(v - thisVal) < 1e-9);
        return idx >= 0 ? idx + 1 : null;
    }

    // Mini-bar: 6 chars, filled proportional to percentile (rank 1 = full).
    function miniBar(rank, outOf) {
        if (rank === null || outOf <= 1) return '';
        const pct = (outOf - rank) / (outOf - 1);
        const f = Math.round(pct * 6);
        return '<span class="sx-bar-filled">' + '█'.repeat(f) + '</span><span class="sx-bar-empty">' + '░'.repeat(6 - f) + '</span>';
    }

    // Sentiment: top-third positive, bottom-third negative, middle neutral.
    function sentiment(rank, outOf) {
        if (rank === null) return 'sx-neu';
        const third = Math.ceil(outOf / 3);
        return rank <= third ? 'sx-pos' : rank > outOf - third ? 'sx-neg' : 'sx-neu';
    }

    const oddsVal  = parseFloat(row.Odds);
    const formScore = parseFloat(row.Form_Score) || 0;
    const jAE      = parseFloat(row.Jockey_AE);
    const tAE      = parseFloat(row.Trainer_AE);
    const sfVal    = parseFloat(row.Sire_Fit);
    const sfPlace  = parseFloat(row.Sire_Place_Fit);
    const sfStarts = parseFloat(row.Sire_Starts);

    const oddsRank = fieldRank(e => { const v = parseFloat(e.Odds); return Number.isFinite(v) && v > 0 ? v : null; }, false);
    // Value rank: longest odds = best (rank 1) — the chaos dimension, inverse of oddsRank.
    const valueRank = fieldRank(e => { const v = parseFloat(e.Odds); return Number.isFinite(v) && v > 0 ? v : null; }, true);
    const formRank = fieldRank(e => parseFloat(e.Form_Score) || 0, true);
    const jRank    = fieldRank(e => { const v = parseFloat(e.Jockey_AE); return Number.isFinite(v) ? v : null; }, true);
    const tRank    = fieldRank(e => { const v = parseFloat(e.Trainer_AE); return Number.isFinite(v) ? v : null; }, true);
    const sfRank   = fieldRank(e => { const v = parseFloat(e.Sire_Fit); return Number.isFinite(v) ? v : null; }, true);

    // Plain-English descriptions for each factor.
    function oddsDesc() {
        if (!Number.isFinite(oddsVal) || oddsVal <= 0) return 'No odds posted yet';
        const tier = oddsRank === 1 ? 'Favorite' : oddsRank <= 2 ? 'Near-favorite'
            : oddsRank <= Math.ceil(total / 2) ? 'Mid-field' : 'Longshot';
        return `${tier} at ${oddsVal.toFixed(1)}×`;
    }
    function formDesc() {
        if (b.raceClass?.isDebut) return 'First career start — no race history';
        const runs = parseLast3Runs(row.Last3);
        const fields = String(row.Last3_Fields || '').split('-'); // Phase 28: field size per past run
        const parts = [];
        if (formScore >= 0.65) parts.push('Excellent form');
        else if (formScore >= 0.35) parts.push('Solid recent form');
        else if (formScore >= 0.10) parts.push('Mixed recent form');
        else if (formScore >= 0.00) parts.push('Weak recent form');
        else parts.push('Burned favorite');
        if (runs.length > 0) {
            const deltas = runs.map((r, i) => {
                if (r.delta === null) return null;
                const label = r.delta > 3 ? '↑sleeper' : (r.delta < 0 && r.favRank <= 5) ? '↓burned' : null;
                if (!label) return null;
                // Field size contextualizes the overachievement: beating Δ in a deeper field
                // counts for more (and is weighted as such in the engine's Form_Score).
                const fld = (fields[i] && fields[i] !== '—') ? ` (${fields[i]}f)` : '';
                return `R${i+1}: Δ${r.delta > 0 ? '+' : ''}${r.delta}${fld} ${label}`;
            }).filter(Boolean);
            if (deltas.length > 0) parts.push(deltas.join(', '));
        }
        return parts.join(' · ');
    }
    function aeDesc(ae, name) {
        const nameStr = name ? `${name} — ` : '';
        if (!Number.isFinite(ae)) return `${nameStr}not scored (low sample)`;
        const pct = Math.round((ae - 1.0) * 100);
        const sign = pct >= 0 ? '+' : '';
        const adj = ae >= 1.3 ? 'elite' : ae >= 1.1 ? 'above market' : ae >= 0.9 ? 'market-neutral' : 'below market';
        return `${nameStr}${sign}${pct}% vs expected (${adj})`;
    }
    function sfDesc() {
        if (!Number.isFinite(sfVal)) return 'No data for this surface/distance';
        const adj = sfVal >= 20 ? 'Strong fit' : sfVal >= 12 ? 'Decent fit' : sfVal >= 6 ? 'Moderate fit' : 'Weak fit';
        const place = Number.isFinite(sfPlace) ? ` / ${sfPlace.toFixed(0)}% place` : '';
        const n = Number.isFinite(sfStarts) ? ` (n=${Math.round(sfStarts)})` : '';
        return `${adj}: ${sfVal.toFixed(1)}% win${place}${n}`;
    }

    // Verdict: one sentence explaining the mark.
    function verdictSentence() {
        if (!mark) return 'Not selected — engine abstained or no clear edge at this Risk level.';
        const markOrder = ['◎', '〇', '▲', '△'];
        const strength = markOrder.indexOf(mark);
        const oddsPct = b.mix.odds;
        // Pick the most explanatory factor based on current risk mix and field rank.
        let topFactors = [];
        if (oddsPct >= 0.6 && oddsRank !== null && oddsRank <= 2) topFactors.push('market position');
        if (formRank !== null && formRank === 1) topFactors.push('best recent form in the field');
        if (jRank !== null && jRank === 1) topFactors.push('top jockey in the field');
        if (sfRank !== null && sfRank === 1) topFactors.push('best sire fit for conditions');
        if (topFactors.length === 0) {
            topFactors.push(oddsPct >= 0.5 ? 'market position' : 'combined form score');
        }
        const reason = topFactors.slice(0, 2).join(' + ');
        const prefix = ['Leads on', 'Second on', 'Competitive via', 'Minor edge on'][strength] || 'Selected for';
        return `Got ${mark} — ${prefix} ${reason}.`;
    }

    // Bottom context line about what Risk is doing.
    function riskNote() {
        const oddsPct = Math.round(b.mix.odds * 100);
        const formPct = Math.round(b.mix.form * 100);
        if (oddsPct >= 70) return `Risk ${b.risk} (${getRiskLabel(b.risk)}) — market is ${oddsPct}% of the score. This is primarily a public-money call.`;
        if (oddsPct <= 30) return `Risk ${b.risk} (${getRiskLabel(b.risk)}) — form & pedigree drive ${formPct}% of the score. Market is largely ignored.`;
        return `Risk ${b.risk} (${getRiskLabel(b.risk)}) — balanced blend: ${oddsPct}% odds, ${formPct}% form/pedigree.`;
    }

    function factorRow(emoji, label, desc, rank, positive) {
        const bar = rank !== null ? miniBar(rank, total) : '';
        const rankStr = rank !== null ? `${rank}/${total}` : '—';
        const cls = positive === true ? 'sx-pos' : positive === false ? 'sx-neg' : 'sx-neu';
        return `<div class="sx-factor">
            <span class="sx-factor-emoji">${emoji}</span>
            <div class="sx-factor-body">
                <span class="sx-factor-label">${label}</span>
                <span class="sx-factor-desc ${cls}">${desc}</span>
            </div>
            <div class="sx-factor-rank">
                <span class="sx-factor-bar">${bar}</span>
                <span class="sx-factor-pos">${rankStr}</span>
            </div>
        </div>`;
    }

    function valueDesc() {
        const o = parseFloat(row.Odds);
        if (!Number.isFinite(o) || o <= 1) return 'No posted odds — no value signal.';
        const w = (risk * risk * 100).toFixed(0);
        if (o >= 50) return `Big overlay at ${o.toFixed(1)} — chaos weight ${w}% rewards the bomb.`;
        if (o >= 15) return `Longshot value at ${o.toFixed(1)} — chaos weight ${w}%.`;
        if (o >= 7)  return `Mild value at ${o.toFixed(1)} — modest chaos lift (${w}%).`;
        return `Short price (${o.toFixed(1)}) — little value to mine even in chaos.`;
    }

    function sent(rank) { return sentiment(rank, total); }
    const oddsP  = sent(oddsRank) === 'sx-pos' ? true : sent(oddsRank) === 'sx-neg' ? false : null;
    // Value is "positive" when this is a genuine longshot (top third by odds length).
    const valueP = sent(valueRank) === 'sx-pos' ? true : sent(valueRank) === 'sx-neg' ? false : null;
    const formP  = sent(formRank) === 'sx-pos' ? true : sent(formRank) === 'sx-neg' ? false : null;
    const jP     = Number.isFinite(jAE) ? (jAE >= 1.1 ? true : jAE < 0.9 ? false : null) : null;
    const tP     = Number.isFinite(tAE) ? (tAE >= 1.1 ? true : tAE < 0.9 ? false : null) : null;
    const sfP    = Number.isFinite(sfVal) ? (sfVal >= 12 ? true : sfVal < 6 ? false : null) : null;

    const maidenBadge = b.raceClass?.isDebut
        ? '<span class="sx-maiden-badge">DEBUT</span>'
        : (b.raceClass?.isMaiden ? '<span class="sx-maiden-badge">MAIDEN</span>' : '');
    const markBadge = mark ? `<span class="sx-mark-badge">${mark}</span>` : '';

    pop.innerHTML = `
        <div class="sx-head">
            ${markBadge}<div class="sx-horse">${horseName}</div>${maidenBadge}
            <button class="sx-close" onclick="closeScoreExplain()" title="Close">✕</button>
        </div>
        <div class="sx-verdict">${verdictSentence()}</div>
        <div class="sx-engine-plan">${enginePlanNote}</div>
        <div class="sx-factors">
            ${factorRow('💴', 'Odds', oddsDesc(), oddsRank, oddsP)}
            ${factorRow('📈', 'Recent Form', formDesc(), formRank, formP)}
            ${factorRow('🏇', 'Jockey', aeDesc(jAE, row.Jockey || null), jRank, jP)}
            ${factorRow('🎯', 'Trainer', aeDesc(tAE, row.Trainer || null), tRank, tP)}
            ${factorRow('🧬', 'Sire Fit', sfDesc(), sfRank, sfP)}
            ${risk > 50 ? factorRow('🎲', 'Value (chaos)', valueDesc(), valueRank, valueP) : ''}
        </div>
        <div class="sx-risk-note">${riskNote()}</div>
    `;

    // Anchor: place to the right of the trigger if there's room, else left, else below.
    pop.style.display = 'block';
    const rect = anchor.getBoundingClientRect();
    const popRect = pop.getBoundingClientRect();
    const margin = 8;
    let left = rect.right + margin;
    let top = rect.top;
    if (left + popRect.width > window.innerWidth - 8) left = rect.left - popRect.width - margin;
    if (left < 8) left = 8;
    if (top + popRect.height > window.innerHeight - 8) top = window.innerHeight - popRect.height - 8;
    if (top < 8) top = 8;
    pop.style.left = (left + window.scrollX) + 'px';
    pop.style.top  = (top + window.scrollY) + 'px';
}

// Live-update the popover when the risk slider moves while it's open.
function refreshScoreExplainIfOpen() {
    if (!scoreExplainState.raceId || !scoreExplainState.horseId) return;
    const pop = document.getElementById('score-explain-popover');
    if (!pop || pop.style.display === 'none') return;
    const entries = globalRaceEntries[scoreExplainState.raceId] || [];
    const row = entries.find(e => String(e.Horse_ID).split('.')[0] === String(scoreExplainState.horseId));
    if (!row) return;
    // Use the current popover position as the anchor proxy.
    renderScoreExplain(row, pop);
}

// ============================================================
// POST-RACE "WHY IT WON" AUTOPSY (s54) — the teaching half of the engine.
// The pre-race ⓘ popover explains why the engine PICKED a horse; this explains,
// after the race settles, why the actual WINNER beat the field — read on the SAME
// factors, so picking and winning are judged on one ruler. It grades each result
// CHALK / CATCHABLE / SEMI / FREAK (mirrors tools/backtest/upset-autopsy.mjs:
// merit = explainPowerScore form.subtotal + pedigree.subtotal, odds removed), and
// shows how the engine's own ◎ fared. Frontend-only: reads the settled Finish +
// the scoring fields already on each row. Honest by design — when a winner is weak
// on every signal it says FREAK rather than inventing a reason.
// ============================================================
let raceAutopsyState = { raceId: null };

const AUTOPSY_BUCKET = {
    chalk:     { label: 'CHALK', short: 'Chalk', emoji: '✓', color: '#7ed6df',
                 meaning: 'A top-3 favorite won — the market had this right. If you lost here it was the bet TYPE, not the pick.' },
    catchable: { label: 'CATCHABLE', short: 'Catchable', emoji: '🎯', color: '#1dd1a1',
                 meaning: 'An upset, but the winner ranked among the field’s best on form + breeding. A capable horse the crowd underrated — the kind you can learn to spot.' },
    semi:      { label: 'SEMI-READABLE', short: 'Semi', emoji: '〰', color: '#feca57',
                 meaning: 'An upset; the winner was middling on the stats. Partly readable, partly variance — hard to call in advance.' },
    freak:     { label: 'FREAK', short: 'Freak', emoji: '🎲', color: '#ff6b6b',
                 meaning: 'An upset; the winner was weak on every signal. Unforecastable — the honest response is cheaper, wider bets, not re-picking.' },
};

function autopsyOrdinal(n) {
    if (!Number.isFinite(n)) return '—';
    const s = ['th', 'st', 'nd', 'rd'], v = n % 100;
    return n + (s[(v - 20) % 10] || s[v] || s[0]);
}

// A race is autopsy-ready once a real finish order with a 1st exists.
function raceIsSettledForAutopsy(raceId) {
    const entries = globalRaceEntries[raceId] || [];
    return entries.some(e => parseFinishRank(e.Finish) === 1);
}

function computeRaceAutopsy(raceId) {
    const entries = globalRaceEntries[raceId] || [];
    if (!entries.length) return null;
    const risk = getCurrentAutoPickRisk();
    const field = entries.length;

    // Per-horse: the engine's own breakdown + odds-removed MERIT (form+pedigree
    // subtotal) — exactly the grade the offline upset-autopsy uses.
    const scored = entries.map(e => {
        const b = explainPowerScore(e, risk);
        return {
            e,
            hid: String(e.Horse_ID).split('.')[0],
            name: e.Horse || e.Horse_ID || '',
            merit: (b.form?.subtotal || 0) + (b.pedigree?.subtotal || 0),
            finish: parseFinishRank(e.Finish),
            fav: parseInt(e.Fav, 10) || 999,
            odds: parseFloat(e.Odds),
            form: parseFloat(e.Form_Score),
            sire: parseFloat(e.Sire_Fit),
            surf: parseFloat(e.Surface_Win_Pct),
        };
    });

    const winner = scored.find(s => s.finish === 1);
    if (!winner) return null;

    // Field-rank helper (1 = best).
    function ranker(extract, higherBetter) {
        const vals = scored.map(extract).filter(v => v !== null && Number.isFinite(v))
            .sort((a, c) => higherBetter ? c - a : a - c);
        return (s) => {
            const v = extract(s);
            if (v === null || !Number.isFinite(v)) return null;
            const idx = vals.findIndex(x => Math.abs(x - v) < 1e-9);
            return idx >= 0 ? idx + 1 : null;
        };
    }
    const meritRankOf = ranker(s => s.merit, true);
    const formRankOf  = ranker(s => s.form, true);
    const sireRankOf  = ranker(s => s.sire, true);
    const surfRankOf  = ranker(s => s.surf, true);
    const oddsRankOf  = ranker(s => (Number.isFinite(s.odds) && s.odds > 0 ? s.odds : null), false); // 1 = favorite

    const statsRank = meritRankOf(winner);
    const isUpset = winner.fav >= 4;
    let bucket;
    if (!isUpset) bucket = 'chalk';
    else if (statsRank !== null && statsRank <= field / 3) bucket = 'catchable';
    else if (statsRank !== null && statsRank > field * 2 / 3) bucket = 'freak';
    else bucket = 'semi';

    // The engine's own pre-race plan (mark-blind) + where its picks finished.
    const plan = getEngineMarkPlanForRace(raceId);
    const planByHid = {};
    (plan.assignments || []).forEach(a => { planByHid[String(a.h_id)] = a.symbol; });
    const anchorAsg = (plan.assignments || []).find(a => a.symbol === '◎');
    const anchor = anchorAsg ? scored.find(s => s.hid === String(anchorAsg.h_id)) : null;
    const winnerMark = planByHid[winner.hid] || null;

    const top3 = scored.filter(s => s.finish !== null && s.finish <= 3).sort((a, c) => a.finish - c.finish);

    return {
        raceId, risk, field, winner, statsRank, isUpset, bucket, winnerMark, anchor, top3,
        ranks: { merit: statsRank, form: formRankOf(winner), sire: sireRankOf(winner),
                 surf: surfRankOf(winner), odds: oddsRankOf(winner) },
    };
}

// The winner's factor lines, read on the engine's own ruler — "what it had."
function autopsyWinnerFactors(a) {
    const w = a.winner, field = a.field, R = a.ranks, out = [];
    const third = Math.max(1, Math.ceil(field / 3));
    const sentOf = (rank) => rank === null ? 'neu' : rank <= third ? 'pos' : rank > field - third ? 'neg' : 'neu';

    // Market position — for a WINNER, being a longshot that came through is the value story.
    if (R.odds !== null) {
        const o = Number.isFinite(w.odds) ? w.odds.toFixed(1) + '×' : '—';
        let desc;
        if (w.fav === 1) desc = `Sent off favorite at ${o} — the market’s pick delivered`;
        else if (w.fav >= 4) desc = `Longshot at ${o} (market rank ${R.odds}/${field}) — the crowd underrated it`;
        else desc = `Near-favorite at ${o} (market rank ${R.odds}/${field})`;
        out.push({ emoji: '💴', label: 'Market', desc, rank: R.odds, sent: w.fav >= 4 ? 'pos' : 'neu' });
    }
    // Recent form
    if (R.form !== null) {
        const f = w.form;
        let d;
        if (!Number.isFinite(f)) d = 'No recent-form figure';
        else if (f >= 0.65) d = 'Excellent recent form — was rounding into shape';
        else if (f >= 0.35) d = 'Solid recent form';
        else if (f >= 0.10) d = 'Mixed recent form — form alone didn’t flag it';
        else d = 'Weak recent form — not visible in the form lines';
        out.push({ emoji: '📈', label: 'Recent form', desc: `${d} (rank ${R.form}/${field})`, rank: R.form, sent: sentOf(R.form) });
    }
    // Sire fit
    if (R.sire !== null && Number.isFinite(w.sire)) {
        const adj = w.sire >= 15 ? 'Strong' : w.sire >= 9 ? 'Decent' : 'Light';
        out.push({ emoji: '🧬', label: 'Sire fit', desc: `${adj} breeding fit for today (${w.sire.toFixed(1)}%, rank ${R.sire}/${field})`, rank: R.sire, sent: sentOf(R.sire) });
    }
    // Surface fit
    if (R.surf !== null && Number.isFinite(w.surf)) {
        const adj = w.surf >= 20 ? 'Proven' : w.surf >= 10 ? 'Decent' : 'Light';
        out.push({ emoji: '🏟️', label: 'Surface fit', desc: `${adj} record on this surface (${w.surf.toFixed(1)}% win, rank ${R.surf}/${field})`, rank: R.surf, sent: sentOf(R.surf) });
    }
    // Cold-engine teaching tie-in: did the winner match a watched cold-value angle (H7)?
    const e = w.e;
    const days = (e.Days_Since_Last == null || e.Days_Since_Last === '') ? null : parseInt(e.Days_Since_Last, 10);
    if (w.fav >= 9 && days != null && days >= COLD_FRESH_LO && days <= COLD_FRESH_HI) {
        out.push({ emoji: '💧', label: 'Cold engine', rank: null, sent: 'pos',
            desc: `Fresh longshot — won off a ${days}-day break. Matches the H7 “fresh longshot” place-overlay the cold engine is watching.` });
    }
    return out;
}

function autopsyEngineLine(a) {
    if (!a.anchor) {
        return a.winnerMark
            ? `The engine had no clear ◎, but did mark the winner <b>${a.winnerMark}</b>.`
            : 'The engine abstained on this race (no clear anchor) — nothing to grade against the result.';
    }
    const an = a.anchor;
    if (an.hid === a.winner.hid) {
        return `The engine’s ◎ <b>${escapeHtml(an.name)}</b> <span class="ra-good">WON</span> — the pre-race read held up.`;
    }
    const fin = an.finish !== null ? autopsyOrdinal(an.finish) : 'unplaced';
    let tail;
    if (a.winnerMark) {
        tail = `It still flagged the winner (<b>${a.winnerMark}</b>), just not on top.`;
    } else if (an.fav <= 2 && a.winner.fav >= 4) {
        tail = 'It anchored the favorite on market position; the winner beat it on merit the short price hid.';
    } else {
        tail = 'The winner went unmarked — a different profile came through.';
    }
    return `The engine’s ◎ was <b>${escapeHtml(an.name)}</b> (finished ${fin}). ${tail}`;
}

function autopsyTakeaway(a) {
    const w = a.winner;
    switch (a.bucket) {
        case 'chalk':
            return 'A favorite won — the lesson here is in the bet STRUCTURE (what you staked and how wide), not the pick.';
        case 'catchable': {
            const best = [['form', a.ranks.form], ['breeding fit', a.ranks.sire], ['surface record', a.ranks.surf]]
                .filter(([, r]) => r === 1).map(([n]) => n);
            const lead = best.length ? `the field’s best ${best.join(' and ')}` : 'a top-tier merit profile';
            return `Catchable: the winner had ${lead} despite long odds. At a higher Risk setting the engine leans toward exactly this kind of underrated horse — a pattern worth learning to see.`;
        }
        case 'semi':
            return 'Half-readable: there was some signal here, but also real variance. Worth noting, not worth chasing.';
        case 'freak':
            return `Freak: at ${Number.isFinite(w.odds) ? w.odds.toFixed(1) + '×' : 'long odds'} the winner was weak on every signal the engine reads. Nothing could have flagged it — the honest response is cheaper, wider bets, not trying to out-pick this.`;
    }
    return '';
}

function ensureRaceAutopsyModal() {
    let m = document.getElementById('race-autopsy-modal');
    if (m) return m;
    m = document.createElement('div');
    m.id = 'race-autopsy-modal';
    m.className = 'race-autopsy-modal';
    m.style.display = 'none';
    m.addEventListener('click', (e) => { if (e.target === m) closeRaceAutopsy(); });
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeRaceAutopsy(); });
    document.body.appendChild(m);
    return m;
}

function closeRaceAutopsy() {
    const m = document.getElementById('race-autopsy-modal');
    if (m) m.style.display = 'none';
    raceAutopsyState = { raceId: null };
}

function openRaceAutopsy(raceId) {
    raceAutopsyState = { raceId };
    renderRaceAutopsy(raceId);
}

// Re-render in place if the operator moves the Risk slider while it's open
// (merit/ranks shift with risk, just like the pre-race popover).
function refreshRaceAutopsyIfOpen() {
    const m = document.getElementById('race-autopsy-modal');
    if (raceAutopsyState.raceId && m && m.style.display !== 'none') renderRaceAutopsy(raceAutopsyState.raceId);
}

function renderRaceAutopsy(raceId) {
    const m = ensureRaceAutopsyModal();
    const info = globalRaceInfo[raceId] || {};
    const title = `${trackName(info.place) || ''} R${info.race_number || ''}`.trim();

    const a = computeRaceAutopsy(raceId);
    if (!a) {
        m.innerHTML = `<div class="ra-panel"><div class="ra-head"><div class="ra-title">Why It Won — ${escapeHtml(title)}</div>
            <button class="ra-close" onclick="closeRaceAutopsy()" title="Close">✕</button></div>
            <div class="ra-empty">No finish order yet for this race — nothing to autopsy.</div></div>`;
        m.style.display = 'flex';
        return;
    }

    const bk = AUTOPSY_BUCKET[a.bucket];
    const w = a.winner;
    const wOdds = Number.isFinite(w.odds) ? w.odds.toFixed(1) + '×' : '—';

    // Result strip: top-3 finishers with their pre-race market rank.
    const resultStrip = a.top3.map(s => {
        const pos = autopsyOrdinal(s.finish);
        const fav = Number.isFinite(s.fav) && s.fav < 999 ? `${s.fav}${['st', 'nd', 'rd'][s.fav - 1] || 'th'} fav` : '—';
        const od = Number.isFinite(s.odds) ? s.odds.toFixed(1) + '×' : '';
        return `<div class="ra-fin"><span class="ra-fin-pos">${pos}</span><span class="ra-fin-name">${escapeHtml(s.name)}</span><span class="ra-fin-meta">${fav} · ${od}</span></div>`;
    }).join('');

    // Winner factor rows (mini-bar + sentiment), reusing the explainer's visual grammar.
    const factorsHtml = autopsyWinnerFactors(a).map(f => {
        let bar = '';
        if (f.rank !== null && a.field > 1) {
            const pct = (a.field - f.rank) / (a.field - 1);
            const fill = Math.round(pct * 6);
            bar = `<span class="ra-bar-filled">${'█'.repeat(fill)}</span><span class="ra-bar-empty">${'░'.repeat(6 - fill)}</span>`;
        }
        const rankStr = f.rank !== null ? `${f.rank}/${a.field}` : '';
        return `<div class="ra-factor">
            <span class="ra-factor-emoji">${f.emoji}</span>
            <div class="ra-factor-body">
                <span class="ra-factor-label">${f.label}</span>
                <span class="ra-factor-desc ra-${f.sent}">${f.desc}</span>
            </div>
            <div class="ra-factor-rank"><span class="ra-factor-bar">${bar}</span><span class="ra-factor-pos">${rankStr}</span></div>
        </div>`;
    }).join('');

    const meritStr = a.statsRank !== null ? `${a.statsRank}/${a.field}` : '—';

    m.innerHTML = `<div class="ra-panel">
        <div class="ra-head">
            <div class="ra-title">Why It Won — ${escapeHtml(title)}</div>
            <span class="ra-bucket" style="color:${bk.color};border-color:${bk.color}66;background:${bk.color}14;">${bk.label}</span>
            <button class="ra-close" onclick="closeRaceAutopsy()" title="Close">✕</button>
        </div>

        <div class="ra-winner">
            <span class="ra-winner-tag">🏆 Winner</span>
            <span class="ra-winner-name">${escapeHtml(w.name)}</span>
            <span class="ra-winner-odds">${wOdds} · ${Number.isFinite(w.fav) && w.fav < 999 ? autopsyOrdinal(w.fav) + ' favorite' : 'unranked'}</span>
            ${a.winnerMark ? `<span class="ra-winner-mark">engine: ${a.winnerMark}</span>` : ''}
        </div>

        <div class="ra-meaning" style="border-left-color:${bk.color};">${bk.meaning}</div>

        <div class="ra-section-label">Finish (with pre-race market rank)</div>
        <div class="ra-result">${resultStrip || '<div class="ra-empty-sm">Top-3 finish order unavailable.</div>'}</div>

        <div class="ra-section-label">What the winner had (engine’s own factors, ranked in the field)</div>
        <div class="ra-factors">${factorsHtml || '<div class="ra-empty-sm">No scored factors available for this horse.</div>'}</div>
        <div class="ra-merit">Odds-removed merit (form + breeding) ranked it <b>${meritStr}</b> in the field before the race.</div>

        <div class="ra-section-label">The engine’s call</div>
        <div class="ra-engine">${autopsyEngineLine(a)}</div>

        <div class="ra-takeaway">${autopsyTakeaway(a)}</div>

        <div class="ra-foot">Read at Risk ${a.risk} · a learning aid on settled results, not a betting signal. Some winners are genuinely unreadable — it will say so.</div>
    </div>`;
    m.style.display = 'flex';
}

function getCurrentAutoPickRisk(riskOverride = null) {
    const hasOverride = riskOverride !== null && riskOverride !== 'null' && riskOverride !== undefined;
    // Discipline mode replaces the slider entirely — pin to a market-trusting low risk. An explicit
    // numeric riskOverride still wins (so backtest sweeps / what-if probes can force a value).
    if (isDisciplineMode() && !hasOverride) return DISCIPLINE_RISK;

    let currentRisk = parseInt(document.getElementById('risk-slider')?.value, 10);
    if (isNaN(currentRisk)) currentRisk = 50;

    if (hasOverride) {
        const override = parseInt(riskOverride, 10);
        if (!isNaN(override)) currentRisk = override;
    }

    return currentRisk;
}

function applyAutoPickSelectionsToRace(r_id, riskOverride = null) {
    if (isRaceLocked(r_id)) {
        return { changed: false, currentRisk: getCurrentAutoPickRisk(riskOverride), reason: 'locked' };
    }

    const entries = globalRaceEntries[r_id];
    if (!entries || entries.length === 0) {
        return { changed: false, currentRisk: getCurrentAutoPickRisk(riskOverride), reason: 'empty' };
    }

    const currentRisk = getCurrentAutoPickRisk(riskOverride);

    // Phase 29 v2: the engine OWNS this race's main marks (◎〇▲△). It picks the COUNT
    // (1-6, or 0 = abstain → leave the race clean) from field shape + risk. We write the
    // plan authoritatively over the existing main marks, preserving X eliminations.
    const plan = getEngineMarkPlanForRace(r_id, { riskOverride });
    const target = {};                       // h_id → symbol (the engine's plan)
    plan.assignments.forEach(a => { target[a.h_id] = a.symbol; });

    let changed = false;
    // Remove any main mark not matching the plan (clears the race entirely on abstain).
    for (const [k, v] of Object.entries(globalMarks)) {
        if (!k.startsWith(`${r_id}_`) || !v || v === 'X') continue;
        const h = k.slice(r_id.length + 1);
        if (target[h] !== v) { globalMarks[k] = null; changed = true; }
    }
    // Apply the plan's marks.
    Object.entries(target).forEach(([h, sym]) => {
        const key = `${r_id}_${h}`;
        if (globalMarks[key] !== sym) { globalMarks[key] = sym; changed = true; }
    });

    if (changed) {
        touchRaceMeta(r_id, { markSource: 'auto-pick', riskSlider: currentRisk, engineCount: plan.count, engineShape: plan.shape });
    }

    return { changed, currentRisk, count: plan.count, shape: plan.shape,
             reason: changed ? (plan.count ? 'updated' : 'abstained') : (plan.count ? 'unchanged' : 'abstained-empty') };
}

// --- AUTO-PICK ALGORITHM ---
async function autoPick(event, r_id, riskOverride = null) {
    event.stopPropagation();

    const result = applyAutoPickSelectionsToRace(r_id, riskOverride);
    if (!result.changed) {
        updateRaceActionButtons(r_id);
        return;
    }

    await saveMarksToServer();

    raceSorts[r_id] = { col: 'Default', asc: true };
    applySortLogic(r_id, 'Default', true);
    document.getElementById(`tbody-${r_id}`).innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);
    refreshRaceHeaderSortLabels(r_id);

    updateRaceActionButtons(r_id);
    updateRiskBadge(r_id);
    updateAutoBetHighlighting();
}

// --- REORDER EXISTING PICKS ---
async function reorderPicks(event, r_id) {
    event.stopPropagation();
    if (isRaceLocked(r_id)) return;

    const entries = globalRaceEntries[r_id];
    if (!entries || entries.length === 0) return;

    const mainSymbols = ["◎", "〇", "▲", "△"];
    let markedHorses = [];

    // 1. Gather ONLY the horses that currently have a main symbol
    for (const [k, v] of Object.entries(globalMarks)) {
        if (k.startsWith(`${r_id}_`) && mainSymbols.includes(v)) {
            markedHorses.push({ key: k, h_id: k.split('_')[1] });
        }
    }

    if (markedHorses.length === 0) return;

    // The mark COUNT is preserved; reorder only re-ranks which horse gets which symbol.
    // Sequence is ◎〇▲ then △ repeated, so a 5/6-mark (nagashi) race keeps all its △.
    const markCount = markedHorses.length;
    const seq = markSequenceForCount(markCount);

    // 2. Calculate Power Score using the Slider!
    const currentRisk = getCurrentAutoPickRisk();

    let scoredHorses = entries
        .filter(row => markedHorses.some(m => m.h_id === String(row.Horse_ID).split('.')[0]))
        .map(row => {
            return { h_id: String(row.Horse_ID).split('.')[0], power: calculatePowerScore(row, currentRisk) };
        });

    scoredHorses.sort((a, b) => b.power - a.power);

    // 3. WIPE the old symbols to prevent cloning!
    markedHorses.forEach(m => {
        globalMarks[m.key] = null;
    });

    // 4. Reassign the symbols in their new, mathematically correct order!
    for (let i = 0; i < Math.min(seq.length, scoredHorses.length); i++) {
        const newKey = `${r_id}_${scoredHorses[i].h_id}`;
        globalMarks[newKey] = seq[i];
    }

    // 5. Save and instantly snap the UI into the new order
    touchRaceMeta(r_id, { markSource: 'reordered', riskSlider: currentRisk });
    await saveMarksToServer();

    raceSorts[r_id] = { col: 'Default', asc: true };
    applySortLogic(r_id, 'Default', true);
    document.getElementById(`tbody-${r_id}`).innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);
    refreshRaceHeaderSortLabels(r_id);
    updateRaceActionButtons(r_id);
    updateRiskBadge(r_id);
    updateAutoBetHighlighting();
}

// --- BET SAFETY INDICATOR ---
function getEffectiveRiskForRace(r_id) {
    const entries = globalRaceEntries[r_id];
    if (!entries || entries.length === 0) return null;

    const mainSymbols = ["◎", "〇", "▲", "△"];
    const userPickIds = [];
    for (const [k, v] of Object.entries(globalMarks)) {
        if (k.startsWith(`${r_id}_`) && mainSymbols.includes(v)) {
            userPickIds.push(k.split('_')[1]);
        }
    }
    if (userPickIds.length === 0) return null;

    const currentRisk = parseInt(document.getElementById('risk-slider').value);
    if (isNaN(currentRisk)) return null;
    const EPSILON = 1e-9;


    // For each risk level, compute "regret": how much score the user left on the
    // table vs. the ideal top-N picks at that risk level, normalized to [0,1].
    // The risk level with minimum regret is the one the user's picks best match.
    const N = userPickIds.length;
    const getRegret = (risk) => {
        const scored = entries
            .map(row => ({ h_id: String(row.Horse_ID).split('.')[0], power: calculatePowerScore(row, risk) }))
            .sort((a, b) => b.power - a.power);
        const topNSum = scored.slice(0, N).reduce((s, h) => s + h.power, 0);
        if (topNSum <= 0) return 1;
        const userSum = userPickIds.reduce((s, id) => {
            const h = scored.find(h => h.h_id === id);
            return s + (h ? Math.max(0, h.power) : 0);
        }, 0);
        return (topNSum - userSum) / topNSum;
    };

    const currentRegret = getRegret(currentRisk);

    let bestRisk = currentRisk;
    let bestRegret = Infinity;
    for (let risk = 0; risk <= 100; risk += 1) {
        const regret = getRegret(risk);
        if (
            regret < bestRegret - EPSILON ||
            (Math.abs(regret - bestRegret) <= EPSILON && Math.abs(risk - currentRisk) < Math.abs(bestRisk - currentRisk))
        ) {
            bestRegret = regret;
            bestRisk = risk;
        }
    }

    // Positive picksDelta means the picks are riskier than the slider target.
    return { bestRisk, currentRisk, currentRegret, picksDelta: bestRisk - currentRisk };
}

// Returns the exact symbol placements the auto-pick logic would make at current slider value.
function getAutoBetPreviewAssignmentsForRace(r_id) {
    const entries = globalRaceEntries[r_id];
    if (!entries || entries.length === 0) return [];

    const mainSymbols = ["◎", "〇", "▲", "△"];
    const usedSymbols = [];
    const blockedHorseIds = [];
    for (const [k, v] of Object.entries(globalMarks)) {
        if (!k.startsWith(`${r_id}_`) || !v) continue;
        blockedHorseIds.push(k.split('_')[1]);
        if (mainSymbols.includes(v)) {
            usedSymbols.push(v);
        }
    }

    const availableSymbols = mainSymbols.filter(symbol => !usedSymbols.includes(symbol));
    if (availableSymbols.length === 0) return [];

    const currentRisk = getCurrentAutoPickRisk();

    const scored = entries
        .filter(row => !blockedHorseIds.includes(String(row.Horse_ID).split('.')[0]))
        .map(row => ({ h_id: String(row.Horse_ID).split('.')[0], power: calculatePowerScore(row, currentRisk) }))
        .sort((a, b) => b.power - a.power);

    const assignments = [];
    for (let i = 0; i < Math.min(availableSymbols.length, scored.length); i++) {
        assignments.push({ h_id: scored[i].h_id, symbol: availableSymbols[i] });
    }

    return assignments;
}

// Interpolates color from yellow (on target) toward red (riskier) or cyan (safer)
function getDeviationColor(delta) {
    const t = Math.max(-100, Math.min(100, delta)) / 100; // -1 to +1
    if (t >= 0) {
        // yellow #f9ca24 → red #ff0000
        const r = 249 + Math.round((255 - 249) * t);
        const g = Math.round(202 * (1 - t));
        const b = Math.round(36  * (1 - t));
        return `rgb(${r},${g},${b})`;
    } else {
        // yellow #f9ca24 → cyan #0abde3
        const abs = -t;
        const r = Math.round(249 * (1 - abs) + 10  * abs);
        const g = Math.round(202 * (1 - abs) + 189 * abs);
        const b = Math.round(36  * (1 - abs) + 227 * abs);
        return `rgb(${r},${g},${b})`;
    }
}

function updateRiskBadge(r_id) {
    const badge = document.getElementById(`risk-badge-${r_id}`);
    if (!badge) return;

    // Risk-alignment ("On Target / Riskier / Safer") is a pre-bet aid — meaningless once the
    // race is settled. Hide it in review mode.
    if (raceIsSettledForAutopsy(r_id)) {
        badge.style.display = 'none';
        return;
    }

    if (!(appConfig.ui?.betSafetyIndicator ?? true)) {
        badge.style.display = 'none';
        return;
    }

    const result = getEffectiveRiskForRace(r_id);
    if (result === null) {
        badge.style.display = 'none';
        return;
    }

    const { bestRisk, currentRisk, currentRegret, picksDelta } = result;
    const absDelta = Math.abs(picksDelta);
    const color = getDeviationColor(picksDelta);
    const impliedRiskText = `Implied auto-risk: ${bestRisk}`;

    let text, title;
    if (currentRegret <= 1e-9 || absDelta <= 10) {
        text  = "✓ On Target";
        title = `Your picks align well with the slider (Risk ${currentRisk}). ${impliedRiskText}.`;
    } else if (picksDelta > 0) {
        text  = `▲ Riskier`;
        title = `Your picks are riskier than your slider target (slider: ${currentRisk}, picks ~${bestRisk}). ${impliedRiskText}.`;
    } else {
        text  = `▼ Safer`;
        title = `Your picks are safer than your slider target (slider: ${currentRisk}, picks ~${bestRisk}). ${impliedRiskText}.`;
    }

    badge.style.display = 'inline-block';
    badge.style.color = color;
    badge.style.borderColor = color;
    badge.title = title;
    badge.textContent = `⚡ ${text}`;
}

function updateAllRiskBadges() {
    Object.keys(globalRaceEntries).forEach(r_id => updateRiskBadge(r_id));
}

// Returns the engine's unconditional top-4 picks for a race, ignoring any existing marks.
// Used by the Engine Picks sidebar and agreement-% stats — intentionally mark-blind.
// Self-invalidating memo: the unconditional ranking only changes when the risk slider moves or the
// race's entries are reloaded. Keying on (risk, entriesRef) means a data reload (new array) or a risk
// change recomputes automatically — no manual clear. Collapses the several calls per race the grid +
// side-bet strip make each render into ONE calculatePowerScore pass (a big post-fetch render win).
const _unconditionalRankCache = new Map(); // r_id -> { risk, entriesRef, result }
function getUnconditionalAutoBetRankingsForRace(r_id) {
    const entries = globalRaceEntries[r_id];
    if (!entries || entries.length === 0) return [];
    const currentRisk = getCurrentAutoPickRisk();
    const cached = _unconditionalRankCache.get(r_id);
    if (cached && cached.risk === currentRisk && cached.entriesRef === entries) return cached.result;
    const symbols = ['◎', '〇', '▲', '△'];
    const result = entries
        .map(row => ({ h_id: String(row.Horse_ID).split('.')[0], power: calculatePowerScore(row, currentRisk) }))
        .sort((a, b) => b.power - a.power)
        .slice(0, symbols.length)
        .map((e, i) => ({ h_id: e.h_id, symbol: symbols[i] }));
    _unconditionalRankCache.set(r_id, { risk: currentRisk, entriesRef: entries, result });
    return result;
}

// ─── Phase 29 v2: Mark-Count Engine ─────────────────────────────────────────
// Decides HOW MANY marks (0-6; 0 = abstain) the engine wants on a race, from the
// field SHAPE (power-score gaps) modulated by the RISK slider, then assigns
// ◎〇▲△△△ (truncated to N) to the top-N runners. N auto-selects the OrePro
// template downstream (1=複勝 … 6=◎-nagashi×5) — the engine's only job is the count.
// Built to counter "bet every race and lose": the abstention rules return 0 on
// races that don't fit the risk stance, so bulk-apply skips them.
// All thresholds are named constants — tune from live results (SLIDER_TUNING.md).
const ENGINE_TUNING = {
    MIN_FIELD: 8,         // fields smaller than this → abstain (too random to model)
    STANDOUT_GAP: 0.30,   // relative p1−p2 gap that marks a "dominant favorite"
    AXIS_GAP: 0.22,       // min relative p1−p2 gap to allow a nagashi axis (counts 5-6)
    BREAK_MIN: 0.18,      // a relative gap ≥ this counts as a real "break" in the field
    SHORT_FAV_ODDS: 1.8,  // ◎ shorter than this + no overlay → skip at low/mid risk
    OVERLAY_LO: 8,        // a top-ranked runner priced in [LO, HI] = a longshot the
    OVERLAY_HI: 70,       //   engine likes → "overlay/value present" in the race
    SAFE_MAX: 35,         // risk ≤ this = SAFE stance
    CHAOS_MIN: 65,        // risk ≥ this = CHAOS stance
    // Default-OrePro safety-net mode: the marginal (4th, then 3rd) mark is TRIMMED if its
    // win odds exceed this risk-scaled ceiling (a "true longshot" the engine won't back).
    OREPRO_TRIM_BASE: 8,  // odds ceiling at risk 0 (≈ trim anything longer than 8:1)
    OREPRO_TRIM_SPAN: 40, // ceiling rises by SPAN × (risk/100); CHAOS keeps all (∞)
};

// The engine's full decision for a race: { count, assignments:[{h_id,symbol}],
// reason, shape, detail }. Mark-blind (reads the whole field); callers that must
// respect existing user marks use getMarkAwareAutoBetRankingsForRace below.
function getEngineMarkPlanForRace(r_id, opts = {}) {
    const T = ENGINE_TUNING;
    const empty = (reason) => ({ count: 0, assignments: [], reason, shape: reason, detail: null });
    const entries = globalRaceEntries[r_id];
    if (!entries || entries.length === 0) return empty('empty');

    const risk = getCurrentAutoPickRisk(opts.riskOverride ?? null);
    const r = Math.max(0, Math.min(100, risk)) / 100;
    const fieldSize = entries.length;

    // Score & sort every runner (mark-blind shape read).
    const scored = entries.map(row => {
        const odds = parseFloat(row.Odds);
        return {
            h_id: String(row.Horse_ID).split('.')[0],
            power: calculatePowerScore(row, risk),
            odds: (Number.isFinite(odds) && odds > 0) ? odds : null,
        };
    }).sort((a, b) => b.power - a.power);

    // Abstention: tiny field — too much variance to bet into. EXCEPTION (operator pref): when the
    // resolved bet type is the small-field TOKEN (set by the Auto per-race chooser), don't sit it out —
    // throw a minimal 2-mark bet on the top 2 by power, provided there are ≥2 runners and odds to rank
    // them. Otherwise abstain as before.
    if (fieldSize < T.MIN_FIELD) {
        let smallComp = opts.compositionOverride;
        if (smallComp === undefined) { try { smallComp = resolveBetComposition(r_id); } catch (_) { smallComp = null; } }
        const smallHasOdds = scored.some(e => e.odds !== null);
        if (isSmallFieldTokenComposition(smallComp) && fieldSize >= 2 && smallHasOdds) {
            const syms = ['◎', '〇'];
            const assignments = scored.slice(0, 2).map((e, i) => ({ h_id: e.h_id, symbol: syms[i] }));
            return { count: 2, assignments, reason: 'small-field-token', shape: 'small-field-token',
                     detail: { smallField: true, fieldSize } };
        }
        return empty('small-field');
    }

    // ── Shape read from the top-6 power gaps ─────────────────────────────────
    const K = Math.min(6, scored.length);
    const top = scored.slice(0, K);
    const spread = top[0].power - top[K - 1].power;
    const wideOpen = !(spread > 0);                  // fully flat (featureless maiden)

    const relGap = [];
    for (let i = 0; i < K - 1; i++) relGap.push(spread > 0 ? (top[i].power - top[i + 1].power) / spread : 0);

    const standout = relGap[0] || 0;                 // favorite dominance (p1−p2, relative)
    let breakIdx = 0, breakVal = -1;
    for (let i = 0; i < Math.min(4, relGap.length); i++) {
        if (relGap[i] > breakVal) { breakVal = relGap[i]; breakIdx = i; }
    }
    const naturalCount = breakVal >= T.BREAK_MIN ? breakIdx + 1 : 0; // 0 = tight pack, no break
    const openPackBehindStandout = standout >= T.STANDOUT_GAP &&
        relGap.slice(1).every(g => g < T.BREAK_MIN);  // clear ◎ but no second break behind

    // ── Value / overlay signals ──────────────────────────────────────────────
    const favOdds = top[0].odds;
    const underpricedFav = favOdds !== null && favOdds < T.SHORT_FAV_ODDS;
    const hasOverlay = top.some(e => e.odds !== null && e.odds >= T.OVERLAY_LO && e.odds <= T.OVERLAY_HI);
    const hasAxis = standout >= T.AXIS_GAP && favOdds !== null;

    // ── Shape classification (no count yet) ──────────────────────────────────
    let shape;
    if (wideOpen)                                              shape = 'wide-open';
    else if (openPackBehindStandout)                          shape = 'standout+pack';
    else if (naturalCount === 1 && standout >= T.STANDOUT_GAP) shape = 'lone-favorite';
    else if (naturalCount === 2)                              shape = 'two-clear';
    else if (naturalCount === 3)                              shape = 'tight-top3';
    else                                                      shape = 'tight-pack';

    // ── Count = PRESET BAND → RISK POSITION → SHAPE NUDGE (Phase 34) ──────────
    // Three inputs stack, each owning a different layer so they can't fight:
    //   1. the active bet PRESET sets a hard count band (min–max = a fence the count can't escape);
    //   2. the RISK slider sets the position inside that band (low = fewer/safer, high = wider);
    //   3. the field SHAPE nudges ±1 within the band (a lone favorite → fewer; a flat pack → more).
    // The chosen bet STRUCTURE then decides what these N marks form (see buildRaceBetLines).
    const zone = risk <= T.SAFE_MAX ? 'SAFE' : risk >= T.CHAOS_MIN ? 'CHAOS' : 'BLEND';

    // Pre-odds: SAFE/BLEND both rely on market prices to know who is chalk vs value.
    // CHAOS only needs form signals, so it still runs. Return abstain so the highlight
    // overlay stays blank and the user isn't shown picks that have no odds basis.
    const hasOdds = scored.some(e => e.odds !== null);
    if (!hasOdds && zone !== 'CHAOS') return empty('no-odds');

    // Resolve the active preset → its plan {min,max,tilt,requireAxis}. Defensive: this runs inside
    // the bulk Auto-Bet-Day sweep, so a throw here must never abort the day → fall back to a
    // neutral default plan on any failure.
    let comp = null, plan = PRESET_PLANS_DEFAULT;
    try { comp = opts.compositionOverride || resolveBetComposition(r_id); plan = presetPlanForComposition(comp); }
    catch (e) { console.warn('getEngineMarkPlanForRace: preset-plan lookup failed for', r_id, e); }

    // Nagashi (and any requireAxis preset) needs a genuine ◎ standout to bank on. No axis → abstain
    // rather than suggest a bankerless nagashi (operator's call: strategy-abstain is nagashi-only).
    if (plan.requireAxis && !hasAxis) return empty('nagashi-no-axis');

    // Shape nudge: a dominant single trims toward the low end; a flat/packed field widens toward the
    // top. The nudge can never break the preset fence (the clamp below enforces that).
    let shapeNudge = 0;
    if (shape === 'lone-favorite' || shape === 'two-clear') shapeNudge = -1;
    else if (shape === 'tight-pack' || shape === 'wide-open') shapeNudge = +1;

    // Count: band bottom + risk-scaled position + shape nudge, clamped to the fence and field size.
    const band = Math.max(0, plan.max - plan.min);
    let target = plan.min + Math.round(r * band) + shapeNudge;
    target = Math.max(plan.min, Math.min(plan.max, target));
    target = Math.min(target, fieldSize);

    // Preset-floor contract gate (unchanged intent): a count below the composition's mark floor
    // can't form a single line of the preset → abstain rather than suggest unplaceable marks.
    let presetFloor = 1;
    try { const f = compositionMarkFloor(comp); if (Number.isFinite(f) && f >= 1) presetFloor = f; }
    catch (e) { console.warn('getEngineMarkPlanForRace: preset-floor lookup failed for', r_id, e); }
    if (presetFloor > target) return empty('below-preset-floor');

    // Selection TILT: the ◎ banker is ALWAYS your top conviction pick (raw-risk #1) — the preset
    // never moves your banker. The tilt only re-leans the SUPPORTING marks: consistency presets
    // (tilt<0) rank them with a chalk-leaning risk, ceiling presets (tilt>0) lean toward overlays.
    // Count uses the RAW risk above; only the supporting order uses the tilted risk, so the two
    // knobs stay independent. tilt 0 → reuse the neutral `scored` order (no second scoring pass).
    const axis = scored[0];
    let supporting = scored.slice(1);
    if (plan.tilt !== 0) {
        const selRisk = Math.max(0, Math.min(100, risk + plan.tilt));
        supporting = entries.map(row => {
            const odds = parseFloat(row.Odds);
            return { h_id: String(row.Horse_ID).split('.')[0], power: calculatePowerScore(row, selRisk),
                     odds: (Number.isFinite(odds) && odds > 0) ? odds : null };
        }).filter(e => e.h_id !== axis.h_id).sort((a, b) => b.power - a.power);
    }
    const ordered = [axis, ...supporting];

    const seq = markSequenceForCount(target);
    const assignments = [];
    for (let i = 0; i < seq.length && i < ordered.length; i++) {
        assignments.push({ h_id: ordered[i].h_id, symbol: seq[i] });
    }
    return {
        count: target, assignments, reason: shape, shape,
        detail: { standout: +standout.toFixed(2), naturalCount, hasAxis, hasOverlay, underpricedFav, wideOpen,
                  preset: plan.id, band: [plan.min, plan.max], tilt: plan.tilt, shapeNudge, target },
    };
}

// Abstain reasons that mean "this BET TYPE doesn't fit this race" (vs genuinely unbettable). The
// abstain-backup preset rescues only these — shared by the live preview and the Auto Bet Day sweep.
const ENGINE_PRESET_FIT_ABSTAINS = new Set(['nagashi-no-axis', 'below-preset-floor']);

// ── Phase 35: per-race bet-type auto-selection ────────────────────────────────
// Chosen as a DAY preset ("🧪 Auto — engine picks per race" in the day dropdown). When active it
// REPLACES the day-wide preset: each race's bet TYPE is chosen from its field SHAPE, then the engine
// sizes the marks for that preset. Reuses the abstain-backup's tagged-override machinery
// (setAutoBackupOverride) so it self-corrects each sweep and never clobbers a manual override or a
// locked race. Manual/locked are skipped upstream.
const SHAPE_TO_PRESET = {
    'lone-favorite': 'win_place',     // one dominant horse → back the one
    'two-clear':     'quinella_wide', // two clear of the rest → the pair
    'standout+pack': 'nagashi_chase', // clear ◎, open chasers → axis + spread
    'tight-top3':    'trio_chase',    // three clustered on top → trio box + wide net
    'tight-pack':    'trio_chase',    // packed, no clean break → chase the cluster
    'wide-open':     'wide_safe',     // flat / no read → the safest net
};
// On when the race's DAY preset is the "Auto (per race)" choice (per-date, from the day composition).
function isAutoBetTypePerRace(rid) {
    const date = (rid && globalRaceInfo[rid]?.clean_date) || currentActiveDate || '';
    return isAutoPerRaceComposition(getDayBetComposition(date));
}
// The presetId the per-race engine would choose for this race, or null (mode off / no usable read).
// Reads the field SHAPE via a neutral probe (balanced never preset-abstains on the six field shapes)
// so the choice doesn't depend on whichever day preset is active; genuine skips (tiny field / no
// odds / empty card) leave shape un-mapped → null, so those races stay unbet as before.
function autoBetTypePresetForRace(r_id) {
    if (!isAutoBetTypePerRace(r_id)) return null;
    // A MANUAL per-race bet override always wins — never replace the operator's explicit choice.
    // (Engine-created overrides carry the betCompositionAutoBackup tag; those are re-decided freely.)
    if (getRaceBetCompositionOverride(r_id) && !isAutoBackupOverride(r_id)) return null;
    const probe = getEngineMarkPlanForRace(r_id, { compositionOverride: compositionFromPreset('balanced') });
    // Small field: don't sit it out — throw the token 2-mark Quinella+Wide on the top 2 (operator pref).
    // The engine plan still enforces ≥2 runners + odds; if it can't form, it abstains and the sweep skips.
    if (probe.shape === 'small-field') return SMALL_FIELD_TOKEN_ID;
    return SHAPE_TO_PRESET[probe.shape] || null;
}

// Live preview of the engine plan WITH the abstain-backup preset applied transiently (no committed
// per-race override). If the active preset abstains because the bet type doesn't fit, and a backup
// preset is set + would produce a pick, returns the BACKUP plan flagged viaBackup. Lets the live
// highlight overlay + abstain pill SHOW the backup before the operator runs Auto Bet Day.
function getLiveEnginePlanWithBackup(r_id) {
    // Phase 35: experimental per-race mode REPLACES the day preset — plan with the shape-chosen
    // preset so the highlight overlay + pill preview the bet type each race will actually get.
    const perRaceId = autoBetTypePresetForRace(r_id);
    if (perRaceId) {
        const pplan = getEngineMarkPlanForRace(r_id, { compositionOverride: compositionForAutoPreset(perRaceId) });
        if (pplan.count > 0) return { plan: pplan, viaPerRace: true, perRaceId };
        return { plan: pplan, viaBackup: false };  // genuine skip → no preview pick
    }
    const plan = getEngineMarkPlanForRace(r_id);
    if (plan.count > 0) return { plan, viaBackup: false };
    const backup = getAbstainBackupPreset();
    if (backup === 'none' || !ENGINE_PRESET_FIT_ABSTAINS.has(plan.shape)) return { plan, viaBackup: false };
    const bplan = getEngineMarkPlanForRace(r_id, { compositionOverride: compositionFromPreset(backup) });
    return bplan.count > 0 ? { plan: bplan, viaBackup: true, backupId: backup } : { plan, viaBackup: false };
}

// Mark-aware view of the engine plan: same target COUNT, but respects marks the user
// already placed (keeps their singletons, counts their △) and fills only the remaining
// slots from unmarked horses. Used by the highlight overlay. Engine abstains → [].
// Applies the abstain-backup preset as a live preview so backup races highlight too.
function getMarkAwareAutoBetRankingsForRace(r_id) {
    const entries = globalRaceEntries[r_id];
    if (!entries || entries.length === 0) return [];

    const plan = getLiveEnginePlanWithBackup(r_id).plan;
    if (plan.count === 0) return [];                     // engine abstains → highlight nothing
    const seq = markSequenceForCount(plan.count);
    const currentRisk = getCurrentAutoPickRisk();

    // What the user has already marked.
    const takenSingles = new Set();   // which of ◎〇▲ the user already used
    let userTriangles = 0;
    const markedHorses = new Set();
    entries.forEach(row => {
        const h_id = String(row.Horse_ID).split('.')[0];
        const mark = globalMarks[`${r_id}_${h_id}`];
        if (!mark || mark === 'X') return;
        markedHorses.add(h_id);
        if (mark === '△') userTriangles++; else takenSingles.add(mark);
    });

    // Slots the engine still needs to fill.
    const needSingles = ['◎', '〇', '▲'].filter((s, i) => i < seq.length && !takenSingles.has(s));
    const triCount = seq.filter(s => s === '△').length;
    const needTriangles = Math.max(0, triCount - userTriangles);
    if (needSingles.length === 0 && needTriangles === 0) return [];

    // Unmarked horses by power, longshot-tolerant (graded ceiling; see SLIDER_TUNING.md).
    //   risk 0 → ceiling ~12   risk 50 → ~30   risk 80 → ~75   risk 100 → ∞
    const r = Math.max(0, Math.min(100, currentRisk)) / 100;
    const oddsCeiling = currentRisk >= 100 ? Infinity : 12 + Math.pow(r, 2) * 240;
    const pool = entries
        .map(row => ({ h_id: String(row.Horse_ID).split('.')[0], power: calculatePowerScore(row, currentRisk), isLongshot: (parseFloat(row.Odds) || 9999) > oddsCeiling }))
        .filter(e => !markedHorses.has(e.h_id))
        .sort((a, b) => {
            if (a.isLongshot !== b.isLongshot) return a.isLongshot ? 1 : -1;
            return b.power - a.power;
        });

    const result = [];
    let idx = 0;
    for (const s of needSingles) { if (idx >= pool.length) break; result.push({ h_id: pool[idx++].h_id, symbol: s }); }
    for (let t = 0; t < needTriangles && idx < pool.length; t++) result.push({ h_id: pool[idx++].h_id, symbol: '△' });
    return result;
}

// TRADITIONAL_ROLES: assign each mark by structural role rather than linear power rank.
//   Slider zones: SAFE (<40) / BLEND (40-60) / CHAOS (>60). BLEND defers to BOX_OPT.
//   Fallback per spec: if a role filter finds no candidate, use the BOX_OPT pick for that slot.
function getTraditionalRoleAssignments(r_id) {
    const entries = globalRaceEntries[r_id];
    if (!entries || entries.length === 0) return [];

    const risk = getCurrentAutoPickRisk();
    const zone = riskZone(risk);

    // BLEND zone: SAFE and CHAOS rules diverge — defer to BOX_OPT (mark-aware) for the whole race.
    if (zone === 'BLEND') return getMarkAwareAutoBetRankingsForRace(r_id);

    const symbols = ['◎', '〇', '▲', '△'];

    // Score every entry once. NinkiDelta + burned-fav signals come from parsed Last3 runs.
    const scored = entries.map(row => {
        const runs = parseLast3Runs(row.Last3);
        return {
            h_id: String(row.Horse_ID).split('.')[0],
            row,
            power: calculatePowerScore(row, risk),
            favRank: parseInt(row.Fav, 10) || null,
            runs,
            deltaPos: ninkiDeltaMaxPositive(runs),
            burnedDelta: mostRecentBurnedDelta(runs),
        };
    });

    // Track user-applied marks so the engine doesn't overwrite the user's hand-picks.
    const takenSymbols = new Set();
    const markedHorses = new Set();
    entries.forEach(row => {
        const h_id = String(row.Horse_ID).split('.')[0];
        const mark = globalMarks[`${r_id}_${h_id}`];
        if (mark) {
            markedHorses.add(h_id);
            if (symbols.includes(mark)) takenSymbols.add(mark);
        }
    });
    if (takenSymbols.size === symbols.length) return [];

    const assigned = new Set();
    const result = [];
    const pool = () => scored.filter(e => !markedHorses.has(e.h_id) && !assigned.has(e.h_id));

    // The BOX_OPT fallback pick — highest unassigned power score.
    const topByPower = (p) => p.slice().sort((a, b) => b.power - a.power)[0] || null;

    // Field spread for abstention: if role picker returns null and the remaining pool
    // is too tightly clustered, skip the mark rather than forcing a weak BOX_OPT pick.
    const allPower = scored.map(e => e.power).sort((a, b) => b - a);
    const fieldSpread = allPower.length >= 2 ? allPower[0] - allPower[Math.min(allPower.length - 1, 5)] : 0;

    const pick = (symbol, picker) => {
        if (takenSymbols.has(symbol)) return;
        const p = pool();
        let candidate = picker(p);
        if (!candidate) {
            if (fieldSpread > 0) {
                const sorted = p.slice().sort((a, b) => b.power - a.power);
                if (sorted.length >= 2 && (sorted[0].power - sorted[1].power) / fieldSpread < 0.03) return;
            }
            candidate = topByPower(p);
        }
        if (candidate) {
            assigned.add(candidate.h_id);
            result.push({ h_id: candidate.h_id, symbol });
        }
    };

    // ◎ Honmei
    pick('◎', p => {
        if (zone === 'SAFE') return p.find(e => e.favRank === 1) || null;
        // CHAOS: absolute highest raw score from our custom Form/Pedigree model.
        return topByPower(p);
    });

    // 〇 Taiko
    pick('〇', p => {
        if (zone === 'SAFE') return p.find(e => e.favRank === 2) || null;
        return topByPower(p);
    });

    // ▲ Dark Horse — value sleeper via positive NinkiFinishDelta.
    // CHAOS doubles the delta weight to aggressively fish for longshot snipers.
    pick('▲', p => {
        const mult = zone === 'CHAOS' ? 2.0 : 1.0;
        const ranked = p
            .map(e => ({ ref: e, score: e.deltaPos * mult }))
            .filter(x => x.score > 0)
            .sort((a, b) => b.score - a.score);
        return ranked[0]?.ref || null;
    });

    // △ Longshot
    pick('△', p => {
        if (zone === 'SAFE') return topByPower(p); // safe ticket filler
        // CHAOS: redemption-arc longshot — outside top-5 fav today, burned-fav in most-recent
        // run, with high rolling jockey or trainer place% (latent ability the market dropped).
        const candidates = p
            .filter(e => e.favRank !== null && e.favRank > 5)
            .filter(e => e.burnedDelta !== null)
            .map(e => {
                const j = parseFloat(e.row.Jockey_Place_Pct) || 0;
                const t = parseFloat(e.row.Trainer_Place_Pct) || 0;
                return { ref: e, consistency: Math.max(j, t) };
            })
            .filter(x => x.consistency > 0)
            .sort((a, b) => b.consistency - a.consistency);
        return candidates[0]?.ref || null;
    });

    return result;
}

// Phase 34 (partial): flag races that HAVE marks but whose marks can't form ANY line of the active
// preset (e.g. 1 mark on Trio chase, floor 2) → the bet will NOT place and Apply will skip it.
// Surfaces it in the race header so the operator can switch preset or place a custom bet instead of
// being silently skipped. Recomputed on every mark/preset change (called from updateAutoBetHighlighting).
function updateWontPlaceBadges() {
    Object.keys(globalRaceEntries).forEach(r_id => {
      try {
        const meta = document.getElementById(`header-meta-${r_id}`);
        if (!meta) return;
        const existing = meta.querySelector('.wont-place-badge');
        let show = false, floorN = 0, presetName = '';
        if (countRaceMarks(r_id) > 0) {
            const race = findRaceObjById(r_id);
            if (race) {
                const built = buildRaceBetLines(race);
                if (!(built.lines || []).length) {
                    show = true;
                    const comp = resolveBetComposition(r_id);
                    floorN = compositionMarkFloor(comp);
                    presetName = compositionLabel(comp);
                }
            }
        }
        if (show) {
            const title = `This race's marks can't form any line of "${presetName}" (needs ≥${floorN} marks). ` +
                          `It will be SKIPPED at Apply — switch the preset or place a custom bet.`;
            const html = ` <span class="wont-place-badge" title="${escapeHtml(title)}" ` +
                `style="display:inline-block;font-size:0.72em;font-weight:700;padding:1px 7px;margin-left:6px;border-radius:10px;` +
                `background:#7a1f1f;color:#ffd9d9;border:1px solid #c0392b;vertical-align:middle;">⚠ won't place (need ≥${floorN})</span>`;
            if (existing) existing.outerHTML = html; else meta.insertAdjacentHTML('beforeend', html);
        } else if (existing) {
            existing.remove();
        }
      } catch (e) {
        console.warn('updateWontPlaceBadges: skipped race', r_id, e);
      }
    });
}

// Phase 34: flag races where the ENGINE abstains (no auto-pick) for an ACTIONABLE reason — a nagashi
// with no clear axis to bank on, or a field the preset can't fit — so the operator can spot them and
// place a different bet type by hand. Mark-blind (the engine's own view), independent of any marks
// already placed. Deliberately skips transient/no-data reasons ('no-odds' = odds not published yet,
// 'empty' = no card) which aren't actionable and would otherwise pill every pre-odds race. Skips a
// race already showing the red "won't place" badge so we never stack two pills saying the same thing.
const ENGINE_ABSTAIN_PILL = {
    'nagashi-no-axis':    'no clear axis horse to bank a nagashi on',
    'below-preset-floor': 'the engine wants fewer marks than this bet type needs',
    'small-field':        'field too small to model confidently',
};
// ── Cold-engine "value" PREVIEW (s52) — INFORMATIONAL ONLY, never bets or marks anything. ──
// Surfaces the backtest-derived longshot edges (tuning_hypotheses.md H7/H8) on each race header so
// the operator can eyeball them live during the ≥3-weekend confirmation window. Point-in-time inputs
// come from the s52 backend fields Days_Since_Last / Last_Surface / Last_Distance (ETag races-v11);
// on an older payload these are undefined and the whole thing silently no-ops. Two rules:
//   💧 VALUE (candidate back): a longshot (odds rank ≥9) returning FRESH (61–120 days off) that is NOT
//      switching onto dirt → the place overlay (H7, cleaned by H8).
//   🚫 TRAP (candidate fade): any horse switching ONTO dirt at mid/long odds (rank ≥4) → the market
//      underrates the surface switch; these crater (H8).
const COLD_FRESH_LO = 61, COLD_FRESH_HI = 120;
// s52: per-horse cold-value PREVIEW pill (H7/H8) — INFORMATIONAL ONLY, bets/marks nothing. Returns the
// inline pill HTML for one runner (or '' when no flag / toggled off / older payload missing the fields).
// Built straight into the row (see buildTableBody's Horse cell) so it survives every re-render/reprice.
//   💧 fresh — a longshot (rank ≥9) back from a 61–120 day break, NOT switching to dirt → place candidate (H7).
//   🚫 dirt  — switching ONTO dirt at mid/long odds (rank ≥4) → a fade; the market underrates it (H8).
// s54: predicted running style on the UPCOMING card — Lead/Press/Close/Deep inferred from the
// horse's LAST start's corner positions (backend field Last_Perf = that run's PerformanceJson).
// A handicapping read on paper ("this one usually leads"). Thresholds mirror horseRunStyle() in
// the profile; kept separate so the card pill has its own "predicted, from last start" tooltip.
function predictedStylePill(r_id, row) {
    const perf = row.Last_Perf;
    if (!perf) return '';
    let p = perf;
    if (typeof perf === 'string') { try { p = JSON.parse(perf); } catch { return ''; } }
    if (!p || !Array.isArray(p.corners)) return '';
    const pos = p.corners.map(c => parseInt(c, 10)).filter(n => Number.isFinite(n) && n > 0);
    if (!pos.length) return '';
    const early = pos[0];
    // Normalize early position by field size. The last run's own field size isn't on the card, so use
    // THIS race's field size as a proxy (JRA fields are similar enough for a reading-aid read); without
    // it, blank early corners in sprints make everyone look like a deep closer.
    const fieldN = (globalRaceEntries[r_id] || []).length;
    const n = fieldN > 1 ? fieldN : Math.max(...pos);
    const ratio = early / n;
    let label, color;
    if (early <= 1)        { label = 'Lead';  color = '#ff6b9d'; }
    else if (ratio <= 0.30){ label = 'Press'; color = '#ffc04a'; }
    else if (ratio <= 0.66){ label = 'Close'; color = '#6cc6ff'; }
    else                   { label = 'Deep';  color = '#ff7b6b'; }
    const tip = `Predicted running style from its last start — ${label}. Lead = front-runner · Press = stalker · ` +
        'Close = off-pace · Deep = deep closer (from the horse’s corner positions last time).';
    const base = 'display:inline-block;font-size:0.7em;font-weight:700;padding:0 6px;border-radius:8px;' +
                 'vertical-align:middle;white-space:nowrap;';
    return `<span class="run-style-pill" title="${escapeHtml(tip)}" style="${base}color:${color};border:1px solid ${color}66;background:${color}1a;">${label}</span>`;
}

function coldValuePillForRow(r_id, row) {
    if (appConfig.ui?.coldValuePreview === false) return '';
    // The FACTS (switch-to-dirt, fresh off a break) are known as soon as the card loads; only the
    // VALUE judgment (is it a longshot?) needs odds. So show a dimmed "pending" chip on the factual
    // setup before odds post, then brighten it to the real cold-engine flag once the price confirms
    // the longshot gate — or drop it if it prices short (resolved to "not a value signal").
    const fav = parseInt(row.Fav, 10);
    const hasOdds = Number.isFinite(fav) && fav >= 1;
    const todaySurface = String(globalRaceInfo[r_id]?.surface || '').toLowerCase();
    const lastSurface = String(row.Last_Surface || '').toLowerCase();
    const days = (row.Days_Since_Last == null || row.Days_Since_Last === '') ? null : parseInt(row.Days_Since_Last, 10);
    const toDirt = lastSurface && lastSurface !== 'jump' && todaySurface === 'dirt' && lastSurface !== 'dirt';
    const isFresh = days != null && days >= COLD_FRESH_LO && days <= COLD_FRESH_HI && !toDirt;
    const base = 'display:inline-block;font-size:0.7em;font-weight:700;margin-left:5px;padding:0 6px;' +
                 'border-radius:8px;vertical-align:middle;white-space:nowrap;';
    const pill = (label, colors, title, pending) =>
        ` <span class="cold-value-pill${pending ? ' cold-pending' : ''}" title="${escapeHtml(title)}" style="${base}${colors}${pending ? 'opacity:0.4;' : ''}">${label}</span>`;

    if (toDirt) {
        const colors = 'background:#3a1e1e;color:#ffc9c9;border:1px solid #a84444;';
        if (hasOdds && fav >= 4)
            return pill('🚫 dirt', colors,
                'Cold-value PREVIEW (informational — bets nothing): switching ONTO dirt at mid/long odds — a fade. ' +
                'The market underrates the surface switch and these crater (H8). Edge NOT confirmed yet.', false);
        if (!hasOdds)
            return pill('🚫 dirt', colors,
                'Switching ONTO dirt from its last start — a setup the cold engine FADES (H8). Dimmed until odds ' +
                'post; brightens if it prices as 4th choice or longer (otherwise it’s not the fade).', true);
        return ''; // priced as a short favorite → not the fade
    }
    if (isFresh) {
        const colors = 'background:#10303a;color:#aee9ff;border:1px solid #2f86a8;';
        if (hasOdds && fav >= 9)
            return pill(`💧 fresh ${days}d`, colors,
                `Cold-value PREVIEW (informational — bets nothing): fresh longshot — back from a ${days}-day break at ` +
                'long odds, not switching to dirt. Candidate PLACE overlay (H7). Watch live; NOT confirmed yet.', false);
        if (!hasOdds)
            return pill(`💧 fresh ${days}d`, colors,
                `Coming off a ${days}-day break — the cold engine’s fresh-longshot PLACE overlay (H7). Dimmed until ` +
                'odds post; brightens if it prices as 9th choice or longer (otherwise it’s not the overlay).', true);
        return ''; // priced shorter than 9th choice → not the overlay
    }
    return '';
}

function updateEngineAbstainBadges() {
    Object.keys(globalRaceEntries).forEach(r_id => {
      try {
        const meta = document.getElementById(`header-meta-${r_id}`);
        if (!meta) return;
        const existing = meta.querySelector('.engine-abstain-badge, .engine-backup-badge');
        let html = '';
        if (isAutoBackupOverride(r_id)) {
            const label = compositionLabel(getRaceBetCompositionOverride(r_id));
            if (isAutoBetTypePerRace(r_id)) {
                // Phase 35: this race's bet type was auto-chosen from its field shape (per-race mode).
                const title = `Auto bet-type mode chose "${label}" for this race from its field shape. ` +
                              `Clear the per-race bet override to undo.`;
                html = ` <span class="engine-backup-badge" title="${escapeHtml(title)}" ` +
                    `style="display:inline-block;font-size:0.72em;font-weight:700;padding:1px 7px;margin-left:6px;border-radius:10px;` +
                    `background:#14361f;color:#b9f0c9;border:1px solid #2f8f57;vertical-align:middle;">🧪 auto: ${escapeHtml(label)}</span>`;
            } else {
                // Rescued by Auto Bet Day: the main bet type didn't fit, so it was bet with the backup.
                const title = `Auto Bet Day rescued this race: your main bet type didn't fit, so it was bet ` +
                              `with "${label}" instead. Clear the per-race bet override to undo.`;
                html = ` <span class="engine-backup-badge" title="${escapeHtml(title)}" ` +
                    `style="display:inline-block;font-size:0.72em;font-weight:700;padding:1px 7px;margin-left:6px;border-radius:10px;` +
                    `background:#1f3a5a;color:#cfe6ff;border:1px solid #2f6fb0;vertical-align:middle;">↩ backup: ${escapeHtml(label)}</span>`;
            }
        } else if (!meta.querySelector('.wont-place-badge')) {
            const live = getLiveEnginePlanWithBackup(r_id);
            if (live.viaPerRace) {
                // Phase 35 preview: per-race mode WILL bet this race as the shape-chosen type on the next run.
                const label = live.perRaceId === SMALL_FIELD_TOKEN_ID
                    ? 'Small-field 2-bet (Q+Wide)'
                    : (BET_PRESETS[live.perRaceId]?.label || 'auto');
                const title = `Auto bet-type mode will bet this race as "${label}" (chosen from its field shape) ` +
                              `when you run Auto Bet Day. (Preview — not committed yet.)`;
                html = ` <span class="engine-backup-badge" title="${escapeHtml(title)}" ` +
                    `style="display:inline-block;font-size:0.72em;font-weight:700;padding:1px 7px;margin-left:6px;border-radius:10px;` +
                    `background:#14361f;color:#b9f0c9;border:1px solid #2f8f57;vertical-align:middle;">🧪 auto: ${escapeHtml(label)} (preview)</span>`;
            } else if (live.viaBackup) {
                // Preview: main preset abstained, but the backup preset would bet this race.
                const label = BET_PRESETS[live.backupId]?.label || 'backup';
                const title = `Your main bet type doesn't fit this race. The backup preset "${label}" will be ` +
                              `used here when you run Auto Bet Day. (Preview — not committed yet.)`;
                html = ` <span class="engine-backup-badge" title="${escapeHtml(title)}" ` +
                    `style="display:inline-block;font-size:0.72em;font-weight:700;padding:1px 7px;margin-left:6px;border-radius:10px;` +
                    `background:#1f3a5a;color:#cfe6ff;border:1px solid #2f6fb0;vertical-align:middle;">↩ backup: ${escapeHtml(label)} (preview)</span>`;
            } else if (live.plan.count === 0 && ENGINE_ABSTAIN_PILL[live.plan.shape]) {
                const title = `The auto-pick engine has no suggestion here (${ENGINE_ABSTAIN_PILL[live.plan.shape]}). ` +
                              `Consider a different bet type or a manual/custom bet for this race.`;
                html = ` <span class="engine-abstain-badge" title="${escapeHtml(title)}" ` +
                    `style="display:inline-block;font-size:0.72em;font-weight:700;padding:1px 7px;margin-left:6px;border-radius:10px;` +
                    `background:#5a4a1a;color:#ffe9b0;border:1px solid #c9a227;vertical-align:middle;">⚙ engine: no pick</span>`;
            }
        }
        if (html) {
            if (existing) existing.outerHTML = html; else meta.insertAdjacentHTML('beforeend', html);
        } else if (existing) {
            existing.remove();
        }
      } catch (e) {
        console.warn('updateEngineAbstainBadges: skipped race', r_id, e);
      }
    });
}

function updateAutoBetHighlighting() {
    document.querySelectorAll('.mark-btn.auto-bet-preview').forEach(btn => btn.classList.remove('auto-bet-preview'));
    updateWontPlaceBadges();      // always — independent of the highlight toggle
    updateEngineAbstainBadges();  // always — the operator wants to see abstains regardless of the toggle

    if (!isAutoBetHighlightingEnabled()) return;

    const mode = getVotingMarkMode();
    Object.keys(globalRaceEntries).forEach(r_id => {
        const assignments = mode === 'TRADITIONAL_ROLES'
            ? getTraditionalRoleAssignments(r_id)
            : getMarkAwareAutoBetRankingsForRace(r_id);
        assignments.forEach(({ h_id, symbol }) => {
            const btn = document.getElementById(`btn_${r_id}_${h_id}_${symbol}`);
            if (btn) btn.classList.add('auto-bet-preview');
        });
    });
}

function normalizeRacesPayload(data) {
    return {
        upcoming: data.upcoming_races_by_date || data.races_by_date || {},
        past: data.past_races_by_date || {}
    };
}

function syncUpcomingRefreshButtonState() {
    const btn = document.getElementById('btn-upcoming-refresh');
    if (!btn) return;
    btn.dataset.action = 'legacy-refresh';
    btn.textContent = '🛰️ Update Upcoming Cards';
}

function getSortedActiveDates() {
    return Object.keys(globalRacesByDate).sort();
}

// Phase 38: every race day the calendar should expose — loaded days ∪ skeleton days.
// Used for month availability + day highlighting; the tab/switch logic still keys off
// the loaded-only getSortedActiveDates() and lazy-loads a skeleton day when opened.
function getAllCalendarDates() {
    return [...new Set([
        ...Object.keys(globalRacesByDate),
        ...Object.keys(globalCalendarSkeleton)
    ])].sort();
}

// The day to show first on a fresh load: the earliest UPCOMING day, else the most recent PAST day.
// Read from the lightweight calendar skeleton so we can pick WITHOUT having loaded any day's detail.
function pickInitialActiveDate() {
    const days = Object.keys(globalCalendarSkeleton || {});
    if (!days.length) return null;
    const upcoming = days.filter(d => globalCalendarSkeleton[d]?.timeline === 'upcoming').sort();
    if (upcoming.length) return upcoming[0];
    const past = days.filter(d => globalCalendarSkeleton[d]?.timeline !== 'upcoming').sort();
    return past.length ? past[past.length - 1] : days.sort()[0];
}

function getMonthKey(dateStr) {
    return dateStr ? String(dateStr).slice(0, 7) : null;
}

function getAvailableCalendarMonths() {
    return [...new Set(getAllCalendarDates().map(getMonthKey).filter(Boolean))].sort();
}

// Phase 38: fetch the lightweight calendar skeleton (all race days, no entries) so the
// calendar can highlight and navigate to any day; heavy detail loads on demand per day.
async function loadCalendarSkeleton() {
    try {
        const res = await fetch('/api/races/calendar', { cache: 'no-cache' });
        if (!res.ok) return;
        const data = await res.json();
        const next = {};
        (data.days || []).forEach(d => {
            if (d && d.date) next[d.date] = { count: d.count || 0, timeline: d.timeline || 'past' };
        });
        globalCalendarSkeleton = next;
    } catch (e) {
        // Non-fatal — calendar degrades to loaded-only days
    }
}

function formatCalendarMonth(monthKey) {
    if (!monthKey) return '';
    const [year, month] = monthKey.split('-').map(Number);
    return new Date(Date.UTC(year, month - 1, 1)).toLocaleDateString('en-US', {
        month: 'long',
        year: 'numeric',
        timeZone: 'UTC'
    });
}

function formatActiveDateLabel(dateStr) {
    if (!dateStr) return 'No day selected';
    const [year, month, day] = String(dateStr).split('-').map(Number);
    if (!year || !month || !day) return dateStr;
    return new Date(Date.UTC(year, month - 1, day)).toLocaleDateString('en-US', {
        weekday: 'short',
        month: 'short',
        day: 'numeric',
        timeZone: 'UTC'
    });
}

function updateActiveDateNavigator() {
    const dates = getAllCalendarDates();
    const labelEl = document.getElementById('active-date-label');
    const metaEl = document.getElementById('active-date-meta');
    const prevBtn = document.getElementById('active-date-prev');
    const nextBtn = document.getElementById('active-date-next');
    if (!labelEl || !metaEl || !prevBtn || !nextBtn) return;

    if (!dates.length || !currentActiveDate) {
        labelEl.textContent = 'No day selected';
        metaEl.textContent = '0 races';
        prevBtn.disabled = true;
        nextBtn.disabled = true;
        return;
    }

    const currentIndex = dates.indexOf(currentActiveDate);
    const safeIndex = currentIndex >= 0 ? currentIndex : 0;
    const raceCount = globalRacesByDate[currentActiveDate]?.length || 0;
    labelEl.textContent = formatActiveDateLabel(currentActiveDate);
    metaEl.textContent = `${raceCount} race${raceCount === 1 ? '' : 's'}`;
    prevBtn.disabled = safeIndex <= 0;
    nextBtn.disabled = safeIndex >= dates.length - 1;
}

function shiftActiveDate(step) {
    const dates = getAllCalendarDates();
    if (!dates.length || !currentActiveDate) return;
    const currentIndex = dates.indexOf(currentActiveDate);
    const safeIndex = currentIndex >= 0 ? currentIndex : 0;
    const nextIndex = Math.min(dates.length - 1, Math.max(0, safeIndex + step));
    if (nextIndex === safeIndex) return;
    switchMainTab(dates[nextIndex]);
}

function toggleCalendarPopover(event) {
    if (event) event.stopPropagation();
    const popover = document.getElementById('calendar-popover');
    if (!popover) return;
    const isOpen = popover.style.display !== 'none';
    popover.style.display = isOpen ? 'none' : 'block';
    if (!isOpen) {
        // Re-render so the popover always reflects the latest calendar state.
        renderRaceCalendar();
        // Close on first outside click after opening.
        setTimeout(() => {
            document.addEventListener('click', closeCalendarPopoverOnOutside, { once: true });
        }, 0);
    }
}

function closeCalendarPopoverOnOutside(event) {
    const popover = document.getElementById('calendar-popover');
    const btn = document.getElementById('active-date-calendar-btn');
    if (!popover) return;
    if (popover.contains(event.target) || (btn && btn.contains(event.target))) {
        // Re-arm: stay open, but listen again for the next outside click.
        document.addEventListener('click', closeCalendarPopoverOnOutside, { once: true });
        return;
    }
    popover.style.display = 'none';
}

function findNearestAvailableDate(targetDate, dates) {
    if (!targetDate || !Array.isArray(dates) || dates.length === 0) return null;
    if (dates.includes(targetDate)) return targetDate;

    for (const date of dates) {
        if (date >= targetDate) return date;
    }

    return dates[dates.length - 1];
}

function renderRaceCalendar() {
    const calendar = document.getElementById('race-calendar');
    if (!calendar) return;

    const dates = getSortedActiveDates();
    const months = getAvailableCalendarMonths();

    if (!months.length) {
        calendar.innerHTML = '<div class="race-calendar-empty-note">No race days loaded.</div>';
        updateActiveDateNavigator();
        return;
    }

    const selectedDate = findNearestAvailableDate(currentActiveDate, dates) || dates[0] || null;
    currentActiveDate = selectedDate;
    currentTimelineTab = selectedDate ? (globalDateTimelineByDate[selectedDate] || 'upcoming') : 'upcoming';

    const selectedMonth = getMonthKey(selectedDate);
    if (!currentCalendarMonth || !months.includes(currentCalendarMonth)) {
        currentCalendarMonth = selectedMonth || months[0];
    }

    const monthIndex = months.indexOf(currentCalendarMonth);
    const monthKey = monthIndex >= 0 ? months[monthIndex] : months[0];
    currentCalendarMonth = monthKey;

    const [year, month] = monthKey.split('-').map(Number);
    const firstDay = new Date(Date.UTC(year, month - 1, 1));
    const leadingBlanks = firstDay.getUTCDay();
    const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
    const weekdays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const cells = [];

    for (let i = 0; i < leadingBlanks; i += 1) {
        cells.push('<div class="race-calendar-cell is-empty"></div>');
    }

    for (let day = 1; day <= daysInMonth; day += 1) {
        const dateStr = `${monthKey}-${String(day).padStart(2, '0')}`;
        const loaded = globalRacesByDate[dateStr];
        const skel = globalCalendarSkeleton[dateStr];

        if (!loaded && !skel) {
            cells.push(`
                <div class="race-calendar-cell" title="${dateStr}">
                    <div class="race-calendar-daynum" style="padding: 8px; color: #4b5565;">${day}</div>
                </div>
            `);
            continue;
        }

        // Skeleton days (not yet loaded) get a subtle cue and lazy-load on click.
        const count = loaded ? loaded.length : skel.count;
        const timeline = loaded ? (globalDateTimelineByDate[dateStr] || 'upcoming') : (skel.timeline || 'past');
        const skeletonClass = loaded ? '' : ' is-skeleton';
        const activeClass = dateStr === currentActiveDate ? ' is-selected' : '';
        cells.push(`
            <button type="button" class="race-calendar-day timeline-${timeline}${activeClass}${skeletonClass}" onclick="selectCalendarDate('${dateStr}')" title="${dateStr} \u2022 ${count} race${count === 1 ? '' : 's'}${loaded ? '' : ' \u2022 click to load'}">
                <div class="race-calendar-daynum">${day}</div>
                <div class="race-calendar-meta">
                    <span class="race-calendar-count">${count}</span>
                </div>
            </button>
        `);
    }

    const selectedCount = globalRacesByDate[currentActiveDate]?.length || 0;

    calendar.innerHTML = `
        <div class="race-calendar-header">
            <button type="button" class="race-calendar-nav" onclick="changeCalendarMonth(-1)" ${monthIndex <= 0 ? 'disabled' : ''}>◀</button>
            <div class="race-calendar-heading">
                <div class="race-calendar-title">${formatCalendarMonth(monthKey)}</div>
                <div class="race-calendar-summary">Selected: ${currentActiveDate} • ${selectedCount} race${selectedCount === 1 ? '' : 's'}</div>
            </div>
            <button type="button" class="race-calendar-nav" onclick="changeCalendarMonth(1)" ${monthIndex >= months.length - 1 ? 'disabled' : ''}>▶</button>
        </div>
        <div class="race-calendar-weekdays">${weekdays.map(day => `<div class="race-calendar-weekday">${day}</div>`).join('')}</div>
        <div class="race-calendar-grid">${cells.join('')}</div>
    `;
    updateActiveDateNavigator();
}

function changeCalendarMonth(step) {
    const months = getAvailableCalendarMonths();
    if (!months.length) return;

    const currentIndex = Math.max(0, months.indexOf(currentCalendarMonth));
    const nextIndex = Math.min(months.length - 1, Math.max(0, currentIndex + step));
    const nextMonth = months[nextIndex];
    if (!nextMonth) return;

    currentCalendarMonth = nextMonth;
    const monthDates = getSortedActiveDates().filter(date => getMonthKey(date) === nextMonth);
    if (monthDates.length) {
        switchMainTab(monthDates[0]);
    } else {
        renderRaceCalendar();
    }
}

async function selectCalendarDate(date) {
    // Phase 38: a day that's highlighted from the skeleton but not yet loaded — pull its
    // heavy detail on demand, rebuild the tab shells, then activate it.
    if (date && !globalRacesByDate[date] && globalCalendarSkeleton[date]) {
        const loaded = await loadRaceDay(date);
        if (loaded) {
            renderDayTabsAndSchedules(date);
            renderWeekendWatchlist();
            renderVoteHistory();
            renderEnginePicks();
            updateQuickStats();
            return;
        }
    }
    switchMainTab(date);
}

// Render one date's race cards into its pre-existing tab shell.
// Called immediately for the active date; called on demand for others via switchMainTab.
function renderDateTab(date, collapseBeforeTime = null, keepOpenRaceId = null) {
    if (renderedDates.has(date)) return;
    const tabEl = document.getElementById(`tab-${date}`);
    if (!tabEl) return;
    renderedDates.add(date);

    const dateTimeline = globalDateTimelineByDate[date] || 'upcoming';
    const races = globalRacesByDate[date] || [];
    let html = '';

    races.forEach(race => {
        const r_id = race.info.race_id;

        let shouldCollapse = false;
        if (
            dateTimeline === 'upcoming' &&
            isFirstLoad &&
            collapseBeforeTime &&
            race.info.time !== "TBA" &&
            race.info.sort_time
        ) {
            // Prefer the unambiguous +09:00 form; bare sort_time + AM/PM display time
            // confuses parseRaceSortTime's CT-heuristic and shifts races ~14h.
            const raceTime = parseRaceSortTime(race.info.sort_time_iso || race.info.sort_time, race.info);
            if (raceTime && raceTime < collapseBeforeTime && r_id !== keepOpenRaceId) {
                shouldCollapse = true;
            }
        }

        const arrow = shouldCollapse ? "▶" : "▼";
        const collapsedClass = shouldCollapse ? "collapsed" : "";

        applySortLogic(r_id, raceSorts[r_id].col, raceSorts[r_id].asc);

        let hasBld = false;
        let hasWatch = false;
        let hasMixed = false;
        let maxIntensity = 0;
        globalRaceEntries[r_id].forEach(row => {
            if (!row.familyTracking) {
                row.familyTracking = calculateFamilyTracking(row.Horse_ID, row.Sire_ID, row.Dam_ID, row.BMS_ID);
            }

            const tracking = row.familyTracking;
            const weights = tracking?.weights || { bld_weight: 0, watch_weight: 0 };

            if (tracking.isMixed) hasMixed = true;
            if (weights.bld_weight > 0) hasBld = true;
            if (weights.watch_weight > 0) hasWatch = true;
            if (tracking.intensity > maxIntensity) maxIntensity = tracking.intensity;
        });

        const rowsHtml = buildTableBody(r_id, globalRaceEntries[r_id]);

        let headerClass = "race-header";
        if (hasWatch) headerClass += " has-watch";
        else if (hasMixed) headerClass += " row-mixed";
        else if (hasBld) headerClass += " has-bld";

        if (maxIntensity > 0) {
            if (maxIntensity <= 0.33) headerClass += " intensity-light";
            else if (maxIntensity <= 0.50) headerClass += " intensity-medium";
            else if (maxIntensity <= 0.66) headerClass += " intensity-strong";
            else headerClass += " intensity-very-strong";
        }

        let usedCount = 0;
        const mainSymbols = ["◎", "〇", "▲", "△"];
        for (const [k, v] of Object.entries(globalMarks)) {
            if (k.startsWith(`${r_id}_`) && mainSymbols.includes(v)) usedCount++;
        }

        const isLocked = isRaceLocked(r_id);
        // A race with results is in REVIEW mode — betting controls (Auto / Clear / Smart Sort)
        // are moot, so hide them even when the DATE is still classified 'upcoming' (today's races
        // stay 'upcoming' until the JST day rolls over, so a settled race would otherwise keep them).
        const raceSettled = raceIsSettledForAutopsy(r_id);
        const autoStyle = (raceSettled || usedCount >= 4) ? "display: none;" : "display: inline-block;";
        const reorderStyle = (!raceSettled && usedCount >= 4 && !isLocked) ? "display: inline-block;" : "display: none;";
        const lockLabel = isLocked ? "🔓 Unlock Bets" : "🔒 Lock Bets";
        const lockClass = isLocked ? " is-locked" : "";
        const clearStyle = (!raceSettled && countRaceMarks(r_id) > 0) ? "display: inline-block;" : "display: none;";

        const localName = localizeRaceName(race.info.race_name) || localizeRaceClass(race.info.race_class);
        const winBadgesHtml = buildRaceWinBadgesHtml(race);
        const historyBtnHtml = dateTimeline === 'past' && !raceHasHistoryData(race)
            ? `<button class="btn-history-refresh" onclick="refreshRaceHistory(event, '${r_id}')" title="Fetch finish positions and result data for this race">📜 Update History</button>`
            : "";
        // Post-race teaching: the winner's grade (Chalk/Catchable/Semi/Freak) — click for why.
        // Gate on the race being SETTLED (a real winner exists), not on the date being 'past' —
        // so the "why it won" pill appears the moment a race finishes, even on today's card.
        let autopsyBtnHtml = "";
        if (raceSettled && raceHasHistoryData(race)) {
            let label = 'Result', color = '#ffb454', emoji = '🔍', grade = '';
            try {
                const a = computeRaceAutopsy(r_id);
                if (a) { const bk = AUTOPSY_BUCKET[a.bucket]; label = bk.short; color = bk.color; emoji = bk.emoji; grade = ` (graded ${bk.short})`; }
            } catch (_) {}
            autopsyBtnHtml = `<button class="btn-why-won" style="border-color:${color};color:${color};" onclick="event.stopPropagation(); openRaceAutopsy('${r_id}')" title="Post-race teaching${grade} — why the winner beat the field, read on the engine's own factors. Click for the breakdown.">${emoji} ${label}</button>`;
        }
        // Odds-trend graph (Phase 37): only for upcoming/live cards, where odds history accrues.
        const trendsBtnHtml = dateTimeline !== 'past'
            ? `<button class="btn-odds-trends" onclick="event.stopPropagation(); showOddsHistory('${r_id}')" title="Odds over time for every runner">📈 Trends</button>`
            : "";
        // Devil's Advocate export (Phase 36): upcoming/live only — copies a prompt + JSON for any LLM.
        const exportBtnHtml = dateTimeline !== 'past'
            ? `<button class="btn-ai-export" onclick="event.stopPropagation(); exportRaceForAI('${r_id}')" title="Copy a devil's-advocate prompt + data for Claude/ChatGPT">🤖 Export for AI</button>`
            : "";
        // Slider-tuning export (dev-only): a compact prose-free sweep of the engine's marks
        // across slider positions, for tuning the scoring curve. No LLM persona — just numbers.
        const tuneBtnHtml = (dateTimeline !== 'past' && isDevModeEnabled())
            ? `<button class="btn-ai-export dev-only" onclick="event.stopPropagation(); exportRaceForTuning('${r_id}')" title="Copy a prose-free slider sweep (engine marks at 0/25/50/75/99/100) for tuning">🎚️ Tuning Export</button>`
            : "";
        // Dev-only: build this race's COMPOSED custom ticket and drop it in the OrePro cart (NO submit).
        // For validating the custom-bet pipeline before it's wired into the live Apply flow.
        const devBetBtnHtml = (dateTimeline !== 'past' && isDevModeEnabled())
            ? `<button id="btn-devbet-${r_id}" class="btn-ai-export dev-only" onclick="event.stopPropagation(); placeCustomBetNoSubmit(event, '${r_id}')" title="DEV: place this race's composed custom ticket into the OrePro cart (no submit) for review">🧪 Place Custom (no submit)</button>`
            : "";

        html += `<div id="race-${r_id}" style="margin-bottom: 25px;">
            <h3 id="header-${r_id}" class="${headerClass} ${collapsedClass}" onclick="toggleRace('${r_id}')">
                <span id="arrow-${r_id}" class="collapse-arrow">${arrow}</span> <span id="header-meta-${r_id}">${raceStatusEmoji(race)} ${race.info.time} | ${trackName(race.info.place)} R${race.info.race_number}: ${localName}${raceSurfaceDistChip(race.info)} ${winBadgesHtml}</span>

                ${historyBtnHtml}
                ${autopsyBtnHtml}
                ${trendsBtnHtml}
                ${exportBtnHtml}
                ${tuneBtnHtml}
                ${devBetBtnHtml}

                <span class="autopick-group auto-group-${r_id}" style="${autoStyle}">
                    <button class="btn-autopick auto-group-${r_id}" onclick="autoPick(event, '${r_id}', null)" title="Auto-pick using the sidebar risk slider — hover for Safe / Lucky" ${isLocked ? 'disabled' : ''}>🎲 Auto ▾</button>
                    <span class="autopick-flyout">
                        <button class="btn-autopick-safe auto-group-${r_id}" onclick="autoPick(event, '${r_id}', 20)" title="Force Risk to 20 (safer, fewer longshots)" ${isLocked ? 'disabled' : ''}>🛡️ Safe Bet</button>
                        <button class="btn-autopick-lucky auto-group-${r_id}" onclick="autoPick(event, '${r_id}', 75)" title="Force Risk to 75 (longshot upside)" ${isLocked ? 'disabled' : ''}>🍀 Lucky</button>
                    </span>
                </span>
                <button id="btn-clear-${r_id}" class="btn-clear-bets" style="${clearStyle}" onclick="clearRaceBets(event, '${r_id}')" title="Clear all marks in this race" ${isLocked ? 'disabled' : ''}>🧹 Clear Bets</button>
                <button id="btn-lock-${r_id}" class="btn-lock-bets${lockClass}" onclick="toggleRaceLock(event, '${r_id}')" title="${isLocked ? 'Unlock to allow mark changes' : 'Lock to prevent any mark changes in this race'}">${lockLabel}</button>

                <button id="btn-reorder-${r_id}" class="btn-reorder" style="${reorderStyle}" onclick="reorderPicks(event, '${r_id}')" title="Reorder Chosen Picks" ${isLocked ? 'disabled' : ''}>✨ Smart Sort</button>
                <span id="risk-badge-${r_id}" class="risk-badge" style="display:none;" onclick="event.stopPropagation()"></span>
            </h3>
            <div id="content-${r_id}" class="race-content ${collapsedClass}">
                <div id="bets-strip-${r_id}">${sideBetStripHtml(r_id)}</div>
                <table class="${dateTimeline === 'past' && (appConfig.ui?.cleanPastRaceCards ?? true) ? 'past-race' : ''}">
                    <thead id="thead-${r_id}">${buildTableHeaderRow(r_id)}</thead>
                    <tbody id="tbody-${r_id}">${rowsHtml}</tbody>
                </table>
            </div>
        </div>`;
    });

    tabEl.innerHTML = html;

    if (isDevModeEnabled()) {
        const el = document.getElementById('dev-paint-time');
        if (el && !el.dataset.paintRecorded) {
            el.dataset.paintRecorded = '1';
            const totalMs = Math.round(performance.now());
            el.textContent = `⚡ json:${_devFetchMs}ms state:${_devStateMs}ms sidebar:${_devSidebarMs}ms render:${_devRenderMs}ms total:${totalMs}ms`;
            console.log(`[DevMode] Breakdown: json=${_devFetchMs}ms state=${_devStateMs}ms sidebar=${_devSidebarMs}ms render=${_devRenderMs}ms total=${totalMs}ms`);
            bootMark('firstPaint');
        }
    }
}

// Dev-only: print the boot-phase timeline (scriptStart → initComplete) as both deltas-from-previous
// (how long THIS phase took) and deltas-from-scriptStart (running total) — answers "what is `total`
// actually waiting on" without re-measuring on every guess. Call once init() resolves.
function printBootBreakdown() {
    if (!_bootMarks.length) return;
    const t0 = _bootMarks[0].t;
    let prev = t0;
    const rows = _bootMarks.map(m => {
        const row = { phase: m.label, sincePrevMs: m.t - prev, sinceStartMs: m.t - t0 };
        prev = m.t;
        return row;
    });
    console.log('[BootTiming] phase breakdown (ms):');
    console.table(rows);
}

function renderDayTabsAndSchedules(preferredDate = null, collapseBeforeTime = null, keepOpenRaceId = null) {
    // Build tab shells for EVERY race day (loaded ∪ skeleton) so the date arrows / calendar can reach
    // any day; only the loaded active day renders content now, the rest lazy-load when opened.
    const dates = getAllCalendarDates();
    const scheds = document.getElementById('schedules-container');
    renderedDates.clear();
    scheds.innerHTML = "";

    // Reset dev paint timing for this render cycle
    const paintEl = document.getElementById('dev-paint-time');
    if (paintEl) { paintEl.textContent = ''; delete paintEl.dataset.paintRecorded; }

    if (dates.length === 0) {
        currentActiveDate = null;
        renderRaceCalendar();
        updateWinningVotesFocusButton();
        scheds.innerHTML = `<div class="tab-content active"><div style="color:#888; font-size:14px; text-align:center; padding:30px 10px;">No race days available.</div></div>`;
        return;
    }

    let activeDate = findNearestAvailableDate(preferredDate, dates)
        || findNearestAvailableDate(currentActiveDate, dates)
        || dates[0];

    currentActiveDate = activeDate;
    currentTimelineTab = globalDateTimelineByDate[activeDate] || currentTimelineTab;
    currentCalendarMonth = getMonthKey(activeDate) || currentCalendarMonth;
    renderRaceCalendar();
    refreshDayBetStructure(); // load + reflect this day's bet structure

    // Cheap init for ALL dates: locks, index tags, sort state, race class.
    // Runs fast (no HTML building) so global state is ready before any tab renders.
    dates.forEach(date => {
        // Skeleton-only (not-yet-loaded) days have no timeline yet — take it from the skeleton so the
        // date label/coloring is right before the day's detail is fetched.
        if (!globalDateTimelineByDate[date]) {
            globalDateTimelineByDate[date] = globalCalendarSkeleton[date]?.timeline || 'upcoming';
        }
        const dateTimeline = globalDateTimelineByDate[date] || 'upcoming';
        (globalRacesByDate[date] || []).forEach(race => {
            const r_id = race.info.race_id;
            if (isAutoLockPastVotesEnabled() && dateTimeline === 'past') {
                raceLocks[r_id] = true;
            }
            // Restore a persisted lock (sunk-cost basis: a locked race = a placed bet).
            // lockStateAtSave is saved into the marks blob but historically was never
            // re-applied on load, so locks evaporated on refresh. Now they survive.
            if (globalRaceMeta[r_id]?.lockStateAtSave === true) {
                raceLocks[r_id] = true;
            }
            race.entries.forEach((row, idx) => { row.original_index = idx; row._raceId = r_id; });
            globalRaceEntries[r_id] = race.entries;
            globalRaceClass[r_id] = raceClassFlags(race?.info?.race_class);
            if (!raceSorts[r_id]) {
                raceSorts[r_id] = { col: globalSort.col, asc: globalSort.asc };
            }
        });
    });

    // Build all tab shell divs in one innerHTML pass — no race content yet, just the wrappers.
    scheds.innerHTML = dates.map(date =>
        `<div id="tab-${date}" class="tab-content${date === activeDate ? ' active' : ''}"></div>`
    ).join('');

    // Render only the active date now; all others render on demand in switchMainTab.
    renderDateTab(activeDate, collapseBeforeTime, keepOpenRaceId);

    updateWinningVotesFocusButton();
    updateLiveViewPopoutAvailability();
    if (winningVotesFocusEnabled) {
        applyWinningVotesFocus();
    }
    if (currentMainView === 'voting') {
        renderLiveViewPanel();
    }
}

// Populate the in-memory race globals from a {upcoming:{date:[races]}, past:{...}} map.
// Shared by the full load (loadRaces) and the Phase 38 lazy per-day load (loadRaceDay).
// Idempotent per race_id (entry preload is gated), so re-merging an already-loaded date
// is a no-op for entries; callers only pass not-yet-loaded dates to avoid list dupes.
function populateGlobalsFromTimeline(allByDate) {
    ["upcoming", "past"].forEach(timeline => {
        const byDate = allByDate[timeline] || {};
        Object.keys(byDate).forEach(date => {
            if (!globalRacesByDate[date]) {
                globalRacesByDate[date] = byDate[date];
                globalDateTimelineByDate[date] = timeline;
            } else {
                // Same JST date appears in both timelines (race day: some past, some upcoming).
                // Append without duplicating; keep 'upcoming' as the date-level label.
                const existingIds = new Set(globalRacesByDate[date].map(r => r.info.race_id));
                byDate[date].forEach(r => {
                    if (!existingIds.has(r.info.race_id)) globalRacesByDate[date].push(r);
                });
            }

            // Sort merged array so earlier races (finished) appear first.
            if (globalRacesByDate[date].length > 1) {
                globalRacesByDate[date].sort((a, b) => {
                    const tA = a.info.sort_time_iso || a.info.sort_time || '';
                    const tB = b.info.sort_time_iso || b.info.sort_time || '';
                    return tA < tB ? -1 : tA > tB ? 1 : 0;
                });
            }

            byDate[date].forEach(race => {
                const r_id = race.info.race_id;

                // Preload entries for all timelines so cross-timeline features (export)
                // work immediately without requiring the user to switch tabs first.
                if (!globalRaceEntries[r_id]) {
                    race.entries.forEach((row, idx) => {
                        if (row.original_index === undefined) {
                            row.original_index = idx;
                        }
                        row._raceId = r_id;
                    });
                    globalRaceEntries[r_id] = race.entries;
                    globalRaceClass[r_id]   = raceClassFlags(race?.info?.race_class);
                }

                globalRaceInfo[r_id] = { ...race.info, _timeline: timeline };

                race.entries.forEach(row => {
                    searchableHorses.push({
                        name: row.Horse,
                        date: date,
                        r_id: r_id,
                        h_id: String(row.Horse_ID).split('.')[0],
                        track: trackName(race.info.place),
                        r_num: race.info.race_number,
                        timeline: timeline
                    });
                });

                // Prefer the unambiguous +09:00 form; bare sort_time + AM/PM display time
                // confuses parseRaceSortTime's CT-heuristic and shifts races ~14h.
                const raceTime = parseRaceSortTime(race.info.sort_time_iso || race.info.sort_time, race.info);
                if (timeline === "upcoming" && race.info.time !== "TBA" && raceTime) {
                    upcomingRaces.push({
                        time: raceTime,
                        name: `${trackName(race.info.place)} R${race.info.race_number}`,
                        r_id: r_id
                    });
                }
            });
        });
    });
}

// Phase 38: lazy-load one JST race day's full detail on demand (calendar navigation to a
// day outside the initial 14-day window). Fetches /api/races?date=, merges into the globals
// via the shared populate path, then rebuilds tab shells so the day can render. Returns true
// if the day now has data loaded.
async function loadRaceDay(date) {
    if (!date) return false;
    if (globalRacesByDate[date]) return true; // already loaded
    try {
        const res = await fetch(`/api/races?date=${encodeURIComponent(date)}`, { cache: 'no-store' });
        if (!res.ok) return false;
        const data = applyTimeDisplayToRacesPayload(await res.json().catch(() => ({})));
        const timelineData = normalizeRacesPayload(data);
        const dayByDate = { upcoming: timelineData.upcoming || {}, past: timelineData.past || {} };
        // Fold into the master map so a later full re-render still sees this day.
        ["upcoming", "past"].forEach(tl => {
            Object.keys(dayByDate[tl]).forEach(d => {
                globalAllRacesByDate[tl] = globalAllRacesByDate[tl] || {};
                if (!globalAllRacesByDate[tl][d]) globalAllRacesByDate[tl][d] = dayByDate[tl][d];
            });
        });
        populateGlobalsFromTimeline(dayByDate);
        upcomingRaces.sort((a, b) => a.time - b.time);
        return !!globalRacesByDate[date];
    } catch (e) {
        console.warn('loadRaceDay failed for', date, e);
        return false;
    }
}

// --- RENDER DASHBOARD ---
async function loadRaces() {
    const t0 = performance.now();
    appendDebugLine('loadRaces started (lazy per-day)');

    // Reset cached structures for a clean rebuild.
    upcomingRaces = [];
    searchableHorses = [];
    globalRaceEntries = {};
    globalRaceClass = {};
    globalRaceInfo = {};
    globalRacesByDate = {};
    globalDateTimelineByDate = {};
    globalAllRacesByDate = { upcoming: {}, past: {} };

    // The calendar skeleton (cheap: date + count + timeline, NO entry data) tells us every race day.
    // refreshDataAndUI loads it just before us, but ensure it's present so we can pick a day to show.
    if (!globalCalendarSkeleton || !Object.keys(globalCalendarSkeleton).length) {
        await loadCalendarSkeleton();
    }

    // LAZY LOAD — the big win on a slow link. Fetch ONLY the day we're about to show (first load: the
    // earliest upcoming day, else the most recent past day; a refresh: whatever day you're on). Every
    // OTHER day stays a lightweight skeleton entry and pulls its heavy detail on demand when opened
    // (switchMainTab / selectCalendarDate / the date arrows). First paint moves ~1 day (~30 KB) instead
    // of the whole 14-day window (~450 KB+).
    const keepCurrent = !isFirstLoad && currentActiveDate && globalCalendarSkeleton[currentActiveDate];
    const initialDate = keepCurrent ? currentActiveDate : pickInitialActiveDate();

    const _fetchT0 = performance.now();
    if (initialDate) await loadRaceDay(initialDate); // fetch + populate globals for just this one day
    _devFetchMs = Math.round(performance.now() - _fetchT0); // single-day fetch + parse
    _devStateMs = 0;

    let collapseBeforeTime = null;
    let keepOpenRaceId = null;
    if (isFirstLoad && upcomingRaces.length > 0) {
        const now = new Date();
        const nextUpcomingIndex = upcomingRaces.findIndex(r => r.time > now);
        if (nextUpcomingIndex > -1) {
            collapseBeforeTime = upcomingRaces[nextUpcomingIndex].time;
            // Keep the race that is most likely in-progress expanded.
            if (nextUpcomingIndex > 0) keepOpenRaceId = upcomingRaces[nextUpcomingIndex - 1].r_id;
        }
    }

    const _sidebarT0 = performance.now();
    renderWeekendWatchlist();
    renderVoteHistory();
    renderEnginePicks();
    updateQuickStats();
    _devSidebarMs = Math.round(performance.now() - _sidebarT0);

    const _renderT0 = performance.now();
    currentActiveDate = (initialDate && globalRacesByDate[initialDate])
        ? initialDate
        : (getSortedActiveDates()[0] || null);
    currentTimelineTab = currentActiveDate
        ? (globalDateTimelineByDate[currentActiveDate] || globalCalendarSkeleton[currentActiveDate]?.timeline || 'past')
        : 'past';
    currentCalendarMonth = currentActiveDate ? getMonthKey(currentActiveDate) : getAvailableCalendarMonths()[0] || null;

    renderDayTabsAndSchedules(currentActiveDate, collapseBeforeTime, keepOpenRaceId);
    _devRenderMs = Math.round(performance.now() - _renderT0);
    syncVotingViewAvailability();
    updateLiveViewPopoutAvailability();
    updateAllRiskBadges();
    updateAutoBetHighlighting();

    isFirstLoad = false;
    appendDebugLine(`loadRaces completed in ${(performance.now() - t0).toFixed(0)}ms`);
}

// --- TAB SWITCHING ---
function switchSidebarTab(tab) {
    document.querySelectorAll('.sidebar .tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.sidebar .tab-content').forEach(c => c.classList.remove('active'));
    event.target.classList.add('active');
    document.getElementById(`side-tab-${tab}`).classList.add('active');
}

async function switchMainTab(date) {
    const dates = getAllCalendarDates();
    const nextDate = findNearestAvailableDate(date, dates) || date;
    if (!nextDate) return;

    // Lazy-load the day's heavy detail if we only have its skeleton entry so far (date arrows /
    // jump-to-race can land on a not-yet-loaded day). The shell already exists from the all-days pass.
    // Lazy-load the day's heavy detail if we only have its skeleton entry. After loading, a FULL
    // re-render wires up the new day's entries/locks (the cheap-init inside renderDayTabsAndSchedules)
    // and renders it — mirrors selectCalendarDate. The fast path below handles already-loaded days.
    if (!globalRacesByDate[nextDate] && globalCalendarSkeleton[nextDate]) {
        const loaded = await loadRaceDay(nextDate);
        if (loaded) {
            currentActiveDate = nextDate;
            currentTimelineTab = globalDateTimelineByDate[nextDate] || currentTimelineTab;
            currentCalendarMonth = getMonthKey(nextDate) || currentCalendarMonth;
            updateOreProSyncDateDisplay();
            refreshDayBetStructure();
            renderDayTabsAndSchedules(nextDate);
            updateAllRiskBadges();
            updateAutoBetHighlighting();
            winningVotesFocusEnabled = false;
            syncVotingViewAvailability();
            updateLiveViewPopoutAvailability();
            updateWinningVotesFocusButton();
            renderWeekendWatchlist();
            renderVoteHistory();
            renderEnginePicks();
            updateQuickStats();
            return;
        }
    }

    currentActiveDate = nextDate;
    currentTimelineTab = globalDateTimelineByDate[nextDate] || currentTimelineTab;
    currentCalendarMonth = getMonthKey(nextDate) || currentCalendarMonth;
    updateOreProSyncDateDisplay();
    refreshDayBetStructure(); // load + reflect this day's bet structure

    // Lazy-render the target date if it hasn't been built yet.
    renderDateTab(nextDate);
    updateAllRiskBadges();
    updateAutoBetHighlighting();

    document.querySelectorAll('#schedules-container .tab-content').forEach(c => {
        c.classList.toggle('active', c.id === `tab-${nextDate}`);
    });
    winningVotesFocusEnabled = false;
    syncVotingViewAvailability();
    updateLiveViewPopoutAvailability();
    updateWinningVotesFocusButton();
    renderRaceCalendar();
    renderWeekendWatchlist();
    renderVoteHistory();
    renderEnginePicks();
    updateQuickStats();
    if (currentMainView === 'voting') {
        renderLiveViewPanel();
    }
}

// Creates the individual prediction buttons (◎, 〇, ▲, △)
function createMarkBtn(r_id, h_id, symbol, key) {
    const isActive = globalMarks[key] === symbol;
    const isLocked = !!raceLocks[r_id];
    let activeClass = isActive ? `active-${symbol}` : '';

    // If it's not active, AND it's a singleton main vote (◎〇▲), check if it's stolen!
    // △ is repeatable (Phase 29 v2 multi-longshot) and X is unlimited — neither dims.
    if (!isActive && symbol !== 'X' && symbol !== '△') {
        for (const [k, v] of Object.entries(globalMarks)) {
            if (k.startsWith(`${r_id}_`) && v === symbol) {
                activeClass = "dimmed-symbol";
                break;
            }
        }
    }

    const lockClass = isLocked ? "locked" : "";
    const disabledAttr = isLocked ? "disabled" : "";
    return `<button id="btn_${key}_${symbol}" class="mark-btn ${activeClass} ${lockClass}" ${disabledAttr} onclick="toggleMark('${r_id}', '${h_id}', '${symbol}')">${symbol}</button>`;
}

function countRaceMarks(r_id) {
    let markCount = 0;
    for (const [k, v] of Object.entries(globalMarks)) {
        if (k.startsWith(`${r_id}_`) && v) markCount++;
    }
    return markCount;
}

function countRaceMainBets(r_id) {
    let usedCount = 0;
    const mainSymbols = ["◎", "〇", "▲", "△"];
    for (const [k, v] of Object.entries(globalMarks)) {
        if (k.startsWith(`${r_id}_`) && mainSymbols.includes(v)) usedCount++;
    }
    return usedCount;
}

function clearStoredMarksForRace(r_id) {
    let cleared = 0;
    for (const [k, v] of Object.entries(globalMarks)) {
        if (k.startsWith(`${r_id}_`) && v) {
            globalMarks[k] = null;
            cleared += 1;
        }
    }
    return cleared;
}

function isRaceLocked(r_id) {
    return !!raceLocks[r_id];
}

function updateRaceActionButtons(r_id) {
    const isLocked = isRaceLocked(r_id);
    const markCount = countRaceMarks(r_id);
    const usedCount = countRaceMainBets(r_id);

    const clearBtn = document.getElementById(`btn-clear-${r_id}`);
    if (clearBtn) {
        clearBtn.style.display = markCount > 0 ? "inline-block" : "none";
        clearBtn.disabled = isLocked;
        clearBtn.title = isLocked ? "Unlock this race to clear marks" : "Clear all marks in this race";
    }

    const lockBtn = document.getElementById(`btn-lock-${r_id}`);
    if (lockBtn) {
        lockBtn.innerText = isLocked ? "🔓 Unlock Bets" : "🔒 Lock Bets";
        lockBtn.classList.toggle('is-locked', isLocked);
        lockBtn.title = isLocked
            ? "Unlock to allow mark changes"
            : "Lock to prevent any mark changes in this race";
    }

    const header = document.getElementById(`header-${r_id}`);
    if (header) {
        header.classList.toggle('votes-locked', isLocked);
    }

    const autoBtns = document.querySelectorAll(`.auto-group-${r_id}`);
    const reorderBtn = document.getElementById(`btn-reorder-${r_id}`);

    autoBtns.forEach(btn => {
        btn.style.display = (usedCount >= 4) ? "none" : "inline-block";
        btn.disabled = isLocked;
    });

    if (reorderBtn) {
        reorderBtn.style.display = (usedCount >= 4) ? "inline-block" : "none";
        reorderBtn.disabled = isLocked;
    }
}

async function clearRaceBets(event, r_id) {
    event.stopPropagation();
    if (isRaceLocked(r_id)) {
        alert('This race is locked. Unlock bets first.');
        return;
    }

    const info = globalRaceInfo[r_id];
    const label = info
        ? `${trackName(info.place)} R${info.race_number}`
        : r_id;
    if (!confirm(`Clear all bets for ${label}?\n\nThis cannot be undone.`)) return;

    const cleared = clearStoredMarksForRace(r_id);
    if (!cleared) return;

    touchRaceMeta(r_id, { markSource: 'manual', manualAdjustmentsDelta: 1 });
    await saveMarksToServer();

    const sortState = raceSorts[r_id] || { col: 'Default', asc: true };
    raceSorts[r_id] = sortState;
    applySortLogic(r_id, sortState.col, sortState.asc);
    const tbody = document.getElementById(`tbody-${r_id}`);
    if (tbody) tbody.innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);
    refreshRaceHeaderSortLabels(r_id);
    updateRaceActionButtons(r_id);
    updateRiskBadge(r_id);
    updateAutoBetHighlighting();
    updateWinningVotesFocusButton();
    if (winningVotesFocusEnabled) applyWinningVotesFocus();
    if (currentMainView === 'voting') renderLiveViewPanel();
}

function toggleRaceLock(event, r_id) {
    event.stopPropagation();
    raceLocks[r_id] = !raceLocks[r_id];

    const tbody = document.getElementById(`tbody-${r_id}`);
    if (tbody) tbody.innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);

    updateRaceActionButtons(r_id);
    updateAutoBetHighlighting();
    updateQuickStats(); // locking = placing a bet → Day Net moves immediately
    // Persist the lock so it survives refresh AND so the backend can read it as the
    // sunk-cost "placed bet" signal. touchRaceMeta re-snapshots lockStateAtSave.
    touchRaceMeta(r_id);
    saveMarksToServer()
        .then(() => refreshSunkCostStat())  // locking = placing a bet → tally moves
        .catch(() => { /* best-effort; lock still live in-session */ });
}

/**
 * Lock every race that shares a JST clean_date with the given race. Used after
 * an OrePro apply so the operator can't accidentally edit marks for races that
 * have already been bet. Idempotent — already-locked races stay locked.
 */
// Lock a SINGLE race after it's been applied/submitted (edit-protection for just that
// race). Used by the per-race Apply path so applying one race doesn't lock the marks on
// other races you're still working on for the day.
// Freeze the placed bet's per-line breakdown onto the race meta at APPLY time, so the bet's
// exact shape + per-line stake is recorded as-placed: future-proof for custom-ticket analysis,
// and immune to later day/per-race structure or stake changes. Captures the RESOLVED structure
// (per-race override → day setting → default) at the moment of apply. First-freeze-wins
// (idempotent); never touches imported races (they carry real OrePro ¥ already).
// NOTE: this runs only from the OrePro apply path — manual lock is a marks-guard and does NOT
// freeze, so pricing for a locked-but-unapplied race still follows the live resolved structure.
function freezeBetProfileAtApply(rid) {
    const meta = globalRaceMeta[rid];
    if (meta?.betProfile && meta.betProfile.actualStaked != null) return; // imported: real ¥, leave it
    if (meta?.betProfile?.betLines?.length) return;                        // already frozen
    const plan = buildRaceBetLines({ info: { race_id: rid } });
    if (!plan.lines || !plan.lines.length) return;
    const lines = plan.lines.map(l => ({
        ticket: l.ticket, method: l.method, label: l.label,
        horses: (l.horses || []).map(h => ({ pp: h.pp })),
        comboCount: l.comboCount, stakePerCombo: l.stakePerCombo,
        ...(l.axisPp ? { axisPp: l.axisPp } : {}),
        ...(l.kind === 'side' ? { kind: 'side' } : {})
    }));
    // Append any CONFIRMED loyalty side bets (kind:'side') so they freeze alongside the spine. C#
    // scores them into a SEPARATE side-P/L bucket, keeping the Discipline recovery % honest. The
    // active set is whatever survived your removals in the Bets strip — manual-confirm, not auto-place.
    const sideLines = buildSideBetLines(rid);
    for (const s of sideLines) lines.push({ ...s, horses: (s.horses || []).map(h => ({ pp: h.pp })) });
    // The frozen betLines ARE the record of what was placed (C# scores them verbatim). We also
    // stash the composition label so applied races can show "Trio chase + Wide net" etc.
    const comp = resolveBetComposition(rid);
    const bp = { compositionLabel: compositionLabel(comp), stake: plan.staked, betLines: lines };
    if (plan.runners.length >= 5) bp.extrapolated = true; // 5-6 mark plans are approximate at scale
    globalRaceMeta[rid] = { ...(meta || {}), betProfile: bp };
}

function lockSingleRaceAfterSubmit(raceId) {
    const rid = String(raceId || '').trim();
    if (!rid || raceLocks[rid]) return 0;
    raceLocks[rid] = true;
    freezeBetProfileAtApply(rid); // record the bet's line breakdown as placed (apply path)
    touchRaceMeta(rid); // persist lockStateAtSave; the apply flow saves the blob once after.
    const tbody = document.getElementById(`tbody-${rid}`);
    if (tbody) tbody.innerHTML = buildTableBody(rid, globalRaceEntries[rid]);
    updateRaceActionButtons(rid);
    updateAutoBetHighlighting();
    return 1;
}

function lockAllRacesForRaceDay(raceId) {
    const info = globalRaceInfo[raceId];
    const date = info?.clean_date;
    if (!date) return 0;

    let locked = 0;
    Object.keys(globalRaceInfo).forEach(rid => {
        if ((globalRaceInfo[rid]?.clean_date || '') !== date) return;
        if (!raceLocks[rid]) {
            raceLocks[rid] = true;
            touchRaceMeta(rid); // lock = marks-guard only; bet freezes at apply, not here.
            locked++;
        }
        const tbody = document.getElementById(`tbody-${rid}`);
        if (tbody) tbody.innerHTML = buildTableBody(rid, globalRaceEntries[rid]);
        updateRaceActionButtons(rid);
    });
    updateAutoBetHighlighting();
    return locked;
}

// Marked race-ids on the active day (a "bet" = a race carrying at least one main mark).
function getActiveDayMarkedRaceIds() {
    const date = currentActiveDate;
    if (!date) return [];
    return Object.keys(globalRaceInfo).filter(r_id =>
        (globalRaceInfo[r_id]?.clean_date || '') === date &&
        Object.keys(collectRaceMainMarks(r_id) || {}).length > 0);
}

// True when every marked race on the active day is already locked (and there's ≥1).
function areAllActiveDayBetsLocked() {
    const ids = getActiveDayMarkedRaceIds();
    return ids.length > 0 && ids.every(r_id => !!raceLocks[r_id]);
}

// Voting-tab "Lock All / Unlock All Bets": one button that flips based on state.
// Locking = "placed bet" (counts toward sunk cost / Day Net); this is the manual
// catch-all when auto-lock-after-submit didn't fire, and now also the way to UNDO it.
async function toggleLockAllBetsForActiveDay() {
    const date = currentActiveDate;
    if (!date) { alert('No active day selected.'); return; }

    const ids = getActiveDayMarkedRaceIds();
    if (ids.length === 0) { alert(`No marked races for ${date}.`); return; }

    const unlocking = areAllActiveDayBetsLocked(); // all locked → this click unlocks
    let changed = 0;
    ids.forEach(r_id => {
        const want = !unlocking; // locking → true, unlocking → false
        if (!!raceLocks[r_id] === want) return;
        raceLocks[r_id] = want;
        // lock = marks-guard only; the bet record freezes at apply, not on manual lock.
        touchRaceMeta(r_id);
        changed++;
        const tbody = document.getElementById(`tbody-${r_id}`);
        if (tbody) tbody.innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);
        updateRaceActionButtons(r_id);
    });

    if (changed > 0) {
        try { await saveMarksToServer(); } catch (_) { /* state still live in-session */ }
    }
    updateAutoBetHighlighting();
    updateQuickStats();
    refreshSunkCostStat();
    updateLockAllBetsButton();
    if (currentMainView === 'voting') renderLiveViewPanel();

    if (unlocking) {
        const already = ids.length - changed;
        alert(`Unlocked ${changed} bet${changed === 1 ? '' : 's'} for ${date}`
            + (already > 0 ? ` (${already} already unlocked).` : '.'));
    } else {
        const already = ids.length - changed;
        alert(`Locked ${changed} bet${changed === 1 ? '' : 's'} for ${date}`
            + (already > 0 ? ` (${already} already locked).` : '.'));
    }
}

// Swap the button between 🔒 Lock All and 🔓 Unlock All depending on whether the
// active day's marked races are all locked. Safe no-op if the button isn't present.
function updateLockAllBetsButton() {
    const btn = document.getElementById('btn-lock-all-bets');
    if (!btn) return;
    if (areAllActiveDayBetsLocked()) {
        btn.textContent = '🔓 Unlock All Bets';
        btn.title = 'All marked races for the selected day are locked — click to UNLOCK them all (removes them from placed-bet / sunk-cost tracking).';
    } else {
        btn.textContent = '🔒 Lock All Bets';
        btn.title = 'Lock every marked race for the selected day — locking marks them as placed bets so they count toward your sunk cost / Day Net.';
    }
}

async function toggleMark(r_id, h_id, symbol) {
    if (isRaceLocked(r_id)) return;

    const keyA = `${r_id}_${h_id}`;
    const oldSymA = globalMarks[keyA]; 
    const newSymA = symbol;            

    if (oldSymA === newSymA) {
        globalMarks[keyA] = null;
        document.getElementById(`btn_${keyA}_${newSymA}`).className = "mark-btn";
    } else {
        let keyB = null;

        // Steal the symbol from another horse only for the SINGLETON main votes (◎〇▲).
        // △ is repeatable (Phase 29 v2: counts 5-6 = multiple longshots → nagashi) and X is
        // unlimited, so neither steals.
        if (newSymA !== 'X' && newSymA !== '△') {
            for (const [k, v] of Object.entries(globalMarks)) {
                if (k.startsWith(`${r_id}_`) && v === newSymA && k !== keyA) {
                    keyB = k; break;
                }
            }
        }

        // Wipe the UI slate clean for Horse A (Added 'X' to the array)
        ['◎', '〇', '▲', '△', 'X'].forEach(sym => { 
            const btn = document.getElementById(`btn_${keyA}_${sym}`);
            if(btn) btn.className = "mark-btn"; 
        });

        // The Swap logic for main votes
        if (keyB) {
            globalMarks[keyB] = oldSymA;
            ['◎', '〇', '▲', '△', 'X'].forEach(sym => { 
                const btn = document.getElementById(`btn_${keyB}_${sym}`);
                if(btn) btn.className = "mark-btn"; 
            });
            if (oldSymA) {
                const btnB = document.getElementById(`btn_${keyB}_${oldSymA}`);
                if (btnB) btnB.className = `mark-btn active-${oldSymA}`;
            }
        }

        globalMarks[keyA] = newSymA;
        const btnA = document.getElementById(`btn_${keyA}_${newSymA}`);
        if (btnA) btnA.className = `mark-btn active-${newSymA}`;
    }

    // Silently sync the new state to the Python backend
    touchRaceMeta(r_id, { markSource: 'manual', manualAdjustmentsDelta: 1 });
    await saveMarksToServer();

    // NEW: Instantly re-sort and re-render the table so voted horses snap to the top!
    applySortLogic(r_id, raceSorts[r_id].col, raceSorts[r_id].asc);
    document.getElementById(`tbody-${r_id}`).innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);
    updateRaceActionButtons(r_id);
    updateRiskBadge(r_id);
    updateAutoBetHighlighting();
    updateWinningVotesFocusButton();
    updateQuickStats();
    if (winningVotesFocusEnabled) applyWinningVotesFocus();
}

// --- API CALLS ---
let logInterval = null;

// Single-flight + coalescing guard around the marks-blob POST. saveMarksToServer serializes the
// ENTIRE in-memory blob and the server does a last-write-wins overwrite, and it's called
// fire-and-forget from ~8 places. Two overlapping POSTs used to race: whichever the server processed
// LAST won, so a staler snapshot (serialized a moment before a bet was frozen at apply) could silently
// clobber a fresher one — the s60 "appliedAt timestamp but no betProfile record" bug. This wrapper
// guarantees only ONE POST is ever in flight; if any saves are requested while one runs, exactly one
// more runs afterward with the LATEST in-memory state. So the final write always reflects current
// state and no out-of-order clobber is possible. Callers keep the same `await`/`.then()` contract.
let _marksSaveInFlight = null;   // Promise of the running save-loop, or null when idle
let _marksSavePending  = false;  // a save was requested while the loop was busy → run once more
function saveMarksToServer() {
    if (_marksSaveInFlight) { _marksSavePending = true; return _marksSaveInFlight; }
    _marksSaveInFlight = (async () => {
        let lastErr = null;
        try {
            do {
                _marksSavePending = false;
                // Re-catch per iteration so a transient POST failure doesn't drop a queued newer save.
                try { await _postMarksBlobNow(); lastErr = null; }
                catch (e) { lastErr = e; }
            } while (_marksSavePending);
        } finally {
            _marksSaveInFlight = null;
        }
        if (lastErr) throw lastErr;   // surface the final failure to awaiting callers (unchanged contract)
    })();
    return _marksSaveInFlight;
}

async function _postMarksBlobNow() {
    const cleanMarks = Object.fromEntries(
        Object.entries(globalMarks).filter(([, v]) => v !== null && v !== undefined && v !== '')
    );
    const cleanRaceMeta = Object.fromEntries(
        Object.entries(globalRaceMeta).filter(([raceId, meta]) => {
            return raceId && meta && typeof meta === 'object' && !Array.isArray(meta);
        }).map(([raceId, meta]) => [raceId, {
            savedAt: meta.savedAt || null,
            updatedAt: meta.updatedAt || null,
            markSource: meta.markSource || null,
            strategySnapshot: {
                riskSlider: Number.isFinite(Number(meta.strategySnapshot?.riskSlider)) ? Number(meta.strategySnapshot.riskSlider) : null,
                riskLabel: meta.strategySnapshot?.riskLabel || null,
                formulaWeights: meta.strategySnapshot?.formulaWeights && typeof meta.strategySnapshot.formulaWeights === 'object' && !Array.isArray(meta.strategySnapshot.formulaWeights)
                    ? meta.strategySnapshot.formulaWeights
                    : {},
                engineShape: typeof meta.strategySnapshot?.engineShape === 'string' ? meta.strategySnapshot.engineShape : null,
                engineCount: Number.isFinite(Number(meta.strategySnapshot?.engineCount)) ? Number(meta.strategySnapshot.engineCount) : null
            },
            manualAdjustments: Number.isFinite(Number(meta.manualAdjustments)) ? Number(meta.manualAdjustments) : 0,
            lockStateAtSave: typeof meta.lockStateAtSave === 'boolean' ? meta.lockStateAtSave : null,
            activeSymbols: Array.isArray(meta.activeSymbols) ? meta.activeSymbols.map(symbol => String(symbol || '').trim()).filter(Boolean) : [],
            betProfile: normalizeBetProfile(meta.betProfile),
            // Per-race bet COMPOSITION override (Voting tab). Preserve only valid shapes.
            ...(normalizeComposition(meta.betComposition) ? { betComposition: normalizeComposition(meta.betComposition) } : {}),
            // Phase 34: tag marking an override as auto-created by Auto Bet Day's backup-preset rescue.
            ...(meta.betCompositionAutoBackup === true ? { betCompositionAutoBackup: true } : {}),
            // Side bets: the explicit per-race active set (whitelisted so removals/re-adds survive reload).
            ...(Array.isArray(meta.sideBets) ? { sideBets: meta.sideBets.map(h => String(h || '').trim()).filter(Boolean) } : {})
        }])
    );

    await fetch('/api/marks', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            version: globalMarksVersion || 2,
            marks: cleanMarks,
            raceMeta: cleanRaceMeta
        })
    });
}

function appendConsoleLine(message) {
    const consoleBox = document.getElementById('scrape-console');
    if (!consoleBox) return;
    if ((appConfig?.ui?.showConsole ?? false) && isDevModeEnabled()) consoleBox.style.display = 'block';
    const prefix = consoleBox.textContent && consoleBox.textContent.trim() ? '\n' : '';
    consoleBox.textContent += `${prefix}${message}`;
    consoleBox.scrollTop = consoleBox.scrollHeight;
}

function appendDebugLine(message) {
    if (!isDebugConsoleEnabled()) return;
    const stamp = new Date().toTimeString().split(' ')[0];
    appendConsoleLine(`[Debug ${stamp}] ${message}`);
}

async function triggerPost(url) {
    try {
        const res = await fetch(url, { method: 'POST' });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || data.message || `HTTP ${res.status}`);
        if (url === '/api/dict/wipe') {
            const runtimeCount = Number(data?.cleared?.runtimeEntries || 0);
            const dbCount = Number(data?.cleared?.dbEntries || 0);
            const fileDeleted = Boolean(data?.cleared?.legacyFileDeleted);
            alert(
                `${data.message || 'Translation memory cleared.'}\n\n` +
                `Runtime entries cleared: ${runtimeCount}\n` +
                `DB entries cleared: ${dbCount}\n` +
                `Legacy file deleted: ${fileDeleted ? 'yes' : 'no'}`
            );
        }
        await refreshDataAndUI();
    } catch (err) {
        alert(`Request failed: ${err.message}`);
    }
}

function jvlinkPanelGetPayload() {
    const dataSpec = (document.getElementById('jvlink-test-dataspec')?.value || '').trim();
    const fromDate = (document.getElementById('jvlink-test-from-date')?.value || '').trim();
    const sid = (document.getElementById('jvlink-test-sid')?.value || '').trim();
    const maxRecordsRaw = Number(document.getElementById('jvlink-test-max-records')?.value || 20);
    const dataOptionRaw = Number(document.getElementById('jvlink-test-data-option')?.value || 1);
    const skipSetServiceKey = Boolean(document.getElementById('jvlink-test-skip-key')?.checked);

    return {
        data_spec: dataSpec,
        from_date: fromDate,
        sid: sid || null,
        max_records: Math.min(500, Math.max(1, Number.isFinite(maxRecordsRaw) ? maxRecordsRaw : 20)),
        data_option: Math.min(3, Math.max(1, Number.isFinite(dataOptionRaw) ? dataOptionRaw : 1)),
        skip_set_service_key: skipSetServiceKey
    };
}

function setJvlinkPanelStatus(message, isError = false) {
    const statusEl = document.getElementById('jvlink-test-status');
    if (!statusEl) return;
    statusEl.textContent = message || '';
    statusEl.classList.remove('ok', 'error');
    statusEl.classList.add(isError ? 'error' : 'ok');
}

function setJvlinkPanelOutput(payload) {
    const outEl = document.getElementById('jvlink-test-output');
    if (!outEl) return;
    const compactMode = Boolean(document.getElementById('jvlink-test-compact')?.checked);
    const output = compactMode ? compactJvlinkPanelOutput(payload || {}) : (payload || {});
    outEl.textContent = JSON.stringify(output, null, 2);
}

function compactJvlinkPanelOutput(payload) {
    const status = String(payload?.status || '').toLowerCase();
    if (payload?.probe && typeof payload.probe === 'object') {
        const p = payload.probe;
        return {
            status: payload.status,
            runId: payload.runId,
            probe: {
                ok: p.ok,
                openOk: p.openOk,
                readOk: p.readOk,
                readTransport: p.readTransport || '',
                openCode: p.openCode,
                error: p.error,
                warnings: p.warnings || [],
                statusCode: p.statusCode,
                statusPollCount: p.statusPollCount,
                downloadCount: p.downloadCount,
                readCount: p.readCount,
                lastFileTimestamp: p.lastFileTimestamp,
                sampleCount: Array.isArray(p.readSamples) ? p.readSamples.length : 0,
                samplePreview: Array.isArray(p.readSamples) ? p.readSamples.slice(0, 5) : []
            }
        };
    }

    if (payload?.stream && typeof payload.stream === 'object') {
        const s = payload.stream;
        return {
            status: payload.status,
            stream: {
                ok: s.ok,
                openOk: s.openOk,
                readOk: s.readOk,
                readTransport: s.readTransport || '',
                openCode: s.openCode,
                error: s.error,
                warnings: s.warnings || [],
                statusCode: s.statusCode,
                statusPollCount: s.statusPollCount,
                downloadCount: s.downloadCount,
                readCount: s.readCount,
                lastFileTimestamp: s.lastFileTimestamp,
                recordCount: Array.isArray(s.records) ? s.records.length : 0,
                recordPreview: Array.isArray(s.records) ? s.records.slice(0, 8) : []
            },
            saved: payload.saved || {}
        };
    }

    if (payload?.scan && typeof payload.scan === 'object') {
        const s = payload.scan;
        return {
            status: payload.status,
            scan: {
                ok: s.ok,
                fromDate: s.fromDate,
                maxRecordsPerRun: s.maxRecordsPerRun,
                scanDataOptions: s.scanDataOptions || [],
                runCount: s.runCount,
                runsOpenOk: s.runsOpenOk,
                runsWithRecords: s.runsWithRecords,
                observedRecordSpecs: s.observedRecordSpecs || [],
                observedFilePrefixes: s.observedFilePrefixes || [],
                runPreview: Array.isArray(s.runs) ? s.runs.slice(0, 12) : [],
                notes: s.notes || []
            }
        };
    }

    if (payload?.bridge && typeof payload.bridge === 'object') {
        const b = payload.bridge;
        return {
            status: payload.status,
            bridge: {
                ok: b.ok,
                error: b.error || '',
                version: b.version || '',
                runner: b.runner || ''
            }
        };
    }

    if (payload?.layout && typeof payload.layout === 'object') {
        return {
            status: payload.status,
            layout: payload.layout
        };
    }

    if (status === 'error' || status === 'warning' || status === 'ok') {
        return payload;
    }
    return payload;
}

function toggleJvlinkPanelButtons(disabled) {
    document.querySelectorAll('.jvlink-tool-block button').forEach(btn => {
        btn.disabled = Boolean(disabled);
    });
}

function consoleTimestamp() {
    return new Date().toTimeString().split(' ')[0];
}

async function runJvlinkPanelCall(label, runner) {
    toggleJvlinkPanelButtons(true);
    setJvlinkPanelStatus(`${label}: running...`, false);
    appendConsoleLine(`[${consoleTimestamp()}] ▶ ${label}…`);
    try {
        const response = await runner();
        setJvlinkPanelOutput(response);
        const ok = !response?.error;
        setJvlinkPanelStatus(`${label}: ${ok ? 'ok' : 'error'}`, !ok);
        const summary = jvlinkResponseSummary(response);
        appendConsoleLine(`[${consoleTimestamp()}] ${ok ? '✓' : '✗'} ${label}${summary ? ': ' + summary : ''}`);
    } catch (err) {
        setJvlinkPanelOutput({ error: String(err?.message || err) });
        setJvlinkPanelStatus(`${label}: ${String(err?.message || err)}`, true);
        appendConsoleLine(`[${consoleTimestamp()}] ✗ ${label}: ${String(err?.message || err)}`);
    } finally {
        toggleJvlinkPanelButtons(false);
    }
}

function jvlinkResponseSummary(response) {
    if (!response || typeof response !== 'object') return '';
    const parts = [];
    const status = String(response.status || '');
    if (status && status.toLowerCase() !== 'ok') parts.push(status);
    if (response.jvlink_version) parts.push(response.jvlink_version);
    if (response.ingestion_status) parts.push(response.ingestion_status);
    if (response.message && response.message !== status) parts.push(response.message);
    if (response.staged_record_count != null) parts.push(`staged=${response.staged_record_count}`);
    if (response.stored != null) parts.push(`stored=${response.stored}`);
    if (response.skipped != null) parts.push(`skipped=${response.skipped}`);
    if (response.parsed_ra != null) parts.push(`ra=${response.parsed_ra}`);
    if (response.parsed_se != null) parts.push(`se=${response.parsed_se}`);
    if (response.parsed_o1 != null) parts.push(`o1=${response.parsed_o1}`);
    if (response.failed != null && response.failed > 0) parts.push(`failed=${response.failed}`);
    if (response.error) parts.push(`error: ${response.error}`);
    return parts.join(', ');
}

async function runJvlinkStatusTest() {
    await runJvlinkPanelCall('Status', async () => {
        const res = await fetch('/api/jvlink/status');
        return await res.json();
    });
}

async function runJvlinkStorageLayoutTest() {
    await runJvlinkPanelCall('Storage Layout', async () => {
        const res = await fetch('/api/jvlink/storage-layout');
        return await res.json();
    });
}

async function runJvlinkOpenSettingsTest() {
    const payload = jvlinkPanelGetPayload();
    await runJvlinkPanelCall('Open Settings', async () => {
        return await postJson('/api/jvlink/open-settings', {
            sid: payload.sid || null
        });
    });
}

async function runJvlinkProbeOpenTest() {
    const payload = jvlinkPanelGetPayload();
    if (!payload.data_spec || !payload.from_date) {
        setJvlinkPanelStatus('Probe Open: data_spec and from_date are required', true);
        return;
    }

    await runJvlinkPanelCall('Probe Open', async () => {
        return await postJson('/api/jvlink/probe-open', {
            data_spec: payload.data_spec,
            from_date: payload.from_date,
            max_read_calls: 3,
            data_option: payload.data_option,
            max_status_wait_seconds: 60,
            skip_set_service_key: payload.skip_set_service_key,
            sid: payload.sid || null
        });
    });
}

async function runJvlinkStreamSampleTest() {
    const payload = jvlinkPanelGetPayload();
    if (!payload.data_spec || !payload.from_date) {
        setJvlinkPanelStatus('Stream Sample: data_spec and from_date are required', true);
        return;
    }

    await runJvlinkPanelCall('Stream Sample', async () => {
        return await postJson('/api/jvlink/stream-sample', {
            data_spec: payload.data_spec,
            from_date: payload.from_date,
            max_records: payload.max_records,
            data_option: payload.data_option,
            max_status_wait_seconds: 12,
            skip_set_service_key: payload.skip_set_service_key,
            sid: payload.sid || null
        });
    });
}

function sleepMs(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function runJvlinkStreamSampleAutoTest() {
    const payload = jvlinkPanelGetPayload();
    if (!payload.data_spec || !payload.from_date) {
        setJvlinkPanelStatus('Auto Wait Stream: data_spec and from_date are required', true);
        return;
    }

    await runJvlinkPanelCall('Auto Wait Stream', async () => {
        const maxAttempts = 8;
        const delayMs = 5000;
        let last = null;
        let attempt = 0;

        for (attempt = 1; attempt <= maxAttempts; attempt++) {
            setJvlinkPanelStatus(`Auto Wait Stream: attempt ${attempt}/${maxAttempts}...`, false);
            const response = await postJson('/api/jvlink/stream-sample', {
                data_spec: payload.data_spec,
                from_date: payload.from_date,
                max_records: payload.max_records,
                max_status_wait_seconds: 45,
                data_option: payload.data_option,
                skip_set_service_key: payload.skip_set_service_key,
                sid: payload.sid || null
            });
            last = response;

            const stream = response?.stream || {};
            const records = Array.isArray(stream.records) ? stream.records : [];
            const hasRecords = records.length > 0;
            if (hasRecords) {
                break;
            }

            const dl = Number(stream.downloadCount || 0);
            const st = Number(stream.statusCode || 0);
            const stillDownloading = dl > 0 && st < dl;
            if (!stillDownloading) {
                break;
            }

            if (attempt < maxAttempts) {
                await sleepMs(delayMs);
            }
        }

        if (!last) {
            return {
                status: 'error',
                message: 'No response received from stream endpoint.'
            };
        }

        return {
            ...last,
            auto: {
                attemptsUsed: attempt,
                maxAttempts,
                delayMs,
                stoppedReason: (Array.isArray(last?.stream?.records) && last.stream.records.length > 0)
                    ? 'records-received'
                    : 'no-more-progress-or-timeout'
            }
        };
    });
}

async function runJvlinkRefreshUpcoming() {
    await runJvlinkPanelCall('Refresh Cache', async () => {
        const payload = jvlinkPanelGetPayload();
        return await postJson('/api/jvlink/refresh-upcoming', {
            data_spec: payload.data_spec || null,
            max_status_wait_seconds: 180,
            skip_set_service_key: payload.skip_set_service_key,
            sid: payload.sid || null
        });
    });
}

async function runJvlinkStreamSummaryTest() {
    await runJvlinkPanelCall('Stream Summary', async () => {
        const res = await fetch('/api/jvlink/stream-summary?limit=50');
        return await res.json();
    });
}

async function runJvlinkCapabilityScanTest() {
    const payload = jvlinkPanelGetPayload();
    if (!payload.from_date) {
        setJvlinkPanelStatus('Capability Scan: from_date is required', true);
        return;
    }

    await runJvlinkPanelCall('Capability Scan', async () => {
        return await postJson('/api/jvlink/capability-scan', {
            from_date: payload.from_date,
            max_status_wait_seconds: 30,
            max_records_per_run: Math.min(200, Math.max(10, payload.max_records || 40)),
            data_options: [1, 2],
            sid: payload.sid || null,
            skip_set_service_key: payload.skip_set_service_key
        });
    });
}

async function runJvlinkLoadWeekendRaces() {
    const payload = jvlinkPanelGetPayload();
    if (!payload.from_date) {
        setJvlinkPanelStatus('Load Weekend Races: from_date is required', true);
        return;
    }

    await runJvlinkPanelCall('Load Weekend Races', async () => {
        return await postJson('/api/jvlink/load-weekend-races', {
            from_date: payload.from_date,
            max_records: Math.min(5000, Math.max(100, payload.max_records || 5000)),
            max_status_wait_seconds: 120
        });
    });
}

async function runJvlinkLoadMasterDataInitial() {
    await runJvlinkPanelCall('Load Master Data (Initial - may take 30-60 min)', async () => {
        return await postJson('/api/jvlink/load-master-data', {
            is_initial: true,
            max_records: 500000,
            max_status_wait_seconds: 3600
        });
    });
}

async function runJvlinkLoadMasterDataIncremental() {
    await runJvlinkPanelCall('Load Master Data (Incremental)', async () => {
        return await postJson('/api/jvlink/load-master-data', {
            is_initial: false,
            max_records: 50000,
            max_status_wait_seconds: 180
        });
    });
}

async function runJvlinkRefreshRaceCards() {
    await runJvlinkPanelCall('Refresh Race Cards', async () => {
        const res = await fetch('/api/jvlink/refresh-race-cards', { method: 'POST' });
        return await res.json();
    });
}

async function runJvlinkParseRecords() {
    await runJvlinkPanelCall('Parse Records', async () => {
        const res = await fetch('/api/jvlink/parse-records', { method: 'POST' });
        return await res.json();
    });
}

async function runJvlinkFetchCurrentOdds() {
    await runJvlinkPanelCall('Fetch Live Odds', async () => {
        const res = await fetch('/api/jvlink/fetch-current-odds', { method: 'POST' });
        return await res.json();
    });
}

async function runJvlinkLoadBloodline() {
    await runJvlinkPanelCall('Load Bloodline (BLDN)', async () => {
        const res = await fetch('/api/jvlink/load-bloodline', { method: 'POST' });
        return await res.json();
    });
}

async function runJvlinkBackfillHnNames() {
    await runJvlinkPanelCall('Backfill HN EN Names', async () => {
        const res = await fetch('/api/jvlink/backfill-hn-names', { method: 'POST' });
        return await res.json();
    });
}

// Phase 8: ingest staged KS/CH records into jockeys/trainers and backfill JockeyCode/
// TrainerCode on race_entries from staged SE records, then refresh rolling stats.
async function runJvlinkIngestJockeysTrainers() {
    await runJvlinkPanelCall('Ingest Jockeys + Trainers', async () => {
        const res = await fetch('/api/jvlink/ingest-jockeys-trainers', { method: 'POST' });
        return await res.json();
    });
}

async function runJvlinkRefreshJtStats() {
    await runJvlinkPanelCall('Refresh J/T Stats', async () => {
        const res = await fetch('/api/jvlink/refresh-jockey-trainer-stats', { method: 'POST' });
        return await res.json();
    });
}

async function runJvlinkSidecarLog() {
    toggleJvlinkPanelButtons(true);
    appendConsoleLine(`[${consoleTimestamp()}] ▶ Sidecar Log…`);
    try {
        const res = await fetch('/api/jvlink/sidecar-log?lines=40');
        const data = await res.json();
        if (data.lines && data.lines.length > 0) {
            data.lines.forEach(line => appendConsoleLine(line));
        } else {
            appendConsoleLine('(sidecar log is empty)');
        }
    } catch (err) {
        appendConsoleLine(`[${consoleTimestamp()}] ✗ Sidecar Log: ${String(err?.message || err)}`);
    } finally {
        toggleJvlinkPanelButtons(false);
    }
}

// Keep these on window for inline onclick handlers in index.html.
window.runJvlinkStatusTest = runJvlinkStatusTest;
window.runJvlinkStorageLayoutTest = runJvlinkStorageLayoutTest;
window.runJvlinkOpenSettingsTest = runJvlinkOpenSettingsTest;
window.runJvlinkProbeOpenTest = runJvlinkProbeOpenTest;
window.runJvlinkStreamSampleTest = runJvlinkStreamSampleTest;
window.runJvlinkStreamSampleAutoTest = runJvlinkStreamSampleAutoTest;
window.runJvlinkStreamSummaryTest = runJvlinkStreamSummaryTest;
window.runJvlinkCapabilityScanTest = runJvlinkCapabilityScanTest;
window.runJvlinkLoadWeekendRaces = runJvlinkLoadWeekendRaces;
window.runJvlinkLoadMasterDataInitial = runJvlinkLoadMasterDataInitial;
window.runJvlinkLoadMasterDataIncremental = runJvlinkLoadMasterDataIncremental;
window.runJvlinkRefreshRaceCards = runJvlinkRefreshRaceCards;
window.runJvlinkParseRecords = runJvlinkParseRecords;
window.runJvlinkFetchCurrentOdds = runJvlinkFetchCurrentOdds;
window.runJvlinkSidecarLog = runJvlinkSidecarLog;

async function postJson(url, payload) {
    const res = await fetch(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload || {})
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
        throw new Error(data.detail || data.message || `HTTP ${res.status}`);
    }
    return data;
}

async function createDataBackup() {
    try {
        const data = await postJson('/api/data/backup', {});
        alert(`Backup created automatically: backups/${data.filename || 'backup.zip'}`);
    } catch (err) {
        alert(`Backup failed: ${err.message}`);
    }
}

async function restoreLatestBackup() {
    const proceed = confirm(
        "Restore the latest backup now?\n\nThis replaces current data/ contents.\nA safety backup will be created first only if data/ currently has files."
    );
    if (!proceed) return;

    try {
        const result = await postJson('/api/data/backup/restore', {
            use_latest: true,
            create_safety_backup: true
        });

        const safety = result.safety_backup
            ? `Safety backup created: backups/${result.safety_backup}`
            : 'No safety backup needed (data folder was empty).';

        alert(`Restore complete from: backups/${result.restored_from}\nFiles restored: ${result.restored_files}\n${safety}`);
        await refreshDataAndUI();
    } catch (err) {
        alert(`Restore failed: ${err.message}`);
    }
}

async function exportLegacyBundle() {
    try {
        const result = await postJson('/api/data/legacy/export', {});
        alert(
            `Legacy recovery bundle created: backups/${result.filename}\n` +
            `Files exported: ${Array.isArray(result.files) ? result.files.length : 0}`
        );
    } catch (err) {
        alert(`Legacy export failed: ${err.message}`);
    }
}

async function importLegacyBundle() {
    const proceed = confirm(
        'Import deprecated legacy files from the data/ folder into SQLite now?\n\n' +
        'This is only needed for one-off recovery from old JSON/TXT/PKL storage.'
    );
    if (!proceed) return;

    try {
        const result = await postJson('/api/data/legacy/import', { overwrite_existing: false });
        const imported = result.imported || {};
        const totalImported =
            (imported.config ? 1 : 0) +
            Number(imported.bloodlines || 0) +
            Number(imported.watchlist || 0) +
            Number(imported.marks || 0) +
            Number(imported.raceMeta || 0) +
            Number(imported.horses || 0) +
            Number(imported.races || 0) +
            Number(imported.oreproDays || 0);
        const footer = totalImported === 0
            ? '\n\nNo legacy data was imported. This usually means the old JSON/TXT/PKL files are missing, empty, or SQLite already has the data.'
            : '';
        alert(
            'Legacy import complete.\n\n' +
            `Config: ${imported.config ? 'imported' : 'skipped'}\n` +
            `Bloodlines: ${imported.bloodlines || 0}\n` +
            `Watchlist: ${imported.watchlist || 0}\n` +
            `Marks: ${imported.marks || 0}\n` +
            `Race meta: ${imported.raceMeta || 0}\n` +
            `Horses: ${imported.horses || 0}\n` +
            `Races: ${imported.races || 0}\n` +
            `OrePro days: ${imported.oreproDays || 0}` +
            footer
        );
        await refreshDataAndUI();
    } catch (err) {
        alert(`Legacy import failed: ${err.message}`);
    }
}

async function refreshUpcomingRacesLite() {
    const btn = document.getElementById('btn-upcoming-refresh');
    const action = btn?.dataset?.action || 'check';
    const originalLabel = btn?.textContent || '';
    const startedAt = performance.now();
    let progressTimer = null;
    const formatElapsedSeconds = () => Math.round((performance.now() - startedAt) / 1000);
    if (btn) {
        btn.disabled = true;
        if (action === 'apply') btn.textContent = '⏳ Applying Pending Updates...';
        if (action === 'check') btn.textContent = '⏳ Checking Pending Updates...';
        if (action === 'legacy-refresh') btn.textContent = '⏳ Updating Upcoming Cards...';
    }

    try {
        appendConsoleLine('[Prefetch] Legacy upcoming refresh started. This can take several minutes when many races are refreshed.');
        progressTimer = setInterval(() => {
            appendConsoleLine(`[Prefetch] Legacy upcoming refresh still running... ${formatElapsedSeconds()}s elapsed.`);
        }, 12000);
        const data = await postJson('/api/races/upcoming/refresh', {});
        await refreshDataAndUI();
        const failedCount = Array.isArray(data.failed_races) ? data.failed_races.length : 0;
        appendConsoleLine(
            `[Prefetch] Legacy upcoming refresh complete in ${(performance.now() - startedAt).toFixed(0)}ms ` +
            `(races=${data.updated_races || 0}, rows=${data.updated_rows || 0}, failed=${failedCount}).`
        );
        alert(`Upcoming refresh complete. Races updated: ${data.updated_races || 0}, rows updated: ${data.updated_rows || 0}, failed races: ${failedCount}.`);
    } catch (err) {
        appendConsoleLine(`[Prefetch] Update action failed: ${err.message}`);
        alert(`Update action failed: ${err.message}`);
    } finally {
        if (progressTimer) clearInterval(progressTimer);
        if (btn) btn.disabled = false;
        if (btn) btn.textContent = originalLabel;
        syncUpcomingRefreshButtonState();
    }
}

async function importDayResultsFromCalendar() {
    const dateInput = document.getElementById('import-day-date');
    const targetDate = (dateInput?.value || '').trim();
    if (!targetDate) {
        alert('Pick a day first.');
        return;
    }

    const proceed = confirm(`Import race results for ${targetDate}?\n\nThe app will prefer history data and fall back to result data.`);
    if (!proceed) return;

    appendConsoleLine(`[Import] Requested day import for ${targetDate}...`);

    try {
        const result = await postJson('/api/races/day/import-results', { date: targetDate });
        await refreshDataAndUI();
        switchMainTab(targetDate);
        appendConsoleLine(
            `[Import] Completed ${result.date}: races_found=${result.races_found}, imported=${result.races_imported}, ` +
            `updated_entries=${result.updated_entries}, history=${result.sources?.history || 0}, ` +
            `result=${result.sources?.result || 0}, result_direct=${result.sources?.result_direct || 0}`
        );
        alert(
            `Import complete for ${result.date}.\n` +
            `Races found: ${result.races_found}\n` +
            `New races imported: ${result.races_imported}\n` +
            `Entries updated: ${result.updated_entries}\n` +
            `Source usage -> history: ${result.sources?.history || 0}, result: ${result.sources?.result || 0}, result_direct: ${result.sources?.result_direct || 0}`
        );
    } catch (err) {
        appendConsoleLine(`[Import] Failed for ${targetDate}: ${err.message}`);
        alert(`Import failed: ${err.message}`);
    }
}

async function deleteDayData() {
    const dateInput = document.getElementById('delete-day-date');
    const scopeInput = document.getElementById('delete-day-scope');
    const targetDate = (dateInput?.value || '').trim();
    const scope = (scopeInput?.value || 'marks').trim();

    if (!targetDate) {
        alert('Pick a day first.');
        return;
    }

    const warningByScope = {
        marks: 'This will remove all marks for races on that day.',
        entries: 'This will remove all race entries for that day from cache.',
        all: 'This will remove marks, entries, and day horse dictionary entries.'
    };
    const confirmed = confirm(`${warningByScope[scope] || 'Proceed?'}\n\nDay: ${targetDate}`);
    if (!confirmed) return;

    try {
        const result = await postJson('/api/day/delete', { date: targetDate, scope: scope });
        alert(`Done. Races removed: ${result.removed_races}, marks removed: ${result.removed_marks}, horse dict entries removed: ${result.removed_horse_entries}`);
        if (scope === 'marks' || scope === 'all') {
            const marksRes = await fetch('/api/marks');
            const marksPayload = normalizeMarksPayload(await marksRes.json());
            globalMarks = marksPayload.marks;
            globalRaceMeta = marksPayload.raceMeta;
            globalMarksVersion = marksPayload.version;
        }
        await refreshDataAndUI();
        switchMainTab(targetDate);
    } catch (err) {
        alert(`Delete failed: ${err.message}`);
    }
}

async function refreshRaceHistory(event, r_id) {
    event.stopPropagation();
    const raceInfo = globalRaceInfo[r_id] || {};
    const raceDate = raceInfo.clean_date || null;

    try {
        const result = await postJson(`/api/races/${encodeURIComponent(r_id)}/refresh-history`, {});
        await refreshDataAndUI();
        if (raceDate) switchMainTab(raceDate);
        alert(`History refreshed for ${result.updated_entries || 0} entries.`);
    } catch (err) {
        alert(`History refresh failed: ${err.message}`);
    }
}

async function closeServerInstances() {
    const confirmed = confirm('Close all running UMAnager server instances on port 8000?');
    if (!confirmed) return;

    try {
        await fetch('/api/server/shutdown', { method: 'POST' });

        // Best-effort clean exit: close this tab/window after server shutdown signal.
        setTimeout(() => {
            try {
                window.open('', '_self');
                window.close();
            } catch (e) {
                
            }

            // Fallback if browser blocks window.close() for user-opened tabs.
            if (!window.closed) {
                window.location.replace('about:blank');
            }
        }, 150);
    } catch (err) {
        alert(`Failed to send shutdown command: ${err.message}`);
    }
}

async function triggerScrape(mode) {
    document.getElementById('btn-new-race').disabled = true;
    document.getElementById('btn-all-race').disabled = true;
    
    // Reveal and prepare the console
    const consoleBox = document.getElementById('scrape-console');
    if ((appConfig?.ui?.showConsole ?? false) && isDevModeEnabled()) consoleBox.style.display = 'block';
    consoleBox.textContent = "Waking up scraper...";
    
    // Start pinging the Python server for console text every 500 milliseconds
    logInterval = setInterval(fetchLogs, 500);

    try {
        const scrapeRes = await fetch('/api/scrape', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({mode: mode})
        });
        const scrapeData = await scrapeRes.json().catch(() => ({}));
        if (!scrapeRes.ok) {
            throw new Error(scrapeData.detail || scrapeData.message || `HTTP ${scrapeRes.status}`);
        }

        await fetchLogs(); // Grab any final lines
        await loadRaces();

        // Load master horse pedigree data (automatic incremental update)
        try {
            consoleBox.textContent += "\n\nLoading horse pedigree data...";
            const masterRes = await postJson('/api/jvlink/load-master-data', {
                is_initial: false,
                max_records: 50000,
                max_status_wait_seconds: 180
            });
            if (masterRes.data?.ok) {
                consoleBox.textContent += `\n✓ Horse pedigree loaded (${masterRes.data.recordsRead || 0} records)`;
            } else {
                consoleBox.textContent += `\n⚠ Pedigree load skipped or incomplete`;
            }
        } catch (e) {
            consoleBox.textContent += `\n⚠ Pedigree load error: ${e.message}`;
        }

        if (Number(scrapeData.cached_races || 0) === 0) {
            const activeEngine = String(appConfig?.backend?.dataEngine || scrapeData?.data_engine || 'nk').toLowerCase();
            if (activeEngine === 'jv') {
                alert('JRA-VAN scrape completed but cached 0 races. Strict engine isolation is active, so Netkeiba fallback is disabled. This means no JV-native races were discovered/decoded for the current window yet. Check scrape console logs.');
            } else {
                alert('Scrape completed but cached 0 races. This usually means no races matched the current discovery window. Try Full Re-Scrape and check scrape console logs.');
            }
        }
    } catch (err) {
        alert(`Scrape failed: ${err.message}`);
    } finally {
        clearInterval(logInterval);
        document.getElementById('btn-new-race').disabled = false;
        document.getElementById('btn-all-race').disabled = false;
    }
}

async function fetchLogs() {
    try {
        const res = await fetch('/api/scrape/log');
        const data = await res.json();
        const consoleBox = document.getElementById('scrape-console');
        
        // Keep scraper logs as text to avoid rendering arbitrary HTML from logs.
        consoleBox.textContent = data.logs.join('\n');
        
        // Auto-scroll to the absolute bottom so you always see the latest action
        consoleBox.scrollTop = consoleBox.scrollHeight;
    } catch (e) {
        
    }
}

// --- Live View Popout ---
function parseFinishRank(value) {
    const text = String(value ?? '').trim();
    const match = text.match(/\d+/);
    if (!match) return null;
    const rank = parseInt(match[0], 10);
    return Number.isFinite(rank) ? rank : null;
}

function collectRaceMainMarks(raceId) {
    const marks = {};
    const validSymbols = new Set(["◎", "〇", "▲", "△"]);

    for (const [key, symbol] of Object.entries(globalMarks)) {
        if (!symbol || !validSymbols.has(symbol)) continue;
        const [r_id, h_id] = key.split('_');
        if (r_id !== raceId || !h_id) continue;
        marks[symbol] = h_id;
    }

    return marks;
}

function setRaceCollapsedState(r_id, shouldCollapse) {
    const content = document.getElementById(`content-${r_id}`);
    const header = document.getElementById(`header-${r_id}`);
    const arrow = document.getElementById(`arrow-${r_id}`);
    if (!content || !header || !arrow) return;

    content.classList.toggle('collapsed', shouldCollapse);
    header.classList.toggle('collapsed', shouldCollapse);
    arrow.innerText = shouldCollapse ? '▶' : '▼';
}

// Phase 11 backward: look up actual payouts for a past race from
// race.info.results_json (populated by HR parser). Returns ¥ amounts per
// hit type; 0 means data unavailable or combo not found.
function lookupRacePayouts(race, ppByRank) {
    const raw = race?.info?.results_json;
    if (!raw || !ppByRank) return { win: 0, quinella: 0, trio: 0 };

    let payouts;
    try { payouts = typeof raw === 'string' ? JSON.parse(raw) : raw; }
    catch (_) { return { win: 0, quinella: 0, trio: 0 }; }

    const findPayout = (arr, comboKey) => {
        if (!Array.isArray(arr)) return 0;
        const key = JSON.stringify(comboKey);
        for (const slot of arr) {
            const slotCombo = Array.isArray(slot?.combo) ? [...slot.combo].sort((a,b) => a-b) : null;
            if (!slotCombo) continue;
            if (JSON.stringify(slotCombo) === key) {
                return parseInt(slot?.payout, 10) || 0;
            }
        }
        return 0;
    };

    const pp1 = ppByRank[1], pp2 = ppByRank[2], pp3 = ppByRank[3];
    return {
        win:      (pp1 ? findPayout(payouts.win, [pp1]) : 0),
        quinella: (pp1 && pp2 ? findPayout(payouts.quinella, [pp1, pp2].sort((a,b) => a-b)) : 0),
        trio:     (pp1 && pp2 && pp3 ? findPayout(payouts.trio, [pp1, pp2, pp3].sort((a,b) => a-b)) : 0)
    };
}

// ── Template-aware bet outcome (sunk-cost / winnings) ──────────────────────
// The OLD model assumed you always bet Win + Q Box + T Box. You don't — OrePro
// auto-fires ONE template per mark-count (your 6-slot ladder). This evaluator
// mirrors that ladder so winnings reflect the bets you ACTUALLY placed.
// Keep in sync with OREPRO_CAPABILITIES.md + the C# TemplateBetEvaluator.
const BET_UNIT_YEN = 100; // each ladder combo is a ¥100 ticket

// All marked runners for a race WITH their post positions. Unlike collectRaceMainMarks
// (symbol-keyed → drops duplicate △), this keeps every marked horse — required for the
// 5/6-mark templates that carry multiple △.
function collectRaceMarkedRunners(raceId) {
    const validSymbols = new Set(["◎", "〇", "▲", "△"]);
    const ppByHorse = {};
    (globalRaceEntries[raceId] || []).forEach(row => {
        const hid = String(row?.Horse_ID ?? '').split('.')[0].trim();
        const pp = parseInt(row?.PP, 10);
        if (hid && Number.isFinite(pp) && pp > 0) ppByHorse[hid] = pp;
    });
    const runners = [];
    for (const [key, symbol] of Object.entries(globalMarks)) {
        if (!symbol || !validSymbols.has(symbol)) continue;
        const us = key.indexOf('_');
        if (us < 0) continue;
        const r_id = key.slice(0, us), h_id = key.slice(us + 1);
        if (r_id !== raceId || !h_id) continue;
        runners.push({ horseId: h_id, symbol, pp: ppByHorse[h_id] || null });
    }
    return runners;
}

// DISCIPLINE = ENGINE-DRIVEN, NOT MARK-DRIVEN. The disciplined place (or small-field token) bets the
// COLD ENGINE'S OWN ◎ (top-3 ranking), sourced live — so your ◎〇▲△ marks are pure analysis and can
// NEVER move a bet. This is the full decouple that kills last weekend's accidental side bets at the
// root: marks and money are no longer the same thing under Discipline. Returns the same runner shape
// as collectRaceMarkedRunners ({ horseId, symbol, pp }); the place line uses only the ◎, the token
// boxes the set. Empty only when the field itself is empty.
function collectDisciplineEngineRunners(raceId) {
    const ppByHorse = {};
    (globalRaceEntries[raceId] || []).forEach(row => {
        const hid = String(row?.Horse_ID ?? '').split('.')[0].trim();
        const pp = parseInt(row?.PP, 10);
        if (hid && Number.isFinite(pp) && pp > 0) ppByHorse[hid] = pp;
    });
    return getUnconditionalAutoBetRankingsForRace(raceId)
        .slice(0, 3)
        .map(p => ({ horseId: p.h_id, symbol: p.symbol, pp: ppByHorse[p.h_id] || null }))
        .filter(r => r.pp);
}

function findSlotPayout(arr, combo) {
    if (!Array.isArray(arr)) return 0;
    const key = JSON.stringify([...combo].sort((a, b) => a - b));
    for (const slot of arr) {
        const sc = Array.isArray(slot?.combo) ? [...slot.combo].sort((a, b) => a - b) : null;
        if (sc && JSON.stringify(sc) === key) return parseInt(slot?.payout, 10) || 0;
    }
    return 0;
}

function nCk(n, k) { // combinations
    if (k < 0 || k > n) return 0;
    let r = 1;
    for (let i = 0; i < k; i++) r = r * (n - i) / (i + 1);
    return Math.round(r);
}

// Build concrete bet LINES from a composition (the user's chosen line list) for a given set
// of marked runners. Each composition line carries a TOTAL ¥; per-combo = floor100(total ÷ 点数)
// so amounts are real ¥100 OrePro units. A line is dropped if the mark count can't form it
// (e.g. trio with 2 marks) or it's underfunded (< ¥100/combo). Each built line keeps a
// `_specIndex` back-pointer so the composer can show its 点数/¥ readout. Returns { lines, staked }.
// OrePro-faithful pricing for the DEFAULT easy-mode template (単勝◎ + 馬連BOX + 3連複BOX, 50/30/20).
// OrePro auto-fires this when marks are sent, and it prices it differently from our generic uniform-
// spend redistributor: the WIN line is a fixed anchor (the remainder), and when the 3連複 can't form
// (n=2) its budget cascades to the 馬連 (→ 50/50), NOT proportionally onto the 単勝. This MIRRORS
// TemplateBetEvaluator.BuildLines(Default) exactly, so the lines we FREEZE at apply match what OrePro
// actually placed — keeping the recap reconciled to the yen. (T2-1: JS used to diverge here, pricing
// a 2-mark default as 6300/3700; C# already priced it 5000/5000 to match OrePro. JS now agrees.)
function buildOreProDefaultLines(comp, runners, honmei, hasHonmei) {
    const round100 = x => Math.max(100, Math.round((Number(x) || 0) / 100) * 100);
    const n = (runners || []).length;
    const yenOf = (type) => { const l = (comp?.lines || []).find(x => x.type === type); return l ? Math.max(0, parseInt(l.yen, 10) || 0) : 0; };
    const total = (comp?.lines || []).reduce((s, l) => s + Math.max(0, parseInt(l.yen, 10) || 0), 0);
    if (total <= 0) return { lines: [], staked: 0 };
    const quinNom = yenOf('quinella_box');
    const trioNom = yenOf('trio_box');
    const hasTrio = n >= 3 && trioNom > 0;

    const out = [];
    // Box lines priced from their shares (n=2: the 馬連 absorbs the dropped 3連複 budget); the 単勝
    // then takes the remainder so the placed total lands EXACTLY on the intended ¥ (mirrors C#).
    if (n >= 2 && quinNom > 0) {
        const c = nCk(n, 2);
        const lineTotal = quinNom + (hasTrio ? 0 : trioNom);
        const per = c > 0 ? round100(lineTotal / c) : 0;
        if (per >= 100) out.push({ ticket: BET_LINE_TYPES.quinella_box.ticket, method: BET_LINE_TYPES.quinella_box.method, label: BET_LINE_TYPES.quinella_box.jpLabel, horses: runners.slice(), comboCount: c, stakePerCombo: per, _specIndex: 1 });
    }
    if (hasTrio) {
        const c = nCk(n, 3);
        const per = c > 0 ? round100(trioNom / c) : 0;
        if (per >= 100) out.push({ ticket: BET_LINE_TYPES.trio_box.ticket, method: BET_LINE_TYPES.trio_box.method, label: BET_LINE_TYPES.trio_box.jpLabel, horses: runners.slice(), comboCount: c, stakePerCombo: per, _specIndex: 2 });
    }
    if (hasHonmei) {
        const others = out.reduce((s, l) => s + l.stakePerCombo * l.comboCount, 0);
        const winPer = total - others; // fixed anchor = exact remainder
        if (winPer >= 100) out.unshift({ ticket: BET_LINE_TYPES.win.ticket, method: BET_LINE_TYPES.win.method, label: BET_LINE_TYPES.win.jpLabel, horses: [honmei], comboCount: 1, stakePerCombo: winPer, _specIndex: 0 });
    }
    const staked = out.reduce((s, l) => s + l.stakePerCombo * l.comboCount, 0);
    return { lines: out, staked };
}

function buildLinesFromComposition(comp, runners) {
    const floor100 = x => Math.floor((Number(x) || 0) / 100) * 100;
    const n = (runners || []).length;
    const honmei = runners.find(r => r.symbol === '◎') || runners[0] || null;
    const hasHonmei = !!(honmei && honmei.pp);

    // The OrePro easy-mode DEFAULT prices via its own faithful allocator (mirrors C#); every other
    // composition is a CUSTOM bet we place verbatim, so the generic uniform-spend redistribution below
    // is correct for those (OrePro places exactly the lines we send). Only the default needs mirroring.
    if (compositionPresetId(comp) === DEFAULT_PRESET) {
        return buildOreProDefaultLines(comp, runners, honmei, hasHonmei);
    }

    // UNIFORM-SPEND model (operator pref 2026-06-12): a composition always bets its FULL intended
    // total (Σ line ¥, e.g. ¥10,000) regardless of how many marks the race has. Lines that can't
    // form at this mark count — a 3連複 box with <3 marks, a ワイド with <2, a 単勝 with no ◎ —
    // have their budget REDISTRIBUTED proportionally across the lines that CAN form. When nothing
    // drops (e.g. the tuned 4-mark card) this is a no-op and the preset ¥ are preserved exactly.
    const specs = (comp?.lines || [])
        .map((spec, idx) => ({ idx, t: BET_LINE_TYPES[spec.type], yen: Math.max(0, parseInt(spec.yen, 10) || 0) }))
        .filter(s => s.t && s.yen > 0);
    const targetTotal = specs.reduce((sum, s) => sum + s.yen, 0);
    if (targetTotal <= 0) return { lines: [], staked: 0 };

    // Formable at this mark count: enough marks, ≥1 combination, and (for ◎-anchored lines) a ◎.
    const formable = specs
        .map(s => ({ ...s, combos: s.t.combos(n) }))
        .filter(s => n >= s.t.minMarks && s.combos > 0
                     && !((s.t.pick === 'honmei' || s.t.pick === 'opp') && !hasHonmei));
    if (!formable.length) return { lines: [], staked: 0 }; // e.g. 1 mark on a box-only preset → can't bet

    // Scale the formable lines back up to the full target, then quantize to ¥100/combo.
    const formableYen = formable.reduce((sum, s) => sum + s.yen, 0);
    let built = formable.map(s => {
        const scaled = targetTotal * s.yen / formableYen;
        const perCombo = floor100(s.combos === 1 ? scaled : scaled / s.combos);
        return { s, perCombo, lineTotal: perCombo * s.combos };
    }).filter(b => b.perCombo >= 100); // can't place a real <¥100/combo ticket
    if (!built.length) return { lines: [], staked: 0 };

    // Floor-rounding leaves a sub-¥100×combos remainder vs target. Hand it to the most flexible
    // survivor (fewest combos — a 1-combo line absorbs any ¥100 remainder exactly), so the placed
    // total lands on the target. In the redistribution cases a low-combo survivor almost always exists.
    const remainder = floor100(targetTotal - built.reduce((sum, b) => sum + b.lineTotal, 0));
    if (remainder > 0) {
        const absorber = built.reduce((best, b) => (b.s.combos < best.s.combos ? b : best), built[0]);
        const addPerCombo = floor100(remainder / absorber.s.combos);
        if (addPerCombo > 0) { absorber.perCombo += addPerCombo; absorber.lineTotal = absorber.perCombo * absorber.s.combos; }
    }

    const out = built.map(({ s, perCombo }) => {
        const t = s.t;
        let horses, axisPp;
        if (t.pick === 'honmei')      { horses = [honmei]; }
        else if (t.pick === 'opp')    { horses = runners.filter(r => r !== honmei); axisPp = honmei.pp; }
        else                          { horses = runners.slice(); }
        return {
            ticket: t.ticket, method: t.method, label: t.jpLabel,
            horses, ...(axisPp ? { axisPp } : {}),
            comboCount: s.combos, stakePerCombo: perCombo, _specIndex: s.idx,
        };
    });
    const staked = out.reduce((sum, l) => sum + l.stakePerCombo * l.comboCount, 0);
    return { lines: out, staked };
}

// ── The bet-plan seam ────────────────────────────────────────────────────────
// A race's placed bets = a list of bet LINES. buildRaceBetLines() derives them from the
// resolved COMPOSITION (per-race override → day setting → default preset), OR returns the
// FROZEN lines verbatim once the race is applied. Each line is self-describing:
//   { ticket, method, label, horses:[{pp}], axisPp?, comboCount, stakePerCombo }
// so staked = Σ comboCount·stakePerCombo and won is priced per line. When applied, the built
// list is frozen and the C# side scores that exact list — the two never drift for placed bets.
function buildRaceBetLines(race) {
    const info = race?.info || {};
    const raceId = String(info.race_id || '').trim();

    // Scratched horses (取消/除外) are removed from the bet by OrePro, which shrinks the
    // ticket's 点数 and stake — e.g. a 4-mark 3連複 box with one 除外 collapses to a single
    // combo (¥400 → ¥100, as seen on Kyoto R9 2026-05-31). Drop them so the synthesized
    // ticket matches what actually fires. Reads the authoritative entry.Scratched flag
    // (backend-set from the SE 異常区分 code; removal codes 取消=1 / 除外=2 / 競走除外=3 per the
    // JRA-VAN spec). Only populates once results settle, so this is a no-op pre-race.
    // NOTE: 中止 (raced but stopped) is code 4, NOT a scratch — that ticket stands and loses with
    // full combos — so the backend leaves Scratched=false for it.
    const scratchedHorseIds = new Set(
        (Array.isArray(race?.entries) ? race.entries : [])
            .filter(row => row?.Scratched === true)
            .map(row => String(row?.Horse_ID ?? '').split('.')[0].trim())
            .filter(Boolean)
    );
    // Under Discipline the bet is engine-driven: source runners from the cold engine's own ◎ ranking,
    // not the user's marks (full decouple — marks are analysis-only). A per-race bet override is an
    // explicit manual intent, so it still reads marks. Frozen/locked bets bypass this entirely below.
    const useEngineRunners = isDisciplineMode() && !getRaceBetCompositionOverride(raceId);
    const runners = (useEngineRunners ? collectDisciplineEngineRunners(raceId) : collectRaceMarkedRunners(raceId))
        .filter(r => !scratchedHorseIds.has(r.horseId));
    const n = runners.length;
    if (n === 0) return { runners, lines: [], staked: 0 };

    // If this race froze its bet lines at APPLY (or carries explicit custom-ticket lines),
    // use them verbatim — the bet is recorded as actually placed, immune to later day/per-race
    // composition or stake changes. C# scores this exact frozen list, so they never drift.
    const _profile = getRaceBetProfile(raceId);
    const _frozen = _profile?.betLines;
    if (Array.isArray(_frozen) && _frozen.length) {
        const lines = _frozen.map(l => ({ ...l, horses: (l.horses || []).map(h => ({ ...h })) }));
        const staked = lines.reduce((s, l) => s + (l.stakePerCombo || 0) * (l.comboCount || 0), 0);
        return { runners, lines, staked: Math.round(staked) };
    }

    // Not frozen → price LIVE from the resolved composition (per-race override → day → default).
    const comp = resolveBetComposition(raceId);
    const built = buildLinesFromComposition(comp, runners);
    return { runners, lines: built.lines, staked: built.staked };
}

// Score one bet line against the finishing result. Returns ¥ won for that line.
// Extend here for new (ticket, method) combos when dynamic bets arrive.
function scoreBetLine(line, t3, t3set, payouts) {
    const stakeFactor = (line.stakePerCombo || 0) / 100; // payouts are per-¥100
    const pps = (line.horses || []).map(h => h.pp).filter(Boolean);
    let won = 0;
    if (line.ticket === 'win') {
        // 単勝 — single horse must finish 1st (t3[0] = winner).
        const pp = pps[0];
        if (pp && t3[0] === pp) won += findSlotPayout(payouts.win, [pp]) * stakeFactor;
    } else if (line.ticket === 'quinella' && line.method === 'box') {
        // 馬連 box — any pair of picks that are the top 2 (t3[0], t3[1]) in either order.
        const top2 = new Set([t3[0], t3[1]]);
        for (let i = 0; i < pps.length; i++) for (let j = i + 1; j < pps.length; j++)
            if (top2.has(pps[i]) && top2.has(pps[j]))
                won += findSlotPayout(payouts.quinella, [pps[i], pps[j]]) * stakeFactor;
    } else if (line.ticket === 'place') {
        const pp = pps[0];
        if (pp && t3set.has(pp)) won += findSlotPayout(payouts.place, [pp]) * stakeFactor;
    } else if (line.ticket === 'wide' && line.method === 'box') {
        for (let i = 0; i < pps.length; i++) for (let j = i + 1; j < pps.length; j++)
            if (t3set.has(pps[i]) && t3set.has(pps[j]))
                won += findSlotPayout(payouts.wide, [pps[i], pps[j]]) * stakeFactor;
    } else if (line.ticket === 'trio' && line.method === 'box') {
        const set = new Set(pps);
        if (t3.every(pp => set.has(pp))) won += findSlotPayout(payouts.trio, t3) * stakeFactor;
    } else if (line.ticket === 'trio' && line.method === 'nagashi1') {
        if (line.axisPp && t3set.has(line.axisPp)) {
            const opp = new Set(pps);
            const others = t3.filter(pp => pp !== line.axisPp);
            if (others.length === 2 && others.every(pp => opp.has(pp)))
                won += findSlotPayout(payouts.trio, t3) * stakeFactor;
        }
    }
    // Unknown (ticket, method) → 0 for now; add a branch when dynamic bets introduce it.
    return Math.round(won);
}

// Evaluate what the race's placed bets actually won. Returns staked, won, and per-line
// detail for display. Source-agnostic: consumes whatever buildRaceBetLines() produced.
function evaluateTemplateOutcome(race) {
    const info = race?.info || {};
    const raceId = String(info.race_id || '').trim();
    const raceLabel = `${trackName(info.place)} R${info.race_number || '?'}`.trim();
    const plan = buildRaceBetLines(race);
    const out = { raceId, raceLabel, markCount: plan.runners.length, hasResults: false,
                  staked: plan.staked, won: 0, lines: [], anyHit: false };
    if (plan.runners.length === 0) return out;

    // IMPORTED races carry the ACTUAL OrePro money (actualStaked / actualWon) — this is the
    // ground truth and OVERRIDES any synthesized template. Day-list text is lossy (no per-line
    // breakdown), so we report the recorded staked/won directly. Without this, the synthesized
    // 馬連/3連複 box would score "hits" on tickets the user never actually placed (e.g. a 馬連 box
    // cashing while the real 3連複 box lost) — the source of the phantom-win bug.
    const _imp = getRaceBetProfile(raceId);
    if (_imp && _imp.actualStaked != null) {
        const won = _imp.actualWon != null ? _imp.actualWon : 0;
        out.staked = _imp.actualStaked;
        out.won = won;
        out.anyHit = won > 0;
        out.hasResults = true; // imported = settled by definition
        out.lines = won > 0 ? [{ label: '実績', payout: won }] : [];
        return out;
    }

    const entries = Array.isArray(race?.entries) ? race.entries : [];
    const top3pp = {};
    entries.forEach(row => {
        const rank = parseFinishRank(row?.Finish);
        if (!rank || rank < 1 || rank > 3) return;
        const pp = parseInt(row?.PP, 10);
        if (top3pp[rank] == null && Number.isFinite(pp) && pp > 0) top3pp[rank] = pp;
    });
    if (!(top3pp[1] && top3pp[2] && top3pp[3])) return out; // unsettled → staked known, won 0

    let payouts = null;
    try { const raw = info.results_json; payouts = typeof raw === 'string' ? JSON.parse(raw) : raw; }
    catch (_) { payouts = null; }
    // s60 fix: finish positions and the payout table are two separate fields that can land in the
    // browser at slightly different times (a live results patch updating entries[].Finish before
    // info.results_json refreshes). Scoring against an empty/missing payout object would silently
    // read every line as a loss — exactly what happened on a real Kokura win tonight: the race
    // header showed "lost" and Day Net didn't move, while the backend (reading the same DB column
    // directly, never racing a stale client copy) correctly pinged the win to Discord. Wait for
    // actual payout data instead of declaring hasResults on an incomplete read — the next poll/patch
    // re-evaluates this from scratch, so it self-corrects with no extra invalidation needed.
    const hasAnyPayoutData = payouts && typeof payouts === 'object'
        && ['win', 'place', 'quinella', 'wide', 'trio'].some(k => Array.isArray(payouts[k]) && payouts[k].length > 0);
    if (!hasAnyPayoutData) return out; // top-3 known but payouts not populated yet — stay pending
    out.hasResults = true;

    const t3 = [top3pp[1], top3pp[2], top3pp[3]];
    const t3set = new Set(t3);
    for (const line of plan.lines) {
        const won = scoreBetLine(line, t3, t3set, payouts);
        if (won > 0) { out.won += won; out.lines.push({ label: line.label, payout: won }); }
    }
    out.anyHit = out.won > 0;
    return out;
}

function getDayOverallHitSummary(targetDate) {
    const date = String(targetDate || '').trim();
    const timeline = globalDateTimelineByDate[date] || null;
    const races = Array.isArray(globalRacesByDate[date]) ? globalRacesByDate[date] : [];

    if (!date || timeline !== 'past' || !races.length) {
        return {
            visible: false,
            total: 0,
            correct: 0,
            rate: 0,
            winningRaceIds: []
        };
    }

    // A "win" = the bet you ACTUALLY placed cashed. computeRaceNet prices the real
    // template (place/wide/trio box/nagashi, or frozen/imported lines) for LOCKED races
    // with settled results, and returns null otherwise — so this matches OrePro's 的中数
    // and the Day-Total-Net display exactly. (Was the loose honmei||quinella||trio recap,
    // which counted "would-have-hit" shapes you never bet → phantom wins.)
    const winningRaceIds = [];
    let total = 0;
    let correct = 0;

    races.forEach(race => {
        const net = computeRaceNet(race);
        if (!net) return;
        total += 1;
        if (net.anyHit) {
            correct += 1;
            const r_id = String(race?.info?.race_id || '').trim();
            if (r_id) winningRaceIds.push(r_id);
        }
    });

    const rate = total > 0 ? Math.round((correct / total) * 100) : 0;
    return {
        visible: total > 0,
        total,
        correct,
        rate,
        winningRaceIds
    };
}

function getDayDetailedHitSummary(targetDate) {
    const date = String(targetDate || '').trim();
    const timeline = globalDateTimelineByDate[date] || null;
    const races = Array.isArray(globalRacesByDate[date]) ? globalRacesByDate[date] : [];

    // Actual-bet outcome (mirrors OrePro's 的中数 / 収支 / 回収率), priced from the real
    // placed template via computeRaceNet (locked + settled only).
    const summary = {
        visible: false,
        date,
        timeline,
        betRaces: 0,   // placed (locked) + settled races
        betHits: 0,    // of those, how many actually cashed
        staked: 0,
        won: 0,
        net: 0,
        recovery: 0
    };

    if (!date || timeline !== 'past' || !races.length) {
        return summary;
    }

    races.forEach(race => {
        const net = computeRaceNet(race);
        if (!net) return;
        summary.betRaces += 1;
        summary.staked += net.spentYen;
        summary.won += net.wonYen;
        if (net.anyHit) summary.betHits += 1;
    });

    summary.net = Math.round(summary.won - summary.staked);
    summary.recovery = summary.staked > 0 ? Math.round((summary.won / summary.staked) * 100) : 0;
    summary.visible = summary.betRaces > 0;
    return summary;
}

function pct(part, total) {
    if (!total) return 0;
    return Math.round((part / total) * 100);
}

function buildVotingRecapHtml(targetDate) {
    const summary = getDayDetailedHitSummary(targetDate);
    if ((summary.timeline || '') !== 'past') {
        return '';
    }

    if (!summary.visible) {
        return '<div class="voting-recap-note">No placed (locked) bets for this day yet. Lock a race\'s marks to record it as a bet.</div>';
    }

    const hitRate = pct(summary.betHits, summary.betRaces);
    const sign = summary.net >= 0 ? '+' : '-';
    const netCls = summary.net >= 0 ? 'is-positive' : 'is-negative';
    return `
    <div class="voting-recap-grid">
        <div class="voting-recap-item"><span>的中数 Bet Hits</span><strong>${summary.betHits}/${summary.betRaces} (${hitRate}%)</strong></div>
        <div class="voting-recap-item"><span>収支 Net</span><strong class="${netCls}">${sign}¥${Math.abs(summary.net).toLocaleString()}</strong></div>
        <div class="voting-recap-item"><span>回収率 Recovery</span><strong>${summary.recovery}%</strong></div>
        <div class="voting-recap-item"><span>Staked / Won</span><strong>¥${Math.round(summary.staked).toLocaleString()} / ¥${Math.round(summary.won).toLocaleString()}</strong></div>
    </div>`;
}

function applyWinningVotesFocus() {
    const summary = getDayOverallHitSummary(currentActiveDate);
    if (!summary.visible) return;

    const winSet = new Set(summary.winningRaceIds);
    (globalRacesByDate[currentActiveDate] || []).forEach(race => {
        const r_id = String(race?.info?.race_id || '').trim();
        if (!r_id) return;

        const shouldCollapse = winningVotesFocusEnabled ? !winSet.has(r_id) : false;
        setRaceCollapsedState(r_id, shouldCollapse);
    });

    applyWinningVotesFocusToVotingSidebar(summary);
}

function applyWinningVotesFocusToVotingSidebar(summary) {
    const sidebar = document.getElementById('voting-sidebar-display');
    if (!sidebar) return;

    const winSet = new Set(summary?.winningRaceIds || []);
    sidebar.querySelectorAll('.voting-race-card').forEach(card => {
        const r_id = String(card.dataset.rid || '').trim();
        const shouldCollapse = winningVotesFocusEnabled && summary?.visible ? !winSet.has(r_id) : false;
        card.classList.toggle('is-collapsed', shouldCollapse);
    });
}

function updateWinningVotesFocusButton() {
    const btn = document.getElementById('btn-winning-votes-focus');
    if (!btn) return;

    const summary = getDayOverallHitSummary(currentActiveDate);
    const shouldShow = summary.visible || currentMainView === 'voting';
    if (!shouldShow) {
        winningVotesFocusEnabled = false;
        btn.style.display = 'none';
        btn.classList.remove('is-active');
        return;
    }

    btn.style.display = 'inline-block';
    if (!summary.visible) {
        winningVotesFocusEnabled = false;
        btn.classList.remove('is-active');
        btn.textContent = '🏁 Hit N/A (0/0)';
        btn.title = 'Hit rate appears once there are scored races with results.';
        return;
    }

    btn.classList.toggle('is-active', winningVotesFocusEnabled);
    const modeLabel = winningVotesFocusEnabled ? ' (Winners Only)' : '';
    btn.textContent = `🏁 Hit ${summary.rate}% (${summary.correct}/${summary.total})${modeLabel}`;
    btn.title = winningVotesFocusEnabled
        ? 'Showing only races where at least one of your bet types hit. Click to reset.'
        : 'Collapse non-winning races for this day.';
}

function toggleWinningVotesFocus() {
    const summary = getDayOverallHitSummary(currentActiveDate);
    if (!summary.visible) return;

    winningVotesFocusEnabled = !winningVotesFocusEnabled;
    applyWinningVotesFocus();
    updateWinningVotesFocusButton();
}

function computeRaceNet(race) {
    // Placed (locked) bets only, priced from the ACTUAL template (place/wide/trio).
    const out = evaluateTemplateOutcome(race);
    if (out.markCount === 0 || !isRaceLocked(out.raceId) || !out.hasResults) return null;
    return { wonYen: out.won, spentYen: out.staked, netYen: out.won - out.staked, anyHit: out.anyHit };
}

function buildDayTotalNetHtml(date) {
    const races = globalRacesByDate[date] || [];
    const timeline = globalDateTimelineByDate[date] || '';
    if (timeline !== 'past' || !races.length) return '';

    let won = 0, spent = 0, scored = 0, hits = 0;
    races.forEach(race => {
        const net = computeRaceNet(race);
        if (!net) return;
        won += net.wonYen;
        spent += net.spentYen;
        scored += 1;
        if (net.anyHit) hits += 1;
    });
    if (!scored) return '';

    const net = Math.round(won - spent);
    const sign = net >= 0 ? '+' : '-';
    const abs = Math.abs(net).toLocaleString();
    const cls = net >= 0 ? 'is-positive' : 'is-negative';
    const hitPct = scored > 0 ? Math.round((hits / scored) * 100) : 0;
    return `
        <span class="voting-day-total-label">Today</span>
        <span class="voting-day-total-net ${cls}">${sign}¥${abs}</span>
        <span class="voting-day-total-detail">won ¥${Math.round(won).toLocaleString()} − spent ¥${Math.round(spent).toLocaleString()}</span>
        <span class="voting-day-total-detail">Hit ${hits}/${scored} (${hitPct}%)</span>
    `;
}

// Voting tab — bet-type breakdown. Tallies how many of the day's marked races resolve to each bet
// preset (the per-race type in Auto mode, or the day preset otherwise) so the operator can see the
// spread at a glance — especially useful under 🧪 Auto where every race can be a different type.
function buildBetTypeBreakdownHtml(date) {
    const races = globalRacesByDate[date] || [];
    if (!races.length) return '';
    const counts = {};
    let total = 0, placed = 0;
    races.forEach(race => {
        const rid = String(race?.info?.race_id || '').trim();
        if (!rid || countRaceMarks(rid) === 0) return;
        let label = 'Custom';
        try { label = compositionLabel(resolveBetComposition(rid)); } catch (_) {}
        counts[label] = (counts[label] || 0) + 1;
        total += 1;
        if (globalOreProApplyState?.[rid]?.submitted) placed += 1;
    });
    if (!total) return '';
    const chips = Object.entries(counts).sort((a, b) => b[1] - a[1]).map(([label, n]) =>
        `<span style="display:inline-flex;align-items:center;gap:5px;background:#1b2230;border:1px solid #2a3850;`
        + `border-radius:12px;padding:2px 10px;font-size:12px;color:#cdd9e8;white-space:nowrap;">`
        + `<b style="color:#7fd1a0;">${n}×</b> ${escapeHtml(label)}</span>`
    ).join('');
    return `<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:6px 2px;">`
        + `<span style="font-size:12px;color:#9fb2c8;font-weight:600;">Bet types — ${total} marked`
        + `${placed ? `, ${placed} placed` : ''}:</span>${chips}</div>`;
}

function buildRaceWinBadgesHtml(race) {
    // Reflect the ACTUAL template bets (place/wide/trio per the ladder), not the old
    // Win/Q/T assumption. Only show badges for a placed (locked) bet, and only on a
    // settled race.
    const out = evaluateTemplateOutcome(race);
    if (out.markCount === 0 || !isRaceLocked(out.raceId) || !out.hasResults) return "";

    const badges = [];
    // Aggregate per-label payouts (a 3-mark wide box can have multiple winning legs).
    const byLabel = {};
    out.lines.forEach(l => { byLabel[l.label] = (byLabel[l.label] || 0) + l.payout; });
    const pillClass = { '複勝': 'race-hit-honmei', 'ワイド': 'race-hit-quinella', '3連複': 'race-hit-trio', '3連複ながし': 'race-hit-trio' };
    Object.entries(byLabel).forEach(([label, pay]) => {
        const cls = pillClass[label] || 'race-hit-trio';
        const labelEn = TICKET_LABEL_EN[label] || label;
        badges.push(`<span class="race-hit-pill ${cls}" title="${labelEn} hit — paid ¥${pay.toLocaleString()}">${labelEn} ¥${pay.toLocaleString()}</span>`);
    });

    // Always show the net pill for a placed bet (even a loss — that's the sunk-cost point).
    const netYen = Math.round(out.won - out.staked);
    const netSign = netYen >= 0 ? '+' : '-';
    const netAbs = Math.abs(netYen).toLocaleString();
    const netClass = netYen >= 0 ? 'race-hit-net-pos' : 'race-hit-net-neg';
    const netTitle = `Net for this race: ${netSign}¥${netAbs} (won ¥${Math.round(out.won).toLocaleString()} − staked ¥${out.staked.toLocaleString()})`;
    badges.push(`<span class="race-hit-pill race-hit-net ${netClass}" title="${netTitle}">Net ${netSign}¥${netAbs}</span>`);

    return `<span class="race-hit-wrap">${badges.join('')}</span>`;
}

function setVotingSidebarRaceCollapsed(raceId, shouldCollapse) {
    const safeRaceId = String(raceId || '').trim();
    if (!safeRaceId) return null;

    const sidebar = document.getElementById('voting-sidebar-display');
    const selector = `.voting-race-card[data-rid="${CSS.escape(safeRaceId)}"]`;
    const card = sidebar?.querySelector(selector) || document.querySelector(selector);
    if (!card) return null;

    const collapsed = !!shouldCollapse;
    sidebarRaceCollapseState[safeRaceId] = collapsed;
    card.classList.toggle('is-collapsed', collapsed);

    const arrow = card.querySelector('.voting-race-arrow');
    if (arrow) {
        arrow.textContent = collapsed ? '▶' : '▼';
    }

    return card;
}

function didOreProAdvanceToNextRace(data, result) {
    const topNextStatus = String(data?.submitFlow?.nextStatus || '').trim().toLowerCase();
    const rowNextStatus = String(result?.submitFlow?.nextStatus || '').trim().toLowerCase();
    const message = String(result?.message || data?.message || '').toLowerCase();

    return topNextStatus === 'ok'
        || rowNextStatus === 'ok'
        || message.includes('opened the next race page')
        || message.includes('moving to the next race');
}

function toggleVotingSidebarRace(raceId) {
    const card = document.querySelector(`.voting-race-card[data-rid="${CSS.escape(String(raceId || ''))}"]`);
    if (!card) return;

    const nextCollapsed = !card.classList.contains('is-collapsed');
    setVotingSidebarRaceCollapsed(raceId, nextCollapsed);
}

function advanceVotingSidebarAfterApply(raceId) {
    const card = setVotingSidebarRaceCollapsed(raceId, true);
    if (!card) return;

    let nextCard = card.nextElementSibling;
    while (nextCard && !nextCard.classList?.contains('voting-race-card')) {
        nextCard = nextCard.nextElementSibling;
    }

    if (nextCard) {
        const nextRaceId = String(nextCard.dataset.rid || '').trim();
        if (nextRaceId) {
            setVotingSidebarRaceCollapsed(nextRaceId, false);
        }
        nextCard.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
}

function buildRaceBetEstimateKey(requestItem) {
    if (!requestItem) return '';
    return `v2|${requestItem.race_id}|${requestItem.honmei_post}|${(requestItem.box_posts || []).join('-')}`;
}

function loadStoredBetEstimateCache() {
    try {
        const raw = window.localStorage.getItem(BET_ESTIMATE_STORAGE_KEY);
        if (!raw) return {};
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};

        const now = Date.now();
        const clean = {};
        Object.entries(parsed).forEach(([raceId, entry]) => {
            if (!raceId || !entry || typeof entry !== 'object' || Array.isArray(entry)) return;
            if (!entry.hash || typeof entry.hash !== 'string') return;
            const requestedAt = Number(entry.requestedAt || 0);
            if (!Number.isFinite(requestedAt) || requestedAt <= 0) return;
            if ((now - requestedAt) > BET_ESTIMATE_MAX_AGE_MS) return;

            clean[raceId] = {
                hash: entry.hash,
                pending: false,
                requestedAt,
                data: entry.data && typeof entry.data === 'object' ? entry.data : null
            };
        });
        return clean;
    } catch (_) {
        return {};
    }
}

function saveBetEstimateCacheToStorage() {
    try {
        const compact = {};
        Object.entries(raceBetEstimateCache || {}).forEach(([raceId, entry]) => {
            if (!raceId || !entry || typeof entry !== 'object') return;
            if (entry.pending) return;
            if (!entry.hash || !entry.data) return;
            compact[raceId] = {
                hash: entry.hash,
                requestedAt: Number(entry.requestedAt || Date.now()),
                data: entry.data
            };
        });
        window.localStorage.setItem(BET_ESTIMATE_STORAGE_KEY, JSON.stringify(compact));
    } catch (_) {
        // Best-effort cache only.
    }
}

function clearBetEstimateCacheForDate(targetDate) {
    const date = String(targetDate || '').trim();
    if (!date) return;
    const races = Array.isArray(globalRacesByDate[date]) ? globalRacesByDate[date] : [];
    races.forEach(race => {
        const raceId = String(race?.info?.race_id || '').trim();
        if (raceId && raceBetEstimateCache[raceId]) {
            delete raceBetEstimateCache[raceId];
        }
    });
    saveBetEstimateCacheToStorage();
}

function getRaceBetEstimateRequest(raceId) {
    const raceMarks = collectRaceMainMarks(raceId);
    if (!MAIN_BET_SYMBOLS.every(symbol => !!raceMarks[symbol])) return null;

    const entries = Array.isArray(globalRaceEntries[raceId]) ? globalRaceEntries[raceId] : [];
    const horseToPost = new Map();
    entries.forEach(row => {
        const horseId = String(row?.Horse_ID || '').split('.')[0].trim();
        const post = parseInt(row?.PP, 10);
        if (horseId && Number.isFinite(post) && post > 0) {
            horseToPost.set(horseId, post);
        }
    });

    const honmeiHorse = raceMarks["◎"];
    const honmeiPost = horseToPost.get(honmeiHorse);
    if (!Number.isFinite(honmeiPost) || honmeiPost <= 0) return null;

    const boxPosts = [];
    MAIN_BET_SYMBOLS.forEach(symbol => {
        const horseId = raceMarks[symbol];
        const post = horseToPost.get(horseId);
        if (Number.isFinite(post) && post > 0 && !boxPosts.includes(post)) {
            boxPosts.push(post);
        }
    });
    boxPosts.sort((a, b) => a - b);
    if (boxPosts.length < 4) return null;

    return {
        race_id: String(raceId || '').trim(),
        honmei_post: honmeiPost,
        box_posts: boxPosts
    };
}

async function refreshBetEstimatesForDate(targetDate, options = {}) {
    const date = String(targetDate || '').trim();
    if (!date) return;
    const force = !!options.force;
    const nowMs = Date.now();
    const pendingStaleMs = 20000;

    const races = Array.isArray(globalRacesByDate[date]) ? globalRacesByDate[date] : [];
    const requestItems = [];

    races.forEach(race => {
        const raceId = String(race?.info?.race_id || '').trim();
        if (!raceId) return;

        const requestItem = getRaceBetEstimateRequest(raceId);
        if (!requestItem) {
            delete raceBetEstimateCache[raceId];
            return;
        }

        const hash = buildRaceBetEstimateKey(requestItem);
        const cacheEntry = raceBetEstimateCache[raceId];
        const isStalePending = !!(
            cacheEntry
            && cacheEntry.pending
            && Number.isFinite(cacheEntry.requestedAt)
            && (nowMs - cacheEntry.requestedAt) > pendingStaleMs
        );
        if (force || !cacheEntry || cacheEntry.hash !== hash || isStalePending || (!cacheEntry.pending && !cacheEntry.data)) {
            requestItems.push(requestItem);
            raceBetEstimateCache[raceId] = { hash, pending: true, requestedAt: nowMs, data: null };
        }
    });

    if (!requestItems.length) return;

    const chunkSize = 3;
    for (let i = 0; i < requestItems.length; i += chunkSize) {
        const chunk = requestItems.slice(i, i + chunkSize);
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 20000);
            const res = await fetch('/api/races/bet-estimate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ races: chunk }),
                signal: controller.signal
            });
            clearTimeout(timeoutId);

            const payload = await res.json().catch(() => ({}));
            if (!res.ok) {
                throw new Error(payload?.detail || payload?.message || `HTTP ${res.status}`);
            }

            const estimates = payload?.estimates && typeof payload.estimates === 'object' ? payload.estimates : {};
            chunk.forEach(item => {
                const raceId = item.race_id;
                const hash = buildRaceBetEstimateKey(item);
                raceBetEstimateCache[raceId] = {
                    hash,
                    pending: false,
                    requestedAt: nowMs,
                    data: estimates[raceId] || {
                        status: 'partial',
                        raceId: raceId,
                        message: 'No estimate payload returned.',
                        warnings: ['No estimate payload returned.']
                    }
                };
            });
            saveBetEstimateCacheToStorage();
        } catch (err) {
            chunk.forEach(item => {
                const raceId = item.race_id;
                const hash = buildRaceBetEstimateKey(item);
                const previous = raceBetEstimateCache[raceId];
                const hasUsablePrevious = !!(
                    previous
                    && previous.hash === hash
                    && !previous.pending
                    && ['ok', 'partial'].includes(previous?.data?.status)
                );
                raceBetEstimateCache[raceId] = {
                    hash,
                    pending: false,
                    requestedAt: nowMs,
                    data: hasUsablePrevious
                        ? previous.data
                        : {
                            status: 'partial',
                            raceId: raceId,
                            message: err?.message || String(err),
                            warnings: [err?.message || String(err)]
                        }
                };
            });
            saveBetEstimateCacheToStorage();
        }
    }

    if (currentMainView === 'voting' && String(currentActiveDate || '').trim() === date) {
        const sidebarDisplay = document.getElementById('voting-sidebar-display');
        if (sidebarDisplay) {
            sidebarDisplay.innerHTML = buildRacecourseCheatHtml(date);
            if (winningVotesFocusEnabled) {
                applyWinningVotesFocusToVotingSidebar(getDayOverallHitSummary(date));
            }
        }
    }
}

async function reEstimateActiveDay() {
    const date = String(currentActiveDate || '').trim();
    if (!date) return;
    const btn = document.getElementById('btn-reestimate-day');
    const prevLabel = btn?.textContent || '';
    try {
        if (btn) {
            btn.disabled = true;
            btn.textContent = '⏳ Re-estimating...';
        }
        clearBetEstimateCacheForDate(date);
        await refreshBetEstimatesForDate(date, { force: true });
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = prevLabel || '🧮 Re-estimate Day';
        }
    }
}

function formatEstimateYen(value) {
    if (value === null || value === undefined || value === '') return '-';
    const num = Number(value);
    if (!Number.isFinite(num)) return '-';
    const rounded = Math.round(num);
    const sign = rounded > 0 ? '+' : '';
    return `${sign}${new Intl.NumberFormat('en-US').format(rounded)}円`;
}

function formatEstimateNetRange(minNet, maxNet) {
    if (
        minNet === null || minNet === undefined || minNet === ''
        || maxNet === null || maxNet === undefined || maxNet === ''
    ) {
        return '-';
    }
    const minVal = Number(minNet);
    const maxVal = Number(maxNet);
    if (!Number.isFinite(minVal) || !Number.isFinite(maxVal)) {
        return '-';
    }
    if (Math.round(minVal) === Math.round(maxVal)) {
        return formatEstimateYen(minVal);
    }
    return `${formatEstimateYen(minVal)} to ${formatEstimateYen(maxVal)}`;
}

function formatEstimateAverageRefund(minPayout, maxPayout) {
    if (
        minPayout === null || minPayout === undefined || minPayout === ''
        || maxPayout === null || maxPayout === undefined || maxPayout === ''
    ) {
        return '-';
    }
    const minVal = Number(minPayout);
    const maxVal = Number(maxPayout);
    if (!Number.isFinite(minVal) || !Number.isFinite(maxVal)) {
        return '-';
    }
    const avg = (minVal + maxVal) / 2;
    return formatEstimateYen(avg);
}

function estimateNetClass(value) {
    if (value === null || value === undefined || value === '') return '';
    const num = Number(value);
    if (!Number.isFinite(num)) return '';
    return num >= 0 ? 'is-positive' : 'is-negative';
}

function estimateWarningsText(estimate) {
    const warnings = Array.isArray(estimate?.warnings) ? estimate.warnings.filter(Boolean) : [];
    if (warnings.length) return warnings.join(' | ');
    const msg = String(estimate?.message || '').trim();
    return msg || '';
}

function estimateValueReason(estimate, key) {
    const win = estimate?.win || {};
    const q = estimate?.quinellaBox || {};
    const t = estimate?.trioBox || {};
    const allHit = estimate?.allHit || {};
    const warningText = estimateWarningsText(estimate);

    if (key === 'winNet') {
        if (win?.net === null || win?.net === undefined || win?.net === '') {
            return 'Win net cannot be computed because ◎ win odds are unavailable.';
        }
        return '';
    }

    if (key === 'quinellaNet' || key === 'quinellaAvgRefund') {
        if (q?.tickets > 0 && Number(q?.resolvedTickets || 0) === 0) {
            return 'Quinella cannot be computed because no odds were returned for the selected box combinations.';
        }
        if (Number(q?.missingTickets || 0) > 0) {
            return `Quinella is partially computed. Missing odds for ${q.missingTickets} combination(s).`;
        }
        if (warningText) return warningText;
        return 'Quinella value is unavailable.';
    }

    if (key === 'trioNet' || key === 'trioAvgRefund') {
        if (t?.tickets > 0 && Number(t?.resolvedTickets || 0) === 0) {
            return 'Trio cannot be computed because no odds were returned for the selected box combinations.';
        }
        if (Number(t?.missingTickets || 0) > 0) {
            return `Trio is partially computed. Missing odds for ${t.missingTickets} combination(s).`;
        }
        if (warningText) return warningText;
        return 'Trio value is unavailable.';
    }

    if (key === 'allHitNet') {
        if (!(allHit?.minNet === null || allHit?.minNet === undefined || allHit?.minNet === '')
            && !(allHit?.maxNet === null || allHit?.maxNet === undefined || allHit?.maxNet === '')) {
            return '';
        }
        const reasons = [];
        if (win?.net === null || win?.net === undefined || win?.net === '') {
            reasons.push('Win leg missing');
        }
        if (q?.minPayout === null || q?.maxPayout === null || q?.minPayout === undefined || q?.maxPayout === undefined) {
            reasons.push('Quinella leg missing');
        }
        if (t?.minPayout === null || t?.maxPayout === null || t?.minPayout === undefined || t?.maxPayout === undefined) {
            reasons.push('Trio leg missing');
        }
        if (reasons.length) {
            return `All Hit cannot be computed: ${reasons.join(', ')}.`;
        }
        if (warningText) return warningText;
        return 'All Hit value is unavailable.';
    }

    return warningText;
}

function chipTitleAttr(reason) {
    const text = String(reason || '').trim();
    if (!text) return '';
    return ` title="${escapeHtml(text)}"`;
}

// Display-only English labels for bet ticket types (internal labels stay Japanese so
// win-badge / line-scoring matching is unaffected).
const TICKET_LABEL_EN = {
    '単勝': 'Win', '複勝': 'Place', '馬連': 'Quinella', 'ワイド': 'Wide',
    '3連複': 'Trio', '3連複ながし': 'Trio', '3連単': 'Trifecta', '実績': 'Actual'
};

// Per-race bet-line breakdown for the voting tab. Shows the actual ticket(s):
//  - imported races (OrePro day text): the recorded total + reconstructed shape where the
//    stake maps cleanly to a standard template (exact line list isn't captured on import);
//  - in-app frozen / synthesized races: each line (type · 点数 · per-combo · total).
// For PAST races each line is marked ✓¥won / ✗ from the actual result (template-aware,
// honoring imported actuals). Returns '' when there is nothing to show.
// Score each given bet line against the race's actual result. Returns a parallel array
// of ¥-won (or null when the race isn't settled / payouts missing).
function scoreLinesForDisplay(race, lines) {
    const entries = Array.isArray(race?.entries) ? race.entries : [];
    const top3pp = {};
    entries.forEach(row => {
        const r = parseFinishRank(row?.Finish);
        const pp = parseInt(row?.PP, 10);
        if (r >= 1 && r <= 3 && top3pp[r] == null && Number.isFinite(pp) && pp > 0) top3pp[r] = pp;
    });
    if (!(top3pp[1] && top3pp[2] && top3pp[3])) return lines.map(() => null);
    let payouts = null;
    try { const raw = race?.info?.results_json; payouts = typeof raw === 'string' ? JSON.parse(raw) : raw; } catch (_) {}
    if (!payouts) return lines.map(() => null);
    const t3 = [top3pp[1], top3pp[2], top3pp[3]];
    const t3set = new Set(t3);
    return lines.map(l => scoreBetLine(l, t3, t3set, payouts));
}

function buildVotingBetLinesHtml(race, timeline) {
    const raceId = String(race?.info?.race_id || '').trim();
    const profile = getRaceBetProfile(raceId);
    const plan = buildRaceBetLines(race);
    const isPast = timeline === 'past';
    const imported = !!(profile && profile.actualStaked != null && !(profile.betLines && profile.betLines.length));

    // Choose the line set to display.
    let displayLines = plan.lines;
    let wonByLine = null;        // parallel ¥-won (past only); null entry = unscored
    let naive = false;           // imported single-template guess (exact combos uncaptured)

    if (imported && plan.staked !== profile.actualStaked) {
        // The synthesized template does NOT reproduce the recorded stake → this was a custom
        // single-template bet (e.g. a ¥400 3連複 box, or a scratch-reduced ¥100 single combo).
        // Reconstruct one line from the marks; exact per-combination detail wasn't captured.
        naive = true;
        const stake = profile.actualStaked;
        const combos = Math.round(stake / 100);
        const n = plan.runners.length;
        const pps = plan.runners.map(r => r.pp).filter(Boolean).sort((a, b) => a - b);
        const per = combos ? Math.round(stake / combos) : stake;
        let label;
        if (n >= 3 && combos === nCk(n, 3))      label = `Trio box [${pps.join('·')}]`;
        else if (n >= 2 && combos === nCk(n, 2)) label = `2-horse box [${pps.join('·')}]`;
        else if (combos === 1)                   label = `Single combo`;
        else                                     label = `Box`;
        displayLines = [{ _label: `${label} · ${combos} combo${combos === 1 ? '' : 's'} ×¥${per.toLocaleString()}`, _stake: stake }];
        if (isPast) wonByLine = [profile.actualWon || 0];   // trust the recorded total
    } else if (isPast) {
        // Synthesized/matched lines (incl. the matched ¥10,000 default template) — score each.
        wonByLine = scoreLinesForDisplay(race, displayLines);
    }

    if (!displayLines.length) return '';

    const chips = displayLines.map((l, i) => {
        let labelText, tot;
        if (l._label) {
            labelText = l._label;
            tot = l._stake;
        } else {
            const combos = l.comboCount || 0;
            const per = Math.round(l.stakePerCombo || 0);
            tot = combos * per;
            const pps = (l.horses || []).map(h => h.pp).filter(Boolean).sort((a, b) => a - b).join('·');
            const axis = l.axisPp ? `axis ${l.axisPp} ` : '';
            const method = l.method === 'box' ? ' box' : (l.method === 'nagashi1' ? ' wheel' : '');
            const labelEn = TICKET_LABEL_EN[l.label] || l.label;
            labelText = `${labelEn}${method} ${axis}[${pps}] · ${combos} combo${combos === 1 ? '' : 's'} ×¥${per.toLocaleString()}`;
        }
        let cls = '', res = '';
        if (isPast && wonByLine && wonByLine[i] != null) {
            const won = wonByLine[i];
            cls = won > 0 ? 'is-positive' : 'is-negative';
            res = won > 0 ? ` ✓ ¥${won.toLocaleString()}` : ' ✗';
        }
        const title = naive ? ` title="Imported from OrePro — exact per-combination breakdown wasn't captured (day-list text is lossy)."` : '';
        return `<span class="bet-estimate-chip ${cls}"${title}>${escapeHtml(labelText)} = ¥${Number(tot).toLocaleString()}${res}</span>`;
    });

    const heading = isPast ? 'Placed' : 'To place';
    return `<div class="bet-estimate-inline" style="flex-wrap:wrap;"><span style="font-size:11px;color:#9aa;align-self:center;margin-right:2px;">${heading}:</span>${chips.join('')}</div>`;
}

function buildRacecourseCheatHtml(targetDate) {
    const date = String(targetDate || '').trim();
    const timeline = globalDateTimelineByDate[date] || '';
    const sMap = { "◎": 1, "〇": 2, "▲": 3, "△": 4, "☆": 5, "消": 6 };
    const oreproRaceMap = getOreProRaceResultMapForActiveDate();
    const bColors = {
        1: { bg: '#f8f9fa', color: '#000', border: '#ccc' },
        2: { bg: '#212529', color: '#fff', border: '#444' },
        3: { bg: '#d26363', color: '#fff', border: '#d26363' },
        4: { bg: '#5970b0', color: '#fff', border: '#5970b0' },
        5: { bg: '#b8b053', color: '#000', border: '#b8b053' },
        6: { bg: '#72af68', color: '#fff', border: '#72af68' },
        7: { bg: '#efa65e', color: '#000', border: '#efa65e' },
        8: { bg: '#dc809a', color: '#000', border: '#dc809a' }
    };

    // Collect all non-X marks for this date, grouped by race
    const raceMarkGroups = {};
    for (const [key, symbol] of Object.entries(globalMarks)) {
        if (!symbol || symbol === 'X') continue;
        const [r_id, h_id] = key.split('_');
        const info = globalRaceInfo[r_id];
        if (!info || (info.clean_date || '') !== date) continue;

        if (!raceMarkGroups[r_id]) raceMarkGroups[r_id] = { info, marks: [] };
        const entries = globalRaceEntries[r_id] || [];
        const row = entries.find(r => String(r.Horse_ID).split('.')[0] === h_id);
        raceMarkGroups[r_id].marks.push({
            symbol,
            rank: sMap[symbol] || 99,
            horse: row ? row.Horse : 'Unknown Horse',
            pp: row ? parseInt(row.PP) || 99 : 99,
            bk: row ? parseInt(row.BK) || 0 : 0,
            fav: row ? String(row.Fav || '').trim() : '',
            finishRank: parseFinishRank(row?.Finish)
        });
    }

    // Discipline mode never writes to globalMarks (the bet is engine-picked, not a manual ◎/〇/▲/△
    // click), so every Discipline race would otherwise be invisible on this cheat sheet even after
    // being bet and locked — the same gap already fixed for applyAllDayVotesToOrePro's "has votes"
    // check (collectDisciplineEngineRunners). Synthesize the same engine mark group here for any race
    // this date without a per-race manual override, so Discipline bets show up like manual ones do.
    if (isDisciplineMode()) {
        for (const [r_id, info] of Object.entries(globalRaceInfo)) {
            if ((info.clean_date || '') !== date) continue;
            if (raceMarkGroups[r_id]) continue; // already covered by real marks above
            if (getRaceBetCompositionOverride(r_id)) continue; // per-race manual override race, not engine-driven
            const runners = collectDisciplineEngineRunners(r_id);
            if (!runners.length) continue;
            const entries = globalRaceEntries[r_id] || [];
            // disciplineRanking flags this as engine ranking shown for CONTEXT — only the ◎ is bet.
            // The render mutes the 〇▲ and labels them "not bet" so a 3-horse ranking never reads as
            // a 3-horse bet (which misled the operator into thinking Discipline placed 3 per race).
            const group = { info, marks: [], disciplineRanking: true };
            runners.forEach(r => {
                const row = entries.find(e => String(e.Horse_ID).split('.')[0] === r.horseId);
                group.marks.push({
                    symbol: r.symbol,
                    rank: sMap[r.symbol] || 99,
                    horse: row ? row.Horse : 'Unknown Horse',
                    pp: r.pp || 99,
                    bk: row ? parseInt(row.BK) || 0 : 0,
                    fav: row ? String(row.Fav || '').trim() : '',
                    finishRank: parseFinishRank(row?.Finish)
                });
            });
            raceMarkGroups[r_id] = group;
        }
    }

    // Flatten all races into a chronological list. Track is shown per-card so
    // mixed venues are unambiguous when sorted by post time.
    const raceCards = [];
    for (const [r_id, group] of Object.entries(raceMarkGroups)) {
        group.marks.sort((a, b) => a.rank - b.rank);
        const info = group.info;
        const track = trackName(info.place);
        const raceNum = parseInt(info.race_number, 10) || 0;
        const entriesArr = globalRaceEntries[r_id] || [];
        const raceObj = { info, entries: entriesArr };
        const sortKey = parseRaceSortTime(info.sort_time_iso || info.sort_time, info);

        raceCards.push({
            r_id,
            track,
            raceNum,
            time: String(info.time || 'TBA'),
            raceName: localizeRaceName(info.race_name) || localizeRaceClass(info.race_class),
            sortKey: sortKey ? sortKey.getTime() : Number.MAX_SAFE_INTEGER,
            winBadgesHtml: timeline === 'past' ? buildRaceWinBadgesHtml(raceObj) : '',
            betLinesHtml: buildVotingBetLinesHtml(raceObj, timeline),
            orepro: oreproRaceMap.get(r_id) || null,
            betEstimate: raceBetEstimateCache[r_id] || null,
            marks: group.marks,
            disciplineRanking: !!group.disciplineRanking
        });
    }

    if (!raceCards.length) {
        return "<p style='text-align:center; color:#888; margin-top:30px;'>No votes for this day yet.</p>";
    }

    raceCards.sort((a, b) => (a.sortKey - b.sortKey) || (a.raceNum - b.raceNum));

    let html = '<div class="export-track-grid">';
    raceCards.forEach(raceCard => {
            const isCollapsed = !!sidebarRaceCollapseState[raceCard.r_id];
            const arrow = isCollapsed ? '▶' : '▼';
            html += `<div class="export-race-card voting-race-card${isCollapsed ? ' is-collapsed' : ''}" data-rid="${escapeHtml(raceCard.r_id)}">`;
            html += `<div class="export-race-title voting-race-title" onclick="toggleVotingSidebarRace('${escapeHtml(raceCard.r_id)}')" title="Click to collapse/expand this race"><span class="voting-race-arrow">${arrow}</span><span class="voting-race-title-text">🕒 ${escapeHtml(raceCard.time)} | <span class="voting-race-track">${escapeHtml(raceCard.track)}</span> R${raceCard.raceNum}: ${escapeHtml(raceCard.raceName || '')} ${raceCard.winBadgesHtml}${getOreProApplyBadge(raceCard.r_id)}</span><button class="toolbar-btn toolbar-btn-muted voting-race-apply-btn" onclick="applySingleRaceVotesToOrePro(event, '${escapeHtml(raceCard.r_id)}')" title="Apply only this race to OrePro">Apply</button></div>`;
            html += `<div class="voting-race-body">`;

            // Per-race bet-structure override (collapsed unless this race already overrides).
            html += buildRaceBetOverrideHtml(raceCard.r_id, timeline);

            if (raceCard.orepro) {
                html += `
                <div class="orepro-race-inline">
                    <span class="orepro-inline-chip">Buy ${escapeHtml(raceCard.orepro.purchaseLabel || '-')}</span>
                    <span class="orepro-inline-chip">Pay ${escapeHtml(raceCard.orepro.payoutLabel || '-')}</span>
                    <span class="orepro-inline-chip ${Number(raceCard.orepro.profit) >= 0 ? 'is-positive' : 'is-negative'}">PnL ${escapeHtml(raceCard.orepro.profitLabel || '-')}</span>
                </div>`;
            }

            // Actual / planned bet-line breakdown for this race (what you bet, with per-line
            // hit/miss on past races). Replaces the old forward "potential win" estimate on
            // finished races (that estimate is only meaningful before the off).
            if (!raceCard.orepro && raceCard.betLinesHtml) {
                html += raceCard.betLinesHtml;
            }

            // Forward "potential win" estimate — pre-race tool only; suppressed once finished.
            if (timeline === 'past') {
                // no forward estimate on settled races
            } else if (!raceCard.orepro && raceCard.betEstimate?.pending) {
                html += `
                <div class="bet-estimate-inline">
                    <span class="bet-estimate-chip">Estimating Win / Q Box / T Box...</span>
                </div>`;
            } else if (!raceCard.orepro && ['ok', 'partial'].includes(raceCard.betEstimate?.data?.status)) {
                const estimate = raceCard.betEstimate.data;
                const purchase = estimate?.purchase || {};
                const win = estimate?.win || {};
                const q = estimate?.quinellaBox || {};
                const t = estimate?.trioBox || {};
                const allHit = estimate?.allHit || {};
                const warningText = estimateWarningsText(estimate);

                const winNetClass = estimateNetClass(win?.net);
                const allHitClass = estimateNetClass(allHit?.maxNet);

                const winNetText = formatEstimateYen(win?.net);
                const winReason = winNetText === '-' ? estimateValueReason(estimate, 'winNet') : warningText;

                html += `
                <div class="bet-estimate-inline">
                    <span class="bet-estimate-chip ${winNetClass}"${chipTitleAttr(winReason)}>◎ Net ${escapeHtml(winNetText)}</span>
                </div>`;
            } else if (!raceCard.orepro && raceCard.betEstimate?.data?.status === 'error') {
                const errMsg = raceCard.betEstimate?.data?.message || 'Estimate unavailable';
                html += `
                <div class="bet-estimate-inline">
                    <span class="bet-estimate-chip is-negative" title="${escapeHtml(String(errMsg))}">Estimate unavailable</span>
                </div>`;
            }

            // In Discipline mode the ◎〇▲ list is the engine's RANKING shown for context — only the ◎
            // is actually bet (a single ¥10k place). Spell that out so three ranked horses never read
            // as three bets, and mute the non-bet rows below.
            if (raceCard.disciplineRanking) {
                html += `<div style="font-size:11px;color:#8fb3c9;margin:2px 0 6px;line-height:1.4;">🧊 Betting the <b>◎ only</b> (place). <span style="color:#8a94a0;">〇 ▲ below = engine ranking, <b>not bet</b>.</span></div>`;
            }

            raceCard.marks.forEach(m => {
                const notBet = raceCard.disciplineRanking && m.symbol !== '◎';
                const c = bColors[m.bk] || { bg: '#444', color: '#fff', border: '#444' };
                const symSize = m.symbol === '◎' ? '19px' : '16px';
                const ppBadge = m.pp !== 99
                    ? `<span style="display:inline-block;width:22px;height:22px;line-height:22px;text-align:center;font-size:12px;font-weight:bold;background:${c.bg};color:${c.color};border:1px solid ${c.border};border-radius:4px;margin-right:4px;">${m.pp}</span>`
                    : `<span style="display:inline-block;width:22px;height:22px;margin-right:4px;"></span>`;
                const markBadge = `<span style="display:inline-block;width:22px;height:22px;line-height:22px;text-align:center;font-size:${symSize};font-weight:bold;background:${c.bg};color:${c.color};border:1px solid ${c.border};border-radius:4px;margin-right:8px;">${escapeHtml(m.symbol)}</span>`;
                const favBadge = m.fav ? `Fav ${escapeHtml(String(m.fav))}` : 'Fav -';
                const finishBadge = timeline === 'past'
                    ? `<span class="voting-finish-badge${m.finishRank ? ` rank-${m.finishRank}` : ''}">Fin ${m.finishRank || '-'}</span>`
                    : '';
                const notBetTag = notBet
                    ? `<span style="font-size:10px;color:#8a94a0;border:1px solid #454b55;border-radius:4px;padding:1px 5px;white-space:nowrap;">not bet</span>`
                    : '';

                html += `
                <div class="export-horse-line" style="margin-bottom:8px;${notBet ? 'opacity:0.5;' : ''}">
                    ${ppBadge}${markBadge}<div style="flex:1;min-width:0;display:flex;justify-content:space-between;gap:10px;">
                        <span style="font-weight:500;">${escapeHtml(String(m.horse || 'Unknown Horse'))}</span>
                        <div class="voting-line-right-meta">
                            ${notBetTag}
                            <span style="font-size:11px;color:#ddd;border:1px solid #555;border-radius:4px;padding:2px 6px;white-space:nowrap;">${favBadge}</span>
                            ${finishBadge}
                        </div>
                    </div>
                </div>`;
            });

            html += `</div>`;
            html += `</div>`;
    });

    html += `</div>`;
    return html;
}

function syncVotingViewAvailability() {
    const votingBtn = document.getElementById('main-view-voting');
    if (!votingBtn) return;

    votingBtn.style.display = 'inline-block';
}

function updateLiveViewPopoutAvailability() {
    const btn = document.getElementById('btn-live-view-popout');
    if (!btn) return;
    const isPast = (globalDateTimelineByDate[currentActiveDate] || '') === 'past';
    btn.style.display = isPast ? 'none' : 'inline-block';
}

let oreproCompanionWindow = null;
const OREPRO_COMPANION_WINDOW_NAME = 'OreProCompanionWindow';
let oreproLastSyncPayload = null;

// Per-race OrePro apply/submit state, persisted server-side. Shape:
// { "<jraRaceId>": { appliedAt, submitted, status, attempts[], marksCount, lastMessage } }
let globalOreProApplyState = {};

// Subset of /api/settings that the frontend cares about for OrePro behavior.
// Kept in sync via loadOrchestratorSettings (called when Settings modal opens) and
// loadOreProSettingsLite at page init.
let globalOreProSettings = {};

// (Retired 2026-06-02: the mark-count template-cost ladder. Stake is now a flat per-race
//  total — getOreProDefaultStake() — that the chosen bet STRUCTURE spreads across its combos.
//  See BET_STRUCTURES + buildRaceBetLines. The old ¥100/¥400 ladder was the dead test setup.)

// When true, render race times in the user's local timezone with AM/PM. When false,
// keep JST 24h (the historical default — operator mentally maps it).
let globalDisplayLocalTime = false;

async function loadOreProSettingsLite() {
    try {
        const res = await fetch('/api/settings');
        if (!res.ok) return;
        const data = await res.json();
        globalOreProSettings = data?.settings || {};
        globalDisplayLocalTime = String(globalOreProSettings.display_local_time || 'false').toLowerCase() === 'true';
    } catch (_) { /* fine — defaults will be used */ }
}

/// Returns a display time string for a race based on globalDisplayLocalTime.
///  - JST mode: returns info.time verbatim (e.g. "09:55")
///  - Local mode: parses sort_time_iso (proper JST offset) → user's tz with AM/PM (e.g. "8:55 PM")
function formatRaceTimeDisplay(info) {
    if (!globalDisplayLocalTime) return String(info?.time || 'TBA');
    const iso = String(info?.sort_time_iso || '').trim();
    if (!iso) return String(info?.time || 'TBA');
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(info?.time || 'TBA');
    return d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
}

/// Walks /api/races response and overwrites every race's info.time with the formatted
/// display string. Call once per fetch. After this, every existing render path that
/// uses info.time gets the right value without further changes.
function applyTimeDisplayToRacesPayload(data) {
    const dicts = [data?.upcoming_races_by_date, data?.past_races_by_date, data?.races_by_date];
    for (const dict of dicts) {
        if (!dict || typeof dict !== 'object') continue;
        for (const date of Object.keys(dict)) {
            const races = dict[date];
            if (!Array.isArray(races)) continue;
            for (const race of races) {
                if (race?.info) race.info.time = formatRaceTimeDisplay(race.info);
            }
        }
    }
    return data;
}

async function loadOreProApplyState() {
    try {
        const res = await fetch('/api/orepro/apply-state');
        if (!res.ok) return;
        const data = await res.json();
        globalOreProApplyState = (data && typeof data === 'object') ? data : {};
    } catch (_) { globalOreProApplyState = {}; }
}

function getOreProApplyBadge(raceId) {
    const st = globalOreProApplyState?.[raceId];
    if (!st) return '';
    if (st.submitted)  return ` <span class="orepro-apply-badge is-submitted" title="Submitted to OrePro at ${escapeHtml(st.submittedAt || '')}">📤 Submitted</span>`;
    if (st.status === 'unknown') return ` <span class="orepro-apply-badge is-applied" title="OrePro submit outcome is unknown — verify before retrying">❔ Verify</span>`;
    if (st.status === 'failed') return ` <span class="orepro-apply-badge is-applied" title="OrePro submit failed: ${escapeHtml(st.lastMessage || '')}">⚠️ Failed</span>`;
    if (st.appliedAt)  return ` <span class="orepro-apply-badge is-applied"   title="Marks applied to OrePro cart at ${escapeHtml(st.appliedAt)} but not submitted">📝 Applied</span>`;
    return '';
}
let raceBetEstimateCache = {};

const MAIN_BET_SYMBOLS = ["◎", "〇", "▲", "△"];

function normalizeOreProDateLabel(dateStr) {
    return String(dateStr || '').replace(/-/g, '').trim();
}

function getOreProRaceResultMapForActiveDate() {
    const payload = oreproLastSyncPayload;
    if (!payload || typeof payload !== 'object') return new Map();

    const syncDate = normalizeOreProDateLabel(payload.kaisai_date);
    const activeDate = normalizeOreProDateLabel(currentActiveDate);
    if (!syncDate || !activeDate || syncDate !== activeDate) return new Map();

    const rows = Array.isArray(payload.myRaceResults) ? payload.myRaceResults : [];
    const result = new Map();
    rows.forEach(row => {
        const raceId = String(row?.raceId || '').trim();
        if (raceId) {
            result.set(raceId, row);
        }
    });
    return result;
}

function isOreProCompanionOpen() {
    return !!(oreproCompanionWindow && !oreproCompanionWindow.closed);
}

// JRA-VAN race_ids are 16 chars (YYYY+MMDD+TT+KK+DD+RR); netkeiba/OrePro use 12 chars
// without the MMDD. Backend converts internally for HTTP calls; we mirror it here so
// the popup can deep-link to the right shutuba page.
function jraToOreproRaceId(raceId) {
    const s = String(raceId || '').trim();
    return s.length === 16 ? s.slice(0, 4) + s.slice(8) : s;
}

async function controlOreProCompanion(action, raceIdForDeepLink) {
    const normalizedAction = action === 'focus' ? 'focus' : 'open';

    // Compute target URL. If we were given a raceId (per-race Apply path), deep-link
    // straight to that race's shutuba page; otherwise open the generic race list.
    const oreproRid = jraToOreproRaceId(raceIdForDeepLink || '');
    const targetUrl = oreproRid
        ? `https://orepro.netkeiba.com/bet/shutuba.html?race_id=${encodeURIComponent(oreproRid)}`
        : 'https://orepro.netkeiba.com/bet/race_list.html';

    // 1) Pop the OrePro window in THIS browser (client-side). The backend rewrite
    //    is purely server-side HTTP via session cookie, so the popup is just visual —
    //    it lets you watch OrePro update as Apply Votes pushes marks into your cart.
    let popupOk = false;
    try {
        if (isOreProCompanionOpen()) {
            try {
                // Navigate the existing window to the target race when we have one.
                if (oreproRid) {
                    try { oreproCompanionWindow.location.href = targetUrl; } catch (_) {}
                }
                oreproCompanionWindow.focus();
            } catch (_) {}
            popupOk = true;
        } else {
            const features = 'width=1200,height=850,menubar=no,toolbar=no,location=yes,status=no,resizable=yes';
            oreproCompanionWindow = window.open(targetUrl, OREPRO_COMPANION_WINDOW_NAME, features);
            popupOk = !!(oreproCompanionWindow && !oreproCompanionWindow.closed);
        }
    } catch (_) { popupOk = false; }

    // 2) Verify the backend has a session cookie configured. Marks won't apply without one.
    let backend = null;
    try {
        const res = await fetch('/api/orepro/companion/window', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: normalizedAction })
        });
        backend = await res.json();
    } catch (err) {
        setOreProSessionStatus(`OrePro backend check failed: ${err?.message || err}`, 'error');
        return null;
    }

    if (!popupOk) {
        setOreProSessionStatus(
            'Popup blocked by your browser. Allow popups for this page, then click Open OrePro again. ' +
            `Backend: ${backend?.message || ''}`,
            'warn'
        );
        return backend;
    }

    const status = backend?.status === 'ok' ? 'ok' : 'warn';
    setOreProSessionStatus(backend?.message || 'OrePro companion window opened.', status);
    return backend;
}

async function openOreProCompanion() {
    return controlOreProCompanion('open');
}

// Under Discipline the OrePro ◎-gate is satisfied by the ENGINE's ◎ (the actual bet target),
// synthesized at apply WITHOUT writing to globalMarks — so the marks store stays clean/analysis-free.
// Just the ◎: the disciplined bet is a single place on it; supporting marks would only invite stray bets.
function collectDisciplineOreProMarks(raceId) {
    const honmeiId = (getUnconditionalAutoBetRankingsForRace(raceId).find(p => p.symbol === '◎') || {}).h_id;
    if (!honmeiId) return [];
    const row = (globalRaceEntries[raceId] || []).find(r => String(r?.Horse_ID || '').split('.')[0].trim() === String(honmeiId));
    const post = parseInt(row?.PP, 10);
    if (!Number.isFinite(post) || post <= 0) return [];
    return [{ symbol: '◎', post, mark_code: '1', horse_id: String(honmeiId) }];
}

function collectOreProMarksFromEntries(raceId, entries) {
    const markPriority = { '◎': 1, '〇': 2, '▲': 3, '△': 4 };
    const marks = [];
    const seenSingles = new Set();
    const seenPairs = new Set();

    (entries || []).forEach(row => {
        const h_id = String(row?.Horse_ID || '').split('.')[0].trim();
        if (!h_id) return;

        const symbol = globalMarks[`${raceId}_${h_id}`];
        const markCode = markPriority[symbol];
        if (!markCode) return;

        const post = parseInt(row?.PP, 10);
        if (!Number.isFinite(post) || post <= 0) return;

        if (markCode !== 4) {
            if (seenSingles.has(markCode)) return;
            seenSingles.add(markCode);
        }

        const pairKey = `${markCode}:${post}`;
        if (seenPairs.has(pairKey)) return;
        seenPairs.add(pairKey);

        marks.push({ symbol, post, mark_code: String(markCode), horse_id: h_id });
    });

    return marks.sort((a, b) => {
        const aCode = Number(a.mark_code || 99);
        const bCode = Number(b.mark_code || 99);
        if (aCode !== bCode) return aCode - bCode;
        return Number(a.post || 0) - Number(b.post || 0);
    });
}

function buildOreProApplyVotesPayload(targetDate) {
    const date = String(targetDate || '').trim();
    const races = Object.entries(globalRaceInfo || {}).map(([r_id, info]) => {
        if (!info) return null;
        if (date && String(info.clean_date || '').trim() !== date) return null;

        const marks = collectOreProMarksFromEntries(r_id, globalRaceEntries[r_id] || []);
        if (!marks.length) return null;

        return {
            race_id: r_id,
            marks,
        };
    }).filter(Boolean);

    return { races, dry_run: false, force_refresh: true };
}

function buildOreProApplyVotesPayloadForRace(raceId) {
    const targetRaceId = String(raceId || '').trim();
    if (!targetRaceId) {
        return { races: [], dry_run: false, force_refresh: true };
    }

    // Under Discipline the bet is engine-driven and the marks store is empty, but OrePro's submit gate
    // still demands a ◎ on the race — so synthesize the engine ◎ for the payload only (never persisted,
    // so the marks store stays analysis-clean). Manual mode reads real marks as before.
    const marks = isDisciplineMode()
        ? collectDisciplineOreProMarks(targetRaceId)
        : collectOreProMarksFromEntries(targetRaceId, globalRaceEntries[targetRaceId] || []);
    if (!marks.length) {
        return { races: [], dry_run: false, force_refresh: true };
    }

    return {
        races: [{ race_id: targetRaceId, marks }],
        dry_run: false,
        force_refresh: true
    };
}

async function clearActiveDayBets() {
    const date = String(currentActiveDate || '').trim();
    if (!date) {
        setOreProSessionStatus('Select a day first, then clear its bets.', 'warn');
        return;
    }

    const racesForDay = Array.isArray(globalRacesByDate[date]) ? globalRacesByDate[date] : [];
    const raceIds = racesForDay
        .map(race => String(race?.info?.race_id || '').trim())
        .filter(Boolean);

    if (!raceIds.length) {
        setOreProSessionStatus(`No races found for ${date}.`, 'warn');
        return;
    }

    let clearableRaces = 0;
    let skippedLocked = 0;
    let marksToClear = 0;

    raceIds.forEach(r_id => {
        const markCount = countRaceMarks(r_id);
        if (!markCount) return;
        if (isRaceLocked(r_id)) {
            skippedLocked += 1;
            return;
        }
        clearableRaces += 1;
        marksToClear += markCount;
    });

    if (!clearableRaces) {
        const lockNote = skippedLocked ? ` ${skippedLocked} locked race(s) still have marks.` : '';
        setOreProSessionStatus(`No unlocked day bets to clear for ${date}.${lockNote}`, 'warn');
        return;
    }

    const confirmed = window.confirm(
        `Clear all saved marks for ${date}?\n\nThis will remove ${marksToClear} mark(s) across ${clearableRaces} unlocked race(s).${skippedLocked ? `\n${skippedLocked} locked race(s) will be skipped.` : ''}`
    );
    if (!confirmed) return;

    const btn = document.getElementById('btn-orepro-clear-day');
    const prevLabel = btn?.textContent || '🧹 Clear Day Bets';

    try {
        if (btn) {
            btn.disabled = true;
            btn.textContent = '⏳ Clearing...';
        }

        const changedRaceIds = [];
        let clearedMarks = 0;

        raceIds.forEach(r_id => {
            if (isRaceLocked(r_id)) return;

            const removed = clearStoredMarksForRace(r_id);
            if (!removed) return;

            clearedMarks += removed;
            touchRaceMeta(r_id, { markSource: 'manual', manualAdjustmentsDelta: 1 });
            changedRaceIds.push(r_id);
        });

        if (!changedRaceIds.length) {
            const lockNote = skippedLocked ? ` Skipped ${skippedLocked} locked race(s).` : '';
            setOreProSessionStatus(`No unlocked day bets were changed for ${date}.${lockNote}`, 'warn');
            return;
        }

        await saveMarksToServer();

        changedRaceIds.forEach(r_id => {
            const sortState = raceSorts[r_id] || { col: 'Default', asc: true };
            raceSorts[r_id] = sortState;
            applySortLogic(r_id, sortState.col, sortState.asc);

            const tbody = document.getElementById(`tbody-${r_id}`);
            if (tbody) tbody.innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);

            refreshRaceHeaderSortLabels(r_id);
            updateRaceActionButtons(r_id);
            updateRiskBadge(r_id);
        });

        updateAutoBetHighlighting();
        updateWinningVotesFocusButton();
        if (winningVotesFocusEnabled) applyWinningVotesFocus();
        if (currentMainView === 'voting') renderLiveViewPanel();

        const lockNote = skippedLocked ? ` Skipped ${skippedLocked} locked race(s).` : '';
        setOreProSessionStatus(`Cleared ${clearedMarks} mark(s) across ${changedRaceIds.length} race(s) for ${date}.${lockNote}`, 'info');
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = prevLabel;
        }
    }
}

// True when the user has hand-placed marks on this race (vs. pure engine auto-pick). Used by
// Auto Bet Day to PROTECT manually-marked races — the operator's workflow is "seed the races I
// know, let the engine fill the rest", so the bulk sweep must skip races I touched myself.
// Signals: markSource carries manual involvement ('manual' or 'mixed'), or a manual-adjust count.
// NOTE: this guards the BULK sweep only; the per-race auto-pick button is an explicit override
// and still overwrites (applyAutoPickSelectionsToRace is intentionally unguarded).
function raceHasUserMarks(r_id) {
    // A race only counts as "hand-marked" (→ protected from the Auto Bet Day sweep) if it STILL
    // has actual marks. 🧹 Clear Day Bets nulls every mark but stamps markSource='manual' +
    // manualAdjustments to record the action — that left every cleared race looking hand-marked,
    // so the sweep skipped them all and set zero marks. Clearing exists precisely to re-evaluate
    // via the engine, so a race with no marks must NOT block Auto Bet Day. Require marks present.
    if (countRaceMarks(r_id) === 0) return false;
    const meta = globalRaceMeta[r_id];
    if (!meta || typeof meta !== 'object') return false;
    const src = String(meta.markSource || '').trim();
    if (src === 'manual' || src === 'mixed') return true;
    if (Number(meta.manualAdjustments) > 0) return true;
    return false;
}

async function autoBetActiveDay() {
    const date = String(currentActiveDate || '').trim();
    if (!date) {
        setOreProSessionStatus('Select a day first, then run Auto Bet Day.', 'warn');
        return;
    }

    const racesForDay = Array.isArray(globalRacesByDate[date]) ? globalRacesByDate[date] : [];
    const now = new Date();
    const upcomingRaceIds = racesForDay
        .map(race => String(race?.info?.race_id || '').trim())
        .filter(Boolean)
        .filter(r_id => {
            const info = globalRaceInfo[r_id] || {};
            const raceTime = parseRaceSortTime(info.sort_time, info);
            return !raceTime || raceTime > now;
        });

    const eligibleRaceIds = upcomingRaceIds.length
        ? upcomingRaceIds
        : ((globalDateTimelineByDate[date] || 'upcoming') === 'upcoming' ? racesForDay
            .map(race => String(race?.info?.race_id || '').trim())
            .filter(Boolean)
            : []);

    if (!eligibleRaceIds.length) {
        setOreProSessionStatus(`No remaining races found for ${date}.`, 'warn');
        return;
    }

    const riskVal = getCurrentAutoPickRisk();
    const confirmed = window.confirm(
        `Auto-pick the remaining races for ${date} using Risk ${riskVal}?\n\nRaces you've hand-marked or locked are left untouched — the engine only fills the rest.\n\nThis only updates marks within UMAnager. Nothing is sent to OrePro until you click Apply Votes.`
    );
    if (!confirmed) return;

    const btn = document.getElementById('btn-orepro-auto-day');
    const prevLabel = btn?.textContent || '⚡ Auto Bet Day';

    try {
        if (btn) {
            btn.disabled = true;
            btn.textContent = '⏳ Auto Betting...';
        }

        let changedRaceIds = [];
        let skippedLocked = 0;
        let skippedManual = 0;
        let abstained = 0;
        let errored = 0;
        let rescued = 0;
        let typed = 0; // Phase 35: races whose bet type was auto-chosen per-race (experimental mode)
        // Phase 34: races the day preset can't fit get re-bet with the backup preset (if set). Only
        // these "bet type doesn't fit" abstains are rescued — NOT genuinely-unbettable ones (tiny
        // field / no odds yet), which stay skipped.
        const backupPreset = getAbstainBackupPreset();

        eligibleRaceIds.forEach(r_id => {
            if (isRaceLocked(r_id)) {
                skippedLocked += 1;
                return;
            }
            // Protect the operator's own picks: the bulk sweep only fills races I haven't
            // hand-marked. (Lock a race, or use the per-race auto-pick button, to override.)
            if (raceHasUserMarks(r_id)) {
                skippedManual += 1;
                return;
            }

            // Isolate each race: a throw on ONE race must not abort the whole day's sweep
            // (that was the "Auto Bet Day set zero marks" regression — one bad race nuked all).
            try {
                // Self-correct: drop any PRIOR auto override so this race re-decides fresh each run
                // (a clear favorite may have emerged since the last sweep).
                clearAutoBackupOverride(r_id);
                // Phase 35: experimental per-race bet-type mode REPLACES the day preset — stamp the
                // shape-chosen preset BEFORE applying, so both the marks and the OrePro apply path
                // use that race's own bet type. Genuine skips (tiny field / no odds) → no preset.
                const perRaceId = autoBetTypePresetForRace(r_id);
                if (perRaceId) setAutoBackupOverride(r_id, perRaceId);
                let result = applyAutoPickSelectionsToRace(r_id, null);
                if (perRaceId) {
                    if (result.count > 0) { typed += 1; }
                    else { clearAutoBackupOverride(r_id); } // genuine skip → don't leave a stale tag
                } else if (result.count === 0 && backupPreset !== 'none' && ENGINE_PRESET_FIT_ABSTAINS.has(result.shape)) {
                    // Legacy abstain-backup rescue (per-race mode OFF): the day preset doesn't FIT
                    // this race → re-bet with the backup preset.
                    setAutoBackupOverride(r_id, backupPreset);
                    const r2 = applyAutoPickSelectionsToRace(r_id, null);
                    if (r2.count > 0) { result = r2; rescued += 1; }
                    else { clearAutoBackupOverride(r_id); } // backup also abstains → leave unbet
                }
                if (result.changed) {
                    changedRaceIds.push(r_id);
                }
                if (result.count === 0) abstained += 1; // engine chose to skip this race
            } catch (e) {
                errored += 1;
                console.error('Auto Bet Day: race', r_id, 'failed —', e);
            }
        });

        if (changedRaceIds.length) {
            await saveMarksToServer();

            changedRaceIds.forEach(r_id => {
                raceSorts[r_id] = { col: 'Default', asc: true };
                applySortLogic(r_id, 'Default', true);

                const tbody = document.getElementById(`tbody-${r_id}`);
                if (tbody) tbody.innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);

                refreshRaceHeaderSortLabels(r_id);
                updateRaceActionButtons(r_id);
                updateRiskBadge(r_id);
            });

            updateAutoBetHighlighting();
            updateWinningVotesFocusButton();
            if (winningVotesFocusEnabled) applyWinningVotesFocus();
            if (currentMainView === 'voting') renderLiveViewPanel();
        }

        // Auto-bet stays local to UMAnager — we don't push to OrePro here. Use Apply Votes
        // afterwards (optionally tweak marks first) to send everything across.
        const lockNote = skippedLocked ? ` Skipped ${skippedLocked} locked race(s).` : '';
        const manualNote = skippedManual ? ` Left ${skippedManual} hand-marked race(s) untouched.` : '';
        const abstainNote = abstained ? ` Engine abstained on ${abstained} race(s) (no good bet at Risk ${riskVal}).` : '';
        const typedNote = typed ? ` 🧪 Auto-chose the bet type per race on ${typed} race(s).` : '';
        const errorNote = errored ? ` ⚠ ${errored} race(s) errored (see console).` : '';
        if (!changedRaceIds.length) {
            setOreProSessionStatus(`Auto-pick finished but produced no new marks for ${date}.${abstainNote}${typedNote}${manualNote}${lockNote}${errorNote}`, 'warn');
            return;
        }
        setOreProSessionStatus(
            `Auto-picked ${changedRaceIds.length} race(s) for ${date} at Risk ${riskVal}. Click Apply Votes to send them to OrePro.${abstainNote}${typedNote}${manualNote}${lockNote}${errorNote}`,
            'info'
        );
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = prevLabel;
        }
    }
}

/// Apply Votes (bulk) — sends each voted race to OrePro one by one, with live progress
/// in the results panel. Each per-race call uses submit_after_apply so the bet is both
/// staged and committed in OrePro. This keeps the platforms separated: Auto Bet Day only
/// updates marks within UMAnager; Apply Votes is the explicit push to OrePro.
// Phase 37 (D/E): route ONE race's apply to the path its composition demands, then submit.
//   • balanced preset (= OrePro's exact default Win+馬連box+三連複box) → MARKS path: send marks,
//     OrePro's generator builds the identical default. Proven, reconciles to the yen.
//   • any other preset / custom composition → CUSTOM path: place the explicit priced lines
//     (e.g. Trio chase + Wide net) so OrePro gets exactly what UMAnager priced.
// Both submit. Easy-mode (簡単投票) is now self-managed server-side (marks path forces it ON,
// custom path forces it OFF), so the operator never toggles it by hand. Returns a normalized
// result so the single-race and day callers share one code path:
//   { ok, expired, submitted, mode, message, data, result }
async function applyRaceRoutedToOrePro(r_id) {
    const useCustom = compositionPresetId(resolveBetComposition(r_id)) !== DEFAULT_PRESET;

    if (useCustom) {
        const race = findRaceObjById(r_id);
        const built = race ? buildOreProCustomLinesForRace(race) : { lines: [] };
        if (!built.lines.length) {
            const noLinesMsg = 'No custom lines to place — mark horses / set a composition first.';
            return { ok: false, expired: false, submitted: false, mode: 'custom',
                     message: built.sideBetWarning ? `${noLinesMsg} ${built.sideBetWarning}` : noLinesMsg,
                     sideBetWarning: built.sideBetWarning, data: null, result: null };
        }

        // MARKS-FIRST (proven 2026-06-13; see memory orepro-custom-bet-api). OrePro's submit gate
        // rejects with 「印が不足しています…◎」 unless ≥1 ◎ exists on the race, and the ◎〇▲△ marks are
        // what render atop the 俺プロフ ticket. So push the race's marks WITHOUT committing (omit
        // submit_after_apply), then place + submit the custom lines below. Non-fatal if no marks
        // resolve — the custom submit still runs and will surface the gate error if marks are truly
        // absent. Re-betting overwrites cleanly (bet_list is replaced, not appended), so no clear step.
        const marksPayload = buildOreProApplyVotesPayloadForRace(r_id);
        if (marksPayload.races.length) {
            try {
                await fetch('/api/orepro/votes/apply', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(marksPayload),   // no submit_after_apply ⇒ marks set, not committed
                });
            } catch (err) {
                console.warn('[OrePro] marks-first step failed for', r_id, '— continuing to custom place+submit', err);
            }
        }

        let data = null;
        try {
            const res = await fetch('/api/orepro/custom-bet/test', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    race_id: r_id, dry_run: false, submit_after_apply: true,
                    lines: built.lines.map(({ type, method, umaban, money }) => ({ type, method, umaban, money })),
                })
            });
            data = await res.json();
        } catch (err) {
            return { ok: false, expired: false, submitted: false, mode: 'custom', message: `request failed: ${err?.message || err}`, data: null, result: null };
        }
        const st = String(data?.status || '').toLowerCase();
        const baseMessage = data?.message || st;
        return { ok: st === 'ok', expired: looksLikeExpiredOreProSession(null, data),
                 submitted: !!data?.submitted, mode: 'custom',
                 message: built.sideBetWarning ? `${baseMessage} ${built.sideBetWarning}` : baseMessage,
                 sideBetWarning: built.sideBetWarning, data, result: data };
    }

    // MARKS path (default / easy mode).
    const payload = buildOreProApplyVotesPayloadForRace(r_id);
    if (!payload.races.length) {
        return { ok: false, expired: false, submitted: false, mode: 'marks', message: 'no resolvable marks', data: null, result: null };
    }
    payload.submit_after_apply = true;
    payload.go_next_race = true;
    let data = null;
    try {
        const res = await fetch('/api/orepro/votes/apply', {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
        });
        data = await res.json();
    } catch (err) {
        return { ok: false, expired: false, submitted: false, mode: 'marks', message: `request failed: ${err?.message || err}`, data: null, result: null };
    }
    const result = Array.isArray(data?.results) ? data.results[0] : (data?.result?.results?.[0] || null);
    const topStatus = String(data?.status || '').trim().toLowerCase();
    const rowStatus = String(result?.status || '').trim().toLowerCase();
    const ok = topStatus !== 'error' && rowStatus !== 'error';
    let msg = result?.message || data?.message || 'applied';
    try { const n = JSON.parse(msg || '{}'); if (n?.message) msg = n.message; } catch (_) {}
    return { ok, expired: looksLikeExpiredOreProSession(result, data), submitted: ok, mode: 'marks', message: msg, data, result };
}

async function applySingleRaceVotesToOrePro(event, raceId) {
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }

    const payload = buildOreProApplyVotesPayloadForRace(raceId);
    payload.submit_after_apply = true;
    payload.go_next_race = true;

    if (!payload.races.length) {
        setOreProSessionStatus(`No valid marks found for race ${raceId}.`, 'warn');
        return;
    }

    setOreProSessionStatus(`Preparing OrePro companion session...`, 'info');

    // Pass raceId so the popup deep-links to this specific race's shutuba page.
    const companion = await controlOreProCompanion('open', raceId);
    if (!companion || companion.status !== 'ok') {
        setOreProSessionStatus(
            companion?.message || 'Could not initialize companion OrePro session. Click Open OrePro and retry.',
            'warn'
        );
        return;
    }

    setOreProSessionStatus(`Applying marks to OrePro for race ${raceId}, then submitting and moving to the next race...`, 'info');

    try {
        // Phase 37: route by this race's composition (default→marks, custom→custom-lines) + submit.
        const routed = await applyRaceRoutedToOrePro(raceId);
        const data = routed.data;
        const result = routed.result;
        const requestCompleted = routed.ok;
        const collapseSucceeded = routed.ok;
        const mode = routed.ok ? 'ok' : 'warn';
        setOreProSessionStatus(`[${routed.mode}] ${routed.message}`, mode);

        // Refresh persistent apply-state badges. After this and a re-render, the race
        // title will show "📝 Applied" or "📤 Submitted".
        await loadOreProApplyState();
        loadVoteHistory().then(renderVoteHistory); // Phase 30: refresh "Voted N×" badges
        try { renderLiveViewPanel(); } catch (_) {}

        if (requestCompleted && isAutoLockAfterSubmitEnabled()) {
            // Lock ONLY this race — applying one race shouldn't lock others you're still editing.
            if (lockSingleRaceAfterSubmit(raceId) > 0) {
                saveMarksToServer().then(() => refreshSunkCostStat()).catch(() => {}); // persist auto-lock (sunk-cost basis)
                updateQuickStats();
            }
        }

        const out = document.getElementById('orepro-sync-results');
        if (out && result) {
            const rid = escapeHtml(String(result?.raceId || raceId));
            const stat = escapeHtml(String(result?.status || 'unknown'));
            const msg = escapeHtml(String(result?.message || ''));
            // Diagnostic dump: show what we sent and what OrePro returned. While we're
            // still hunting the cart-doesn't-apply issue this is invaluable.
            const resolvedDump = result?.resolved ? JSON.stringify(result.resolved, null, 0) : '(none)';
            const cartDump = result?.cartResponse ? JSON.stringify(result.cartResponse, null, 0) : '(no cartResponse)';
            const submitDump = result?.submitFlow ? JSON.stringify(result.submitFlow, null, 0) : '(submit not requested)';
            const preview = Array.isArray(result?.betPreviewLines) ? result.betPreviewLines.join(' | ') : '';
            out.innerHTML = `
                <div class="orepro-sync-title">Apply Votes / Submit / Next (Single Race)</div>
                <div class="orepro-sync-list">[${stat}] race ${rid}: ${msg}</div>
                <details style="margin-top:6px; font-size:11px; font-family:monospace;">
                    <summary>diagnostics</summary>
                    <div style="margin:4px 0;"><b>Resolved (post→seq):</b> ${escapeHtml(resolvedDump)}</div>
                    <div style="margin:4px 0;"><b>cartResponse:</b> ${escapeHtml(cartDump)}</div>
                    <div style="margin:4px 0;"><b>submitFlow:</b> ${escapeHtml(submitDump)}</div>
                    ${preview ? `<div style="margin:4px 0;"><b>preview:</b> ${escapeHtml(preview)}</div>` : ''}
                </details>
            `;
        }

        // If the user wants the v1-style "go to receipt page" UX, navigate the popup to
        // bet_complete.html after a successful submit.
        const submitOk = routed.submitted;
        const navAfter = String(globalOreProSettings?.orepro_nav_to_complete_after_submit || 'false').toLowerCase() === 'true';
        if (submitOk && navAfter && isOreProCompanionOpen()) {
            try {
                const oreproRid = jraToOreproRaceId(raceId);
                oreproCompanionWindow.location.href = `https://orepro.netkeiba.com/bet/bet_complete.html?race_id=${encodeURIComponent(oreproRid)}`;
            } catch (_) { /* popup may be closed/cross-origin */ }
        }

        if (collapseSucceeded) {
            advanceVotingSidebarAfterApply(raceId);
        }
    } catch (err) {
        setOreProSessionStatus(`Failed applying race votes: ${err?.message || err}`, 'error');
    }
}

// ── Dev-mode: place a race's COMPOSED custom bet straight into the OrePro cart (NO submit) ──
// Builds the exact multi-line ticket from the race's resolved composition (the same
// buildRaceBetLines() the UI/sunk-cost use) and pushes it via /api/orepro/custom-bet/test,
// which sets simple_bet=n, adds to the orebet_ cart, and confirms via read-back. Deliberately
// does NOT submit — the ticket waits in the cart for manual review. This is the safe sandbox
// for validating the custom-bet pipeline before it's trusted in the main Apply flow.
// Maps the JS line vocab → OrePro codes:
//   ticket  win→1単  place→2複  quinella→4馬連  wide→5ワイド  trio→7三連複
//   method  normal→0通常  box→2BOX  nagashi1→3ながし
function findRaceObjById(rid) {
    rid = String(rid || '').trim();
    for (const date of Object.keys(globalRacesByDate)) {
        const arr = globalRacesByDate[date];
        if (!Array.isArray(arr)) continue;
        const hit = arr.find(r => String(r?.info?.race_id || '').trim() === rid);
        if (hit) return hit;
    }
    return null;
}

function buildOreProCustomLinesForRace(race) {
    const TYPE   = { win: 1, place: 2, quinella: 4, wide: 5, trio: 7 };
    const METHOD = { normal: 0, box: 2, nagashi1: 3 };
    const plan = buildRaceBetLines(race);
    // Place the CONFIRMED loyalty side bets alongside the spine (so OrePro actually fires them, not just
    // our tracking). They map through the same TYPE/METHOD tables (place=2/normal, wide=5/box).
    const rid = String(race?.info?.race_id || '').trim();
    // plan.lines can ALREADY carry a frozen side-bet line (kind:'side') if this race was locked/frozen
    // on a prior apply (freezeBetProfileAtApply bakes side bets into the frozen betLines). Appending a
    // FRESH buildSideBetLines() on top of an already-frozen plan double-bets the same favorite — two
    // identical bet_ids in one ticket, which OrePro rejects on submit ("price over").
    const hasFrozenSide = (plan.lines || []).some(l => l.kind === 'side');
    const allLines = [...(plan.lines || []), ...(hasFrozenSide ? [] : buildSideBetLines(rid))];
    const lines = [];
    for (const l of allLines) {
        const type = TYPE[l.ticket];
        const method = METHOD[l.method];
        const money = parseInt(l.stakePerCombo, 10) || 0;
        if (type == null || method == null || money <= 0) continue;
        const pps = (l.horses || []).map(h => parseInt(h.pp, 10)).filter(Number.isFinite);
        let umaban;
        if (l.method === 'nagashi1') {
            const axis = parseInt(l.axisPp, 10);
            const partners = pps.filter(p => p !== axis).sort((a, b) => a - b);
            if (!Number.isFinite(axis) || partners.length === 0) continue;
            umaban = `${axis}_${partners.join('-')}`;
        } else if (l.method === 'box') {
            if (pps.length < 2) continue;
            umaban = [...pps].sort((a, b) => a - b).join('-');
        } else { // normal single (win/place)
            if (pps.length < 1) continue;
            umaban = String(pps[0]);
        }
        lines.push({ type, method, umaban, money, _label: l.label, _combos: l.comboCount, ...(l.kind === 'side' ? { kind: 'side' } : {}) });
    }
    // Final safety net: OrePro's bet_id is `_b<type>_c<method>_<umaban>`, and TWO lines that resolve
    // to the SAME bet_id make OrePro reject the whole ticket on submit with reason "price over". That
    // can happen from a corrupted frozen record (a duplicate side line baked in by an earlier bug) or
    // any future path that double-lists a horse. Collapse identical bet_ids to a single line, keeping
    // the LARGER stake (so a real ¥10k spine never loses to a ¥1k dup). This makes a duplicate
    // physically unsubmittable regardless of where it came from.
    const deduped = [];
    const seen = new Map(); // bet_id key -> index in deduped
    for (const l of lines) {
        const key = `${l.type}_${l.method}_${l.umaban}`;
        if (seen.has(key)) {
            const prev = deduped[seen.get(key)];
            if (l.money > prev.money) prev.money = l.money; // keep the larger stake
        } else {
            seen.set(key, deduped.length);
            deduped.push(l);
        }
    }
    const staked = deduped.reduce((s, l) => s + (l.money * (l._combos || 1)), 0);

    // Guard: warn if side bets are CONFIGURED for this race but didn't all make it into the
    // built ticket. Found live 2026-07-18: an incomplete/stale client state (globalRaceMeta or
    // listsData.watchlist not fully loaded yet) can make activeSideBetHorseIds silently return
    // fewer horses than raceMeta.sideBets has configured — the side bet then vanishes with zero
    // error and zero log line. This surfaces that mismatch instead of letting it stay silent.
    // Skipped when hasFrozenSide is true — that's an intentional skip (already-frozen side lines
    // exist from a prior apply), not a drop.
    let sideBetWarning = null;
    if (!hasFrozenSide) {
        const configuredSideBets = Array.isArray(globalRaceMeta[rid]?.sideBets) ? globalRaceMeta[rid].sideBets : [];
        const activeSideBets = activeSideBetHorseIds(rid);
        if (configuredSideBets.length && activeSideBets.length < configuredSideBets.length) {
            const dropped = configuredSideBets.filter(h => !activeSideBets.includes(h));
            sideBetWarning = `⚠ side bet(s) configured for this race did not make it into the ticket ` +
                `(horse id(s): ${dropped.join(', ')}) — Watchlist/marks data may not have been fully ` +
                `loaded when this was built. Check and retry.`;
            console.warn('[SideBetGuard]', rid, sideBetWarning);
        }
    }

    return { lines: deduped, staked, sideBetWarning };
}

async function placeCustomBetNoSubmit(event, raceId) {
    if (event) event.stopPropagation();
    const rid = String(raceId || '').trim();
    const race = findRaceObjById(rid);
    if (!race) { window.alert('Race not found in loaded data.'); return; }

    const built = buildOreProCustomLinesForRace(race);
    if (!built.lines.length) {
        window.alert('No custom bet lines for this race.\n\nMark some horses (and/or set a bet composition) first — the composer needs marks to build the lines.');
        return;
    }

    const label = `${trackName(race.info.place)} R${race.info.race_number}`;
    const summary = built.lines.map(l =>
        `• ${l._label || ('type ' + l.type)}  [${l.umaban}]  ¥${l.money.toLocaleString()}/combo × ${l._combos} = ¥${(l.money * l._combos).toLocaleString()}`
    ).join('\n');
    const ok = window.confirm(
        `🧪 DEV — place custom bet to OrePro CART (no submit)\n\n${label}\n\n${summary}\n\n` +
        `Total: ¥${built.staked.toLocaleString()}\n\n` +
        `This flips your account to easy-mode-OFF (simple_bet=n) and drops these lines in the cart. It does NOT submit. Proceed?`
    );
    if (!ok) return;

    const btn = document.getElementById(`btn-devbet-${rid}`);
    const prev = btn ? btn.textContent : '';
    if (btn) { btn.disabled = true; btn.textContent = '⏳ Placing...'; }

    try {
        const res = await fetch('/api/orepro/custom-bet/test', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                race_id: rid,
                dry_run: false,
                lines: built.lines.map(({ type, method, umaban, money }) => ({ type, method, umaban, money })),
            })
        });
        const data = await res.json();
        const stat = String(data?.status || 'unknown');
        if (stat === 'ok') {
            window.alert(`✅ Placed to cart (no submit)\n\n${(data.betIds || []).join('\n')}\n\nReload OrePro ${label} to review, then submit there if you want it.`);
        } else {
            window.alert(`⚠️ ${stat}: ${data?.message || 'see diagnostics panel'}`);
        }
        const out = document.getElementById('orepro-sync-results');
        if (out) {
            out.innerHTML = `
                <div class="orepro-sync-title">🧪 Dev: Place Custom Bet (no submit) — ${escapeHtml(label)}</div>
                <div class="orepro-sync-list">[${escapeHtml(stat)}] ${escapeHtml(String(data?.message || ''))}</div>
                <details style="margin-top:6px; font-size:11px; font-family:monospace;">
                    <summary>diagnostics</summary>
                    <div style="margin:4px 0;"><b>sent betIds:</b> ${escapeHtml(JSON.stringify(data?.betIds || []))}</div>
                    <div style="margin:4px 0;"><b>cart read-back:</b> ${escapeHtml(JSON.stringify(data?.cartBetIds || []))}</div>
                    <div style="margin:4px 0;"><b>easyModeOff:</b> ${escapeHtml(String(data?.easyModeOff))} · <b>confirmed:</b> ${escapeHtml(String(data?.confirmed))}</div>
                </details>`;
        }
    } catch (err) {
        window.alert(`❌ Failed: ${err?.message || err}`);
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = prev || '🧪 Place Custom (no submit)'; }
    }
}

/// Apply Day Votes (bulk) — iterates every unsubmitted voted race for the active day,
/// pushing each through the proven single-race endpoint with submit_after_apply. Two
/// confirmations: first only if the day has any unbet races (incomplete coverage),
/// second always (final "you sure?" gate before anything hits OrePro).
// Recognize the "OrePro session/cookie expired" failure shape. When the saved cookie is dead,
// OrePro serves a login page instead of the shutuba, which the backend diagnostic flags as
// hasLoginForm=True (and 'no HorseList rows'). Detecting it lets the bulk apply STOP and tell the
// operator plainly — instead of grinding through N cryptic failures that hide the real cause.
function looksLikeExpiredOreProSession(result, data) {
    const status = String(result?.status || data?.status || '').trim().toLowerCase();
    if (status === 'expired') return true; // explicit server signal (when backend hardening ships)
    // Scan EVERY field that can carry OrePro's "you're not logged in" signal so the bulk run STOPS on
    // the first expired-session failure instead of grinding the rest (the 2026-06-21 partial-day bug).
    // The custom-bet path returns the raw add/cart bodies (addResponse/cartRaw) — a dead cookie shows
    // up there as {"status":"NG","reason":"not login"} or a login redirect; the shutuba step returns an
    // "is likely expired" message; the marks path uses hasLoginForm. Catch them all.
    const haystack = [
        result?.message, result?.reason, data?.message, data?.reason,
        data?.addResponse, data?.cartRaw, data?.body,
    ].map(s => String(s || '').toLowerCase()).join(' || ');
    return haystack.includes('hasloginform=true')
        || haystack.includes('session expired')
        || haystack.includes('cookie has expired')
        || haystack.includes('is likely expired')
        || haystack.includes('not login');
}

// Phase 37 C — full-day DRY-RUN PREVIEW. Lists every eligible race's exact tickets — bet type, each
// line's 点数 / per-combo ¥ / line total, the marks, per-race stake, and the day total — all computed
// from buildRaceBetLines(), the SAME pricing the placement path uses, so the preview can't diverge
// from what actually gets placed. watchlistCount (s60+): when > 0, a Watchlist opt-in popup follows
// this one and IS the true final gate — the button/copy here must say so honestly (an earlier version
// claimed "last stop before anything is placed" even when a further step followed, which caused a
// real live incident — see showWatchlistSideBetPopup). Returns a Promise<bool>: true = operator
// confirmed (place, or continue to the Watchlist step), false = cancelled (nothing sent).
function showDayApplyPreview(eligibleRaceIds, date, watchlistCount = 0) {
    return new Promise(resolve => {
        let dayTotal = 0;
        const sections = eligibleRaceIds.map(r_id => {
            const race = findRaceObjById(r_id);
            const plan = race ? buildRaceBetLines(race) : { runners: [], lines: [], staked: 0 };
            dayTotal += plan.staked || 0;
            const label = race ? `${trackName(race.info.place)} R${race.info.race_number}` : r_id;
            let compLabel = ''; try { compLabel = compositionLabel(resolveBetComposition(r_id)); } catch (_) {}
            let autoTag = '';
            if (isAutoBackupOverride(r_id)) {
                autoTag = isAutoBetTypePerRace(r_id)
                    ? ` <span style="font-size:0.82em;color:#b9f0c9;">🧪 auto</span>`
                    : ` <span style="font-size:0.82em;color:#cfe6ff;">↩ backup</span>`;
            }
            const marksStr = (plan.runners || []).map(rn => `${rn.symbol || ''}${rn.pp || '?'}`).join('  ');
            const lineRows = (plan.lines || []).map(l => {
                const m = l.method === 'box' ? ' BOX' : (String(l.method || '').startsWith('nagashi') ? ' ながし' : '');
                const lineTotal = (l.stakePerCombo || 0) * (l.comboCount || 0);
                return `<tr>
                    <td style="padding:1px 6px;">${escapeHtml((l.label || l.ticket) + m)}</td>
                    <td style="padding:1px 6px;text-align:right;color:#9fb2c8;">${l.comboCount}点</td>
                    <td style="padding:1px 6px;text-align:right;color:#9fb2c8;">×¥${(l.stakePerCombo || 0).toLocaleString()}</td>
                    <td style="padding:1px 6px;text-align:right;font-weight:600;">¥${lineTotal.toLocaleString()}</td>
                </tr>`;
            }).join('');
            return `<div style="border:1px solid #243044;border-radius:8px;padding:8px 10px;margin-bottom:8px;background:#141a24;">
                <div style="display:flex;justify-content:space-between;align-items:baseline;">
                    <div><b>${escapeHtml(label)}</b> <span style="color:#9fb2c8;font-size:0.85em;">· ${plan.runners.length} marks · ${escapeHtml(compLabel)}</span>${autoTag}</div>
                    <div style="font-weight:700;">¥${(plan.staked || 0).toLocaleString()}</div>
                </div>
                <div style="color:#cdd9e8;font-size:0.85em;margin:3px 0 5px;letter-spacing:0.5px;">${escapeHtml(marksStr || '—')}</div>
                <table style="width:100%;border-collapse:collapse;font-size:0.85em;">${lineRows}</table>
            </div>`;
        }).join('');

        const hasWatchlistNext = watchlistCount > 0;
        const bodyNote = hasWatchlistNext
            ? `${eligibleRaceIds.length} race(s) below, priced as shown. <b>This does NOT submit yet</b> — next you'll be asked about ${watchlistCount} Watchlist horse${watchlistCount === 1 ? '' : 's'} running today; nothing is sent to OrePro until you choose on that screen.`
            : `${eligibleRaceIds.length} race(s) will be applied + submitted to OrePro. Review every ticket, then confirm — this is the last stop before anything is placed.`;
        const goLabel = hasWatchlistNext
            ? `Continue → Watchlist (${watchlistCount})`
            : `📤 Place ${eligibleRaceIds.length} race(s)`;

        const overlay = document.createElement('div');
        overlay.id = 'day-apply-preview';
        overlay.className = 'modal-overlay';
        const cleanup = (val) => { try { overlay.remove(); } catch (_) {} resolve(val); };
        overlay.onclick = (ev) => { if (ev.target === overlay) cleanup(false); };
        overlay.innerHTML = `
            <div class="modal-content" style="max-width:680px;width:92%;display:flex;flex-direction:column;max-height:88vh;">
                <div class="modal-header">
                    <h3 class="modal-title">📋 Day bet preview — ${escapeHtml(date)}</h3>
                    <div class="modal-header-actions"><button class="close-btn" id="day-preview-x">✖</button></div>
                </div>
                <div style="padding:2px 16px 6px;color:#9fb2c8;font-size:13px;">
                    ${bodyNote}
                </div>
                <div style="overflow:auto;padding:6px 16px;">${sections}</div>
                <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 16px;border-top:1px solid #243044;">
                    <div style="font-size:15px;font-weight:700;">Day total: ¥${dayTotal.toLocaleString()}</div>
                    <div style="display:flex;gap:8px;">
                        <button id="day-preview-cancel" style="padding:7px 14px;border-radius:6px;border:1px solid #3a4a60;background:#1b2230;color:#cdd9e8;cursor:pointer;">Cancel</button>
                        <button id="day-preview-go" style="padding:7px 14px;border-radius:6px;border:1px solid #2f8f57;background:#176b3a;color:#eafff0;font-weight:700;cursor:pointer;">${goLabel}</button>
                    </div>
                </div>
            </div>`;
        document.body.appendChild(overlay);
        document.getElementById('day-preview-go').onclick = () => cleanup(true);
        document.getElementById('day-preview-x').onclick = () => cleanup(false);
        document.getElementById('day-preview-cancel').onclick = () => cleanup(false);
    });
}

async function applyAllDayVotesToOrePro() {
    const date = String(currentActiveDate || '').trim();
    if (!date) {
        setOreProSessionStatus('Select a day first, then apply day votes.', 'warn');
        return;
    }

    const racesForDay = Array.isArray(globalRacesByDate[date]) ? globalRacesByDate[date] : [];
    const allIds = racesForDay.map(r => String(r?.info?.race_id || '').trim()).filter(Boolean);
    if (!allIds.length) {
        setOreProSessionStatus(`No races found for ${date}.`, 'warn');
        return;
    }

    // A race "has votes" if any of ◎〇▲△ is assigned to one of its horses. Under Discipline (and no
    // per-race manual override), the bet is engine-driven and never touches globalMarks — so "has
    // votes" instead means the same engine-◎ source buildRaceBetLines/collectDisciplineEngineRunners
    // will actually bet from. Checking globalMarks here would always read empty and silently skip
    // every race (the "Apply Day Votes did nothing" bug).
    const symbols = ['◎', '〇', '▲', '△'];
    const hasMarks = (r_id) => {
        if (isDisciplineMode() && !getRaceBetCompositionOverride(r_id)) {
            return collectDisciplineEngineRunners(r_id).some(r => r.symbol === '◎');
        }
        const entries = globalRaceEntries[r_id] || [];
        return entries.some(row => {
            const h_id = String(row.Horse_ID).split('.')[0];
            const m = globalMarks[`${r_id}_${h_id}`];
            return m && symbols.includes(m);
        });
    };

    // A race is "already submitted" if OrePro apply-state says so. We skip THOSE (avoid
    // double-submitting), NOT merely-locked ones. Locking is edit-protection only — a
    // locked race must still be appliable, else locking your final pick to guard it on
    // mobile would silently exclude it from the bulk apply (the exact bug reported).
    const isSubmitted = (r_id) => !!(globalOreProApplyState?.[r_id]?.submitted);

    // Phase 34: a marked race that can't form ANY line of its composition (e.g. 1 mark on Trio
    // chase, floor 2) will NOT place — exclude it from the run and report it separately as SKIPPED
    // rather than letting it count as a failure. The operator handles those via custom bets.
    const canForm = (r_id) => {
        const race = findRaceObjById(r_id);
        return !!race && (buildRaceBetLines(race).lines || []).length > 0;
    };
    const marked = allIds.filter(r_id => hasMarks(r_id) && !isSubmitted(r_id));
    const eligible  = marked.filter(canForm);
    const wontPlace = marked.filter(r_id => !canForm(r_id));
    const submittedCount = allIds.filter(r_id => isSubmitted(r_id)).length;
    const unbetCount = allIds.length - marked.length - submittedCount;

    const raceLabel = (r_id) => {
        const race = findRaceObjById(r_id);
        return race ? `${trackName(race.info.place)} R${race.info.race_number}` : r_id;
    };

    if (!eligible.length) {
        const wp = wontPlace.length ? ` (${wontPlace.length} marked race(s) can't form the current preset — handle via custom bet)` : '';
        setOreProSessionStatus(`No placeable unsubmitted races with votes for ${date}.${wp}`, 'warn');
        if (wontPlace.length) {
            window.alert(`⚠️ Nothing to apply for ${date}.\n\n${wontPlace.length} marked race(s) won't place under the current preset (not enough marks):\n` +
                wontPlace.map(raceLabel).map(l => `  • ${l}`).join('\n') +
                `\n\nSwitch the preset for those races, add marks, or place them as custom bets.`);
        }
        return;
    }

    // First confirm: surface BOTH unmarked races and marked-but-unplaceable races.
    if (unbetCount > 0 || wontPlace.length > 0) {
        const parts = [];
        if (unbetCount > 0) parts.push(`${unbetCount} race(s) have no votes yet`);
        if (wontPlace.length > 0) parts.push(`${wontPlace.length} race(s) have marks but WON'T place under the current preset (need more marks): ${wontPlace.map(raceLabel).join(', ')}`);
        const first = window.confirm(
            `⚠️ For ${date}:\n  • ${parts.join('\n  • ')}\n\n` +
            `Apply + submit the ${eligible.length} placeable race(s) and skip the rest?` +
            (wontPlace.length ? `\n\n(The won't-place races are left for you to handle via custom bets.)` : '')
        );
        if (!first) return;
    }

    // Watchlist candidates computed BEFORE the day preview so its button/copy can honestly say whether
    // a further step follows — s60: an earlier version showed "last stop before anything is placed" on
    // this preview even when a Watchlist step still came after it, which is exactly the confusion that
    // led to a real live incident (see showWatchlistSideBetPopup's comment for the full story).
    const watchlistCandidates = sideBetsEnabled() ? collectWatchlistSideBetCandidates(eligible) : [];

    // Day preview (NOT the final gate when Watchlist candidates exist — see watchlistCandidates.length
    // check below). Shows every ticket that will be placed, priced by buildRaceBetLines — the same
    // code the placement path uses — so what you see is what gets sent. Cancel here = nothing hits
    // OrePro, same as always.
    const proceed = await showDayApplyPreview(eligible, date, watchlistCandidates.length);
    if (!proceed) {
        setOreProSessionStatus('Day apply cancelled from the preview — nothing was sent to OrePro.', 'info');
        return;
    }

    // Watchlist opt-in — separate from the disciplined ◎ spine (unaffected either way). If any
    // candidates exist, THIS is the true final gate (Cancel here aborts the whole apply — nothing is
    // sent to OrePro). Silent pass-through (no popup, proceed=true) if none qualify or side bets are
    // off in settings.
    if (watchlistCandidates.length) {
        const proceedAfterWatchlist = await showWatchlistSideBetPopup(eligible, date);
        if (!proceedAfterWatchlist) {
            setOreProSessionStatus('Day apply cancelled from the Watchlist step — nothing was sent to OrePro.', 'info');
            return;
        }
    }

    const btn = document.getElementById('btn-orepro-apply-day');
    const prevLabel = btn?.textContent || '📤 Apply Day Votes';

    try {
        if (btn) { btn.disabled = true; btn.textContent = '⏳ Preparing...'; }

        setOreProSessionStatus('Preparing OrePro companion session...', 'info');
        const companion = await controlOreProCompanion('open', eligible[0]);
        if (!companion || companion.status !== 'ok') {
            setOreProSessionStatus(
                companion?.message || 'Could not initialize companion OrePro session. Click Open OrePro and retry.',
                'warn'
            );
            return;
        }

        // Pre-flight: confirm the cookie is actually LOGGED IN before placing anything, so a dead
        // cookie stops the whole run at zero bets (vs. failing every race one-by-one). Checked
        // against the day's first race shutuba — the reliable signal. (Per-race guardrail #7 still
        // catches a mid-run expiry.)
        if (btn) btn.textContent = '⏳ Checking cookie...';
        setOreProSessionStatus('Checking OrePro login before placing...', 'info');
        try {
            const pre = await checkOreProCookieLoggedIn(eligible[0]);
            if (!pre.loggedIn) {
                setOreProSessionStatus(`Stopped before placing any bets — ${pre.message}`, 'warn');
                window.alert(`⚠️ OrePro cookie check failed — NOTHING was placed.\n\n${pre.message}`);
                return;
            }
        } catch (e) {
            // Probe couldn't reach OrePro — don't hard-block on a transient network blip; the
            // per-race path still stops on the first real "not login".
            console.warn('[OrePro] pre-flight cookie check errored (continuing):', e);
        }

        let okCount = 0;
        let failCount = 0;
        let sessionExpired = false;
        const failureLines = [];
        const succeededRaceIds = [];
        const sideBetWarningLines = [];

        for (let i = 0; i < eligible.length; i++) {
            const r_id = eligible[i];
            if (btn) btn.textContent = `⏳ Applying ${i + 1}/${eligible.length}`;
            setOreProSessionStatus(`Applying race ${i + 1}/${eligible.length} (${r_id})...`, 'info');

            // Phase 37: route each race by its composition (default→marks, custom→custom-lines),
            // submit, and record shared apply-state. The helper has its own try/catch.
            const routed = await applyRaceRoutedToOrePro(r_id);
            if (routed.ok) {
                okCount++;
                succeededRaceIds.push(r_id);
                // A race can place its main bet fine but still silently drop a configured side
                // bet (see buildOreProCustomLinesForRace's guard) — that's NOT a failure (okCount
                // still counts it), but it must not go unnoticed just because the race "succeeded".
                if (routed.sideBetWarning) {
                    sideBetWarningLines.push(`${raceLabel(r_id)}: ${routed.sideBetWarning}`);
                }
            } else {
                failCount++;
                failureLines.push(`[${routed.mode}] ${r_id}: ${routed.message}`);
                // Dead cookie fails every race identically — stop now and report the real cause.
                if (routed.expired) { sessionExpired = true; break; }
            }
        }

        // Cookie died mid-run → don't grind the rest or report cryptic per-race failures.
        // Tell the operator exactly what happened and how to fix it. (finally{} resets the button.)
        if (sessionExpired) {
            await loadOreProApplyState().catch(() => {});
            setOreProSessionStatus(
                'OrePro session expired — your saved cookie is no longer logged in. Update it in Settings → OrePro, then re-run Apply Day Votes. No bets were placed.',
                'error'
            );
            window.alert(
                '❌ OrePro session expired\n\n' +
                'Your saved cookie is no longer logged in, so nothing was placed.\n\n' +
                'Fix:\n' +
                ' 1. Log in at orepro.netkeiba.com\n' +
                ' 2. DevTools → Network → copy the full Cookie header value\n' +
                ' 3. UMAnager → Settings → OrePro → paste → Save\n' +
                ' 4. Re-run Apply Day Votes'
            );
            return;
        }

        // Lock only the races we actually submitted (not empty/no-mark races for the day, and NOT
        // races that failed in this same batch — a mixed batch with any success used to lock/freeze
        // every eligible race including the failed ones, baking their (never-placed) bet shape in as
        // if it had gone through. That stale freeze is what caused the R10 double side-bet bug.
        if (succeededRaceIds.length && isAutoLockAfterSubmitEnabled()) {
            const lockedCount = succeededRaceIds.reduce((n, rid) => n + lockSingleRaceAfterSubmit(rid), 0);
            if (lockedCount > 0) {
                saveMarksToServer().then(() => refreshSunkCostStat()).catch(() => {}); // persist auto-locks once
                updateQuickStats();
            }
        }

        await loadOreProApplyState();
        if (okCount > 0) loadVoteHistory().then(renderVoteHistory); // Phase 30: refresh badges
        try { renderLiveViewPanel(); } catch (_) {}

        // A retry can succeed for only part of an earlier failed batch. Keep the remaining
        // failed/unknown races visible instead of letting the latest response hide them.
        const unresolvedRaceIds = allIds.filter(r_id => {
            const st = globalOreProApplyState?.[r_id];
            return st && !st.submitted && (st.status === 'failed' || st.status === 'unknown');
        });
        const mode = failCount === 0 && unresolvedRaceIds.length === 0 ? 'ok' : (okCount > 0 ? 'warn' : 'error');
        setOreProSessionStatus(`Bulk apply complete for ${date}: ${okCount} ok, ${failCount} failed, ${unresolvedRaceIds.length} still unresolved.`, mode);

        // The bulk run takes a few seconds and the operator usually tabs away — pop a
        // modal alert so they get a clear signal when it's done.
        const alertIcon = failCount === 0 ? '✅' : (okCount > 0 ? '⚠️' : '❌');
        const alertTail = failCount === 0 && unresolvedRaceIds.length === 0
            ? 'All votes submitted successfully.'
            : (okCount > 0
                ? `${okCount} succeeded, ${failCount} failed; ${unresolvedRaceIds.length} remain unresolved (see diagnostics panel).`
                : `All ${failCount} submission(s) failed; ${unresolvedRaceIds.length} remain unresolved (see diagnostics panel).`);
        const unresolvedTail = unresolvedRaceIds.length
            ? `\n\n⚠ Still unresolved from this or an earlier attempt — verify these in OrePro before retrying:\n` +
              unresolvedRaceIds.map(raceLabel).map(l => `  • ${l}`).join('\n')
            : '';
        // Phase 34: explicitly call out the marked races we SKIPPED because they can't form the
        // current preset — so the operator knows to handle them via custom bets.
        const skipTail = wontPlace.length
            ? `\n\n⚠ Skipped ${wontPlace.length} race(s) that won't place under the current preset (handle via custom bet):\n` +
              wontPlace.map(raceLabel).map(l => `  • ${l}`).join('\n')
            : '';
        // A race can succeed its main bet but still silently drop a configured side bet — that's
        // not a failure, so it can't just ride along with alertTail's ok/fail framing. Call it out
        // as its own line so it can't be missed the way a real one almost was live 2026-07-18.
        const sideBetTail = sideBetWarningLines.length
            ? `\n\n⚠ ${sideBetWarningLines.length} race(s) placed but a side bet did NOT make it in — check these:\n` +
              sideBetWarningLines.map(l => `  • ${l}`).join('\n')
            : '';
        window.alert(`${alertIcon} Apply Day Votes finished for ${date}.\n\n${alertTail}${unresolvedTail}${skipTail}${sideBetTail}`);

        // Drop any failure/side-bet-drop detail into the diagnostics panel so the operator can see
        // what didn't go through, even on an otherwise-"ok" run.
        if (failureLines.length || sideBetWarningLines.length) {
            const out = document.getElementById('orepro-sync-results');
            if (out) {
                const failBlock = failureLines.length
                    ? `<div class="orepro-sync-title">Apply Day Votes — Failures (${failCount})</div>
                       <div class="orepro-sync-list" style="font-family:monospace; font-size:11px;">
                           ${failureLines.map(l => `<div>${escapeHtml(l)}</div>`).join('')}
                       </div>`
                    : '';
                const sideBetBlock = sideBetWarningLines.length
                    ? `<div class="orepro-sync-title">Apply Day Votes — Side Bet Drops (${sideBetWarningLines.length})</div>
                       <div class="orepro-sync-list" style="font-family:monospace; font-size:11px;">
                           ${sideBetWarningLines.map(l => `<div>${escapeHtml(l)}</div>`).join('')}
                       </div>`
                    : '';
                out.innerHTML = failBlock + sideBetBlock;
            }
        }
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = prevLabel; }
    }
}

function setOreProSessionStatus(message, mode = 'info') {
    const statusEl = document.getElementById('orepro-session-status');
    if (!statusEl) return;
    statusEl.textContent = message;
    statusEl.classList.remove('ok', 'warn', 'error');
    if (mode === 'ok' || mode === 'warn' || mode === 'error') {
        statusEl.classList.add(mode);
    }
}

function renderOreProHistorySummary(historyPayload) {
    const bar = document.getElementById('orepro-lifetime-bar');
    if (!bar) return;

    const entries = Array.isArray(historyPayload?.entries) ? historyPayload.entries : [];
    const totals = historyPayload?.totals && typeof historyPayload.totals === 'object' ? historyPayload.totals : null;
    const formatYen = (value) => {
        if (value === null || value === undefined || value === '') return '-';
        const number = Number(value);
        if (!Number.isFinite(number)) return escapeHtml(String(value));
        const sign = number > 0 ? '+' : '';
        return `${sign}${new Intl.NumberFormat('en-US').format(number)}円`;
    };
    const formatDateKey = (value) => {
        const raw = String(value || '').trim();
        if (!/^\d{8}$/.test(raw)) return escapeHtml(raw || '-');
        return `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}`;
    };

    if (!totals || !entries.length) {
        bar.classList.add('is-empty');
        bar.innerHTML = `
            <span class="orepro-lifetime-title">Lifetime</span>
            <span class="orepro-lifetime-empty">No saved OrePro history yet. Sync a finished day once and it will persist here.</span>
        `;
        return;
    }

    const profitClass = Number(totals.profit) >= 0 ? 'is-positive' : 'is-negative';
    const bestDay = totals.bestDay?.date
        ? `<span class="orepro-lifetime-note">Best ${formatDateKey(totals.bestDay.date)} ${escapeHtml(totals.bestDay.profitLabel || formatYen(totals.bestDay.profit))}</span>`
        : '';
    const worstDay = totals.worstDay?.date
        ? `<span class="orepro-lifetime-note">Worst ${formatDateKey(totals.worstDay.date)} ${escapeHtml(totals.worstDay.profitLabel || formatYen(totals.worstDay.profit))}</span>`
        : '';

    bar.classList.remove('is-empty');
    bar.innerHTML = `
        <span class="orepro-lifetime-title">Lifetime</span>
        <span class="orepro-lifetime-chip">Days ${escapeHtml(String(totals.days || 0))}</span>
        <span class="orepro-lifetime-chip">Races ${escapeHtml(String(totals.races || 0))}</span>
        <span class="orepro-lifetime-chip">Buy ${escapeHtml(totals.purchaseLabel || formatYen(totals.purchase))}</span>
        <span class="orepro-lifetime-chip">Pay ${escapeHtml(totals.payoutLabel || formatYen(totals.payout))}</span>
        <span class="orepro-lifetime-chip ${profitClass}">PnL ${escapeHtml(totals.profitLabel || formatYen(totals.profit))}</span>
        <span class="orepro-lifetime-chip">ROI ${escapeHtml(String(totals.roiPct || 0))}%</span>
        ${bestDay}
        ${worstDay}
    `;
}

function renderOreProSyncPayload(payload) {
    const out = document.getElementById('orepro-sync-results');
    if (!out) return;

    oreproLastSyncPayload = payload || null;
    if (payload?.historySummary) {
        renderOreProHistorySummary(payload.historySummary);
    }

    const resolvedKaisaiIds = Array.isArray(payload?.resolvedKaisaiIds) ? payload.resolvedKaisaiIds : [];
    const raceIds = Array.isArray(payload?.raceIds) ? payload.raceIds : [];
    const myRaceResults = Array.isArray(payload?.myRaceResults) ? payload.myRaceResults : [];
    const mySummary = payload?.myBetSummary && typeof payload.myBetSummary === 'object' ? payload.myBetSummary : null;
    const fetchedAt = escapeHtml(payload?.fetchedAt || payload?.updatedAt || '');
    const status = escapeHtml(payload?.status || 'idle');
    const message = escapeHtml(payload?.message || 'No sync run yet.');
    const loginLabel = payload?.loggedIn ? 'yes' : 'no';
    const dateLabel = payload?.kaisai_date ? escapeHtml(payload.kaisai_date) : 'current';
    const venueLabel = payload?.kaisai_id ? escapeHtml(payload.kaisai_id) : '-';
    const profileLabel = payload?.debug?.yosokaIdUsed ? escapeHtml(String(payload.debug.yosokaIdUsed)) : '-';
    const resolvedVenueLabel = resolvedKaisaiIds.length ? resolvedKaisaiIds.map(v => escapeHtml(String(v))).join(', ') : '-';

    const formatYen = (value) => {
        if (value === null || value === undefined || value === '') return '-';
        const number = Number(value);
        if (!Number.isFinite(number)) return escapeHtml(String(value));
        const sign = number > 0 ? '+' : '';
        return `${sign}${new Intl.NumberFormat('en-US').format(number)}円`;
    };

    // Show username badge if we got one
    const userEl = document.getElementById('orepro-username-display');
    if (userEl) {
        const uname = payload?.username ? escapeHtml(payload.username) : '';
        if (uname) {
            userEl.style.display = 'block';
            userEl.innerHTML = `✅ Logged in as <strong>${uname}</strong>`;
        } else if (payload?.loggedIn) {
            userEl.style.display = 'block';
            userEl.innerHTML = '✅ Logged in (username not detected)';
        } else {
            userEl.style.display = 'none';
            userEl.innerHTML = '';
        }
    }

    let mySummaryHtml = '';
    if (mySummary && mySummary.races > 0) {
        mySummaryHtml = `
            <div class="orepro-sync-title">My Bets Summary</div>
            <div class="orepro-sync-badges" style="margin-top:4px;">
                <span class="orepro-chip">races: ${escapeHtml(String(mySummary.races || 0))}</span>
                <span class="orepro-chip">purchase: ${escapeHtml(mySummary.purchaseLabel || formatYen(mySummary.purchase))}</span>
                <span class="orepro-chip">payout: ${escapeHtml(mySummary.payoutLabel || formatYen(mySummary.payout))}</span>
                <span class="orepro-chip">profit: ${escapeHtml(mySummary.profitLabel || formatYen(mySummary.profit))}</span>
            </div>
        `;
    } else {
        const accountLoggedIn = payload?.debug?.accountLoggedIn;
        const hint = accountLoggedIn === false
            ? 'No personal bet cards detected because mydata login was not confirmed.'
            : 'No personal bet cards were detected for this sync/day.';
        mySummaryHtml = `<div class="orepro-sync-title">My Bets Summary</div><div class="orepro-sync-list">${escapeHtml(hint)}</div>`;
    }

    const myRaceNoteHtml = myRaceResults.length
        ? `<div class="orepro-sync-list">Per-race purchase, payout, and profit are shown directly in the voting sidebar cards for this day.</div>`
        : '';

    out.innerHTML = `
        <div class="orepro-sync-badges">
            <span class="orepro-chip">status: ${status}</span>
            <span class="orepro-chip">logged-in: ${loginLabel}</span>
            <span class="orepro-chip">date: ${dateLabel}</span>
            <span class="orepro-chip">venue: ${venueLabel}</span>
            <span class="orepro-chip">profile: ${profileLabel}</span>
            <span class="orepro-chip">resolved-venues: ${resolvedVenueLabel}</span>
            <span class="orepro-chip">races-found: ${raceIds.length}</span>
            <span class="orepro-chip">updated: ${fetchedAt || '-'}</span>
        </div>
        <div class="orepro-sync-message">${message}</div>
        ${mySummaryHtml}
        ${myRaceNoteHtml}
    `;

    const sidebarDisplay = document.getElementById('voting-sidebar-display');
    if (sidebarDisplay && currentMainView === 'voting') {
        sidebarDisplay.innerHTML = buildRacecourseCheatHtml(currentActiveDate);
        if (winningVotesFocusEnabled) {
            applyWinningVotesFocusToVotingSidebar(getDayOverallHitSummary(currentActiveDate));
        }
    }
}

function updateOreProSyncDateDisplay() {
    const el = document.getElementById('orepro-sync-date-display');
    if (!el) return;
    if (currentActiveDate) {
        el.textContent = `${currentActiveDate} (from calendar)`;
        el.classList.remove('orepro-sync-date-none');
    } else {
        el.textContent = '← select a day in the calendar';
        el.classList.add('orepro-sync-date-none');
    }
}

async function loadOreProSessionStatus() {
    updateOreProSyncDateDisplay();
    try {
        const [lastRes, historyRes] = await Promise.all([
            fetch('/api/orepro/results/last'),
            fetch('/api/orepro/results/history'),
        ]);
        const last = await lastRes.json();
        const history = await historyRes.json();

        const meta = document.getElementById('orepro-sync-meta');
        if (meta) {
            meta.textContent = 'Cookie storage is disabled. Sync uses public OrePro endpoints/profile ID.';
        }

        renderOreProSyncPayload(last || {});
        renderOreProHistorySummary(last?.historySummary || history || {});
    } catch (err) {
        setOreProSessionStatus(`Failed loading OrePro sync state: ${err?.message || err}`, 'warn');
    }
}

let oreproDiagInitialized = false;
let oreproDiagLines = [];
let oreproLoginHelperPoll = null;
let oreproFrameLoadTimes = [];
let oreproLoopHandled = false;

function logOreProDiagnostic(message, level = 'INFO') {
    const stamp = new Date().toLocaleTimeString('en-US', { hour12: false });
    const line = `[${stamp}] [${level}] ${message}`;
    oreproDiagLines.push(line);
    if (oreproDiagLines.length > 400) {
        oreproDiagLines = oreproDiagLines.slice(-400);
    }

    const wrap = document.getElementById('orepro-diagnostics-wrap');
    const logEl = document.getElementById('orepro-diagnostics-log');
    if (wrap) wrap.style.display = 'block';
    if (logEl) {
        logEl.textContent = oreproDiagLines.join('\n');
        logEl.scrollTop = logEl.scrollHeight;
    }
}

function copyOreProDiagnostics() {
    const fullLog = oreproDiagLines.join('\n');
    if (!fullLog.trim()) return;

    if (navigator.clipboard?.writeText) {
        navigator.clipboard.writeText(fullLog).then(() => {
            logOreProDiagnostic('Diagnostics copied to clipboard.', 'OK');
        }).catch(err => {
            logOreProDiagnostic(`Clipboard copy failed: ${err?.message || err}`, 'WARN');
        });
        return;
    }

    logOreProDiagnostic('Clipboard API not available in this browser context.', 'WARN');
}

async function runOreProDiagnostics() {
    const frame = document.getElementById('live-orepro-iframe');
    const statusEl = document.getElementById('orepro-session-status');

    logOreProDiagnostic('Starting OrePro diagnostics run...');
    logOreProDiagnostic(`UserAgent: ${navigator.userAgent}`);
    logOreProDiagnostic(`navigator.cookieEnabled: ${navigator.cookieEnabled}`);
    logOreProDiagnostic(`document.requestStorageAccessFor available: ${typeof document.requestStorageAccessFor === 'function'}`);
    logOreProDiagnostic(`document.requestStorageAccess available: ${typeof document.requestStorageAccess === 'function'}`);
    logOreProDiagnostic(`document.hasStorageAccess available: ${typeof document.hasStorageAccess === 'function'}`);

    try {
        document.cookie = 'umanager_orepro_diag=1; path=/; max-age=300; SameSite=Lax';
        const cookieReadable = document.cookie.includes('umanager_orepro_diag=1');
        logOreProDiagnostic(`First-party test cookie readable: ${cookieReadable}`);
    } catch (err) {
        logOreProDiagnostic(`First-party test cookie write/read failed: ${err?.message || err}`, 'WARN');
    }

    if (typeof document.hasStorageAccess === 'function') {
        try {
            const hasAccess = await document.hasStorageAccess();
            logOreProDiagnostic(`document.hasStorageAccess(): ${hasAccess}`);
        } catch (err) {
            logOreProDiagnostic(`document.hasStorageAccess() error: ${err?.message || err}`, 'WARN');
        }
    }

    if (frame) {
        logOreProDiagnostic(`iframe src: ${frame.src || '<empty>'}`);
        try {
            const href = frame.contentWindow?.location?.href;
            logOreProDiagnostic(`iframe contentWindow.location.href readable: ${!!href}`);
        } catch (err) {
            logOreProDiagnostic('iframe document is cross-origin (expected): cannot inspect login DOM from parent.', 'WARN');
        }
    }

    if (statusEl) {
        statusEl.textContent = 'Diagnostics complete. Use Copy Log and share it if you want deeper troubleshooting.';
        statusEl.classList.remove('ok', 'warn', 'error');
    }
}

function openOreProLoginHelper() {
    const helper = window.open(OREPRO_URL, 'OreProLoginHelper', 'width=1200,height=850,menubar=no,toolbar=no,location=yes,status=no');
    if (!helper) {
        logOreProDiagnostic('Login helper popup blocked by browser.', 'WARN');
        const statusEl = document.getElementById('orepro-session-status');
        if (statusEl) {
            statusEl.textContent = 'Popup blocked. Allow popups for this site, then click Login Helper again.';
            statusEl.classList.remove('ok', 'warn', 'error');
            statusEl.classList.add('warn');
        }
        return;
    }

    logOreProDiagnostic('Opened OrePro login helper window. Complete login there, then close it.', 'INFO');
    const statusEl = document.getElementById('orepro-session-status');
    if (statusEl) {
        statusEl.textContent = 'Login helper opened. After login, close that window and this panel will reload OrePro.';
        statusEl.classList.remove('ok', 'warn', 'error');
    }

    // Reset load history so post-login reload doesn't re-trigger loop detection
    oreproFrameLoadTimes = [];

    if (oreproLoginHelperPoll) {
        clearInterval(oreproLoginHelperPoll);
    }
    oreproLoginHelperPoll = setInterval(() => {
        if (!helper.closed) return;
        clearInterval(oreproLoginHelperPoll);
        oreproLoginHelperPoll = null;
        oreproLoopHandled = false;  // allow re-detection if login failed
        const frame = document.getElementById('live-orepro-iframe');
        if (frame && frame.src && frame.src !== 'about:blank') {
            frame.src = OREPRO_URL;
        }
        logOreProDiagnostic('Login helper closed; reloaded embedded OrePro frame.', 'INFO');
    }, 800);
}

async function runOreProAuthRescueFlow() {
    logOreProDiagnostic('Starting auth rescue flow (storage request + popup login helper)...', 'INFO');
    await requestOreProSessionAccess();
    openOreProLoginHelper();
}

function setupOreProDiagnostics() {
    if (oreproDiagInitialized) return;
    oreproDiagInitialized = true;

    const frame = document.getElementById('live-orepro-iframe');
    if (!frame) return;

    frame.addEventListener('load', () => {
        logOreProDiagnostic('Iframe load event fired.');
        const now = Date.now();
        oreproFrameLoadTimes.push(now);
        oreproFrameLoadTimes = oreproFrameLoadTimes.filter(ts => now - ts <= 30000);
        const recentFast = oreproFrameLoadTimes.filter(ts => now - ts <= 8000);
        const isLoginLoop = recentFast.length >= 2 || oreproFrameLoadTimes.length >= 3;
        if (isLoginLoop && !oreproLoopHandled && !oreproLoginHelperPoll) {
            oreproLoopHandled = true;
            logOreProDiagnostic('Login loop detected — auto-opening login helper popup.', 'WARN');
            const statusEl = document.getElementById('orepro-session-status');
            if (statusEl) {
                statusEl.textContent = 'Login loop detected — opening login helper. Log in, then close the popup.';
                statusEl.classList.remove('ok', 'warn', 'error');
                statusEl.classList.add('warn');
            }
            openOreProLoginHelper();
        }
        try {
            const href = frame.contentWindow?.location?.href;
            if (href) {
                logOreProDiagnostic(`Iframe URL (same-origin readable): ${href}`);
            }
        } catch (err) {
            logOreProDiagnostic('Cross-origin frame loaded; browser blocks parent inspection (normal).', 'INFO');
        }
    });

    frame.addEventListener('error', () => {
        logOreProDiagnostic('Iframe error event fired while loading OrePro.', 'ERROR');
    });

    logOreProDiagnostic('OrePro diagnostics listeners initialized.');
}

async function requestOreProSessionAccess() {
    const statusEl = document.getElementById('orepro-session-status');
    const frame = document.getElementById('live-orepro-iframe');

    const setStatus = (msg, mode = 'info') => {
        if (!statusEl) return;
        statusEl.textContent = msg;
        statusEl.classList.remove('ok', 'warn', 'error');
        statusEl.classList.add(mode);
    };

    try {
        setStatus('Requesting browser storage access for embedded OrePro login...', 'info');
        logOreProDiagnostic('Attempting storage access request...');

        if (typeof document.requestStorageAccessFor === 'function') {
            await document.requestStorageAccessFor('https://orepro.netkeiba.com');
            logOreProDiagnostic('document.requestStorageAccessFor succeeded.', 'OK');
            setStatus('Storage access granted. Reloading OrePro frame...', 'ok');
            if (frame && frame.src && frame.src !== 'about:blank') {
                frame.src = OREPRO_URL;
            }
            return;
        }

        if (window.top !== window && typeof document.requestStorageAccess === 'function') {
            await document.requestStorageAccess();
            logOreProDiagnostic('document.requestStorageAccess succeeded.', 'OK');
            setStatus('Storage access granted. Reloading OrePro frame...', 'ok');
            if (frame && frame.src && frame.src !== 'about:blank') {
                frame.src = OREPRO_URL;
            }
            return;
        }

        logOreProDiagnostic('No usable storage-access API in this browsing context.', 'WARN');
        setStatus('Storage-access API unavailable here. If login fails, allow third-party cookies for localhost and [*.]netkeiba.com, then run Auth Rescue.', 'warn');
    } catch (err) {
        const msg = err?.message ? String(err.message) : 'request denied';
        logOreProDiagnostic(`Storage access request failed: ${msg}`, 'ERROR');
        setStatus(`Could not enable embedded login access (${msg}). Allow third-party cookies for localhost and [*.]netkeiba.com, then run Auth Rescue.`, 'error');
    }
}

function renderLiveViewPanel() {
    // NOTE: voting-sidebar-title / voting-sidebar-display were retired when the Bets-tab sidebar became
    // the day dashboard (s56). They may be absent now — so DON'T gate the whole render on them, or the
    // bet list (rendered into voting-races-main below) never paints. Guard each optional use instead.
    const sidebarTitle = document.getElementById('voting-sidebar-title');
    const sidebarDisplay = document.getElementById('voting-sidebar-display');
    const mainTitle = document.getElementById('voting-main-title');
    const recapPanel = document.getElementById('voting-recap-panel');
    if (!mainTitle || !recapPanel) return;

    updateLockAllBetsButton(); // sync Lock All / Unlock All on voting-tab render
    const date = String(currentActiveDate || '').trim();
    const timeline = globalDateTimelineByDate[date] || '';
    if (sidebarTitle) sidebarTitle.textContent = `By Racecourse · ${date || 'No day selected'}`;
    if (sidebarDisplay) sidebarDisplay.innerHTML = '';
    const mainRaces = document.getElementById('voting-races-main');
    if (mainRaces) mainRaces.innerHTML = buildRacecourseCheatHtml(date);
    mainTitle.textContent = `OrePro Companion · ${date || 'No day selected'}`;

    if (winningVotesFocusEnabled) {
        applyWinningVotesFocusToVotingSidebar(getDayOverallHitSummary(date));
    }

    if (timeline === 'past') {
        recapPanel.style.display = 'block';
        recapPanel.innerHTML = buildVotingRecapHtml(date);
    } else {
        recapPanel.style.display = 'none';
        recapPanel.innerHTML = '';
    }

    const dayTotalEl = document.getElementById('voting-day-total');
    if (dayTotalEl) {
        const dayTotalHtml = buildDayTotalNetHtml(date);
        if (dayTotalHtml) {
            dayTotalEl.style.display = 'flex';
            dayTotalEl.innerHTML = dayTotalHtml;
        } else {
            dayTotalEl.style.display = 'none';
            dayTotalEl.innerHTML = '';
        }
    }

    const breakdownEl = document.getElementById('voting-bet-breakdown');
    if (breakdownEl) {
        const breakdownHtml = buildBetTypeBreakdownHtml(date);
        breakdownEl.innerHTML = breakdownHtml;
        breakdownEl.style.display = breakdownHtml ? 'block' : 'none';
    }

    refreshBetEstimatesForDate(date);
    loadOreProSessionStatus();
}

function switchMainView(view) {
    if (view === 'voting') currentMainView = 'voting';
    else if (view === 'horse') currentMainView = 'horse';
    else currentMainView = 'races';

    const schedules  = document.getElementById('schedules-container');
    const liveView   = document.getElementById('live-view-container');
    const horseView  = document.getElementById('horse-view-container');
    const racesBtn   = document.getElementById('main-view-races');
    const votingBtn  = document.getElementById('main-view-voting');
    const horseBtn   = document.getElementById('main-view-horse');
    if (!schedules || !liveView || !racesBtn || !votingBtn) return;

    const isVoting = currentMainView === 'voting';
    const isHorse  = currentMainView === 'horse';

    schedules.style.display = (isVoting || isHorse) ? 'none' : 'block';
    liveView.style.display  = isVoting ? 'flex' : 'none';
    if (horseView) horseView.style.display = isHorse ? 'block' : 'none';

    const watchlistPanel = document.getElementById('weekend-watchlist-panel');
    if (watchlistPanel) watchlistPanel.style.display = (isVoting || isHorse) ? 'none' : '';

    racesBtn.classList.toggle('is-active', !isVoting && !isHorse);
    votingBtn.classList.toggle('is-active', isVoting);
    if (horseBtn) horseBtn.classList.toggle('is-active', isHorse);
    document.body.classList.toggle('voting-mode', isVoting);

    syncVotingViewAvailability();
    updateLiveViewPopoutAvailability();
    updateWinningVotesFocusButton();

    if (isVoting) { renderLiveViewPanel(); refreshSunkCostStat(); }
}

// ── Phase 31: Horse Deep-Dive Tab ──────────────────────────────────────────

async function viewHorse(horseId) {
    if (!horseId) return;
    // If opened from the per-race modal, dismiss it so the new profile is visible behind.
    closeHorseRaceModal();
    currentHorseId = horseId;
    switchMainView('horse');

    // Reset scroll so a profile opened from a deep-scrolled one starts at the top.
    const shell = document.querySelector('.horse-view-shell');
    if (shell) shell.scrollTop = 0;

    const content = document.getElementById('horse-profile-content');
    if (content) content.innerHTML = '<div class="horse-loading">Loading profile…</div>';

    try {
        const resp = await fetch(`/api/horses/${encodeURIComponent(horseId)}/profile`);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        renderHorseProfile(data);
    } catch (e) {
        if (content) content.innerHTML = `<div class="horse-error">Failed to load: ${escapeHtml(String(e.message))}</div>`;
    }
}

// Search bar in the empty state
function onHorseSearchInput(val) {
    const drop = document.getElementById('horse-search-dropdown');
    if (!drop) return;
    const q = (val || '').trim();
    if (q.length < 2) { drop.style.display = 'none'; return; }
    fetch(`/api/horses/search?q=${encodeURIComponent(q)}&type=racing`)
        .then(r => r.json())
        .then(data => {
            const results = data.results || [];
            if (!results.length) { drop.style.display = 'none'; return; }
            drop.innerHTML = results.map(r =>
                `<div class="horse-search-item" onclick="viewHorse('${escapeHtml(r.id)}')">
                    <span class="hsi-name">${escapeHtml(r.name || r.id)}</span>
                    ${r.birth_year ? `<span class="hsi-year">${r.birth_year}</span>` : ''}
                 </div>`
            ).join('');
            drop.style.display = 'block';
        })
        .catch(() => { drop.style.display = 'none'; });
}

function renderHorseProfile(data) {
    const content = document.getElementById('horse-profile-content');
    if (!content) return;

    const displayName = data.name_en || data.name_ja || data.horse_id;
    const careerStr = `${data.career_wins}-${data.career_places}-${data.career_shows - data.career_wins - data.career_places}/${data.career_starts}`;

    // Best surface/distance badge
    let bestBadge = '';
    if (data.best_surface && data.best_bucket) {
        const surfIcon = data.best_surface === 'turf' ? '🌿' : '🟤';
        bestBadge = `<span class="horse-best-badge">${surfIcon} ${data.best_surface} · ${data.best_bucket}</span>`;
    }

    // Current odds
    let oddsChip = '';
    if (data.current_odds) {
        oddsChip = `<span class="horse-odds-chip">${data.current_odds}× <small>#${data.current_fav || '?'}</small></span>`;
    }

    let html = `
    <div class="horse-profile-wrap">
      <div class="horse-profile-back">
        <button class="toolbar-btn toolbar-btn-muted" onclick="switchMainView('races')">◀ Back to Races</button>
      </div>

      <!-- Hero card -->
      <div class="horse-hero-card">
        <div class="horse-hero-names">
          <span class="horse-hero-en">${escapeHtml(displayName)}</span>
          ${data.name_ja && data.name_ja !== data.name_en ? `<span class="horse-hero-ja">${escapeHtml(data.name_ja)}</span>` : ''}
          ${data.birth_year ? `<span class="horse-hero-year">${data.birth_year}</span>` : ''}
        </div>
        <div class="horse-hero-meta">
          <div class="horse-hero-lineage">
            ${data.sire_name ? `<span class="hl-item"><span class="hl-label">By</span> ${escapeHtml(data.sire_name)}</span>` : ''}
            ${data.dam_name  ? `<span class="hl-item"><span class="hl-label">Dam</span> ${escapeHtml(data.dam_name)}</span>` : ''}
            ${data.bms_name  ? `<span class="hl-item"><span class="hl-label">BMS</span> ${escapeHtml(data.bms_name)}</span>` : ''}
          </div>
          <div class="horse-hero-stats">
            <span class="horse-stat-block">
              <span class="hs-label">Career</span>
              <span class="hs-value">${escapeHtml(careerStr)}</span>
            </span>
            <span class="horse-stat-block">
              <span class="hs-label">Form (last 5)</span>
              <span class="hs-value horse-form-strip">${escapeHtml(data.last5_form || '—')}</span>
            </span>
            ${bestBadge ? `<span class="horse-stat-block">${bestBadge}</span>` : ''}
            ${oddsChip  ? `<span class="horse-stat-block">${oddsChip}</span>` : ''}
          </div>
        </div>
      </div>

      <!-- Race history -->
      ${buildHorseHistoryHtml(data.history || [])}

      <!-- 3-gen pedigree -->
      ${buildHorsePedigreeHtml(data.pedigree)}

      <!-- Surface × distance grid -->
      ${buildHorseSurfaceGridHtml(data.surface_grid || [])}

      <!-- Sire performance -->
      ${buildHorseSirePerfHtml(data.sire_name, data.sire_perf || [])}

      <!-- Progeny -->
      ${buildHorseProgenyHtml(data.progeny || [], data.progeny_total || 0)}

      <!-- Vote history -->
      ${buildHorseVoteHistoryHtml(data.vote_history || [])}
    </div>`;

    content.innerHTML = html;
}

// Group-B: track going chip — English label + tooltip (JRA codes 1=良 firm … 4=不良 heavy).
function horseGoingChip(code) {
    const m = {
        1: ['Firm',  '#1dd1a1', 'Firm (良) — dry, fast ground'],
        2: ['Good',  '#ffd24a', 'Good (稍重) — slightly soft / a bit of give'],
        3: ['Soft',  '#ff9f43', 'Soft (重) — wet, holding ground'],
        4: ['Heavy', '#ff6b6b', 'Heavy (不良) — very wet, testing ground'],
    };
    const x = m[code];
    if (!x) return '<span class="hh-going-empty" title="Track going not recorded (e.g. an upcoming race — going is published on race day)">—</span>';
    return `<span class="hh-going" style="color:${x[1]};border-color:${x[1]}66;background:${x[1]}1a;" title="Track going — ${x[2]}">${x[0]}</span>`;
}
// Group-B: the horse's run line — closing 3F (s) + corner passing positions, with an explainer tooltip.
function horseRunLine(perf) {
    let p = perf;
    if (typeof perf === 'string') { try { p = JSON.parse(perf); } catch { return '—'; } }
    if (!p || typeof p !== 'object') return '—';
    const l3 = (p.l3f != null && Number.isFinite(+p.l3f)) ? (p.l3f / 10).toFixed(1) : null;
    const corners = Array.isArray(p.corners) ? p.corners.filter(c => c != null) : [];
    if (!l3 && !corners.length) return '—';
    const tip = [
        l3 ? `Closing kick: last-3-furlong (final 600m) time ${l3}s — a lower number is a faster finishing burst` : '',
        corners.length ? `Running position at each corner (early → late): ${corners.join(' → ')} — 1 = leading the field, so low = front-runner, high = closer` : '',
    ].filter(Boolean).join('\n').replace(/"/g, '&quot;');
    // Highlight a standout closing fraction (a fast last-3F is a sign of a strong finishing kick).
    const fast = l3 && parseFloat(l3) < 34.0;
    const l3Html = l3 ? `<b class="hh-l3f"${fast ? ' style="color:#8bd450;" title="Fast closing fraction (sub-34s last 3F)"' : ''}>${l3}</b>` : '';
    return `<span title="${tip}">${l3Html}${corners.length ? `<span class="hh-corners">${corners.join('-')}</span>` : ''}</span>`;
}

// Jockey kanji → romaji (top riders by mount count). Kanji fallback for the long tail
// (rare riders), so an unmapped name still shows rather than blanking. Easy to extend.
const JOCKEY_ROMAJI = {
    'ルメール': 'Lemaire', 'モレイラ': 'Moreira', 'マーカンド': 'Marquand', 'デムーロ': 'Demuro',
    '川田将雅': 'Kawada Y.', '武豊': 'Take Y.', '横山武史': 'Yokoyama T.', '横山和生': 'Yokoyama K.',
    '横山典弘': 'Yokoyama N.', '横山琉人': 'Yokoyama R.', '戸崎圭太': 'Tosaki K.', '松山弘平': 'Matsuyama K.',
    '岩田望来': 'Iwata M.', '岩田康誠': 'Iwata Y.', '坂井瑠星': 'Sakai R.', '菅原明良': 'Sugawara A.',
    '鮫島克駿': 'Sameshima K.', '団野大成': 'Danno T.', '津村明秀': 'Tsumura A.', '三浦皇成': 'Miura K.',
    '西村淳也': 'Nishimura J.', '北村友一': 'Kitamura Y.', '幸英明': 'Ko H.', '和田竜二': 'Wada R.',
    '池添謙一': 'Ikezoe K.', '福永祐一': 'Fukunaga Y.', '田辺裕信': 'Tanabe H.', '吉田隼人': 'Yoshida H.',
    '丸山元気': 'Maruyama G.', '大野拓弥': 'Ohno T.', '菱田裕二': 'Hishida Y.', '丹内祐次': 'Tannai Y.',
    '木幡巧也': 'Kowata T.', '荻野極': 'Ogino K.', '酒井学': 'Sakai M.', '内田博幸': 'Uchida H.',
    '田口貫太': 'Taguchi K.', '菊沢一樹': 'Kikusawa K.', '斎藤新': 'Saito A.', '松若風馬': 'Matsuwaka F.',
    '原優介': 'Hara Y.', '小林美駒': 'Kobayashi M.', '今村聖奈': 'Imamura S.', '永島まなみ': 'Nagashima M.',
    '浜中俊': 'Hamanaka S.', '柴田大知': 'Shibata D.', '秋山真一郎': 'Akiyama S.', '川須栄彦': 'Kawasu H.',
};
function romajiJockey(name) {
    if (!name) return '—';
    return JOCKEY_ROMAJI[name] || name;   // kanji fallback for unmapped riders
}

// Surface as a translated pill (Turf / Dirt / Jump), color-coded like the going chips.
// Bright-on-dark pill (the app is a dark theme — match the going-chip color scheme).
const HH_PILL = 'display:inline-block;padding:0 5px;border-radius:3px;border:1px solid;font-size:11px;font-weight:600;line-height:16px;';
function brightPill(cls, color, label, tip) {
    return `<span class="${cls}" style="${HH_PILL}color:${color};border-color:${color}66;background:${color}1a;" title="${tip}">${label}</span>`;
}
function horseSurfacePill(surface) {
    if (surface === 'turf') return brightPill('hh-surf', '#1dd1a1', 'Turf', 'Turf course');
    if (surface === 'dirt') return brightPill('hh-surf', '#d9a066', 'Dirt', 'Dirt course');
    if (surface === 'jump') return brightPill('hh-surf', '#9aa0a6', 'Jump', 'Jump / steeplechase');
    return '';
}

// Infer running style (Lead / Press / Close / Deep) from the corner passing positions +
// field size — early position relative to the field. A handicapping read at a glance.
function horseRunStyle(perf, fieldSize) {
    let p = perf;
    if (typeof perf === 'string') { try { p = JSON.parse(perf); } catch { return ''; } }
    if (!p || !Array.isArray(p.corners)) return '';
    const pos = p.corners.map(c => parseInt(c, 10)).filter(n => Number.isFinite(n) && n > 0);
    if (!pos.length) return '';
    const early = pos[0];
    const n = fieldSize > 1 ? fieldSize : Math.max(...pos);
    const ratio = early / n;
    let label, color;
    if (early <= 1) { label = 'Lead'; color = '#ff6b9d'; }
    else if (ratio <= 0.30) { label = 'Press'; color = '#ffc04a'; }
    else if (ratio <= 0.66) { label = 'Close'; color = '#6cc6ff'; }
    else { label = 'Deep'; color = '#ff7b6b'; }
    const tip = `Running style (from corner positions): ${label} — early position ${early} of ${n} runners`;
    return brightPill('hh-style', color, label, tip);
}

function buildHorseHistoryHtml(history) {
    if (!history.length) return '<div class="horse-section"><div class="horse-section-title">Form (past performances)</div><p class="horse-empty-msg">No race history found.</p></div>';

    const INITIAL = 10;
    const hasMore = history.length > INITIAL;

    function rowHtml(e, i) {
        const finStr = e.is_upcoming ? '—' : (e.finish != null ? `<span class="hh-finish hh-finish-${e.finish <= 3 ? e.finish : 'other'}">${e.finish}</span>` : '—');
        const oddsStr = e.odds || '—';
        const favStr = e.fav_rank ? `#${e.fav_rank}` : '—';
        const classLabel = localizeRaceName(e.race_name) || localizeRaceClass(e.race_class) || e.race_class || '—';
        const style = e.is_upcoming ? '' : horseRunStyle(e.performance, e.field_size);
        // Cold-engine teaching cue (H8): a run that SWITCHED onto dirt from a non-dirt last start —
        // the strongest fade we measured. `next` (history is newest-first) is this run's prior start.
        const next = history[i + 1];
        const toDirt = e.surface === 'dirt' && next && next.surface && next.surface !== 'dirt' && next.surface !== 'jump';
        // Click any row → modal showing that whole race in the main-page layout,
        // with the viewed horse highlighted. currentHorseId is the horse being profiled.
        return `<tr class="hh-row ${e.is_upcoming ? 'hh-upcoming' : ''} ${i >= INITIAL ? 'hh-extra' : ''} ${toDirt ? 'hh-flag-dirt' : ''}"
                    onclick="openHorseRaceModal('${e.race_id}','${currentHorseId}','${e.race_date}')"
                    title="${toDirt ? 'Cold-engine flag: switched onto dirt from a non-dirt last start (the dirt-switch fade) — ' : ''}View this race's full field">
            <td class="hh-date">${escapeHtml(e.race_date)}</td>
            <td class="hh-track">${escapeHtml(e.track_name)} · ${e.race_number || '?'}</td>
            <td class="hh-class">${escapeHtml(classLabel)}</td>
            <td class="hh-course">${e.distance || '—'}m ${horseSurfacePill(e.surface)} ${horseGoingChip(e.going)}</td>
            <td class="hh-field">${e.field_size || '—'}</td>
            <td class="hh-odds">${escapeHtml(oddsStr)}</td>
            <td class="hh-fav">${escapeHtml(favStr)}</td>
            <td class="hh-finish-cell">${finStr}</td>
            <td class="hh-run">${e.is_upcoming ? '—' : horseRunLine(e.performance)} ${style}</td>
            <td class="hh-jockey">${escapeHtml(romajiJockey(e.jockey))}</td>
        </tr>`;
    }

    const rows = history.map((e, i) => rowHtml(e, i)).join('');
    const expandBtn = hasMore
        ? `<div class="hh-expand-row">
             <button class="toolbar-btn toolbar-btn-muted" onclick="toggleHistoryExpand(this)">Show full career (${history.length})</button>
           </div>`
        : '';

    return `
    <div class="horse-section">
      <div class="horse-section-title">Form (past performances)</div>
      <table class="horse-history-table horse-form-grid">
        <thead>
          <tr>
            <th>Date</th><th>Track · R</th><th>Class</th>
            <th title="Distance, surface (Turf/Dirt) and going (Firm → Good → Soft → Heavy)">Course</th>
            <th title="Field size — number of runners">Field</th><th>Odds</th>
            <th title="Betting favourite rank (#1 = most backed)">Fav</th><th>Fin</th>
            <th title="The run: closing last-3-furlong time (s) + corner positions (1 = leading), and inferred running style">Run / style</th>
            <th>Jockey</th>
          </tr>
        </thead>
        <tbody id="horse-history-tbody">${rows}</tbody>
      </table>
      ${expandBtn}
    </div>`;
}

function toggleHistoryExpand(btn) {
    const extras = document.querySelectorAll('.hh-extra');
    const expanded = extras.length > 0 && extras[0].style.display !== 'none';
    extras.forEach(tr => tr.style.display = expanded ? 'none' : '');
    btn.textContent = expanded ? `Show full career (${extras.length + 10})` : 'Show less';
}

function buildHorsePedigreeHtml(ped) {
    if (!ped) return '';
    if (!ped.sire_name && !ped.dam_name) return '';

    // A box links to that ancestor's profile when we resolved a JRA-runner KettoNum
    // (runner_id). Foreign-only ancestors have no runner row → plain, non-clickable.
    const pedBox = (cls, role, name, runnerId) => {
        const display = escapeHtml(name || '—');
        if (runnerId) {
            return `<div class="ped-box ${cls} ped-clickable" onclick="viewHorse('${escapeHtml(runnerId)}')" title="View ${display}'s profile">
                <span class="ped-role">${role}</span>
                <span class="ped-name ped-link">${display}</span>
            </div>`;
        }
        return `<div class="ped-box ${cls}">
            <span class="ped-role">${role}</span>
            <span class="ped-name">${display}</span>
        </div>`;
    };

    const hasGen3 = ped.sire_sire_name || ped.sire_dam_name || ped.dam_sire_name || ped.dam_dam_name;

    return `
    <div class="horse-section">
      <div class="horse-section-title">Pedigree (3-gen)</div>
      <div class="ped-tree">
        <div class="ped-col ped-gen2">
          ${pedBox('ped-sire', 'Sire', ped.sire_name, ped.sire_runner_id)}
          ${pedBox('ped-dam', 'Dam', ped.dam_name, ped.dam_runner_id)}
        </div>
        ${hasGen3 ? `
        <div class="ped-col ped-gen3">
          ${pedBox('ped-ss', 'PatSire', ped.sire_sire_name, ped.sire_sire_runner_id)}
          ${pedBox('ped-sd', 'PatDam', ped.sire_dam_name, ped.sire_dam_runner_id)}
          ${pedBox('ped-ds', 'MatSire', ped.dam_sire_name, ped.dam_sire_runner_id)}
          ${pedBox('ped-dd', 'MatDam', ped.dam_dam_name, ped.dam_dam_runner_id)}
        </div>` : ''}
      </div>
    </div>`;
}

function buildHorseProgenyHtml(progeny, total) {
    if (!progeny || !progeny.length) return '';
    const chips = progeny.map(p => {
        const yr = p.birth_year ? `<span class="hpr-year">${p.birth_year}</span>` : '';
        const roleIcon = p.role === 'sire' ? '♂' : '♀';
        return `<div class="horse-progeny-chip ${p.role === 'sire' ? 'hpr-sire' : 'hpr-dam'}" onclick="viewHorse('${escapeHtml(p.horse_id)}')" title="As ${p.role === 'sire' ? 'sire' : 'dam'} — view profile">
            <span class="hpr-role">${roleIcon}</span>
            <span class="hpr-name">${escapeHtml(p.name || p.horse_id)}</span>
            ${yr}
        </div>`;
    }).join('');
    const moreNote = (total && total > progeny.length)
        ? `<div class="horse-progeny-more">Showing ${progeny.length} of ${total} (most recent first)</div>`
        : '';
    return `
    <div class="horse-section">
      <div class="horse-section-title">Progeny${total ? ` (${total})` : ''}</div>
      <div class="horse-progeny-grid">${chips}</div>
      ${moreNote}
    </div>`;
}

function buildHorseSurfaceGridHtml(grid) {
    if (!grid.length) return '';

    // Build a table: rows = surfaces, cols = buckets (sprint/mile/middle/long)
    const surfaces = [...new Set(grid.map(g => g.surface))].sort();
    const buckets  = ['sprint', 'mile', 'middle', 'long'];
    const byKey = {};
    grid.forEach(g => { byKey[`${g.surface}_${g.bucket}`] = g; });

    const headerCols = buckets.map(b => `<th>${b}</th>`).join('');
    const bodyRows = surfaces.map(surf => {
        const cols = buckets.map(b => {
            const cell = byKey[`${surf}_${b}`];
            if (!cell || !cell.starts) return '<td class="sg-empty">—</td>';
            return `<td class="sg-cell" title="${cell.wins}W ${cell.places}P / ${cell.starts}S">
                <span class="sg-record">${cell.wins}-${cell.places - cell.wins}-${cell.starts - cell.places}/${cell.starts}</span>
                <span class="sg-pct">${cell.starts ? Math.round(100*cell.wins/cell.starts) : 0}%W</span>
            </td>`;
        }).join('');
        const icon = surf === 'turf' ? '🌿' : '🟤';
        return `<tr><td class="sg-surf">${icon} ${surf}</td>${cols}</tr>`;
    }).join('');

    return `
    <div class="horse-section">
      <div class="horse-section-title">Surface × Distance Record</div>
      <table class="horse-surface-grid">
        <thead><tr><th>Surface</th>${headerCols}</tr></thead>
        <tbody>${bodyRows}</tbody>
      </table>
    </div>`;
}

function buildHorseSirePerfHtml(sireName, perf) {
    if (!perf.length) return '';

    const buckets = ['sprint', 'mile', 'middle', 'long'];
    const surfaces = [...new Set(perf.map(p => p.surface))].sort();
    const byKey = {};
    perf.forEach(p => { byKey[`${p.surface}_${p.bucket}`] = p; });

    const headerCols = buckets.map(b => `<th>${b}</th>`).join('');
    const bodyRows = surfaces.map(surf => {
        const cols = buckets.map(b => {
            const cell = byKey[`${surf}_${b}`];
            if (!cell || !cell.starts) return '<td class="sg-empty">—</td>';
            return `<td class="sg-cell" title="${cell.wins}W / ${cell.starts}S">
                <span class="sg-record">${cell.win_pct}%</span>
                <span class="sg-pct">${cell.place_pct}%P</span>
            </td>`;
        }).join('');
        const icon = surf === 'turf' ? '🌿' : '🟤';
        return `<tr><td class="sg-surf">${icon} ${surf}</td>${cols}</tr>`;
    }).join('');

    return `
    <div class="horse-section">
      <div class="horse-section-title">Sire Performance: ${escapeHtml(sireName || '—')}</div>
      <table class="horse-surface-grid">
        <thead><tr><th>Surface</th>${headerCols}</tr></thead>
        <tbody>${bodyRows}</tbody>
      </table>
    </div>`;
}

function buildHorseVoteHistoryHtml(votes) {
    if (!votes.length) return `
    <div class="horse-section">
      <div class="horse-section-title">Vote History</div>
      <p class="horse-empty-msg">Never voted on this horse.</p>
    </div>`;

    const markColor = { '◎': '#ff4b4b', '〇': '#ff9f43', '▲': '#1dd1a1', '△': '#0abde3' };
    const pills = votes.map(v => {
        const col = markColor[v.mark] || '#888';
        return `<div class="horse-vote-pill">
            <span class="mark-btn" style="background:${col};color:#fff;font-weight:bold">${escapeHtml(v.mark)}</span>
            <span class="hvp-date">${escapeHtml(v.race_date || v.voted_at)}</span>
            <span class="hvp-track">${escapeHtml(v.track_name || '')}</span>
            ${v.race_name ? `<span class="hvp-race">${escapeHtml(v.race_name)}</span>` : ''}
        </div>`;
    }).join('');

    return `
    <div class="horse-section">
      <div class="horse-section-title">Vote History (${votes.length})</div>
      <div class="horse-vote-strip">${pills}</div>
    </div>`;
}

// Phase 31: click a history row → modal with the full race field in the SAME layout
// as the main page's race cards (reusing buildTableHeaderRow + buildTableBody), with
// the profiled horse's row highlighted. Loads the race day into globals on demand via
// the shared loadRaceDay() path, so even races outside the 14-day window work.
async function openHorseRaceModal(raceId, horseId, date) {
    if (!raceId) return;
    // Ensure the race is in globals (fetches + merges the JST day if needed).
    if (!globalRaceEntries[raceId]) {
        const ok = await loadRaceDay(date);
        if (!ok || !globalRaceEntries[raceId]) {
            showCopyToast('Could not load that race.');
            return;
        }
    }

    // Make sure a sort state exists, then order entries like the main view.
    if (!raceSorts[raceId]) raceSorts[raceId] = { col: globalSort.col, asc: globalSort.asc };
    applySortLogic(raceId, raceSorts[raceId].col, raceSorts[raceId].asc);

    const info = globalRaceInfo[raceId] || {};
    const isPast = info._timeline === 'past';
    const localName = localizeRaceName(info.race_name) || localizeRaceClass(info.race_class) || '';
    const titleBits = [
        info.time && info.time !== 'TBA' ? info.time : null,
        info.place ? `${trackName(info.place)} R${info.race_number || ''}` : null,
        localName
    ].filter(Boolean).join(' · ');

    const headerRow = buildTableHeaderRow(raceId);
    const bodyRows  = buildTableBody(raceId, globalRaceEntries[raceId]);

    // Remove any prior modal, then build fresh.
    closeHorseRaceModal();
    const overlay = document.createElement('div');
    overlay.id = 'horse-race-modal';
    overlay.className = 'modal-overlay';
    overlay.onclick = (ev) => { if (ev.target === overlay) closeHorseRaceModal(); };
    overlay.innerHTML = `
        <div class="modal-content modal-content-race">
            <div class="modal-header">
                <h3 class="modal-title">${escapeHtml(titleBits || 'Race')}</h3>
                <div class="modal-header-actions">
                    <button onclick="closeHorseRaceModal()" class="close-btn">✖</button>
                </div>
            </div>
            <div class="horse-race-modal-body">
                <table class="${isPast && (appConfig.ui?.cleanPastRaceCards ?? true) ? 'past-race' : ''}">
                    <thead>${headerRow}</thead>
                    <tbody>${bodyRows}</tbody>
                </table>
            </div>
        </div>`;
    document.body.appendChild(overlay);

    // Highlight the profiled horse's row (scoped to the modal subtree so it doesn't
    // clash with a same-id row that may exist in the main schedules container).
    const hid = String(horseId || '').split('.')[0];
    if (hid) {
        const tr = overlay.querySelector(`#row-${CSS.escape(raceId)}-${CSS.escape(hid)}`);
        if (tr) {
            tr.classList.add('hh-modal-highlight');
            tr.scrollIntoView({ block: 'center' });
        }
    }
}

function closeHorseRaceModal() {
    const el = document.getElementById('horse-race-modal');
    if (el) el.remove();
}

// ── End Phase 31 ─────────────────────────────────────────────────────────────

function buildDailyRecapHtml(targetDate) {
    const date = String(targetDate || '').trim();
    if (!date || (globalDateTimelineByDate[date] || '') !== 'past') {
        return `
        <div class="day-recap-note">
            Overall hit rate appears when you are viewing a past race day.
        </div>`;
    }

    const summary = getDayOverallHitSummary(date);
    if (!summary.visible) {
        return `
        <div class="day-recap-note">
            ${escapeHtml(date)} has no fully-scored voted races yet.
        </div>`;
    }

    return `
    <section class="day-recap-card">
        <div class="day-recap-card-head">
            <h3>📅 ${escapeHtml(String(date))}</h3>
            <span class="day-recap-total">Overall</span>
        </div>
        <div class="day-recap-line">
            <div class="day-recap-line-head">
                <span class="day-recap-label">Bet hits vs placed (actual tickets)</span>
                <span class="day-recap-score">${summary.correct}/${summary.total} (${summary.rate}%)</span>
            </div>
        </div>
    </section>`;
}

async function showExportModal() {
    const targetDate = String(currentActiveDate || '').trim();
    await refreshBetEstimatesForDate(targetDate);
    const oreproRaceMap = getOreProRaceResultMapForActiveDate();
    const sMap = {"◎": 1, "〇": 2, "▲": 3, "△": 4, "☆": 5, "消": 6};
    const bColors = {
        1: { bg: '#f8f9fa', color: '#000', border: '#ccc' },
        2: { bg: '#212529', color: '#fff', border: '#444' },
        3: { bg: '#d26363', color: '#fff', border: '#d26363' },
        4: { bg: '#5970b0', color: '#fff', border: '#5970b0' },
        5: { bg: '#b8b053', color: '#000', border: '#b8b053' },
        6: { bg: '#72af68', color: '#fff', border: '#72af68' },
        7: { bg: '#efa65e', color: '#000', border: '#efa65e' },
        8: { bg: '#dc809a', color: '#000', border: '#dc809a' }
    };

    // Collect all non-X marks for this date, grouped by race
    const raceMarkGroups = {};
    for (const [key, symbol] of Object.entries(globalMarks)) {
        if (!symbol || symbol === 'X') continue;
        const [r_id, h_id] = key.split('_');
        const info = globalRaceInfo[r_id];
        if (!info) continue;
        const dateStr = info.clean_date || '';
        if (!targetDate || dateStr !== targetDate) continue;

        if (!raceMarkGroups[r_id]) raceMarkGroups[r_id] = { info, marks: [] };
        const entries = globalRaceEntries[r_id] || [];
        const horseRow = entries.find(r => String(r.Horse_ID).split('.')[0] === h_id);
        raceMarkGroups[r_id].marks.push({
            symbol,
            rank: sMap[symbol] || 99,
            horse: horseRow ? horseRow.Horse : 'Unknown Horse',
            pp: horseRow ? parseInt(horseRow.PP) || 99 : 99,
            bk: horseRow ? parseInt(horseRow.BK) || 0 : 0,
            fav: horseRow ? String(horseRow.Fav || '').trim() : ''
        });
    }

    // Sort races chronologically by sort_time, then by race number for ties
    const sortedRaces = Object.entries(raceMarkGroups).sort(([, a], [, b]) => {
        const at = a.info.sort_time || '';
        const bt = b.info.sort_time || '';
        if (at !== bt) return at.localeCompare(bt);
        return (parseInt(a.info.race_number) || 0) - (parseInt(b.info.race_number) || 0);
    });

    // Build race-time data array for auto-collapse (embedded as JSON in the popup)
    const collapseRaceTimeData = sortedRaces.map(([r_id, group]) => ({
        safeId: r_id.replace(/[^a-zA-Z0-9-]/g, ''),
        sortTime: group.info.sort_time || '',
        cleanDate: group.info.clean_date || '',
        displayTime: group.info.time || ''
    }));

    // Use the full selected day's races for the countdown so the popout matches the main view.
    const countdownRaceTimeData = (globalRacesByDate[targetDate] || [])
        .map(race => {
            const info = race?.info || {};
            const sortTime = String(info.sort_time || '').trim();
            const timeLabel = String(info.time || '').trim();
            if (!sortTime || !timeLabel || timeLabel === 'TBA') return null;

            return {
                sortTime,
                cleanDate: String(info.clean_date || '').trim(),
                displayTime: timeLabel,
                name: `${trackName(info.place)} R${info.race_number || '?'}`.trim()
            };
        })
        .filter(Boolean)
        .sort((a, b) => a.sortTime.localeCompare(b.sortTime));

    let html = "";
    if (!sortedRaces.length) {
        html = "<p style='text-align:center; color:#888; margin-top:50px;'>No votes cast yet! Make your selections in the grid first.</p>";
    } else {
        sortedRaces.forEach(([r_id, group]) => {
            const info = group.info;
            const track = trackName(info.place);
            const raceNum = parseInt(info.race_number, 10) || 0;
            const time = String(info.time || 'TBA');
            const safeId = r_id.replace(/[^a-zA-Z0-9-]/g, '');

            group.marks.sort((a, b) => a.rank - b.rank);

            html += `<div class="race-wrapper" id="wrapper-${safeId}">`;
            html += `<div class="export-race-title" id="title-${safeId}" onclick="toggleRace('${safeId}')" title="Click to collapse/expand">
                        <span id="arrow-${safeId}" style="display:inline-block;width:15px;font-size:10px;vertical-align:middle;">▼</span>
                        🕒 ${escapeHtml(time)} | ${escapeHtml(track)} R${raceNum}
                     </div>`;
            html += `<div class="export-race-card" id="content-${safeId}">`;

            const orepro = oreproRaceMap.get(r_id) || null;
            const estimateCache = raceBetEstimateCache[r_id] || null;
            if (orepro) {
                html += `
                <div class="popout-finance-inline">
                    <span class="popout-finance-chip">Buy ${escapeHtml(orepro.purchaseLabel || '-')}</span>
                    <span class="popout-finance-chip">Pay ${escapeHtml(orepro.payoutLabel || '-')}</span>
                    <span class="popout-finance-chip ${Number(orepro.profit) >= 0 ? 'is-positive' : 'is-negative'}">PnL ${escapeHtml(orepro.profitLabel || '-')}</span>
                </div>`;
            } else if (estimateCache?.pending) {
                html += `
                <div class="popout-finance-inline">
                    <span class="popout-finance-chip">Estimating Win / Q Box / T Box...</span>
                </div>`;
            } else if (['ok', 'partial'].includes(estimateCache?.data?.status)) {
                const estimate = estimateCache.data;
                const purchase = estimate?.purchase || {};
                const win = estimate?.win || {};
                const q = estimate?.quinellaBox || {};
                const t = estimate?.trioBox || {};
                const allHit = estimate?.allHit || {};
                const warningText = estimateWarningsText(estimate);

                const estBuyText = formatEstimateYen(purchase?.total);
                const winNetText = formatEstimateYen(win?.net);
                const qAvgText = formatEstimateAverageRefund(q?.minPayout, q?.maxPayout);
                const tAvgText = formatEstimateAverageRefund(t?.minPayout, t?.maxPayout);
                const allHitText = formatEstimateNetRange(allHit?.minNet, allHit?.maxNet);

                const estBuyReason = estBuyText === '-' ? (warningText || 'Estimated purchase is unavailable.') : warningText;
                const winReason = winNetText === '-' ? estimateValueReason(estimate, 'winNet') : warningText;
                const qAvgReason = qAvgText === '-' ? estimateValueReason(estimate, 'quinellaAvgRefund') : warningText;
                const tAvgReason = tAvgText === '-' ? estimateValueReason(estimate, 'trioAvgRefund') : warningText;
                const allHitReason = allHitText === '-' ? estimateValueReason(estimate, 'allHitNet') : warningText;
                html += `
                <div class="popout-finance-inline">
                    <span class="popout-finance-chip"${chipTitleAttr(estBuyReason)}>Est Buy ${escapeHtml(estBuyText)}</span>
                    <span class="popout-finance-chip ${estimateNetClass(win?.net)}"${chipTitleAttr(winReason)}>◎ Net ${escapeHtml(winNetText)}</span>
                    <span class="popout-finance-chip"${chipTitleAttr(qAvgReason)}>Q Avg Refund ${escapeHtml(qAvgText)}</span>
                    <span class="popout-finance-chip"${chipTitleAttr(tAvgReason)}>T Avg Refund ${escapeHtml(tAvgText)}</span>
                    <span class="popout-finance-chip ${estimateNetClass(allHit?.maxNet)}"${chipTitleAttr(allHitReason)}>All Hit ${escapeHtml(allHitText)}</span>
                </div>`;
            }

            group.marks.forEach(m => {
                const c = bColors[m.bk] || { bg: '#444', color: '#fff', border: '#444' };
                const symSize = m.symbol === '◎' ? '19px' : '16px';
                const ppBadge = m.pp !== 99
                    ? `<span class="export-horse-post" style="font-size:12px;font-weight:bold;background:${c.bg};color:${c.color};border:1px solid ${c.border};">${m.pp}</span>`
                    : `<span class="export-horse-post is-empty"></span>`;
                const markBadge = `<span class="export-horse-mark" style="font-size:${symSize};font-weight:bold;background:${c.bg};color:${c.color};border:1px solid ${c.border};">${escapeHtml(m.symbol)}</span>`;
                const favBadge = m.fav ? `Fav ${escapeHtml(String(m.fav))}` : 'Fav -';
                const safeHorseName = escapeHtml(String(m.horse || 'Unknown Horse'));

                html += `<div class="export-horse-line" style="margin-bottom:8px;">
                    ${ppBadge}${markBadge}<span class="export-horse-name">${safeHorseName}</span><span class="export-fav-badge export-horse-fav">${favBadge}</span>
                </div>`;
            });

            html += `</div></div>`;
        });
    }

    const collapseRaceTimeJson = JSON.stringify(collapseRaceTimeData);
    const countdownRaceTimeJson = JSON.stringify(countdownRaceTimeData);

    const fullHtml = `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="mobile-web-app-capable" content="yes">
    <meta name="apple-mobile-web-app-capable" content="yes">
    <title>🪟 Live View Popout</title>
    <style>
        body { font-family: sans-serif; background-color: #0c0c0c; color: #fafafa; margin: 0; padding: 20px; }
        .popout-head { position: sticky; top: 0; z-index: 999; background: #0c0c0c; display:flex; justify-content:space-between; align-items:flex-start; gap:16px; margin-bottom:20px; border-bottom:1px solid #333; padding-bottom:10px; }
        .popout-head-main { display:flex; align-items:flex-start; gap:12px; flex-wrap:wrap; }
        .popout-head-actions { display:flex; align-items:center; gap:10px; flex-wrap:wrap; justify-content:flex-end; }
        .popout-layout-btn { background:#222; color:#fafafa; border:1px solid #444; border-radius:4px; padding:6px 12px; font-weight:bold; font-size:13px; cursor:pointer; }
        .popout-layout-btn:hover { background:#2c2f39; }
        .countdown-wrapper { background: rgba(255, 75, 75, 0.1); border: 1px solid rgba(255, 75, 75, 0.3); border-radius: 8px; padding: 6px 18px; text-align: center; min-width: 180px; }
        .countdown-title { font-size: 11px; color: #ff7675; font-weight: bold; letter-spacing: 1px; text-transform: uppercase; }
        .countdown-time { font-size: 24px; font-weight: bold; color: #ff4b4b; font-variant-numeric: tabular-nums; }
        .countdown-race { font-size: 12px; color: #ccc; font-weight: bold; }
        .race-wrapper { margin-bottom: 10px; }
        .export-race-title { font-size: 14px; font-weight: bold; color: #888; margin-bottom: 6px; border-bottom: 1px dotted #444; padding-bottom: 4px; cursor: pointer; user-select: none; transition: 0.2s; }
        .export-race-title:hover { color: #fff; }
        .export-race-title.collapsed { color: #444; border-bottom-style: solid; border-color: #222; margin-bottom: 0; }
        .export-race-card { background: #1a1c23; border: 1px solid #333; border-radius: 6px; padding: 12px; margin-bottom: 4px; }
        .export-race-card.hidden { display: none; }
        .popout-finance-inline { display:flex; flex-wrap:wrap; gap:6px; margin-bottom:10px; }
        .popout-finance-chip { font-size:11px; color:#e8ecf7; border:1px solid #4f5970; border-radius:999px; padding:3px 8px; background: rgba(35, 41, 55, 0.92); white-space:nowrap; }
        .popout-finance-chip.is-positive { color:#c5ffe3; border-color:#375e4f; background:#17342b; }
        .popout-finance-chip.is-negative { color:#ffd7d7; border-color:#744848; background:#3d2020; }
        .export-horse-line { display: flex; align-items: center; gap: 8px; font-size: 14px; }
        .export-horse-post, .export-horse-mark { display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px; line-height:22px; text-align:center; border-radius:4px; flex:0 0 22px; }
        .export-horse-post.is-empty { background: transparent; border-color: transparent; }
        .export-horse-name { flex: 1; min-width: 0; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .export-fav-badge { font-size: 11px; color: #ddd; border: 1px solid #555; border-radius: 4px; padding: 2px 6px; white-space: nowrap; }
        body.reverse-horse-layout .export-horse-fav { order: 1; }
        body.reverse-horse-layout .export-horse-name { order: 2; text-align: right; }
        body.reverse-horse-layout .export-horse-mark { order: 3; }
        body.reverse-horse-layout .export-horse-post { order: 4; }
        .fullscreen-btn { background:#222; color:#fafafa; border:1px solid #444; border-radius:4px; padding:6px 12px; font-weight:bold; font-size:13px; cursor:pointer; }
        .fullscreen-btn:hover { background:#2c2f39; }
        @media (max-width: 720px) {
            body { padding: 12px; }
            .popout-head { flex-direction: column; align-items: stretch; }
            .popout-head-main { flex-direction: column; align-items: stretch; }
            .popout-head-actions { justify-content: stretch; flex-direction: column; }
            .popout-layout-btn { width: 100%; box-sizing: border-box; text-align: center; }
            .fullscreen-btn { width: 100%; box-sizing: border-box; text-align: center; }
            .countdown-wrapper { width: 100%; box-sizing: border-box; }
            .export-horse-line { font-size: 16px; }
            .export-horse-post, .export-horse-mark { width:26px; height:26px; line-height:26px; flex:0 0 26px; }
        }
    </style>
</head>
<body>
    <div class="popout-head">
        <div class="popout-head-main">
            <h3 style="margin:0;font-size:20px;">🪟 Live View · ${escapeHtml(targetDate || 'No date selected')}</h3>
            <div id="popout-countdown-container" class="countdown-wrapper" style="display:none;">
                <div class="countdown-title">Next Race In</div>
                <div id="popout-countdown-time" class="countdown-time">--:--:--</div>
                <div id="popout-countdown-race" class="countdown-race">Loading...</div>
            </div>
        </div>
        <div class="popout-head-actions">
            <button id="btn-toggle-horse-layout" class="popout-layout-btn" onclick="toggleHorseLayout()" type="button">⇄ Layout: Numbers Left</button>
            <button id="btn-fullscreen" class="fullscreen-btn" onclick="toggleFullscreen()" type="button">⛶ Full Screen</button>
            <a href="/tv.html" target="_blank" style="background:#f5a623;color:black;padding:6px 12px;text-decoration:none;border-radius:4px;font-weight:bold;font-size:14px;">📺 TV Mode</a>
            <a href="https://orepro.netkeiba.com/bet/race_list.html" target="_blank" style="background:#1dd1a1;color:black;padding:6px 12px;text-decoration:none;border-radius:4px;font-weight:bold;font-size:14px;">🔗 Open OrePro</a>
        </div>
    </div>
    <div id="race-list">${html}</div>
    <script>
        var collapseRaceTimeData = ${collapseRaceTimeJson};
        var countdownRaceTimeData = ${countdownRaceTimeJson};
        var reverseHorseLayout = false;

        function getSavedHorseLayoutPreference() {
            try {
                return window.localStorage.getItem('umanager-popout-reverse-layout') === '1';
            } catch (err) {
                return false;
            }
        }

        function setReverseHorseLayout(nextValue) {
            reverseHorseLayout = !!nextValue;
            document.body.classList.toggle('reverse-horse-layout', reverseHorseLayout);

            var btn = document.getElementById('btn-toggle-horse-layout');
            if (btn) {
                btn.innerText = reverseHorseLayout ? '⇄ Layout: Numbers Right' : '⇄ Layout: Numbers Left';
                btn.title = reverseHorseLayout
                    ? 'Showing Fav, Name, Mark, Post so the numbers sit on the right edge.'
                    : 'Showing Post, Mark, Name, Fav so the numbers sit on the left edge.';
            }

            try {
                window.localStorage.setItem('umanager-popout-reverse-layout', reverseHorseLayout ? '1' : '0');
            } catch (err) {
                // Ignore storage failures in popup/PiP contexts.
            }
        }

        function toggleHorseLayout() {
            setReverseHorseLayout(!reverseHorseLayout);
        }

        function toggleFullscreen() {
            var btn = document.getElementById('btn-fullscreen');
            var el = document.documentElement;
            if (!document.fullscreenElement) {
                var req = el.requestFullscreen || el.webkitRequestFullscreen || el.mozRequestFullScreen;
                if (req) {
                    req.call(el).catch(function() {});
                }
            } else {
                var exit = document.exitFullscreen || document.webkitExitFullscreen || document.mozCancelFullScreen;
                if (exit) exit.call(document);
            }
        }

        document.addEventListener('fullscreenchange', function() {
            var btn = document.getElementById('btn-fullscreen');
            if (btn) btn.innerText = document.fullscreenElement ? '\u26f6 Exit Full Screen' : '\u26f6 Full Screen';
        });
        document.addEventListener('webkitfullscreenchange', function() {
            var btn = document.getElementById('btn-fullscreen');
            if (btn) btn.innerText = document.fullscreenElement || document.webkitFullscreenElement ? '\u26f6 Exit Full Screen' : '\u26f6 Full Screen';
        });

        function parsePopoutRaceTime(itemOrSortTime, raceInfo) {
            var sortTime = '';
            var info = {};

            if (itemOrSortTime && typeof itemOrSortTime === 'object' && !Array.isArray(itemOrSortTime)) {
                sortTime = itemOrSortTime.sortTime || '';
                info = itemOrSortTime;
            } else {
                sortTime = itemOrSortTime || '';
                info = raceInfo || {};
            }

            if (!sortTime) return null;
            var normalized = String(sortTime).trim().replace(' ', 'T');
            if (!normalized) return null;

            if (/([zZ]|[+-]\d{2}:\d{2})$/.test(normalized)) {
                var explicit = new Date(normalized);
                return isNaN(explicit.getTime()) ? null : explicit;
            }

            var cleanDate = String(info.cleanDate || info.clean_date || '').trim();
            var displayTime = String(info.displayTime || info.display_time || '').trim();
            var sortDate = normalized.slice(0, 10);
            var looksCtLocal = /\b(?:AM|PM)\b/i.test(displayTime) || (!!cleanDate && cleanDate !== sortDate);

            var dt = new Date(looksCtLocal ? normalized : normalized + '+09:00');
            return isNaN(dt.getTime()) ? null : dt;
        }

        function formatCountdown(diff) {
            var d = Math.floor(diff / (1000 * 60 * 60 * 24));
            var h = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
            var m = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
            var s = Math.floor((diff % (1000 * 60)) / 1000);
            var timeStr = String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0') + ':' + String(s).padStart(2, '0');
            return d > 0 ? d + 'd ' + timeStr : timeStr;
        }

        function updateCountdown() {
            var container = document.getElementById('popout-countdown-container');
            var timeEl = document.getElementById('popout-countdown-time');
            var raceEl = document.getElementById('popout-countdown-race');
            if (!container || !timeEl || !raceEl) return;

            var now = new Date();
            var nextRace = null;
            for (var i = 0; i < countdownRaceTimeData.length; i++) {
                var item = countdownRaceTimeData[i];
                if (!item || !item.sortTime) continue;
                var raceTime = parsePopoutRaceTime(item);
                if (raceTime && raceTime > now) {
                    nextRace = { time: raceTime, name: item.name || 'Upcoming Race' };
                    break;
                }
            }

            if (!nextRace) {
                container.style.display = 'none';
                return;
            }

            container.style.display = 'block';
            timeEl.innerText = formatCountdown(nextRace.time - now);
            raceEl.innerText = nextRace.name;
        }

        function toggleRace(safeId) {
            var content = document.getElementById('content-' + safeId);
            var arrow = document.getElementById('arrow-' + safeId);
            var title = document.getElementById('title-' + safeId);
            var isHidden = content.classList.contains('hidden');
            content.classList.toggle('hidden', !isHidden);
            title.classList.toggle('collapsed', !isHidden);
            arrow.innerText = isHidden ? '▼' : '▶';
        }

        function autoCollapseRaces() {
            if (!collapseRaceTimeData.length) return;
            var now = new Date();
            // Find the index of the next race that hasn't started yet
            var nextIdx = -1;
            for (var i = 0; i < collapseRaceTimeData.length; i++) {
                var item = collapseRaceTimeData[i];
                var st = item && item.sortTime;
                if (!st) continue;
                var t = parsePopoutRaceTime(item);
                if (t && t > now) { nextIdx = i; break; }
            }
            if (nextIdx <= 0) return; // nothing to collapse (all future, or all past)
            var inProgressIdx = nextIdx - 1;
            var inProgressWrapper = null;
            for (var j = 0; j < collapseRaceTimeData.length; j++) {
                var sid = collapseRaceTimeData[j].safeId;
                var content = document.getElementById('content-' + sid);
                var arrow = document.getElementById('arrow-' + sid);
                var title = document.getElementById('title-' + sid);
                var wrapper = document.getElementById('wrapper-' + sid);
                if (!content || !arrow || !title) continue;
                if (j < inProgressIdx) {
                    // Collapse older past races
                    content.classList.add('hidden');
                    title.classList.add('collapsed');
                    arrow.innerText = '▶';
                } else if (j === inProgressIdx) {
                    // Keep the in-progress race expanded
                    content.classList.remove('hidden');
                    title.classList.remove('collapsed');
                    arrow.innerText = '▼';
                    inProgressWrapper = wrapper;
                }
                // Future races stay as-is
            }
            // Auto-scroll to the in-progress race (only if off-screen)
            if (inProgressWrapper) {
                setTimeout(function() {
                    inProgressWrapper.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                }, 50);
            }
        }

        setReverseHorseLayout(getSavedHorseLayoutPreference());
        updateCountdown();
        autoCollapseRaces();
        setInterval(updateCountdown, 1000);
        setInterval(autoCollapseRaces, 5000);
    <\/script>
</body>
</html>`;

    if ('documentPictureInPicture' in window) {
        try {
            const pipWindow = await window.documentPictureInPicture.requestWindow({
                width: 800,
                height: 600
            });
            pipWindow.document.write(fullHtml);
            return;
        } catch (err) {
            
        }
    }

    const popup = window.open("", "OreProCheatSheet", "width=850,height=700,menubar=no,toolbar=no,location=no,status=no");
    if (popup) {
        popup.document.open();
        popup.document.write(fullHtml);
        popup.document.close();
    } else {
        alert("Popup blocked! Please allow popups for this site to use the floating cheat sheet.");
    }
}

function closeExportModal() {
    document.getElementById('export-modal').style.display = "none";
}

// ── Phase 37: Odds-history trend graph ──────────────────────────────────────
// Modal with a hand-rolled SVG line chart (no chart-lib dependency). Field mode
// (one line per horse) opens from the race-header 📈 button; single-horse mode
// opens from clicking a horse's odds cell. Data: GET /api/races/{id}/odds-history.
let _oddsHistoryState = null;   // { raceId, series:[{horse_id,name,post_position,points:[{t,odds}]}], single }
let _oddsHistoryFocus = new Set();  // horseIds explicitly shown; EMPTY = show the whole field
let _oddsHistoryGeom = null;    // geometry snapshot for the hover crosshair

// Empty focus = show everything; otherwise show only the focused horses.
function _ohIsShown(horseId) {
    return _oddsHistoryFocus.size === 0 || _oddsHistoryFocus.has(String(horseId));
}
// Step-hold lookup: odds in effect at time `ms` (last change at-or-before ms).
function _ohOddsAtMs(pts, ms) {
    let v = (pts && pts.length) ? pts[0].odds : null;
    if (pts) for (const p of pts) { if (p.ms <= ms) v = p.odds; else break; }
    return v;
}

function oddsHistoryHorseMeta(raceId, horseId) {
    const entries = globalRaceEntries[raceId] || [];
    const row = entries.find(e => String(e.Horse_ID ?? '').split('.')[0] === String(horseId));
    return {
        name: row ? (row.Horse || String(horseId)) : String(horseId),
        pp:   row ? parseInt(row.PP, 10) : NaN,
        fav:  row ? parseInt(row.Fav, 10) : NaN   // current popularity rank (live fallback)
    };
}

async function showOddsHistory(raceId, horseId = null) {
    const modal = document.getElementById('odds-history-modal');
    const body  = document.getElementById('odds-history-body');
    const titleEl = document.getElementById('odds-history-title');
    if (!modal || !body) return;

    const info = globalRaceInfo[raceId] || {};
    const trackTxt = trackName(info.place) || info.place || '';
    const rNum = info.race_number || '?';
    if (horseId) {
        const meta = oddsHistoryHorseMeta(raceId, horseId);
        titleEl.textContent = `📈 ${meta.name} — Odds Trend (${trackTxt} R${rNum})`;
    } else {
        titleEl.textContent = `📈 Odds Trends — ${trackTxt} R${rNum}`;
    }

    _oddsHistoryFocus = new Set();
    body.innerHTML = '<div class="odds-history-empty">Loading…</div>';
    modal.style.display = 'flex';

    try {
        const res = await fetch(`/api/races/${encodeURIComponent(raceId)}/odds-history`, { cache: 'no-store' });
        const data = await res.json();
        // Always keep the FULL field in memory — popularity is a field-wide rank, so even
        // single-horse view needs every runner's odds to compute it. `single` just controls display.
        const series = Array.isArray(data.series) ? data.series : [];
        series.forEach(s => {
            const meta = oddsHistoryHorseMeta(raceId, s.horse_id);
            s.name = meta.name;
            if (!Number.isFinite(s.post_position) && Number.isFinite(meta.pp)) s.post_position = meta.pp;
        });
        _oddsHistoryState = { raceId, series, single: horseId ? String(horseId) : null };
        renderOddsHistory();
    } catch (e) {
        body.innerHTML = '<div class="odds-history-empty">Failed to load odds history.</div>';
    }
}

function closeOddsHistoryModal() {
    const m = document.getElementById('odds-history-modal');
    if (m) m.style.display = 'none';
    _oddsHistoryState = null;
    _oddsHistoryGeom = null;
}

// ── Phase 36: Devil's Advocate Export ───────────────────────────────────────
// Copies a self-contained prompt + JSON for a race to the clipboard, to paste into
// any LLM for a blunt second opinion. Pure frontend — uses globalRaceEntries,
// globalMarks, and the scoring functions.
function _aiRiskStance(risk) {
    if (risk < 40)  return { zone: 'SAFE',  line: "I'm trying to make safe, chalk-heavy bets and protect the bankroll." };
    if (risk <= 60) return { zone: 'BLEND', line: "I'm balancing value and safety." };
    return { zone: 'CHAOS', line: "I'm hunting for value and longshot upsets." };
}
function _aiDash(v) {
    if (v === null || v === undefined) return '—';
    const s = String(v).trim();
    return (s === '' || s === '0') ? '—' : s;
}
function _aiPct(v) {
    const n = parseFloat(v);
    return Number.isFinite(n) ? `${(n * 100).toFixed(0)}%` : '—';
}
function _aiNum(v, d = 2) {
    const n = parseFloat(v);
    return Number.isFinite(n) ? n.toFixed(d) : '—';
}

function exportRaceForAI(r_id) {
    const entries = globalRaceEntries[r_id] || [];
    if (!entries.length) { showCopyToast('No runners to export yet.'); return; }
    const info = globalRaceInfo[r_id] || {};
    const risk = getCurrentAutoPickRisk();
    const stance = _aiRiskStance(risk);

    // Engine power ranking + suggested marks.
    const ranked = entries
        .map(row => ({ h: String(row.Horse_ID).split('.')[0], power: calculatePowerScore(row, risk) }))
        .sort((a, b) => b.power - a.power);
    const prByHorse = {};
    ranked.forEach((e, i) => { prByHorse[e.h] = { rank: i + 1, power: e.power }; });
    const engineMark = {};
    getUnconditionalAutoBetRankingsForRace(r_id).forEach(p => { engineMark[p.h_id] = p.symbol; });

    const localName = localizeRaceName(info.race_name) || localizeRaceClass(info.race_class) || 'Unnamed race';
    const surface = info.surface ? String(info.surface) : '';
    const dist = info.distance ? `${info.distance}m` : '';
    const track = trackName(info.place) || info.place || '';

    // Per-entry data (PP order).
    const rowData = entries.map(row => {
        const h = String(row.Horse_ID).split('.')[0];
        const runs = parseLast3Runs(row.Last3);
        const form = runs.length
            ? runs.map(rn => rn.favRank !== null
                ? `${rn.fin}(fav${rn.favRank},Δ${rn.delta >= 0 ? '+' : ''}${rn.delta})`
                : `${rn.fin}`).join(' ')
            : '—';
        const pr = prByHorse[h] || {};
        return {
            h, pp: parseInt(row.PP, 10) || 99,
            name: row.Horse || h,
            odds: _aiDash(row.Odds), fav: _aiDash(row.Fav),
            you: globalMarks[`${r_id}_${h}`] || '·',
            eng: engineMark[h] || '·',
            form,
            jockey: row.Jockey || row.Jockey_Code || '—',
            jWin: _aiPct(row.Jockey_Win_Pct), jAE: _aiNum(row.Jockey_AE),
            trainer: row.Trainer || row.Trainer_Code || '—',
            tWin: _aiPct(row.Trainer_Win_Pct), tAE: _aiNum(row.Trainer_AE),
            sireFit: Number.isFinite(parseFloat(row.Sire_Fit)) ? `${parseFloat(row.Sire_Fit).toFixed(0)}%` : '—',
            career: row.Record || '—',
            power: pr.power != null ? pr.power.toFixed(1) : '—',
            prank: pr.rank || '—'
        };
    }).sort((a, b) => a.pp - b.pp);

    // Markdown table.
    const header = '| PP | Horse | Odds | Fav | You | Eng | Last-3 (fin·favΔ) | Jockey W%/AE | Trainer W%/AE | SireFit | Career | Pwr(rk) |';
    const sep    = '|---|---|---|---|---|---|---|---|---|---|---|---|';
    const tlines = rowData.map(r =>
        `| ${r.pp === 99 ? '—' : r.pp} | ${r.name} | ${r.odds} | ${r.fav} | ${r.you} | ${r.eng} | ${r.form} | ${r.jockey} ${r.jWin}/${r.jAE} | ${r.trainer} ${r.tWin}/${r.tAE} | ${r.sireFit} | ${r.career} | ${r.power}(${r.prank}) |`);
    const table = [header, sep, ...tlines].join('\n');

    const symName = s => ({ '◎': '◎ Honmei', '〇': '〇 Taiko', '▲': '▲ Dark horse', '△': '△ Longshot', 'X': 'X eliminated' }[s] || s);
    const yourMarks = rowData.filter(r => r.you !== '·').map(r => `  ${symName(r.you)} → #${r.pp === 99 ? '?' : r.pp} ${r.name}`);
    const engMarks  = rowData.filter(r => r.eng !== '·').map(r => `  ${r.eng} → #${r.pp === 99 ? '?' : r.pp} ${r.name}`);
    const disagree  = rowData.filter(r => r.you !== '·' && r.eng !== '·' && r.you !== r.eng)
        .map(r => `  #${r.pp === 99 ? '?' : r.pp} ${r.name}: you ${r.you}, engine ${r.eng}`);

    const prompt = [
        "You are a seasoned keiba (Japanese horse racing) betting fanatic who has taken a newcomer under your wing. You're blunt, opinionated, and deeply knowledgeable about JRA racing. Review my picks for this race and tell me what I'm missing.",
        "",
        "MARK SYSTEM (JRA): ◎ honmei (my #1), 〇 taiko (strong rival), ▲ dark horse (value), △ longshot, X eliminated. Box bets: Quinella Box = ◎+〇+▲ (any 2 finish 1st-2nd); Trio Box = ◎+〇+▲+△ (any 3 finish 1st-2nd-3rd). A risk slider sets my appetite from chalk (safe) to chaos (value/upsets).",
        "",
        `MY RISK STANCE: ${stance.zone} (slider ${risk}/100) — ${stance.line}`,
        "",
        `RACE: ${track} R${info.race_number || '?'} — ${localName}${surface ? ' · ' + surface : ''}${dist ? ' · ' + dist : ''} · ${entries.length} runners · post ${info.time || '?'} JST`,
        "",
        "RUNNERS (Δ = Ninki-Finish delta: favRank − finish, positive = beat the market that day; Pwr = my engine's power score + its field rank):",
        table,
        "",
        "MY MARKS:",
        yourMarks.length ? yourMarks.join('\n') : '  (none set yet)',
        "",
        "WHAT MY ENGINE SUGGESTED (rank-order by power score):",
        engMarks.length ? engMarks.join('\n') : '  (none)',
        disagree.length ? "\nDISAGREEMENTS (my mark ≠ engine):\n" + disagree.join('\n') : "",
        "",
        "DEVIL'S ADVOCATE — do all of this:",
        "1. For each horse I marked, argue why it might fail.",
        "2. For each unmarked horse in the top 6 by odds, argue why I might be overlooking it.",
        "3. Flag any mark that contradicts the data (odds, form, jockey/trainer, sire fit).",
        "4. Suggest ONE alternative mark configuration (◎〇▲△) and explain why it fits my risk stance.",
        "5. Be specific — cite the numbers in the table."
    ].join('\n');

    const json = JSON.stringify({
        race: { id: r_id, track, race_number: info.race_number, name: localName, surface, distance: info.distance, post_jst: info.time, runners: entries.length },
        risk: { slider: risk, zone: stance.zone },
        runners: rowData.map(r => ({
            pp: r.pp === 99 ? null : r.pp, horse: r.name, odds: r.odds, fav_rank: r.fav,
            your_mark: r.you === '·' ? null : r.you, engine_mark: r.eng === '·' ? null : r.eng,
            last3: r.form, jockey: r.jockey, jockey_win: r.jWin, jockey_ae: r.jAE,
            trainer: r.trainer, trainer_win: r.tWin, trainer_ae: r.tAE,
            sire_fit: r.sireFit, career: r.career, power: r.power, power_rank: r.prank
        }))
    }, null, 2);

    _aiCopy(prompt + "\n\n=== STRUCTURED DATA (JSON) ===\n" + json);
}

// Dev tuning export: prose-free slider sweep. Shows the engine's top-4 marks at a range
// of slider positions for ONE race, plus a compact runner table — so the scoring curve
// can be tuned from real fields without LLM narrative noise. No persona, no JSON.
function exportRaceForTuning(r_id) {
    const entries = globalRaceEntries[r_id] || [];
    if (!entries.length) { showCopyToast('No runners to export yet.'); return; }
    const info = globalRaceInfo[r_id] || {};
    const localName = localizeRaceName(info.race_name) || localizeRaceClass(info.race_class) || 'Unnamed race';
    const track = trackName(info.place) || info.place || '';
    const cls = info.race_class ? String(info.race_class) : 'normal';

    const nm = row => row.Horse || String(row.Horse_ID).split('.')[0];
    const ppOf = row => parseInt(row.PP, 10) || 0;

    // Reference table (PP order): odds, fav, sire fit, career — the raw inputs.
    const ref = entries.slice().sort((a, b) => ppOf(a) - ppOf(b)).map(row =>
        `${String(ppOf(row)).padStart(2)} ${nm(row).padEnd(20).slice(0,20)} ` +
        `odds ${_aiDash(row.Odds).padStart(6)}  fav ${_aiDash(row.Fav).padStart(2)}  ` +
        `sireFit ${(Number.isFinite(parseFloat(row.Sire_Fit)) ? parseFloat(row.Sire_Fit).toFixed(0)+'%' : '—').padStart(4)}  ` +
        `career ${row.Record || '—'}`
    );

    // Slider sweep: top-4 by power score at each position (mirrors the auto-pick sort).
    const positions = [0, 25, 50, 75, 90, 99, 100];
    const symbols = ['◎', '〇', '▲', '△'];
    const exportHasOdds = entries.some(row => { const o = parseFloat(row.Odds); return Number.isFinite(o) && o > 0; });
    const sweep = positions.map(risk => {
        const sweepZone = risk <= ENGINE_TUNING.SAFE_MAX ? 'SAFE' : risk >= ENGINE_TUNING.CHAOS_MIN ? 'CHAOS' : 'BLEND';
        if (!exportHasOdds && sweepZone !== 'CHAOS') {
            return `risk ${String(risk).padStart(3)}: (pre-odds · SAFE/BLEND hidden — needs market prices)`;
        }
        const top4 = entries
            .map(row => ({ row, power: calculatePowerScore(row, risk) }))
            .sort((a, b) => b.power - a.power)
            .slice(0, 4);
        const marks = top4.map((e, i) =>
            `${symbols[i]} #${ppOf(e.row)} ${nm(e.row)} (${_aiDash(e.row.Odds)}|f${_aiDash(e.row.Fav)})`
        ).join('   ');
        return `risk ${String(risk).padStart(3)}: ${marks}`;
    });

    const out = [
        "You are an expert in Japanese horse racing (JRA/keiba) and betting-model design. I'm tuning the risk slider on my betting assistant and need you to judge whether its behavior is sound. Be analytical and specific — this is a model review, not a betting tip.",
        "",
        "HOW THE TOOL WORKS:",
        "- It scores every runner and assigns marks ◎ (top pick), 〇 (2nd), ▲ (3rd), △ (4th) — the top 4 by a 'power score'.",
        "- A risk slider (0–100) shifts what the score rewards:",
        "  • 0 = PURE CHALK: marks are strictly the 4 betting favorites (shortest odds), ignoring everything else. (Intentionally degenerate.)",
        "  • 1–99 = a graded blend. As risk rises, the model should progressively favor VALUE/longshots (longer odds with some merit signal) over the favorites. This band must be 'defensible' — every pick should have a rational basis (form, sire fit, jockey/trainer, or sensible value).",
        "  • 100 = PURE YOLO: marks are strictly the 4 LONGEST-odds horses (with any merit). Intentionally degenerate / 'revealed in a dream'. The mirror image of 0.",
        "- Design goals: (a) picks should change SMOOTHLY and visibly across the slider — no big dead plateau where many positions give identical marks; (b) the model should NOT over-chase hopeless longshots in the 1–99 band, especially in MAIDEN/DEBUT races where every horse is unproven; (c) 99 should still be 'grounded' (a defensible longshot), while only 100 goes fully reckless.",
        "",
        "WHAT TO EVALUATE:",
        "1. Does the sweep below progress smoothly from chalk → value → YOLO, or are there plateaus / abrupt jumps?",
        "2. Are the mid-slider (40–70) picks defensible for the stated risk, given this race's data?",
        "3. For this race's class, is the longshot-chasing appropriate or excessive at high risk?",
        "4. Do the degenerate endpoints (0 = favorites, 100 = longest odds) look correct?",
        "5. Suggest any concrete adjustment (e.g. 'value kicks in too early/late', 'risk 75 should still include the favorite').",
        "",
        `RACE: ${track} R${info.race_number || '?'} — ${localName} [class: ${cls}] · ${entries.length} runners`,
        `SWEEP (engine's top-4 marks ◎〇▲△ at each slider position; shown as "#post Name (odds|favRank)"):`,
        '',
        ...sweep,
        '',
        'RUNNERS (PP order — the raw inputs the score draws from):',
        ...ref,
    ].join('\n');

    _aiCopy(out);
}

function _aiCopy(text) {
    const ok = () => showCopyToast('📋 Copied — paste into Claude / ChatGPT');
    const fail = () => showCopyToast('Copy failed — check console');
    if (navigator.clipboard?.writeText) {
        navigator.clipboard.writeText(text).then(ok).catch(() => { _aiCopyFallback(text) ? ok() : fail(); });
    } else {
        _aiCopyFallback(text) ? ok() : fail();
    }
}
function _aiCopyFallback(text) {
    try {
        const ta = document.createElement('textarea');
        ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta); ta.select();
        const ok = document.execCommand('copy');
        document.body.removeChild(ta);
        return ok;
    } catch (e) { console.warn('[AIExport] copy failed', e); return false; }
}

function showCopyToast(msg) {
    let t = document.getElementById('copy-toast');
    if (!t) { t = document.createElement('div'); t.id = 'copy-toast'; document.body.appendChild(t); }
    t.textContent = msg;
    t.classList.add('visible');
    clearTimeout(showCopyToast._timer);
    showCopyToast._timer = setTimeout(() => t.classList.remove('visible'), 2600);
}

function _ohColor(i, n) { return `hsl(${Math.round(i * 360 / Math.max(n, 1))}, 70%, 56%)`; }
function _ohJstClock(ms) {
    const d = new Date(ms + 9 * 3600 * 1000); // UTC → JST wall clock
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mm = String(d.getUTCMinutes()).padStart(2, '0');
    return `${hh}:${mm}`;
}

function renderOddsHistory() {
    const body = document.getElementById('odds-history-body');
    const st = _oddsHistoryState;
    if (!body || !st) return;

    const drawable = st.series.filter(s => Array.isArray(s.points) && s.points.length > 0);
    if (!drawable.length) {
        body.innerHTML = '<div class="odds-history-empty">No odds history recorded for this race yet. '
            + 'Trends start building once live odds polling captures changes.</div>';
        return;
    }

    // Geometry (fixed internal coords; CSS scales width to fit).
    const VBW = 860, VBH = 380, padL = 52, padR = 18, padT = 16, padB = 34;
    const plotW = VBW - padL - padR, plotH = VBH - padT - padB;

    // Which lines to draw: single mode → just that horse; else the focus set (empty = all).
    const showThis = (hid) => st.single ? String(hid) === st.single : _ohIsShown(hid);
    const shown = drawable.filter(s => showThis(s.horse_id));

    // Axis domains scale to the SHOWN series only, so isolating one horse zooms in
    // (the full field is still kept in memory below, but only for popularity ranking).
    let tMin = Infinity, tMax = -Infinity, oMin = Infinity, oMax = -Infinity;
    shown.forEach(s => s.points.forEach(p => {
        const ms = Date.parse(p.t);
        if (ms < tMin) tMin = ms; if (ms > tMax) tMax = ms;
        if (p.odds < oMin) oMin = p.odds; if (p.odds > oMax) oMax = p.odds;
    }));
    if (!(tMax > tMin)) tMax = tMin + 1;          // single timestamp guard
    if (!(oMax > oMin)) { oMin = Math.max(1, oMin * 0.9); oMax = oMax * 1.1 || oMin + 1; }
    const lMin = Math.log(oMin), lMax = Math.log(oMax);

    const xOf = ms => padL + (ms - tMin) / (tMax - tMin) * plotW;
    // Low odds (favorite) at the TOP so a horse being bet (odds shortening) trends UP = good.
    const yOf = od => padT + ((Math.log(od) - lMin) / (lMax - lMin)) * plotH;

    // Y gridlines at "nice" odds values inside the range.
    const niceTicks = [1.2, 1.5, 2, 3, 5, 7, 10, 15, 20, 30, 50, 70, 100, 150, 200, 300, 500];
    let yTicks = niceTicks.filter(v => v >= oMin * 0.999 && v <= oMax * 1.001);
    if (yTicks.length < 2) yTicks = [oMin, oMax];

    let svg = `<svg id="odds-history-svg" viewBox="0 0 ${VBW} ${VBH}" preserveAspectRatio="none" class="odds-history-svg">`;
    // Y grid + labels.
    yTicks.forEach(v => {
        const y = yOf(v).toFixed(1);
        svg += `<line x1="${padL}" y1="${y}" x2="${padL + plotW}" y2="${y}" class="oh-grid"/>`;
        svg += `<text x="${padL - 6}" y="${y}" class="oh-ylabel">${v}</text>`;
    });
    // X axis end labels (JST) + a midpoint.
    [tMin, (tMin + tMax) / 2, tMax].forEach((ms, i) => {
        const x = xOf(ms).toFixed(1);
        const anchor = i === 0 ? 'start' : (i === 2 ? 'end' : 'middle');
        svg += `<text x="${x}" y="${VBH - 10}" class="oh-xlabel" text-anchor="${anchor}">${_ohJstClock(ms)} JST</text>`;
    });

    // Full-field odds-by-time, for computing popularity (= rank of win odds) at any
    // instant — independent of which lines are currently shown.
    const allOdds = drawable.map(s => ({
        horseId: String(s.horse_id),
        pts: s.points.map(p => ({ ms: Date.parse(p.t), odds: p.odds }))
    }));

    // Lines.
    const n = drawable.length;
    const geomSeries = [];
    drawable.forEach((s, i) => {
        if (!showThis(s.horse_id)) return;
        const color = _ohColor(i, n);
        const pts = s.points.map(p => ({ x: xOf(Date.parse(p.t)), y: yOf(p.odds), odds: p.odds, ms: Date.parse(p.t) }));
        // Carry the last value flat to the right edge: odds persist until they change,
        // so a horse whose odds last moved earlier than the field shouldn't look cut off.
        const lastReal = pts[pts.length - 1];
        if (lastReal.ms < tMax) {
            pts.push({ x: xOf(tMax), y: lastReal.y, odds: lastReal.odds, ms: tMax, synthetic: true });
        }
        const path = pts.map(p => `${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
        svg += `<polyline id="oh-line-${i}" points="${path}" fill="none" stroke="${color}" stroke-width="2" class="oh-line"/>`;
        // Dot at the right edge (current value).
        const last = pts[pts.length - 1];
        svg += `<circle cx="${last.x.toFixed(1)}" cy="${last.y.toFixed(1)}" r="3.2" fill="${color}"/>`;
        if (st.single) {
            pts.forEach(p => { if (!p.synthetic) svg += `<circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="2.2" fill="${color}"/>`; });
        }
        geomSeries.push({ idx: i, horseId: String(s.horse_id), name: s.name, color, pts, pp: s.post_position });
    });

    // Popularity-change markers: only when exactly one horse is on screen (single mode or
    // an isolated line), otherwise the field would be a confetti of labels. A horse's rank
    // can shift even when its own odds are flat (a rival's money moves past it), so we
    // evaluate rank at EVERY field timestamp, not just this horse's own points.
    if (geomSeries.length === 1) {
        const horse = geomSeries[0];
        const mine = allOdds.find(o => o.horseId === horse.horseId);
        if (mine) {
            const times = [...new Set(allOdds.flatMap(o => o.pts.map(p => p.ms)))].sort((a, b) => a - b);
            let prevRank = null;
            times.forEach(ms => {
                const myOdds = _ohOddsAtMs(mine.pts, ms);
                if (myOdds === null) return;
                let rank = 1;
                allOdds.forEach(o => {
                    if (o.horseId === horse.horseId) return;
                    const ov = _ohOddsAtMs(o.pts, ms);
                    if (ov !== null && ov < myOdds) rank++;
                });
                if (prevRank !== null && rank !== prevRank) {
                    const x = xOf(ms), y = yOf(myOdds);
                    const improved = rank < prevRank;           // lower number = more popular
                    const col = improved ? '#1dd1a1' : '#f5a623';
                    svg += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="3" fill="${col}" stroke="#11141a" stroke-width="1"/>`;
                    svg += `<text x="${x.toFixed(1)}" y="${(y - 7).toFixed(1)}" class="oh-rankmark" fill="${col}" text-anchor="middle">${improved ? '▲' : '▼'}#${rank}</text>`;
                }
                prevRank = rank;
            });
        }
    }

    // Crosshair guide + hover capture rect.
    svg += `<line id="oh-crosshair" x1="0" y1="${padT}" x2="0" y2="${padT + plotH}" class="oh-crosshair" style="display:none"/>`;
    svg += `<rect id="oh-hover" x="${padL}" y="${padT}" width="${plotW}" height="${plotH}" fill="transparent"/>`;
    svg += `</svg>`;

    // Legend (sorted by current odds asc — favorites first). Popularity = rank of
    // current win odds (computed, not stored). Click isolates / builds a focus set.
    const legendItems = drawable.map((s, i) => {
        const cur = s.points[s.points.length - 1].odds;
        const first = s.points[0].odds;
        return { s, i, cur, first, color: _ohColor(i, n) };
    }).sort((a, b) => a.cur - b.cur);

    let legend = '<div class="odds-history-legend">';
    legendItems.forEach(it => {
        if (st.single && String(it.s.horse_id) !== st.single) return;  // single mode: that horse only
        const ppTxt = Number.isFinite(it.s.post_position) ? it.s.post_position : '–';
        const pop = 1 + legendItems.filter(o => o.cur < it.cur).length;  // rank by current odds
        const move = it.cur < it.first ? `▼ ${it.first.toFixed(1)}→${it.cur.toFixed(1)}`
                   : it.cur > it.first ? `▲ ${it.first.toFixed(1)}→${it.cur.toFixed(1)}`
                   : `${it.cur.toFixed(1)}`;
        const moveCls = it.cur < it.first ? 'oh-move-short' : (it.cur > it.first ? 'oh-move-drift' : '');
        const off = !showThis(it.s.horse_id);
        const click = st.single ? '' : ` onclick="toggleOddsHistoryLine('${escapeHtml(String(it.s.horse_id))}')"`;
        legend += `<span class="oh-legend-item${off ? ' oh-legend-off' : ''}"${click}>`
                + `<span class="oh-swatch" style="background:${it.color}"></span>`
                + `<span class="oh-leg-pp">${ppTxt}</span> `
                + `<span class="oh-leg-name">${escapeHtml(it.s.name)}</span> `
                + `<span class="oh-leg-odds ${moveCls}">${move}</span>`
                + ` <span class="oh-leg-pop" title="Popularity = rank by current odds">#${pop}</span>`
                + `</span>`;
    });
    legend += '</div>';

    const focusing = _oddsHistoryFocus.size > 0;
    const markerNote = '<span class="oh-rankmark-key">▲▼ dots = popularity-rank changes</span>';
    let hintText;
    if (st.single)                          hintText = markerNote;
    else if (_oddsHistoryFocus.size === 1)  hintText = `${markerNote} · tap more to compare · tap to drop`;
    else if (focusing)                      hintText = 'Tap more horses to compare · tap a shown horse to drop it · clear all to show the field';
    else                                    hintText = 'Tap a horse to isolate its line (then add others to compare)';
    const hint = `<div class="odds-history-hint">${hintText}</div>`;
    body.innerHTML = `<div class="odds-history-chart-wrap"><div id="oh-tooltip" class="odds-history-tooltip" style="display:none"></div>${svg}</div>${legend}${hint}`;

    // Stash geometry + wire the hover crosshair.
    _oddsHistoryGeom = { padL, padT, plotW, plotH, tMin, tMax, VBW, VBH, series: geomSeries, allOdds };
    _wireOddsHistoryHover();
}

// Click model: from "whole field", a click isolates that horse; further clicks add
// horses to compare; clicking a shown horse drops it; dropping the last one returns
// to showing the whole field.
function toggleOddsHistoryLine(horseId) {
    const k = String(horseId);
    const f = _oddsHistoryFocus;
    if (f.size === 0)      f.add(k);        // field → isolate this one
    else if (f.has(k))     f.delete(k);     // drop it (empty ⇒ back to whole field)
    else                   f.add(k);        // add another to compare
    renderOddsHistory();
}

// Live-extend the open trend chart from a SignalR OddsUpdated push. Appends a fresh
// point per horse whose odds changed (mirrors the server's change-only capture), then
// re-renders. Popularity is computed from odds, so it updates for free. Focus/zoom are
// module state, so the user's current isolation survives the refresh.
function liveUpdateOddsHistory(payload) {
    const st = _oddsHistoryState;
    if (!st || !payload || String(st.raceId) !== String(payload.raceId)) return;
    const nowIso = new Date().toISOString();
    let changed = false;
    (payload.entries || []).forEach(e => {
        const odds = parseFloat(e.odds);
        if (!Number.isFinite(odds) || odds <= 0) return;
        const s = st.series.find(x => String(x.horse_id) === String(e.horseId));
        if (!s || !Array.isArray(s.points) || !s.points.length) return;
        const last = s.points[s.points.length - 1];
        if (Math.abs(last.odds - odds) < 0.05) return;   // unchanged → skip (matches server dedup)
        s.points.push({ t: nowIso, odds, fav: e.fav ? parseInt(e.fav, 10) : null });
        changed = true;
    });
    if (changed) renderOddsHistory();
}

function _ohSetLineEmphasis(hoverIdx) {
    const g = _oddsHistoryGeom;
    if (!g) return;
    g.series.forEach(s => {
        const el = document.getElementById('oh-line-' + s.idx);
        if (!el) return;
        if (hoverIdx === null) {                 // reset
            el.style.opacity = '1'; el.style.strokeWidth = '2';
        } else if (s.idx === hoverIdx) {          // emphasize hovered
            el.style.opacity = '1'; el.style.strokeWidth = '3.4';
        } else {                                  // dim the rest
            el.style.opacity = '0.18'; el.style.strokeWidth = '1.5';
        }
    });
}

function _wireOddsHistoryHover() {
    const svg = document.getElementById('odds-history-svg');
    const rect = document.getElementById('oh-hover');
    const cross = document.getElementById('oh-crosshair');
    const tip = document.getElementById('oh-tooltip');
    const g = _oddsHistoryGeom;
    if (!svg || !rect || !cross || !tip || !g) return;

    const NEAR = 16; // viewBox units: how close (vertically) the cursor must be to "hover" a line

    const onMove = (evt) => {
        const r = svg.getBoundingClientRect();
        const clientX = evt.touches ? evt.touches[0].clientX : evt.clientX;
        const clientY = evt.touches ? evt.touches[0].clientY : evt.clientY;
        const vx = Math.max(g.padL, Math.min(g.padL + g.plotW, ((clientX - r.left) / r.width) * g.VBW));
        const vy = ((clientY - r.top) / r.height) * g.VBH;
        const frac = (vx - g.padL) / g.plotW;
        const ms = g.tMin + frac * (g.tMax - g.tMin);

        // Time crosshair always tracks the cursor.
        cross.setAttribute('x1', vx.toFixed(1));
        cross.setAttribute('x2', vx.toFixed(1));
        cross.style.display = '';

        // Find the line whose nearest-in-time point is vertically closest to the cursor.
        let best = null, bestDy = Infinity;
        g.series.forEach(s => {
            let np = s.pts[0], nd = Infinity;
            for (const p of s.pts) { const d = Math.abs(p.ms - ms); if (d < nd) { nd = d; np = p; } }
            const dy = Math.abs(np.y - vy);
            if (dy < bestDy) { bestDy = dy; best = { s, np }; }
        });

        if (!best || bestDy > NEAR) {     // not near any line → just the time guide
            tip.style.display = 'none';
            _ohSetLineEmphasis(null);
            return;
        }

        _ohSetLineEmphasis(best.s.idx);

        const ppTxt = Number.isFinite(best.s.pp) ? `${best.s.pp} ` : '';
        // Popularity at this instant = rank of this horse's odds among the full field.
        const all = g.allOdds || [];
        const myOdds = best.np.odds;
        let rank = 1;
        all.forEach(o => {
            if (o.horseId === best.s.horseId) return;
            const ov = _ohOddsAtMs(o.pts, ms);
            if (ov !== null && ov < myOdds) rank++;
        });
        const favTxt = ` · <span class="oh-tip-pop">#${rank} pop</span>`;
        tip.innerHTML = `<div class="oh-tip-time">${_ohJstClock(ms)} JST</div>`
            + `<div class="oh-tip-row"><span class="oh-swatch" style="background:${best.s.color}"></span>`
            + `<span class="oh-tip-pp">${ppTxt}</span>${escapeHtml(best.s.name)} `
            + `<b>${best.np.odds.toFixed(1)}</b>${favTxt}</div>`;
        tip.style.display = '';

        // Anchor the (small) tooltip near the cursor, clamped inside the chart wrap.
        const wrap = svg.parentElement.getBoundingClientRect();
        const px = clientX - wrap.left, py = clientY - wrap.top;
        tip.style.left = Math.min(px + 14, wrap.width - tip.offsetWidth - 8) + 'px';
        tip.style.top = Math.max(8, py - tip.offsetHeight - 10) + 'px';
    };
    const onLeave = () => { cross.style.display = 'none'; tip.style.display = 'none'; _ohSetLineEmphasis(null); };

    rect.addEventListener('mousemove', onMove);
    rect.addEventListener('mouseleave', onLeave);
    rect.addEventListener('touchmove', onMove, { passive: true });
    rect.addEventListener('touchend', onLeave);
}

const OREPRO_URL = 'https://orepro.netkeiba.com/bet/race_list.html';

// --- SETTINGS MODAL ---
async function showSettingsModal() {
    // Populate checkboxes from current config
    if (!appConfig.backend) appConfig.backend = {};
    const currentEngine = String(appConfig.backend?.dataEngine || 'nk').toLowerCase();
    document.getElementById('setting-dataEngine').value = currentEngine === 'jv' ? 'jv' : 'nk';
    document.getElementById('setting-pedigreeLists').checked = appConfig.sidebarTabs?.pedigreeLists ?? true;
    document.getElementById('setting-weekendWatchlist').checked = appConfig.sidebarTabs?.weekendWatchlist ?? true;
    document.getElementById('setting-advancedTools').checked = appConfig.sidebarTabs?.advancedTools ?? false;
    document.getElementById('setting-betSafetyIndicator').checked = appConfig.ui?.betSafetyIndicator ?? true;
    document.getElementById('setting-voteSortingTop').checked = appConfig.ui?.voteSortingTop ?? true;
    document.getElementById('setting-devMode').checked = isDevModeEnabled();
    document.getElementById('setting-debugConsole').checked = isDebugConsoleEnabled();
    document.getElementById('setting-autoLockPastVotes').checked = isAutoLockPastVotesEnabled();
    document.getElementById('setting-autoLockAfterSubmit').checked = isAutoLockAfterSubmitEnabled();
    document.getElementById('setting-cleanPastRaceCards').checked = appConfig.ui?.cleanPastRaceCards ?? true;
    // 🎤 Uma Musume mode — theme state is client-side (localStorage).
    document.getElementById('setting-umamusumeMode').checked = localStorage.getItem(UMM_STORAGE_KEY) === '1';
    document.getElementById('setting-highlightAutoBets').checked = isAutoBetHighlightingEnabled();
    document.getElementById('setting-votingMarkMode').value = getVotingMarkMode();
    document.getElementById('setting-abstainBackupPreset').value = getAbstainBackupPreset();
    { const t = document.getElementById('setting-showEngineDisagreement'); if (t) t.checked = (appConfig.ui?.showEngineDisagreement ?? true); }
    { const t = document.getElementById('setting-sideBetsAuto'); if (t) t.checked = (appConfig.ui?.sideBetsAuto !== false); }
    { const os = document.getElementById('setting-oreproDefaultStake'); if (os) os.value = getOreProDefaultStake(); }
    {
        const auto = detectSuggestedUiScale();
        const override = localStorage.getItem(UI_SCALE_OVERRIDE_KEY);
        const l = document.getElementById('setting-uiScalePercent-val');
        // Screen diagnostics inline (screen.width × devicePixelRatio) — lets a formula-calibration
        // question get answered by reading this label instead of opening devtools (s60).
        const diag = `${window.screen?.width || '?'}px × dpr${(window.devicePixelRatio || 1).toFixed(2)}`;
        if (l) l.textContent = (override !== null ? `${getUiScalePercent()}% (override)` : `${auto}% (auto)`) + ` — ${diag}`;
        const ov = document.getElementById('setting-uiScaleOverride');
        if (ov) ov.value = override !== null ? override : '';
    }
    document.getElementById('setting-showConsole').checked = appConfig.ui?.showConsole ?? true;
    document.getElementById('setting-tvModeSplitPercent').value = Number.isFinite(Number(appConfig.ui?.tvModeSplitPercent))
        ? Number(appConfig.ui?.tvModeSplitPercent)
        : 50;
    document.getElementById('setting-tvModePanelsFlipped').checked = !!appConfig.ui?.tvModePanelsFlipped;
    // Populate formula weight sliders
    const fw = getFormulaWeights();
    document.getElementById('fw-oddsCap').value            = fw.oddsCap;
    document.getElementById('fw-formMultiplier').value     = fw.formMultiplier;
    document.getElementById('fw-freshnessBonus').value     = fw.freshnessBonus;
    document.getElementById('fw-freshnessBreakeven').value = fw.freshnessBreakeven;
    document.getElementById('fw-pedigreeMultiplier').value = fw.pedigreeMultiplier;
    document.getElementById('fw-formWeight').value         = fw.formWeight;
    document.getElementById('fw-sireFitWeight').value      = fw.sireFitWeight;
    document.getElementById('fw-jockeyWeight').value       = fw.jockeyWeight;
    document.getElementById('fw-trainerWeight').value      = fw.trainerWeight;
    syncAllFwSliders();
    renderRaceColumnSettings();
    loadOrchestratorSettings();

    document.getElementById('settings-modal').style.display = 'flex';
}

function closeSettingsModal() {
    document.getElementById('settings-modal').style.display = 'none';
}

// 🎤 Uma Musume mode
// Theme (body.umm-mode class) is managed client-side via localStorage.
// Horse roster add/remove is handled separately from the Character Roster page (umm.html).
// Toggle OFF cleans up any horses that were added via the roster.

const UMM_STORAGE_KEY = 'umanager-umm-theme';

// ── Uma Musume headshot icons ────────────────────────────────────────────────
// Maps a horse name → GameTora circular icon URL, built from the roster's
// standing-portrait paths (chara_stand_<charId>_… → chr_icon_<charId>.png).
// Lets us flag any sire/dam/BMS (or runner) that is an Uma Musume character.
let ummIconByName = null;
function ummNormalizeName(s) { return (s || '').toString().trim().toLowerCase().replace(/\s+/g, ' '); }
function ummIconUrlFromImage(imageUrl) {
    const m = String(imageUrl || '').match(/chara_stand_(\d+)_/);
    return m ? `https://gametora.com/images/umamusume/characters/icons/chr_icon_${m[1]}.png` : null;
}
function ummIconFor(name) {
    if (!ummIconByName) return null;
    return ummIconByName[ummNormalizeName(name)] || null;
}
function ummIconImg(url) {
    return `<img class="umm-uma-icon" src="${escapeHtml(url)}" alt="" loading="lazy" onerror="this.remove()">`;
}
async function loadUmmIconMap() {
    if (ummIconByName) return ummIconByName;
    try {
        const res = await fetch('/api/umamusume/characters');
        if (!res.ok) return (ummIconByName = {});
        const data = await res.json();
        const map = {};
        (data.characters || []).forEach(c => {
            const icon = ummIconUrlFromImage(c.image_url);
            if (!icon) return;
            const key = ummNormalizeName(c.name_en);
            if (key) map[key] = icon;
        });
        ummIconByName = map;
    } catch (e) {
        console.warn('UMM icon map load failed:', e);
        ummIconByName = {};
    }
    return ummIconByName;
}
// DOM pass: add/remove headshots on already-rendered name cells (covers the
// toggle case and first paint that happened before the map finished loading).
function refreshUmmIcons() {
    if (!document.body.classList.contains('umm-mode')) {
        document.querySelectorAll('.umm-uma-icon').forEach(el => el.remove());
        return;
    }
    if (!ummIconByName) return;
    document.querySelectorAll('.name-container').forEach(c => {
        if (c.querySelector('.umm-uma-icon')) return;
        const span = c.querySelector('.name-text');
        if (!span) return;
        const url = ummIconFor(span.textContent);
        if (url) span.insertAdjacentHTML('beforebegin', ummIconImg(url));
    });
}

function applyUmmTheme(enabled) {
    document.body.classList.toggle('umm-mode', enabled);
    // Swap the Pedigree Lists group header emoji
    const summary = document.querySelector('#pedigree-lists-group > summary');
    if (summary) {
        summary.textContent = enabled ? '🎤 Pedigree Lists' : '🎯 Pedigree Lists';
    }
    localStorage.setItem(UMM_STORAGE_KEY, enabled ? '1' : '0');
    // Headshot icons follow the theme.
    if (enabled) loadUmmIconMap().then(refreshUmmIcons);
    else refreshUmmIcons();
}

async function toggleUmamusumeMode(event) {
    const checkbox = event.target;
    const enabling = checkbox.checked;

    // Theme flip is instant — no round-trip needed.
    applyUmmTheme(enabling);

    // Turning OFF: clean up any horses the Character Roster added.
    if (!enabling) {
        checkbox.disabled = true;
        try {
            const resp = await fetch('/api/umamusume/apply', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled: false })
            });
            if (resp.ok) {
                const data = await resp.json();
                if (data.removed > 0) {
                    const listRes = await fetch('/api/lists');
                    listsData = await listRes.json();
                    renderLists();
                    updateRaceHighlighting();
                }
            }
        } catch (err) {
            console.warn('UMM cleanup failed:', err);
        } finally {
            checkbox.disabled = false;
        }
    }
}

function syncFwSlider(el) {
    const valEl = document.getElementById(el.id + '-val');
    if (valEl) valEl.textContent = el.value;
}

function syncAllFwSliders() {
    ['fw-oddsCap','fw-formMultiplier','fw-freshnessBonus','fw-freshnessBreakeven',
     'fw-pedigreeMultiplier','fw-formWeight','fw-sireFitWeight','fw-jockeyWeight','fw-trainerWeight']
    .forEach(id => { const el = document.getElementById(id); if (el) syncFwSlider(el); });
}

function resetFormulaWeights() {
    document.getElementById('fw-oddsCap').value            = 100;
    document.getElementById('fw-formMultiplier').value     = 100;
    document.getElementById('fw-freshnessBonus').value     = 3;
    document.getElementById('fw-freshnessBreakeven').value = 10;
    document.getElementById('fw-pedigreeMultiplier').value = 30;
    document.getElementById('fw-formWeight').value         = 80;
    document.getElementById('fw-sireFitWeight').value      = 10;
    document.getElementById('fw-jockeyWeight').value       = 20;
    document.getElementById('fw-trainerWeight').value      = 20;
    syncAllFwSliders();
    updateSidebarSettings();
}

async function updateSidebarSettings() {
    const previousDataEngine = String(appConfig.backend?.dataEngine || 'nk').toLowerCase();
    // Update config from checkbox values
    appConfig.sidebarTabs = {
        pedigreeLists: document.getElementById('setting-pedigreeLists').checked,
        weekendWatchlist: document.getElementById('setting-weekendWatchlist').checked,
        advancedTools: document.getElementById('setting-advancedTools').checked
    };
    const parseFWInput = (id, def) => { const n = parseFloat(document.getElementById(id).value); return isNaN(n) ? def : n; };
    const parseClampedPercent = (id, def) => {
        const raw = parseFloat(document.getElementById(id).value);
        if (isNaN(raw)) return def;
        return Math.max(20, Math.min(80, raw));
    };
    appConfig.ui = {
        ...appConfig.ui,
        betSafetyIndicator: document.getElementById('setting-betSafetyIndicator').checked,
        voteSortingTop: document.getElementById('setting-voteSortingTop').checked,
        devMode: document.getElementById('setting-devMode').checked,
        debugConsole: document.getElementById('setting-debugConsole').checked,
        autoLockPastVotes: document.getElementById('setting-autoLockPastVotes').checked,
        autoLockAfterSubmit: document.getElementById('setting-autoLockAfterSubmit').checked,
        cleanPastRaceCards: document.getElementById('setting-cleanPastRaceCards').checked,
        showConsole: document.getElementById('setting-showConsole').checked,
        highlightAutoBets: document.getElementById('setting-highlightAutoBets').checked,
        votingMarkMode: (document.getElementById('setting-votingMarkMode').value === 'TRADITIONAL_ROLES') ? 'TRADITIONAL_ROLES' : 'BOX_OPTIMIZATION',
        abstainBackupPreset: (() => { const v = document.getElementById('setting-abstainBackupPreset')?.value; return (v && BET_PRESETS[v]) ? v : 'none'; })(),
        showEngineDisagreement: document.getElementById('setting-showEngineDisagreement') ? !!document.getElementById('setting-showEngineDisagreement').checked : true,
        sideBetsAuto: document.getElementById('setting-sideBetsAuto') ? !!document.getElementById('setting-sideBetsAuto').checked : true,
        oreproDefaultStake: (() => { const v = parseInt(document.getElementById('setting-oreproDefaultStake')?.value, 10); return Number.isFinite(v) && v > 0 ? v : 10000; })(),
        tvModeSplitPercent: parseClampedPercent('setting-tvModeSplitPercent', Number.isFinite(Number(appConfig.ui?.tvModeSplitPercent)) ? Number(appConfig.ui?.tvModeSplitPercent) : 50),
        // uiScalePercent REMOVED (s60) — UI scale is now per-device (localStorage), never synced to
        // the account. It used to live here, which was the root bug: a value tuned on one monitor
        // silently overrode auto-detection on every other device signed into the same account.
        tvModePanelsFlipped: document.getElementById('setting-tvModePanelsFlipped').checked,
        formulaWeights: {
            oddsCap:            parseFWInput('fw-oddsCap',            100),
            formMultiplier:     parseFWInput('fw-formMultiplier',     100),
            freshnessBonus:     parseFWInput('fw-freshnessBonus',       3),
            freshnessBreakeven: parseFWInput('fw-freshnessBreakeven',  10),
            pedigreeMultiplier: parseFWInput('fw-pedigreeMultiplier',  30),
            formWeight:         parseFWInput('fw-formWeight',          80),
            sireFitWeight:      parseFWInput('fw-sireFitWeight',       10),
            jockeyWeight:       parseFWInput('fw-jockeyWeight',         20),
            trainerWeight:      parseFWInput('fw-trainerWeight',        20),
        }
    };
    appConfig.backend = {
        ...appConfig.backend,
        dataEngine: document.getElementById('setting-dataEngine').value === 'jv' ? 'jv' : 'nk'
    };
    
    // Save to server
    await fetch('/api/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(appConfig)
    });

    appendDebugLine(
        `Settings saved: engine=${appConfig.backend?.dataEngine ?? 'nk'}, showConsole=${appConfig.ui?.showConsole ?? true}, ` +
        `debugConsole=${appConfig.ui?.debugConsole ?? false}`
    );
    
    // Apply settings immediately to sidebar
    applyDevModeBodyClass();
    applySidebarSettings();
    updateAllRiskBadges();
    updateAutoBetHighlighting();
    applyRaceTableLayoutSettings();
    renderEnginePicks();
    updateQuickStats();
    refreshSunkCostStat();

    if (previousDataEngine !== appConfig.backend.dataEngine) {
        appendConsoleLine(`[Engine] Switched data engine to ${appConfig.backend.dataEngine.toUpperCase()}. Reloading races...`);
        await refreshDataAndUI();
    }
}

function applyRaceTableLayoutSettings() {
    const cleanPast = appConfig.ui?.cleanPastRaceCards ?? true;
    Object.keys(globalRaceEntries).forEach(r_id => {
        if (!raceSorts[r_id]) {
            raceSorts[r_id] = { col: 'Default', asc: true };
        }

        const thead = document.getElementById(`thead-${r_id}`);
        if (thead) thead.innerHTML = buildTableHeaderRow(r_id);

        const tbody = document.getElementById(`tbody-${r_id}`);
        if (tbody) tbody.innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);

        // Toggle .past-race on the table so the "Clean Past Race Cards" setting takes
        // effect immediately (the class is set inline at render time, so without this
        // the user had to refresh).
        const table = tbody?.parentElement;
        if (table && table.tagName === 'TABLE') {
            const isPast = (globalRaceInfo[r_id]?._timeline === 'past');
            table.classList.toggle('past-race', isPast && cleanPast);
        }

        refreshRaceHeaderSortLabels(r_id);
    });
    updateAutoBetHighlighting();
}

function renderRaceColumnSettings() {
    const container = document.getElementById('setting-race-columns');
    if (!container) return;

    const cols = getRaceColumnsLayout();
    const mob = getMobileColumnVisibility();
    // Header row clarifies the two checkboxes.
    let html = `<div class="setting-column-row setting-column-header">
        <span class="setting-column-label" style="font-weight:600;color:#8ea0c6;">Column</span>
        <span style="font-size:11px;color:#8ea0c6;">🖥️ / 📱</span>
        <div class="setting-column-actions"></div>
    </div>`;
    html += cols.map((c, idx) => {
        const meta = RACE_COLUMN_META[c.key] || { label: c.key };
        const upDisabled = idx === 0 ? 'disabled' : '';
        const downDisabled = idx === cols.length - 1 ? 'disabled' : '';
        return `<div class="setting-column-row">
            <label class="setting-column-label">
                <input type="checkbox" ${c.visible ? 'checked' : ''} onchange="toggleRaceColumnVisibility('${c.key}', this.checked)" title="Visible on desktop">
                <span>${meta.label}</span>
            </label>
            <label title="Visible on mobile (phone width)">
                <input type="checkbox" ${mob[c.key] ? 'checked' : ''} onchange="toggleMobileColumnVisibility('${c.key}', this.checked)">
            </label>
            <div class="setting-column-actions">
                <button type="button" ${upDisabled} onclick="moveRaceColumn('${c.key}', -1)">↑</button>
                <button type="button" ${downDisabled} onclick="moveRaceColumn('${c.key}', 1)">↓</button>
            </div>
        </div>`;
    }).join('');
    container.innerHTML = html;
}

async function toggleMobileColumnVisibility(colKey, visible) {
    const mob = { ...getMobileColumnVisibility() };
    if (!visible) {
        const visibleCount = Object.values(mob).filter(v => v).length;
        if (visibleCount <= 1 && mob[colKey]) {
            alert('At least one race column must remain visible on mobile.');
            renderRaceColumnSettings();
            return;
        }
    }
    mob[colKey] = visible;
    appConfig.ui.raceTableMobileVisibility = mob;
    renderRaceColumnSettings();
    await updateSidebarSettings();
    // Re-render so the change shows up immediately if currently on mobile.
    if (isMobileViewport()) rerenderAllRaceTables();
}

async function moveRaceColumn(colKey, direction) {
    const cols = [...getRaceColumnsLayout()];
    const idx = cols.findIndex(c => c.key === colKey);
    if (idx < 0) return;

    const newIdx = idx + direction;
    if (newIdx < 0 || newIdx >= cols.length) return;

    const tmp = cols[idx];
    cols[idx] = cols[newIdx];
    cols[newIdx] = tmp;

    appConfig.ui.raceTableColumns = cols;
    renderRaceColumnSettings();
    await updateSidebarSettings();
}

async function toggleRaceColumnVisibility(colKey, visible) {
    const cols = getRaceColumnsLayout().map(c => ({ ...c }));
    const target = cols.find(c => c.key === colKey);
    if (!target) return;

    if (!visible) {
        const visibleCount = cols.filter(c => c.visible).length;
        if (visibleCount <= 1 && target.visible) {
            alert('At least one race column must remain visible.');
            renderRaceColumnSettings();
            return;
        }
    }

    target.visible = visible;
    appConfig.ui.raceTableColumns = cols;
    renderRaceColumnSettings();
    await updateSidebarSettings();
    if (!isMobileViewport()) rerenderAllRaceTables();
}

// ── UI scale (per-device, auto — rebuilt s60, no manual slider) ──────────────
// "Everything's too big" on a lower-res monitor = the page renders larger relative to the screen. A
// CSS `zoom` on the root shrinks the whole UI uniformly AND reflows it (so responsive breakpoints get
// more room — unlike transform:scale).
//
// WIDTH-primary (s61 fix). The prior version drove the correction off DPR alone
// (`100 − (dpr−1)×60`). That broke on the operator's daily 2K monitor: the laptop (1080px wide) and
// the 2K monitor (2560px wide) are BOTH set to 125% OS scaling, so they share the identical dpr of
// 1.25 — DPR literally cannot tell a cramped laptop from a spacious 2K screen, and it handed both the
// same 85% (which also tripped the compact-column drop, hiding J%/T% on the roomy monitor). The two
// live data points have the SAME dpr but very different widths, so screen WIDTH is the only signal
// that separates them. Calibrated to real data: 1080px → 85% (laptop, felt right), and anything QHD/2K
// and up (≥1800px) → 100% (plenty of room, no shrink, keep every column). Linear between; clamped.
// (Note: an even-earlier version DID use width but combined it multiplicatively with the dpr signal
// and overshot to the 65% floor — this uses width ALONE, no compounding, so no double-correction.)
//
// PER-DEVICE, not synced to the account (localStorage, not appConfig.ui) — the old bug: uiScalePercent
// lived in the backend-synced settings, so a value tuned for one monitor silently overrode every other
// device signed into the same account, including screens that had never been auto-detected at all.
const UI_SCALE_OVERRIDE_KEY = 'umanager-ui-scale-override'; // dev-only manual override, this device only
function detectSuggestedUiScale() {
    const w = window.screen?.width || 0;
    if (!w) return 100;                 // no usable width signal → leave the app alone
    if (w >= 1800) return 100;          // QHD/2K and wider: plenty of room, never shrink
    // Small/laptop-class widths: gentle shrink so dense tables fit. Anchored to live data
    // (1080px → 85%) and 1800px → 100%, linear between.
    const raw = 85 + (w - 1080) * (100 - 85) / (1800 - 1080);
    return Math.max(65, Math.min(100, Math.round(raw / 5) * 5));
}
function getUiScalePercent() {
    const override = localStorage.getItem(UI_SCALE_OVERRIDE_KEY);
    if (override !== null) {
        const v = parseInt(override, 10);
        if (Number.isFinite(v) && v >= 50 && v <= 130) return v;
    }
    return detectSuggestedUiScale();
}
// Apply both `zoom` (shrinks + reflows) and `--ui-zoom` (lets the body height re-expand to fill the
// viewport — see the body rule in style.css; without it a <100% zoom leaves an empty band at the bottom).
function setRootZoom(z) {
    const el = document.documentElement;
    el.style.zoom = z.toString();
    el.style.setProperty('--ui-zoom', z.toString());
}
function applyUiScale() {
    try { setRootZoom(getUiScalePercent() / 100); } catch (_) {}
}
// Dev-only manual override (Settings → dev mode, #setting-uiScaleOverride) — for a monitor the auto
// heuristic gets wrong. Empty field = clear the override and go back to auto for this device.
function onUiScaleOverrideChange(val) {
    const raw = String(val || '').trim();
    if (!raw) localStorage.removeItem(UI_SCALE_OVERRIDE_KEY);
    else localStorage.setItem(UI_SCALE_OVERRIDE_KEY, String(Math.max(50, Math.min(130, parseInt(raw, 10) || 100))));
    applyUiScale();
    const l = document.getElementById('setting-uiScalePercent-val');
    const diag = `${window.screen?.width || '?'}px × dpr${(window.devicePixelRatio || 1).toFixed(2)}`;
    if (l) l.textContent = (localStorage.getItem(UI_SCALE_OVERRIDE_KEY) !== null
        ? `${getUiScalePercent()}% (override)` : `${detectSuggestedUiScale()}% (auto)`) + ` — ${diag}`;
}

function applySidebarSettings() {
    applyUiScale();   // keep the whole-page scale in sync on init + every settings apply
    const tabs = appConfig.sidebarTabs || {};
    const apply = (elemId, key, defaultOpen) => {
        const el = document.getElementById(elemId);
        if (el) el.open = tabs[key] ?? defaultOpen;
    };
    apply('pedigree-lists-group',     'pedigreeLists',     true);
    apply('weekend-watchlist-group',  'weekendWatchlist',  true);
    apply('advanced-tools-group',     'advancedTools',     false);

    const consoleEl = document.getElementById('scrape-console');
    if (consoleEl) {
        consoleEl.style.display = ((appConfig.ui?.showConsole ?? false) && isDevModeEnabled()) ? 'block' : 'none';
    }
    appendDebugLine('Sidebar settings applied');

    // Keep lock behavior and header controls in sync when settings change.
    Object.keys(globalRaceInfo).forEach(r_id => {
        const timeline = globalRaceInfo[r_id]?._timeline || 'upcoming';
        if (isAutoLockPastVotesEnabled() && timeline === 'past') {
            raceLocks[r_id] = true;
        }
        updateRaceActionButtons(r_id);
    });
}

// --- RACE NAME LOCALIZER ---

// Returns a generic English label for races where NameJa is blank (JRA unnamed non-stakes).
// JRA never stores a display label for these; the class is the only identity.
function localizeRaceClass(rc) {
    if (!rc) return '';
    const m = { debut: 'Debut', maiden: 'Maiden', '1win': '1-Win Class', '2win': '2-Win Class', '3win': '3-Win Class', open: 'Open' };
    return m[rc] || '';
}

function localizeRaceName(name) {
    if (!name) return "";
    let cleanName = name;

    // Phase 21: Check translation dictionary first (for Japanese kanji names)
    // 1. Try stakes races (priority 1)
    if (raceNameDict.stakes && raceNameDict.stakes[name]) {
        return raceNameDict.stakes[name];
    }

    // 2. Try class names (priority 2)
    if (raceNameDict.classNames && raceNameDict.classNames[name]) {
        return raceNameDict.classNames[name];
    }

    // 3. Try special/regional races (priority 3) — expanded Phase 21 dictionary
    if (raceNameDict.specialRaces && raceNameDict.specialRaces[name]) {
        return raceNameDict.specialRaces[name];
    }

    // 4. Fall back to romanized string translations (original logic for romanized inputs)
    // Translate Ages (e.g., "4 Toshi Ijou" -> "4yo+", "3 Toshi" -> "3yo")
    cleanName = cleanName.replace(/(\d+)\s*Toshi\s*Ijou/ig, "$1yo+");
    cleanName = cleanName.replace(/(\d+)\s*Toshi/ig, "$1yo");

    // Translate Classes
    cleanName = cleanName.replace(/Mishouri/ig, "Maiden");
    cleanName = cleanName.replace(/Shinba/ig, "Newcomer");
    cleanName = cleanName.replace(/1 Kachi Kurasu/ig, "ALW (1 Win)");
    cleanName = cleanName.replace(/2 Kachi Kurasu/ig, "ALW (2 Wins)");
    cleanName = cleanName.replace(/3 Kachi Kurasu/ig, "ALW (3 Wins)");
    cleanName = cleanName.replace(/Hanshin Supuringu J/ig, "Hanshin Spring Jump");

    // Jump Races
    cleanName = cleanName.replace(/Shougai/ig, "Jump");

    return cleanName;
}

// ==========================================
// --- HORSE SEARCH ENGINE ---
// ==========================================

let searchDebounceTimer = null;
const SEARCH_DEBOUNCE_MS = 150;  // Wait 150ms after user stops typing

function handleSearchInput() {
    // Clear the previous debounce timer
    if (searchDebounceTimer) {
        clearTimeout(searchDebounceTimer);
    }
    
    // Wait 150ms before filtering (debounce rapid keystrokes)
    searchDebounceTimer = setTimeout(() => {
        performSearch();
    }, SEARCH_DEBOUNCE_MS);
}

function performSearch() {
    const val = document.getElementById('horse-search').value.toLowerCase();
    const box = document.getElementById('search-suggestions');
    currentSearchSelection = -1;

    if (!val) { box.style.display = 'none'; return; }

    const matches = searchableHorses.filter(h => h.name.toLowerCase().includes(val));

    if (matches.length === 0) {
        box.innerHTML = '<div class="suggestion-item" style="color:#888;">No matches found</div>';
        box.style.display = 'block';
        return;
    }

    let html = '';
    matches.slice(0, 10).forEach((m, idx) => {
        html += `<div class="suggestion-item" id="sugg-${idx}" onclick="jumpToHorse('${m.date}', '${m.r_id}', '${m.h_id}', '${m.timeline || "upcoming"}')">
            <strong>${m.name}</strong> <span style="color:#888; font-size:11px;">(${m.track} R${m.r_num})</span>
        </div>`;
    });
    box.innerHTML = html;
    box.style.display = 'block';
}

function handleSearchKey(e) {
    const box = document.getElementById('search-suggestions');
    if (box.style.display === 'none') return;
    
    const items = box.querySelectorAll('.suggestion-item');
    if (items.length === 0 || items[0].innerText.includes('No matches')) return;

    if (e.key === 'ArrowDown') {
        e.preventDefault();
        currentSearchSelection = (currentSearchSelection + 1) % items.length;
        updateSearchSelection(items);
    } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        currentSearchSelection = (currentSearchSelection - 1 + items.length) % items.length;
        updateSearchSelection(items);
    } else if (e.key === 'Enter') {
        e.preventDefault();
        const targetIdx = currentSearchSelection > -1 ? currentSearchSelection : 0;
        items[targetIdx].click();
    }
}

function updateSearchSelection(items) {
    items.forEach((item, idx) => {
        if (idx === currentSearchSelection) item.classList.add('active');
        else item.classList.remove('active');
    });
}

function jumpToHorse(date, r_id, h_id, timeline = null) {
    document.getElementById('search-suggestions').style.display = 'none';
    document.getElementById('horse-search').value = '';

    // 1. Activate the correct day in the calendar-backed schedule.
    switchMainTab(date);

    // 2. Expand the specific race if it is collapsed
    const content = document.getElementById(`content-${r_id}`);
    const header = document.getElementById(`header-${r_id}`);
    const arrow = document.getElementById(`arrow-${r_id}`);

    if (content && content.classList.contains('collapsed')) {
        content.classList.remove('collapsed');
        if (header) header.classList.remove('collapsed');
        if (arrow) arrow.innerText = '▼';
    }

    // 3. Scroll to the horse and flash green!
    setTimeout(() => {
        const rowEl = document.getElementById(`row-${r_id}-${h_id}`);
        if (rowEl) {
            rowEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
            
            // Reset animation instantly if jumping to the same horse twice
            rowEl.classList.remove('highlight-row');
            void rowEl.offsetWidth; 
            rowEl.classList.add('highlight-row');
        }
    }, 100); // 100ms delay ensures the DOM expands the collapsed race first
}

// ==========================================
// --- JUMP TO RACE FEATURE ---
// ==========================================

// Hide search dropdowns if the user clicks anywhere else on the screen
document.addEventListener('click', function(e) {
    const box = document.getElementById('search-suggestions');
    const input = document.getElementById('horse-search');
    if (box && input && e.target !== box && e.target !== input) {
        box.style.display = 'none';
    }

    const listBox = document.getElementById('list-add-suggestions');
    const listInput = document.getElementById('list-add-search');
    if (listBox && listInput && !listBox.contains(e.target) && e.target !== listInput) {
        listBox.style.display = 'none';
    }
});

init();

// --- LIVE PIPELINE (SignalR) ---
// Connects to /hubs/live; the server broadcasts OddsUpdated and ResultsUpdated
// every time the LiveOrchestrator's polling tick lands fresh JV-Link data.
// We patch the affected <td>s in place — no /api/races refetch, no scroll jump.
function findRaceById(raceId) {
    if (!raceId) return null;
    const target = String(raceId);
    for (const list of Object.values(globalRacesByDate || {})) {
        if (!Array.isArray(list)) continue;
        for (const r of list) {
            if (String(r?.info?.race_id || '') === target) return r;
        }
    }
    return null;
}

// Re-renders the dynamic portion of a race's <h3>: status emoji (🕒/⌛/🏁)
// and the win-badge pills (◎ Win, Q Box, T Box). Header time/track stay static;
// only the bits that depend on finish data refresh.
function refreshRaceHeaderMeta(raceId) {
    const meta = document.getElementById(`header-meta-${raceId}`);
    if (!meta) return;
    const race = findRaceById(raceId);
    if (!race) return;
    const info = race.info || {};
    const localName = localizeRaceName(info.race_name) || localizeRaceClass(info.race_class);
    const winBadgesHtml = buildRaceWinBadgesHtml(race);
    meta.innerHTML = `${raceStatusEmoji(race)} ${info.time} | ${trackName(info.place)} R${info.race_number}: ${localName}${raceSurfaceDistChip(info)} ${winBadgesHtml}`;
}

// Patches the payout table into BOTH in-memory race representations — globalRaceInfo[raceId] (a
// separate shallow snapshot taken at render time, line ~5958: `{ ...race.info, _timeline }`), used
// by the home tab's updateQuickStats, and the nested race.info found via findRaceById (the
// globalRacesByDate tree), used by the race-header badge. They are NOT the same object, so patching
// only one leaves the other permanently reading stale/empty payouts. s60 fix — see
// LiveBroadcastService.BroadcastResultsAsync, which now actually sends this.
function patchRaceResultsJson(raceId, resultsJson) {
    if (!raceId || resultsJson == null) return;
    if (globalRaceInfo[raceId]) globalRaceInfo[raceId].results_json = resultsJson;
    const race = findRaceById(raceId);
    if (race?.info) race.info.results_json = resultsJson;
}

function patchRaceEntries(raceId, entries, fields) {
    if (!raceId || !Array.isArray(entries)) return;
    const race = findRaceById(raceId);
    const inMemEntries = Array.isArray(race?.entries) ? race.entries : null;

    entries.forEach(e => {
        const row = document.getElementById(`row-${raceId}-${e.horseId}`);
        if (row) {
            fields.forEach(f => {
                const cell = row.querySelector(`td[data-cell="${f}"]`);
                if (!cell) return;
                if (f === 'finish') {
                    const n = Number(e.finish);
                    const shown = (Number.isFinite(n) && n > 0) ? n : '';
                    cell.textContent = shown;
                    cell.className = `finish-pos finish-pos-${shown}`;
                } else if (f === 'odds') {
                    const cur = parseFloat(e.odds), prev = parseFloat(e.prevOdds);
                    let delta = '';
                    if (!isNaN(prev) && prev > 0 && !isNaN(cur) && cur > 0 && Math.abs(cur - prev) >= 0.2) {
                        delta = cur < prev
                            ? `<span class="odds-short" title="Shortened from ${prev.toFixed(1)}">↑</span>`
                            : `<span class="odds-drift" title="Drifted from ${prev.toFixed(1)}">↓</span>`;
                    }
                    cell.innerHTML = (e.odds || '') + delta;
                } else if (f === 'fav') {
                    cell.textContent = e.fav || '';
                }
            });
        }

        // Mirror into the in-memory race object so header re-render (and any
        // future evaluateTemplateOutcome call) sees the fresh finish/odds/fav values.
        if (inMemEntries) {
            const memRow = inMemEntries.find(x => String(x.Horse_ID ?? '').split('.')[0] === String(e.horseId));
            if (memRow) {
                if (fields.includes('finish')) memRow.Finish = (Number(e.finish) > 0) ? String(e.finish) : '';
                if (fields.includes('odds')) {
                    memRow.Prev_Odds = e.prevOdds ?? '';
                    memRow.Odds      = e.odds ?? '';
                }
                if (fields.includes('fav'))    memRow.Fav    = e.fav ?? '';
            }
        }
    });

    refreshRaceHeaderMeta(raceId);
}

function startLiveHub() {
    if (typeof signalR === 'undefined') {
        console.warn('[LiveHub] signalR client not loaded — skipping.');
        return;
    }
    const conn = new signalR.HubConnectionBuilder()
        .withUrl('/hubs/live')
        .withAutomaticReconnect()
        .build();

    conn.on('OddsUpdated', payload => {
        if (!payload) return;
        patchRaceEntries(payload.raceId, payload.entries, ['odds', 'fav']);
        liveUpdateOddsHistory(payload);   // live-extend the trend chart if it's open for this race
    });

    conn.on('ResultsUpdated', payload => {
        if (!payload) return;
        // Payout table FIRST — patchRaceEntries below re-renders the header (and thus scores the
        // bet) as its last step, so results_json must already be in place before that happens.
        patchRaceResultsJson(payload.raceId, payload.resultsJson);
        patchRaceEntries(payload.raceId, payload.entries, ['finish']);
        refreshSunkCostStat(); // Voting-tab all-time net (server-derived)
        updateQuickStats();    // Home-tab Day Net — s60 fix: this was never called from here before,
                                // so a live win never moved the home tab no matter how long you waited.
    });

    conn.start()
        .then(() => console.log('[LiveHub] connected to /hubs/live'))
        .catch(err => console.warn('[LiveHub] connect failed', err));
}

startLiveHub();

// --- PHASE 6: ORCHESTRATOR SETTINGS ---
async function loadOrchestratorSettings() {
    try {
        const res = await fetch('/api/settings');
        if (!res.ok) return;
        const data = await res.json();
        const s = data.settings || {};
        const set = (id, val) => { const el = document.getElementById(id); if (el) el.value = val ?? ''; };
        set('setting-populate-poll-interval',     s.populate_poll_interval);
        set('setting-odds-poll-interval-prelive', s.odds_poll_interval_prelive);
        set('setting-odds-poll-interval-live',    s.odds_poll_interval_live);
        set('setting-live-window-minutes',        s.live_window_minutes);
        set('setting-discord-webhook-url',        s.discord_webhook_url);
        set('setting-discord-alert-webhook-url',  s.discord_alert_webhook_url);
        set('setting-orepro-session-cookie',      s.orepro_session_cookie);
        set('setting-orepro-login-id',            s.orepro_login_id);
        set('setting-orepro-password',            s.orepro_password);
        set('setting-orepro-user-agent',          s.orepro_user_agent);
        // Per-leg stakes; fall back to the legacy single-stake setting then ¥100.
        const legacyStake = s.bet_estimate_stake_yen ?? '100';
        set('setting-betStakeWin',      s.bet_stake_win_yen      ?? legacyStake);
        set('setting-betStakeQuinella', s.bet_stake_quinella_yen ?? legacyStake);
        set('setting-betStakeTrio',     s.bet_stake_trio_yen     ?? legacyStake);

        // Checkbox for "navigate to bet_complete.html after submit"
        const navCb = document.getElementById('setting-orepro-nav-to-complete');
        if (navCb) navCb.checked = String(s.orepro_nav_to_complete_after_submit || 'false').toLowerCase() === 'true';

        // Checkbox for "display in local timezone"
        const tzCb = document.getElementById('setting-display-local-time');
        if (tzCb) tzCb.checked = String(s.display_local_time || 'false').toLowerCase() === 'true';

        // Keep the lightweight global cache in sync so the apply flow sees fresh values.
        globalOreProSettings = s;
        globalDisplayLocalTime = String(s.display_local_time || 'false').toLowerCase() === 'true';
    } catch (e) {
        console.warn('[Orchestrator] loadSettings failed', e);
    }
}

// Server-side OrePro login: sends the stored (or just-typed) credentials to the Nexus, which does
// the netkeiba login handshake and re-mints the session cookie — no browser cookie-copying needed,
// works from a phone. The Nexus verifies the new session before reporting success.
// Reveal/hide the OrePro password field so the operator can eyeball it for typos (the #1 cause of
// a "netkeiba says incorrect" login failure). Also flips the login-id to plain text alongside.
function toggleOreProPasswordVisible(btn) {
    const pw = document.getElementById('setting-orepro-password');
    if (!pw) return;
    const show = pw.type === 'password';
    pw.type = show ? 'text' : 'password';
    if (btn) btn.textContent = show ? '🙈 Hide' : '👁 Show';
}

async function oreProLoginNow() {
    const btn = document.getElementById('orepro-login-btn');
    const statusEl = document.getElementById('orepro-login-status');
    const id = (document.getElementById('setting-orepro-login-id') || {}).value || '';
    const pw = (document.getElementById('setting-orepro-password') || {}).value || '';
    if (!id.trim() || !pw) {
        if (statusEl) { statusEl.style.color = '#e08060'; statusEl.textContent = 'Enter your login id and password first.'; }
        return;
    }
    // Persist the credentials first so auto-refresh can reuse them later.
    await saveOrchestratorSetting('orepro_login_id', id);
    await saveOrchestratorSetting('orepro_password', pw);

    const prev = btn ? btn.textContent : '';
    if (btn) { btn.disabled = true; btn.textContent = '⏳ Logging in…'; }
    if (statusEl) { statusEl.style.color = '#9ab'; statusEl.textContent = 'Contacting OrePro…'; }
    try {
        const res = await fetch('/api/orepro/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ race_id: firstRaceIdForActiveDay() }),
        });
        const data = await res.json().catch(() => ({ loggedIn: false, message: 'Bad response from server.' }));
        if (statusEl) {
            statusEl.style.color = data.loggedIn ? '#3ddc84' : '#e08060';
            statusEl.textContent = (data.loggedIn ? '✅ ' : '⚠️ ') + (data.message || (data.loggedIn ? 'Logged in.' : 'Login failed.'));
        }
        // Refresh the cookie field to reflect the freshly-minted session.
        if (data.loggedIn) loadOrchestratorSettings();
    } catch (e) {
        if (statusEl) { statusEl.style.color = '#e08060'; statusEl.textContent = 'Login request failed: ' + e.message; }
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = prev; }
    }
}

async function toggleDisplayLocalTime(enabled) {
    globalDisplayLocalTime = !!enabled;
    await saveOrchestratorSetting('display_local_time', enabled ? 'true' : 'false');
    // Reload races so display strings update immediately. Cheap relative to a refresh.
    try { await loadRaces(); } catch (_) { /* ignore — operator can refresh manually */ }
}

async function testOreProCookie() {
    const cookie = (document.getElementById('setting-orepro-session-cookie') || {}).value || '';
    const ua     = (document.getElementById('setting-orepro-user-agent')     || {}).value || '';
    await saveOrchestratorSetting('orepro_session_cookie', cookie);
    if (ua) await saveOrchestratorSetting('orepro_user_agent', ua);
    // Real login probe (not just "is a cookie configured"). Pass the loaded day's first race so
    // the backend can check against a real shutuba page — the reliable signal.
    try {
        const data = await checkOreProCookieLoggedIn(firstRaceIdForActiveDay());
        alert(`${data.loggedIn ? '✅ Logged in' : '⚠️ ' + data.status}: ${data.message}`);
    } catch (e) {
        alert(`OrePro probe failed: ${e.message}`);
    }
}

// Pre-flight cookie probe shared by the Settings button and Apply Day Votes. Returns the
// backend { status, loggedIn, message } object (throws only on a network/parse failure).
async function checkOreProCookieLoggedIn(raceId) {
    const res = await fetch('/api/orepro/cookie-check', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ race_id: String(raceId || '') })
    });
    return await res.json();
}

function firstRaceIdForActiveDay() {
    const date = String(currentActiveDate || '').trim();
    const races = Array.isArray(globalRacesByDate?.[date]) ? globalRacesByDate[date] : [];
    for (const r of races) {
        const id = String(r?.info?.race_id || '').trim();
        if (id) return id;
    }
    return '';
}

async function saveBetStake(leg, value) {
    const n = parseInt(value, 10);
    const clean = (Number.isFinite(n) && n >= 100) ? String(n) : '100';
    const keyByLeg = {
        win:      'bet_stake_win_yen',
        quinella: 'bet_stake_quinella_yen',
        trio:     'bet_stake_trio_yen'
    };
    const key = keyByLeg[leg];
    if (!key) return;
    await saveOrchestratorSetting(key, clean);
    // Keep the local cache in sync so chips re-render with the new stake without a reload.
    globalOreProSettings = { ...(globalOreProSettings || {}), [key]: clean };
    try { await reEstimateActiveDay(); } catch (_) { /* fine */ }
    // Re-render past-race hit chips (they read stake at render time).
    if (typeof rerenderAllRaceTables === 'function') rerenderAllRaceTables();
}

async function saveOrchestratorSetting(key, value) {
    try {
        const res = await fetch(`/api/settings/${encodeURIComponent(key)}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ value: String(value ?? '') })
        });
        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            alert(`Failed to save ${key}: ${err.error || res.statusText}`);
        }
    } catch (e) {
        console.warn('[Orchestrator] saveSetting failed', e);
    }
}

async function testDiscordWebhook() {
    // Saves current URL field then asks the orchestrator to force-tick — any
    // configured webhook receives a quick "phase change" probe if we're already
    // in steady state, so use the dedicated probe endpoint instead.
    const url = (document.getElementById('setting-discord-webhook-url') || {}).value || '';
    await saveOrchestratorSetting('discord_webhook_url', url);
    try {
        const res = await fetch('/api/settings/discord/test', { method: 'POST' });
        const data = await res.json();
        if (res.ok) alert('Discord probe sent. Check the channel.');
        else        alert(`Discord probe failed: ${data.error || res.statusText}`);
    } catch (e) {
        alert(`Discord probe failed: ${e.message}`);
    }
}

async function forceOrchestratorTick() {
    try {
        const res = await fetch('/api/orchestrator/force-tick', { method: 'POST' });
        if (res.ok) console.log('[Orchestrator] force-tick requested');
        else        alert('Force-tick failed');
    } catch (e) {
        alert(`Force-tick failed: ${e.message}`);
    }
}