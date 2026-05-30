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
let sidebarRaceCollapseState = {};
let raceSorts = {}; // Per-race sort state — always kept in sync with globalSort.
let globalSort = { col: 'Default', asc: true }; // Single source of truth; all races mirror this.
let winningVotesFocusEnabled = false;
let searchableHorses = []; // Stores the database for the search bar
let currentSearchSelection = -1; // Tracks keyboard navigation in the dropdown
let appConfig = {}; // NEW: Stores app configuration
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
    return layout.filter(c => c.visible).map(c => c.key);
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

// Slider 0-100 → SAFE (<40), CHAOS (>60), BLEND (40-60).
// BLEND defers to BOX_OPTIMIZATION for divergent role assignments.
function riskZone(riskValue) {
    if (riskValue < 40) return 'SAFE';
    if (riskValue > 60) return 'CHAOS';
    return 'BLEND';
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
    const jstOpts = {hour: '2-digit', minute:'2-digit', second:'2-digit', hour12: false, timeZone: 'Asia/Tokyo'};
    const cstOpts = {hour: '2-digit', minute:'2-digit', second:'2-digit', hour12: true, timeZone: 'America/Chicago'};
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
        badge.classList.remove('phase-waiting', 'phase-posts', 'phase-upcoming', 'phase-live');
        const labelEl = badge.querySelector('.phase-badge-label');

        if (phase === 'LIVE_OPERATIONS') {
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
        _updateTickCountdown();
    } catch { /* silently ignore network errors */ }
}

const _phaseBadgeState = { eta: null, phase: '' };
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
    const { eta, phase } = _phaseBadgeState;
    if (!eta) {
        sub.textContent = phase === 'WAITING_FOR_RACES' ? 'No upcoming races'
                        : phase === 'AWAITING_POSTS'   ? 'Draw pending'
                        : '';
        return;
    }
    const diffMs = eta - Date.now();
    const action = _tickActionLabel[phase] || 'tick';
    if (diffMs <= 0) {
        sub.textContent = `${action} imminent`;
        return;
    }
    const mins = Math.floor(diffMs / 60000);
    const secs = Math.floor((diffMs % 60000) / 1000);
    const timeStr = mins > 0 ? `${mins}m ${String(secs).padStart(2, '0')}s` : `${secs}s`;
    sub.textContent = `Next ${action}: ${timeStr}`;
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

function updateQuickStats() {
    const qsMarks = document.getElementById('qs-marks');
    const qsMarksDetail = document.getElementById('qs-marks-detail');
    const qsAgreement = document.getElementById('qs-agreement');
    const qsAgreementSub = document.getElementById('qs-agreement-sub');
    const qsPL = document.getElementById('qs-pl');
    const qsPLSub = document.getElementById('qs-pl-sub');
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

    // Day P/L: sum across races whose results we have. Spend uses same model as
    // buildRaceWinBadgesHtml — 1 Win + C(marks,2) Q + C(marks,3) T per race.
    const legacyStake = parseFloat(globalOreProSettings?.bet_estimate_stake_yen) || 100;
    const winStake = parseFloat(globalOreProSettings?.bet_stake_win_yen)      || legacyStake;
    const qStake   = parseFloat(globalOreProSettings?.bet_stake_quinella_yen) || legacyStake;
    const tStake   = parseFloat(globalOreProSettings?.bet_stake_trio_yen)     || legacyStake;
    let wonTotal = 0;
    let spentTotal = 0;
    let racesGraded = 0;
    dateRaceIds.forEach(r_id => {
        const race = { info: globalRaceInfo[r_id], entries: globalRaceEntries[r_id] || [] };
        const recap = evaluateRaceRecap(race);
        if (!recap.hasCompleteTop3) return;
        racesGraded++;
        const payouts = lookupRacePayouts(race, recap.ppByRank);
        const marksCount = Object.keys(collectRaceMainMarks(r_id) || {}).length;
        if (marksCount === 0) return;  // user didn't bet this race
        const qCombos = marksCount >= 2 ? marksCount * (marksCount - 1) / 2 : 0;
        const tCombos = marksCount >= 3 ? marksCount * (marksCount - 1) * (marksCount - 2) / 6 : 0;
        spentTotal += winStake + (qCombos * qStake) + (tCombos * tStake);
        if (recap.honmeiHit)   wonTotal += payouts.win      * winStake / 100;
        if (recap.quinellaHit) wonTotal += payouts.quinella * qStake   / 100;
        if (recap.trioHit)     wonTotal += payouts.trio     * tStake   / 100;
    });
    if (racesGraded === 0 || spentTotal === 0) {
        qsPL.textContent = '—';
        qsPLSub.textContent = 'no results yet';
        qsPL.classList.remove('quick-stat-pos', 'quick-stat-neg');
    } else {
        const net = Math.round(wonTotal - spentTotal);
        const sign = net >= 0 ? '+' : '−';
        qsPL.textContent = `${sign}¥${Math.abs(net).toLocaleString()}`;
        qsPLSub.textContent = `¥${Math.round(spentTotal).toLocaleString()} staked`;
        qsPL.classList.toggle('quick-stat-pos', net >= 0);
        qsPL.classList.toggle('quick-stat-neg', net < 0);
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
        container.innerHTML = '<div class="ww-empty">Add horses to your Watchlist to see them here.</div>';
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
        container.innerHTML = `<div class="ww-empty">No watchlist horses running on ${activeDate}.</div>`;
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

// ==========================================
// --- LIST MANAGEMENT & UI REFRESH SUITE ---
// ==========================================

async function refreshDataAndUI() {
    // 1. Save scroll position so the screen doesn't jump
    const scrollY = window.scrollY;
    
    // 2. Refresh the Grid & Weekend Watchlist (must load races FIRST to populate searchableHorses)
    await loadRaces();
    
    // 3. Refresh the Sidebar Lists (needs searchableHorses populated)
    const listRes = await fetch('/api/lists');
    listsData = await listRes.json();
    renderLists();
    // loadRaces() called these before listsData was populated, so re-render now.
    renderWeekendWatchlist();
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
    const marksRes = await fetch('/api/marks');
    const marksPayload = normalizeMarksPayload(await marksRes.json());
    globalMarks = marksPayload.marks;
    globalRaceMeta = marksPayload.raceMeta;
    globalMarksVersion = marksPayload.version;

    // NEW: Load config file
    const configRes = await fetch('/api/config');
    appConfig = await configRes.json();
    // Migrate jockeyWeight from old default 40 → 20 (A/E shrinkage fix).
    // Only fires if the user never manually changed it away from the old default.
    if (appConfig.ui?.formulaWeights?.jockeyWeight === 40) {
        appConfig.ui.formulaWeights.jockeyWeight = 20;
    }
    applyDevModeBodyClass();
    // Restore UMM theme from localStorage (theme is client-side; no round-trip needed).
    applyUmmTheme(localStorage.getItem(UMM_STORAGE_KEY) === '1');
    relocateSearchBar();

    // Phase 21: Load race name translation dictionary
    try {
        const dictRes = await fetch('/static/race_name_dict.json');
        raceNameDict = await dictRes.json();
    } catch (e) {
        console.warn('Failed to load race_name_dict.json:', e);
        raceNameDict = { stakes: {}, classNames: {} };
    }

    // Load OrePro per-race apply state so the Apply button can reflect history.
    await loadOreProApplyState();
    // Load OrePro behavior settings (e.g. navigate-to-receipt-after-submit).
    await loadOreProSettingsLite();

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
    
    // NEW: Apply sidebar settings
    applySidebarSettings();
    
    await refreshDataAndUI();
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

// --- ADD / REMOVE / SNIPE ACTIONS ---

async function quickAdd(id, listType) {
    const res = await fetch('/api/snipe', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({id: id, list_type: listType})
    });
    const data = await res.json();
    
    // If successful, refresh only the sidebar lists (keep scroll position)
    if(data.status === "success") await refreshListsOnly();
    else alert(data.message);
}

async function quickAddFromHover(id, listType, nameEncoded) {
    // Symmetric with removeHorseFromHover: edit listsData locally + POST /api/lists.
    // /api/snipe is a stub that returns { status: "not_implemented" } with no
    // message field, so the old fetch path produced "alert(undefined)".
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
    
    return `
    <div class="name-container">
        <span class="${nameClass}">${escapedName}</span>
        <div class="hover-menu">
            ${btnHtml}
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
        
        // If all 4 are cast, forcefully collapse the UI for this race!
        if (usedCount >= 4) {
            const content = document.getElementById(`content-${r_id}`);
            const header = document.getElementById(`header-${r_id}`);
            const arrow = document.getElementById(`arrow-${r_id}`);
            
            if (content && !content.classList.contains('collapsed')) {
                content.classList.add('collapsed');
                if (header) header.classList.add('collapsed');
                if (arrow) arrow.innerText = '▶';
            }
        } else if (!firstUnvotedRaceId && usedCount < 4) {
            // Track the first unvoted race
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

    cols.forEach(col => {
        const meta = RACE_COLUMN_META[col];
        if (!meta) return;

        if (meta.sortable) {
            const sortKey = meta.sortKey;
            html += `<th class="sortable" data-col="${col}" id="th-${r_id}-${sortKey}" onclick="setSort('${r_id}', '${sortKey}')">${meta.label} ${getSortIcon(r_id, sortKey)}</th>`;
        } else {
            html += `<th data-col="${col}">${meta.label}</th>`;
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

// Generates the inner rows (Pulled out of loadRaces to be reusable)
function buildTableBody(r_id, entries) {
    let rowsHtml = "";
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
            Shirushi: `<td style="min-width: 170px;">
                ${createMarkBtn(r_id, h_id, '◎', key)}
                ${createMarkBtn(r_id, h_id, '〇', key)}
                ${createMarkBtn(r_id, h_id, '▲', key)}
                ${createMarkBtn(r_id, h_id, '△', key)}
                ${createMarkBtn(r_id, h_id, 'X', key)}
            </td>`,
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
            Horse: `<td style="font-weight: bold;">${horseStr} <button class="score-explain-trigger" title="Explain auto-pick score" onclick="openScoreExplain(event, '${r_id}', '${h_id}')">ⓘ</button></td>`,
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
                    oddsDelta = curOdds < prevOdds
                        ? `<span class="odds-short" title="Shortened from ${prevOdds.toFixed(1)}">↓</span>`
                        : `<span class="odds-drift" title="Drifted from ${prevOdds.toFixed(1)}">↑</span>`;
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
                        : {}
                },
                manualAdjustments: Number.isFinite(Number(meta.manualAdjustments)) ? Number(meta.manualAdjustments) : 0,
                lockStateAtSave: typeof meta.lockStateAtSave === 'boolean' ? meta.lockStateAtSave : null,
                activeSymbols: Array.isArray(meta.activeSymbols)
                    ? meta.activeSymbols.map(symbol => String(symbol || '').trim()).filter(Boolean)
                    : []
            };
        });
    }

    const version = Number(isVersioned ? payload.version : 2);
    normalized.version = Number.isFinite(version) && version > 0 ? version : 2;
    return normalized;
}

function getCurrentRiskValue() {
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
            formulaWeights: getFormulaWeightsSnapshot()
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

    // 4. THE SLIDER MIXER
    // At Risk 0: 100% Odds, 0% Form/Pedigree
    // At Risk 100: 0% Odds, 100% Form/Pedigree
    const oddsWeight = 1.0 - risk;
    const formWeight = risk;
    const pedWeight  = risk;

    let totalScore = (baseOddsScore * oddsWeight) + (baseFormScore * formWeight) + (basePedScore * pedWeight);

    // 5. Ultimate Tie-Breaker
    // If scores tie (or if it's Risk 0 and odds aren't posted yet), the true Fav always wins by a fraction.
    const favRank = parseFloat(row.Fav) || 999;
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
    const oddsMix = 1.0 - risk;
    const formMix = risk;
    const pedMix  = risk;

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
        formLines.push({ label: `Form (Ninki-Δ) ${formScoreVal.toFixed(3)} × ${fw.formWeight}`, value: last3Contrib });
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

    // MIX
    const oddsMixed = baseOddsScore * oddsMix;
    const formMixed = baseFormScore * formMix;
    const pedMixed  = basePedScore  * pedMix;

    // TIEBREAKER
    const favRank = parseFloat(row.Fav) || 999;
    const tiebreaker = -(favRank * 0.0001);

    const total = oddsMixed + formMixed + pedMixed + tiebreaker;

    return {
        total,
        risk: Math.round(risk * 100),
        raceClass: cls,
        mix: { odds: oddsMix, form: formMix, ped: pedMix },
        odds:    { lines: oddsLines, subtotal: baseOddsScore, mixed: oddsMixed },
        form:    { lines: formLines, subtotal: baseFormScore, mixed: formMixed },
        pedigree:{ lines: pedLines,  subtotal: basePedScore,  mixed: pedMixed  },
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

    // Derive the mark the auto-pick engine would assign — ranked by score, not globalMarks.
    // The user may have manually overridden marks; the popover always explains the engine's view.
    const autoPickMarks = ['◎', '〇', '▲', '△'];
    const scored = (allEntries.length ? allEntries : [row])
        .map(e => ({ id: String(e.Horse_ID).split('.')[0], score: calculatePowerScore(e, risk) }))
        .sort((a, c) => c.score - a.score);
    const autoRank = scored.findIndex(s => s.id === String(horseId)) + 1;
    const mark = autoRank >= 1 && autoRank <= 4 ? autoPickMarks[autoRank - 1] : null;

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
                return label ? `R${i+1}: Δ${r.delta > 0 ? '+' : ''}${r.delta} ${label}` : null;
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

    function sent(rank) { return sentiment(rank, total); }
    const oddsP  = sent(oddsRank) === 'sx-pos' ? true : sent(oddsRank) === 'sx-neg' ? false : null;
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
        <div class="sx-factors">
            ${factorRow('💴', 'Odds', oddsDesc(), oddsRank, oddsP)}
            ${factorRow('📈', 'Recent Form', formDesc(), formRank, formP)}
            ${factorRow('🏇', 'Jockey', aeDesc(jAE, row.Jockey || null), jRank, jP)}
            ${factorRow('🎯', 'Trainer', aeDesc(tAE, row.Trainer || null), tRank, tP)}
            ${factorRow('🧬', 'Sire Fit', sfDesc(), sfRank, sfP)}
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

function getCurrentAutoPickRisk(riskOverride = null) {
    let currentRisk = parseInt(document.getElementById('risk-slider')?.value, 10);
    if (isNaN(currentRisk)) currentRisk = 50;

    if (riskOverride !== null && riskOverride !== 'null' && riskOverride !== undefined) {
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

    const allSymbols = ["◎", "〇", "▲", "△"];
    const usedSymbols = [];
    const markedHorses = [];

    for (const [k, v] of Object.entries(globalMarks)) {
        if (k.startsWith(`${r_id}_`) && v) {
            if (allSymbols.includes(v)) usedSymbols.push(v);
            markedHorses.push(k.split('_')[1]);
        }
    }

    const availableSymbols = allSymbols.filter(symbol => !usedSymbols.includes(symbol));
    const currentRisk = getCurrentAutoPickRisk(riskOverride);
    if (availableSymbols.length === 0) {
        return { changed: false, currentRisk, reason: 'full' };
    }

    const scoredHorses = entries
        .filter(row => !markedHorses.includes(String(row.Horse_ID).split('.')[0]))
        .map(row => ({
            h_id: String(row.Horse_ID).split('.')[0],
            power: calculatePowerScore(row, currentRisk)
        }))
        .sort((a, b) => b.power - a.power);

    let changed = false;
    for (let i = 0; i < Math.min(availableSymbols.length, scoredHorses.length); i++) {
        const key = `${r_id}_${scoredHorses[i].h_id}`;
        if (globalMarks[key] !== availableSymbols[i]) {
            globalMarks[key] = availableSymbols[i];
            changed = true;
        }
    }

    if (changed) {
        touchRaceMeta(r_id, { markSource: 'auto-pick', riskSlider: currentRisk });
    }

    return { changed, currentRisk, reason: changed ? 'updated' : 'unchanged' };
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
    for (let i = 0; i < Math.min(mainSymbols.length, scoredHorses.length); i++) {
        const newKey = `${r_id}_${scoredHorses[i].h_id}`;
        globalMarks[newKey] = mainSymbols[i];
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
function getUnconditionalAutoBetRankingsForRace(r_id) {
    const entries = globalRaceEntries[r_id];
    if (!entries || entries.length === 0) return [];
    const symbols = ['◎', '〇', '▲', '△'];
    const currentRisk = getCurrentAutoPickRisk();
    return entries
        .map(row => ({ h_id: String(row.Horse_ID).split('.')[0], power: calculatePowerScore(row, currentRisk) }))
        .sort((a, b) => b.power - a.power)
        .slice(0, symbols.length)
        .map((e, i) => ({ h_id: e.h_id, symbol: symbols[i] }));
}

// Returns engine suggestions that respect any marks the user has already set.
// - Symbols already assigned by the user are skipped (their horse is done).
// - Remaining symbols are filled by the highest-scoring completely-unmarked horses.
// This means: if the user gives 〇 to a horse the engine wanted ◎ for, the engine
// accepts the 〇, moves on, and suggests ◎ for the next best unmarked horse.
function getMarkAwareAutoBetRankingsForRace(r_id) {
    const entries = globalRaceEntries[r_id];
    if (!entries || entries.length === 0) return [];
    const symbols = ['◎', '〇', '▲', '△'];
    const currentRisk = getCurrentAutoPickRisk();

    // Which symbols already have a user-assigned horse, and which horses have any mark.
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

    // All symbols are user-assigned — nothing for the engine to suggest.
    if (takenSymbols.size === symbols.length) return [];

    // Horses ranked by power score, excluding any that already have a mark.
    // At Risk ≤ 70: longshots (odds > 30) are sorted to the back so ◎/〇 naturally
    // go to realistic contenders first. They remain available for ▲/△ slots.
    const guardActive = currentRisk <= 70;
    const pool = entries
        .map(row => ({ h_id: String(row.Horse_ID).split('.')[0], power: calculatePowerScore(row, currentRisk), isLongshot: guardActive && (parseFloat(row.Odds) || 9999) > 30 }))
        .filter(e => !markedHorses.has(e.h_id))
        .sort((a, b) => {
            if (a.isLongshot !== b.isLongshot) return a.isLongshot ? 1 : -1;
            return b.power - a.power;
        });

    // Abstention: skip lower-tier marks when scores are too clustered to differentiate.
    // ◎ always assigned if there's a pool. For each subsequent mark, require a minimum
    // gap between that candidate and the next-ranked horse (proves it's a real tier break).
    const result = [];
    let poolIdx = 0;
    for (const symbol of symbols) {
        if (takenSymbols.has(symbol)) continue;
        if (poolIdx >= pool.length) break;
        if (poolIdx > 0 && pool.length >= 4) {
            const fieldSpread = pool[0].power - pool[Math.min(pool.length - 1, 5)].power;
            if (fieldSpread > 0) {
                const gapFromNext = pool[poolIdx - 1].power - pool[poolIdx].power;
                if (gapFromNext / fieldSpread < 0.03) {
                    poolIdx++;
                    continue;
                }
            }
        }
        result.push({ h_id: pool[poolIdx++].h_id, symbol });
    }
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

function updateAutoBetHighlighting() {
    document.querySelectorAll('.mark-btn.auto-bet-preview').forEach(btn => btn.classList.remove('auto-bet-preview'));

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

function getMonthKey(dateStr) {
    return dateStr ? String(dateStr).slice(0, 7) : null;
}

function getAvailableCalendarMonths() {
    return [...new Set(getSortedActiveDates().map(getMonthKey).filter(Boolean))].sort();
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
    const dates = getSortedActiveDates();
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
    const dates = getSortedActiveDates();
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
        const races = globalRacesByDate[dateStr];

        if (!races) {
            cells.push(`
                <div class="race-calendar-cell" title="${dateStr}">
                    <div class="race-calendar-daynum" style="padding: 8px; color: #4b5565;">${day}</div>
                </div>
            `);
            continue;
        }

        const timeline = globalDateTimelineByDate[dateStr] || 'upcoming';
        const activeClass = dateStr === currentActiveDate ? ' is-selected' : '';
        cells.push(`
            <button type="button" class="race-calendar-day timeline-${timeline}${activeClass}" onclick="selectCalendarDate('${dateStr}')" title="${dateStr} \u2022 ${races.length} race${races.length === 1 ? '' : 's'}">
                <div class="race-calendar-daynum">${day}</div>
                <div class="race-calendar-meta">
                    <span class="race-calendar-count">${races.length}</span>
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

function selectCalendarDate(date) {
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
        const autoStyle = (usedCount >= 4) ? "display: none;" : "display: inline-block;";
        const reorderStyle = (usedCount >= 4 && !isLocked) ? "display: inline-block;" : "display: none;";
        const lockLabel = isLocked ? "🔓 Unlock Bets" : "🔒 Lock Bets";
        const lockClass = isLocked ? " is-locked" : "";
        const clearStyle = countRaceMarks(r_id) > 0 ? "display: inline-block;" : "display: none;";

        const localName = localizeRaceName(race.info.race_name) || localizeRaceClass(race.info.race_class);
        const winBadgesHtml = buildRaceWinBadgesHtml(race);
        const historyBtnHtml = dateTimeline === 'past' && !raceHasHistoryData(race)
            ? `<button class="btn-history-refresh" onclick="refreshRaceHistory(event, '${r_id}')" title="Fetch finish positions and result data for this race">📜 Update History</button>`
            : "";
        // Odds-trend graph (Phase 37): only for upcoming/live cards, where odds history accrues.
        const trendsBtnHtml = dateTimeline !== 'past'
            ? `<button class="btn-odds-trends" onclick="event.stopPropagation(); showOddsHistory('${r_id}')" title="Odds over time for every runner">📈 Trends</button>`
            : "";
        // Devil's Advocate export (Phase 36): upcoming/live only — copies a prompt + JSON for any LLM.
        const exportBtnHtml = dateTimeline !== 'past'
            ? `<button class="btn-ai-export" onclick="event.stopPropagation(); exportRaceForAI('${r_id}')" title="Copy a devil's-advocate prompt + data for Claude/ChatGPT">🤖 Export for AI</button>`
            : "";

        html += `<div id="race-${r_id}" style="margin-bottom: 25px;">
            <h3 id="header-${r_id}" class="${headerClass} ${collapsedClass}" onclick="toggleRace('${r_id}')">
                <span id="arrow-${r_id}" class="collapse-arrow">${arrow}</span> <span id="header-meta-${r_id}">${raceStatusEmoji(race)} ${race.info.time} | ${trackName(race.info.place)} R${race.info.race_number}: ${localName} ${winBadgesHtml}</span>

                ${historyBtnHtml}
                ${trendsBtnHtml}
                ${exportBtnHtml}

                <button class="btn-autopick-safe auto-group-${r_id}" style="${autoStyle}" onclick="autoPick(event, '${r_id}', 20)" title="Force Risk to 20" ${isLocked ? 'disabled' : ''}>🛡️ Safe Bet</button>
                <button class="btn-autopick auto-group-${r_id}" style="${autoStyle}; margin-left: 8px;" onclick="autoPick(event, '${r_id}', null)" title="Use Sidebar Slider" ${isLocked ? 'disabled' : ''}>🎲 Auto</button>
                <button class="btn-autopick-lucky auto-group-${r_id}" style="${autoStyle}" onclick="autoPick(event, '${r_id}', 75)" title="Force Risk to 75" ${isLocked ? 'disabled' : ''}>🍀 Lucky</button>
                <button id="btn-clear-${r_id}" class="btn-clear-bets" style="${clearStyle}" onclick="clearRaceBets(event, '${r_id}')" title="Clear all marks in this race" ${isLocked ? 'disabled' : ''}>🧹 Clear Bets</button>
                <button id="btn-lock-${r_id}" class="btn-lock-bets${lockClass}" onclick="toggleRaceLock(event, '${r_id}')" title="${isLocked ? 'Unlock to allow mark changes' : 'Lock to prevent any mark changes in this race'}">${lockLabel}</button>

                <button id="btn-reorder-${r_id}" class="btn-reorder" style="${reorderStyle}" onclick="reorderPicks(event, '${r_id}')" title="Reorder Chosen Picks" ${isLocked ? 'disabled' : ''}>✨ Smart Sort</button>
                <span id="risk-badge-${r_id}" class="risk-badge" style="display:none;" onclick="event.stopPropagation()"></span>
            </h3>
            <div id="content-${r_id}" class="race-content ${collapsedClass}">
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
        }
    }
}

function renderDayTabsAndSchedules(preferredDate = null, collapseBeforeTime = null, keepOpenRaceId = null) {
    const dates = Object.keys(globalRacesByDate).sort();
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

    // Cheap init for ALL dates: locks, index tags, sort state, race class.
    // Runs fast (no HTML building) so global state is ready before any tab renders.
    dates.forEach(date => {
        const dateTimeline = globalDateTimelineByDate[date] || 'upcoming';
        (globalRacesByDate[date] || []).forEach(race => {
            const r_id = race.info.race_id;
            if (isAutoLockPastVotesEnabled() && dateTimeline === 'past') {
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

// --- RENDER DASHBOARD ---
async function loadRaces() {
    const t0 = performance.now();
    appendDebugLine('loadRaces started');
    // cache: 'no-cache' re-validates every time (sends If-None-Match) but allows
    // 304 short-circuits — browser skips response.json() when ETag matches, saving
    // 2-4s of V8 JSON.parse on unchanged data. ETag v5 + 30s server cache makes
    // the ETag trustworthy. 'no-store' was overkill and blocked all 304s.
    const _fetchT0 = performance.now();
    const racesRes = await fetch('/api/races', { cache: 'no-cache' });
    const _headersMs = Math.round(performance.now() - _fetchT0);
    appendDebugLine(`/api/races status=${racesRes.status}`);
    const data = applyTimeDisplayToRacesPayload(await racesRes.json().catch(() => ({})));
    _devFetchMs = Math.round(performance.now() - _fetchT0); // headers + body download + JSON.parse
    if (!racesRes.ok) {
        const detail = data?.detail || data?.message || `HTTP ${racesRes.status}`;
        appendConsoleLine(`[Races] Failed to load races: ${detail}`);
        appendDebugLine(`loadRaces failed in ${(performance.now() - t0).toFixed(0)}ms`);
        throw new Error(detail);
    }
    appendDebugLine(
        `Payload days: upcoming=${Object.keys(data.upcoming_races_by_date || data.races_by_date || {}).length}, ` +
        `past=${Object.keys(data.past_races_by_date || {}).length}`
    );
    const timelineData = normalizeRacesPayload(data);
    // Reset cached structures for a clean rebuild.
    upcomingRaces = [];
    searchableHorses = [];
    globalRaceEntries = {};
    globalRaceClass = {};
    globalRaceInfo = {};
    globalRacesByDate = {};
    globalDateTimelineByDate = {};
    globalAllRacesByDate = {
        upcoming: timelineData.upcoming || {},
        past: timelineData.past || {}
    };

    const _stateT0 = performance.now();
    ["upcoming", "past"].forEach(timeline => {
        Object.keys(globalAllRacesByDate[timeline]).forEach(date => {
            if (!globalRacesByDate[date]) {
                globalRacesByDate[date] = globalAllRacesByDate[timeline][date];
                globalDateTimelineByDate[date] = timeline;
            } else {
                // Same JST date appears in both timelines (race day: some past, some upcoming).
                // Append without duplicating; keep 'upcoming' as the date-level label.
                const existingIds = new Set(globalRacesByDate[date].map(r => r.info.race_id));
                globalAllRacesByDate[timeline][date].forEach(r => {
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

            globalAllRacesByDate[timeline][date].forEach(race => {
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

    upcomingRaces.sort((a, b) => a.time - b.time);
    _devStateMs = Math.round(performance.now() - _stateT0);

    let collapseBeforeTime = null;
    let keepOpenRaceId = null;
    if (isFirstLoad && upcomingRaces.length > 0) {
        const now = new Date();
        const nextUpcomingIndex = upcomingRaces.findIndex(r => r.time > now);
        if (nextUpcomingIndex > -1) {
            const nextUpcomingRace = upcomingRaces[nextUpcomingIndex];
            collapseBeforeTime = nextUpcomingRace.time;

            // Keep the race that is most likely in-progress expanded.
            if (nextUpcomingIndex > 0) {
                keepOpenRaceId = upcomingRaces[nextUpcomingIndex - 1].r_id;
            }
        }
    }

    const _sidebarT0 = performance.now();
    renderWeekendWatchlist();
    renderEnginePicks();
    updateQuickStats();
    _devSidebarMs = Math.round(performance.now() - _sidebarT0);

    const _renderT0 = performance.now();
    const hasUpcoming = Object.keys(globalAllRacesByDate.upcoming || {}).length > 0;
    const upcomingDates = Object.keys(globalAllRacesByDate.upcoming || {}).sort();
    const pastDates = Object.keys(globalAllRacesByDate.past || {}).sort();
    const allDates = getSortedActiveDates();

    if (isFirstLoad) {
        currentActiveDate = upcomingDates[0] || pastDates[pastDates.length - 1] || allDates[0] || null;
    } else {
        currentActiveDate = findNearestAvailableDate(currentActiveDate, allDates)
            || upcomingDates[0]
            || pastDates[pastDates.length - 1]
            || allDates[0]
            || null;
    }

    currentTimelineTab = currentActiveDate
        ? (globalDateTimelineByDate[currentActiveDate] || (hasUpcoming ? "upcoming" : "past"))
        : (hasUpcoming ? "upcoming" : "past");
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

function switchMainTab(date) {
    const dates = getSortedActiveDates();
    const nextDate = findNearestAvailableDate(date, dates);
    if (!nextDate) return;

    currentActiveDate = nextDate;
    currentTimelineTab = globalDateTimelineByDate[nextDate] || currentTimelineTab;
    currentCalendarMonth = getMonthKey(nextDate) || currentCalendarMonth;
    updateOreProSyncDateDisplay();

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

    // If it's not active, AND it's not the X button, check if it's stolen!
    if (!isActive && symbol !== 'X') {
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
}

/**
 * Lock every race that shares a JST clean_date with the given race. Used after
 * an OrePro apply so the operator can't accidentally edit marks for races that
 * have already been bet. Idempotent — already-locked races stay locked.
 */
function lockAllRacesForRaceDay(raceId) {
    const info = globalRaceInfo[raceId];
    const date = info?.clean_date;
    if (!date) return 0;

    let locked = 0;
    Object.keys(globalRaceInfo).forEach(rid => {
        if ((globalRaceInfo[rid]?.clean_date || '') !== date) return;
        if (!raceLocks[rid]) {
            raceLocks[rid] = true;
            locked++;
        }
        const tbody = document.getElementById(`tbody-${rid}`);
        if (tbody) tbody.innerHTML = buildTableBody(rid, globalRaceEntries[rid]);
        updateRaceActionButtons(rid);
    });
    updateAutoBetHighlighting();
    return locked;
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
        
        // ONLY steal the symbol from another horse if it's a main vote! (Allows infinite X's)
        if (newSymA !== 'X') {
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

async function saveMarksToServer() {
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
                    : {}
            },
            manualAdjustments: Number.isFinite(Number(meta.manualAdjustments)) ? Number(meta.manualAdjustments) : 0,
            lockStateAtSave: typeof meta.lockStateAtSave === 'boolean' ? meta.lockStateAtSave : null,
            activeSymbols: Array.isArray(meta.activeSymbols) ? meta.activeSymbols.map(symbol => String(symbol || '').trim()).filter(Boolean) : []
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

function evaluateRaceRecap(race) {
    const info = race?.info || {};
    const raceId = String(info.race_id || '').trim();
    const raceLabel = `${trackName(info.place)} R${info.race_number || '?'}`.trim();
    const entries = Array.isArray(race?.entries) ? race.entries : [];

    const finishByRank = {};
    const ppByRank = {};
    entries.forEach(row => {
        const rank = parseFinishRank(row?.Finish);
        if (!rank || rank < 1 || rank > 3) return;

        const horseId = String(row?.Horse_ID ?? '').split('.')[0].trim();
        if (!horseId) return;
        if (!finishByRank[rank]) {
            finishByRank[rank] = horseId;
            const pp = parseInt(row?.PP, 10);
            if (Number.isFinite(pp) && pp > 0) ppByRank[rank] = pp;
        }
    });

    const hasCompleteTop3 = !!(finishByRank[1] && finishByRank[2] && finishByRank[3]);
    if (!hasCompleteTop3) {
        return {
            raceId,
            raceLabel,
            hasCompleteTop3: false,
            honmeiHit: false,
            quinellaHit: false,
            trioHit: false,
            ppByRank: {}
        };
    }

    const marks = collectRaceMainMarks(raceId);
    const pickedSet = new Set(Object.values(marks).filter(Boolean));
    const top1 = finishByRank[1];
    const top2 = finishByRank[2];
    const top3 = finishByRank[3];

    return {
        raceId,
        raceLabel,
        hasCompleteTop3: true,
        honmeiHit: !!marks["◎"] && marks["◎"] === top1,
        quinellaHit: pickedSet.has(top1) && pickedSet.has(top2),
        trioHit: pickedSet.has(top1) && pickedSet.has(top2) && pickedSet.has(top3),
        ppByRank
    };
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

    const winningRaceIds = [];
    let total = 0;
    let correct = 0;
    let votedRaces = 0;

    races.forEach(race => {
        const r_id = String(race?.info?.race_id || '').trim();
        if (!r_id) return;

        const marks = collectRaceMainMarks(r_id);
        const hasVotes = Object.keys(marks).length > 0;
        if (!hasVotes) return;
        votedRaces += 1;

        const recap = evaluateRaceRecap(race);
        if (!recap.hasCompleteTop3) return;

        total += 1;
        const isCorrect = recap.honmeiHit || recap.quinellaHit || recap.trioHit;
        if (isCorrect) {
            correct += 1;
            winningRaceIds.push(r_id);
        }
    });

    const rate = total > 0 ? Math.round((correct / total) * 100) : 0;
    return {
        visible: votedRaces > 0 && total > 0,
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

    const summary = {
        visible: false,
        date,
        timeline,
        votedRaces: 0,
        totalScored: 0,
        honmei: 0,
        quinella: 0,
        trio: 0
    };

    if (!date || timeline !== 'past' || !races.length) {
        return summary;
    }

    races.forEach(race => {
        const r_id = String(race?.info?.race_id || '').trim();
        if (!r_id) return;

        const marks = collectRaceMainMarks(r_id);
        if (!Object.keys(marks).length) return;
        summary.votedRaces += 1;

        const recap = evaluateRaceRecap(race);
        if (!recap.hasCompleteTop3) return;

        summary.totalScored += 1;
        if (recap.honmeiHit) summary.honmei += 1;
        if (recap.quinellaHit) summary.quinella += 1;
        if (recap.trioHit) summary.trio += 1;
    });

    summary.visible = summary.votedRaces > 0;
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
        return '<div class="voting-recap-note">No votes found for this day yet.</div>';
    }

    if (!summary.totalScored) {
        return `<div class="voting-recap-note">${escapeHtml(summary.date)} has votes, but result rows are not fully scored yet.</div>`;
    }

    return `
    <div class="voting-recap-grid">
        <div class="voting-recap-item"><span>Voted Races</span><strong>${summary.votedRaces}</strong></div>
        <div class="voting-recap-item"><span>Scored Races</span><strong>${summary.totalScored}</strong></div>
        <div class="voting-recap-item"><span>◎ Hit</span><strong>${summary.honmei}/${summary.totalScored} (${pct(summary.honmei, summary.totalScored)}%)</strong></div>
        <div class="voting-recap-item"><span>Q Box Hit</span><strong>${summary.quinella}/${summary.totalScored} (${pct(summary.quinella, summary.totalScored)}%)</strong></div>
        <div class="voting-recap-item"><span>T Box Hit</span><strong>${summary.trio}/${summary.totalScored} (${pct(summary.trio, summary.totalScored)}%)</strong></div>
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
    const recap = evaluateRaceRecap(race);
    if (!recap.hasCompleteTop3) return null;
    const marksCount = Object.keys(collectRaceMainMarks(recap.raceId) || {}).length;
    if (!marksCount) return null;

    const payouts = lookupRacePayouts(race, recap.ppByRank);
    const legacyStake = parseFloat(globalOreProSettings?.bet_estimate_stake_yen) || 100;
    const winStake = parseFloat(globalOreProSettings?.bet_stake_win_yen)      || legacyStake;
    const qStake   = parseFloat(globalOreProSettings?.bet_stake_quinella_yen) || legacyStake;
    const tStake   = parseFloat(globalOreProSettings?.bet_stake_trio_yen)     || legacyStake;

    const wonYen = (recap.honmeiHit   ? payouts.win      * winStake / 100 : 0)
                 + (recap.quinellaHit ? payouts.quinella * qStake   / 100 : 0)
                 + (recap.trioHit     ? payouts.trio     * tStake   / 100 : 0);
    const qCombos = marksCount >= 2 ? marksCount * (marksCount - 1) / 2 : 0;
    const tCombos = marksCount >= 3 ? marksCount * (marksCount - 1) * (marksCount - 2) / 6 : 0;
    const spentYen = winStake + (qCombos * qStake) + (tCombos * tStake);
    const anyHit = recap.honmeiHit || recap.quinellaHit || recap.trioHit;
    return { wonYen, spentYen, netYen: wonYen - spentYen, anyHit };
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

function buildRaceWinBadgesHtml(race) {
    const recap = evaluateRaceRecap(race);
    if (!recap.hasCompleteTop3) return "";

    // Look up actual ¥ payouts from HR data. Per-¥100 ticket; scaled to the
    // operator's per-leg stake (win/quinella/trio) so labels reflect what they
    // actually would have won at their real bet sizing.
    const payouts = lookupRacePayouts(race, recap.ppByRank);
    const legacyStake = parseFloat(globalOreProSettings?.bet_estimate_stake_yen) || 100;
    const winStake = parseFloat(globalOreProSettings?.bet_stake_win_yen)      || legacyStake;
    const qStake   = parseFloat(globalOreProSettings?.bet_stake_quinella_yen) || legacyStake;
    const tStake   = parseFloat(globalOreProSettings?.bet_stake_trio_yen)     || legacyStake;
    const fmtYen = (n, stake) => n > 0 ? `¥${Math.round(n * stake / 100).toLocaleString()}` : '';

    const winYen = fmtYen(payouts.win, winStake);
    const qYen   = fmtYen(payouts.quinella, qStake);
    const tYen   = fmtYen(payouts.trio, tStake);

    const badges = [];
    if (recap.honmeiHit)   badges.push(`<span class="race-hit-pill race-hit-honmei" title="◎ Honmei hit${winYen ? ` — paid ${winYen}` : ''}">◎ Win${winYen ? ` ${winYen}` : ''}</span>`);
    if (recap.quinellaHit) badges.push(`<span class="race-hit-pill race-hit-quinella" title="Quinella Box hit${qYen ? ` — paid ${qYen}` : ''}">Q Box${qYen ? ` ${qYen}` : ''}</span>`);
    if (recap.trioHit)     badges.push(`<span class="race-hit-pill race-hit-trio" title="Trio Box hit${tYen ? ` — paid ${tYen}` : ''}">T Box${tYen ? ` ${tYen}` : ''}</span>`);

    if (!badges.length) return "";

    // Net pill: per-race won minus per-race spend across all three legs.
    // Spend model matches the day-recap: 1 Win ticket + C(marks,2) Q combos + C(marks,3) T combos.
    const wonYen   = (recap.honmeiHit   ? payouts.win      * winStake / 100 : 0)
                   + (recap.quinellaHit ? payouts.quinella * qStake   / 100 : 0)
                   + (recap.trioHit     ? payouts.trio     * tStake   / 100 : 0);
    const marksCount = Object.keys(collectRaceMainMarks(recap.raceId) || {}).length;
    const qCombos = marksCount >= 2 ? marksCount * (marksCount - 1) / 2 : 0;
    const tCombos = marksCount >= 3 ? marksCount * (marksCount - 1) * (marksCount - 2) / 6 : 0;
    const spentYen = winStake + (qCombos * qStake) + (tCombos * tStake);
    const netYen = Math.round(wonYen - spentYen);
    const netSign = netYen >= 0 ? '+' : '-';
    const netAbs = Math.abs(netYen).toLocaleString();
    const netClass = netYen >= 0 ? 'race-hit-net-pos' : 'race-hit-net-neg';
    const netTitle = `Net for this race: ¥${netSign}${netAbs} (won ¥${Math.round(wonYen).toLocaleString()} − spent ¥${spentYen.toLocaleString()})`;
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
            orepro: oreproRaceMap.get(r_id) || null,
            betEstimate: raceBetEstimateCache[r_id] || null,
            marks: group.marks
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

            if (raceCard.orepro) {
                html += `
                <div class="orepro-race-inline">
                    <span class="orepro-inline-chip">Buy ${escapeHtml(raceCard.orepro.purchaseLabel || '-')}</span>
                    <span class="orepro-inline-chip">Pay ${escapeHtml(raceCard.orepro.payoutLabel || '-')}</span>
                    <span class="orepro-inline-chip ${Number(raceCard.orepro.profit) >= 0 ? 'is-positive' : 'is-negative'}">PnL ${escapeHtml(raceCard.orepro.profitLabel || '-')}</span>
                </div>`;
            }

            if (!raceCard.orepro && raceCard.betEstimate?.pending) {
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

            raceCard.marks.forEach(m => {
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

                html += `
                <div class="export-horse-line" style="margin-bottom:8px;">
                    ${ppBadge}${markBadge}<div style="flex:1;min-width:0;display:flex;justify-content:space-between;gap:10px;">
                        <span style="font-weight:500;">${escapeHtml(String(m.horse || 'Unknown Horse'))}</span>
                        <div class="voting-line-right-meta">
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
// { "<jraRaceId>": { appliedAt, submitted, submittedAt, marksCount, lastMessage } }
let globalOreProApplyState = {};

// Subset of /api/settings that the frontend cares about for OrePro behavior.
// Kept in sync via loadOrchestratorSettings (called when Settings modal opens) and
// loadOreProSettingsLite at page init.
let globalOreProSettings = {};

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

        marks.push({ symbol, post, mark_code: String(markCode) });
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

    const marks = collectOreProMarksFromEntries(targetRaceId, globalRaceEntries[targetRaceId] || []);
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
        `Auto-pick all remaining unlocked races for ${date} using Risk ${riskVal}?\n\nThis only updates marks within UMAnager. Nothing is sent to OrePro until you click Apply Votes.`
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

        eligibleRaceIds.forEach(r_id => {
            if (isRaceLocked(r_id)) {
                skippedLocked += 1;
                return;
            }

            const result = applyAutoPickSelectionsToRace(r_id, null);
            if (result.changed) {
                changedRaceIds.push(r_id);
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
        if (!changedRaceIds.length) {
            setOreProSessionStatus(`Auto-pick finished but produced no new marks for ${date}.${lockNote}`, 'warn');
            return;
        }
        setOreProSessionStatus(
            `Auto-picked ${changedRaceIds.length} race(s) for ${date} at Risk ${riskVal}. Click Apply Votes to send them to OrePro.${lockNote}`,
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
        const res = await fetch('/api/orepro/votes/apply', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();

        const result = Array.isArray(data?.results)
            ? data.results[0]
            : (data?.result?.results?.[0] || null);
        const topStatus = String(data?.status || '').trim().toLowerCase();
        const rowStatus = String(result?.status || '').trim().toLowerCase();
        const requestCompleted = topStatus !== 'error' && rowStatus !== 'error';
        const collapseSucceeded = didOreProAdvanceToNextRace(data, result)
            || requestCompleted;
        const mode = collapseSucceeded || rowStatus === 'ok' ? 'ok' : 'warn';
        let serverMessage = result?.message || data?.message || `Applied votes for race ${raceId}.`;
        try {
            const nested = JSON.parse(serverMessage || '{}');
            if (nested?.message) serverMessage = nested.message;
        } catch (_) {}

        setOreProSessionStatus(serverMessage, mode);

        // Refresh persistent apply-state badges. After this and a re-render, the race
        // title will show "📝 Applied" or "📤 Submitted".
        await loadOreProApplyState();
        try { renderLiveViewPanel(); } catch (_) {}

        if ((requestCompleted || rowStatus === 'ok') && isAutoLockAfterSubmitEnabled()) {
            lockAllRacesForRaceDay(raceId);
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
        const submitOk = result?.submitFlow?.submitStatus === 'ok';
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

/// Apply Day Votes (bulk) — iterates every unsubmitted voted race for the active day,
/// pushing each through the proven single-race endpoint with submit_after_apply. Two
/// confirmations: first only if the day has any unbet races (incomplete coverage),
/// second always (final "you sure?" gate before anything hits OrePro).
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

    // A race "has votes" if any of ◎〇▲△ is assigned to one of its horses.
    const symbols = ['◎', '〇', '▲', '△'];
    const hasMarks = (r_id) => {
        const entries = globalRaceEntries[r_id] || [];
        return entries.some(row => {
            const h_id = String(row.Horse_ID).split('.')[0];
            const m = globalMarks[`${r_id}_${h_id}`];
            return m && symbols.includes(m);
        });
    };

    const eligible = allIds.filter(r_id => hasMarks(r_id) && !isRaceLocked(r_id));
    const lockedCount = allIds.filter(r_id => isRaceLocked(r_id)).length;
    const unbetCount = allIds.length - eligible.length - lockedCount;

    if (!eligible.length) {
        setOreProSessionStatus(`No unsubmitted races with votes for ${date}.`, 'warn');
        return;
    }

    // First confirm: only if some races on the day have no marks at all.
    if (unbetCount > 0) {
        const first = window.confirm(
            `⚠️ ${unbetCount} of ${allIds.length} race(s) for ${date} have no votes yet.\n\n` +
            `Apply votes for the ${eligible.length} race(s) that DO have marks and skip the rest?`
        );
        if (!first) return;
    }

    // Second confirm: final sanity check before anything hits OrePro.
    const second = window.confirm(
        `📤 About to APPLY + SUBMIT ${eligible.length} race(s) to OrePro for ${date}.\n\n` +
        `Each race will be staged and committed one by one. This is final — proceed?`
    );
    if (!second) return;

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

        let okCount = 0;
        let failCount = 0;
        const failureLines = [];

        for (let i = 0; i < eligible.length; i++) {
            const r_id = eligible[i];
            if (btn) btn.textContent = `⏳ Applying ${i + 1}/${eligible.length}`;
            setOreProSessionStatus(`Applying race ${i + 1}/${eligible.length} (${r_id})...`, 'info');

            const payload = buildOreProApplyVotesPayloadForRace(r_id);
            if (!payload.races.length) {
                failCount++;
                failureLines.push(`[skip] ${r_id}: no resolvable marks`);
                continue;
            }
            payload.submit_after_apply = true;
            payload.go_next_race = (i < eligible.length - 1);

            try {
                const res = await fetch('/api/orepro/votes/apply', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                const data = await res.json();
                const result = Array.isArray(data?.results) ? data.results[0] : (data?.result?.results?.[0] || null);
                const topStatus = String(data?.status || '').trim().toLowerCase();
                const rowStatus = String(result?.status || '').trim().toLowerCase();
                if (topStatus !== 'error' && rowStatus !== 'error') {
                    okCount++;
                } else {
                    failCount++;
                    failureLines.push(`[${rowStatus || topStatus || 'error'}] ${r_id}: ${result?.message || data?.message || 'unknown'}`);
                }
            } catch (err) {
                failCount++;
                failureLines.push(`[exception] ${r_id}: ${err?.message || err}`);
            }
        }

        if (okCount > 0 && isAutoLockAfterSubmitEnabled()) lockAllRacesForRaceDay(eligible[0]);

        await loadOreProApplyState();
        try { renderLiveViewPanel(); } catch (_) {}

        const mode = failCount === 0 ? 'ok' : (okCount > 0 ? 'warn' : 'error');
        setOreProSessionStatus(`Bulk apply complete for ${date}: ${okCount} ok, ${failCount} failed.`, mode);

        // The bulk run takes a few seconds and the operator usually tabs away — pop a
        // modal alert so they get a clear signal when it's done.
        const alertIcon = failCount === 0 ? '✅' : (okCount > 0 ? '⚠️' : '❌');
        const alertTail = failCount === 0
            ? 'All votes submitted successfully.'
            : (okCount > 0
                ? `${okCount} succeeded, ${failCount} failed (see diagnostics panel).`
                : `All ${failCount} submission(s) failed (see diagnostics panel).`);
        window.alert(`${alertIcon} Apply Day Votes finished for ${date}.\n\n${alertTail}`);

        // Drop any failure detail into the diagnostics panel so the operator can see what didn't go through.
        if (failureLines.length) {
            const out = document.getElementById('orepro-sync-results');
            if (out) {
                out.innerHTML = `
                    <div class="orepro-sync-title">Apply Day Votes — Failures (${failCount})</div>
                    <div class="orepro-sync-list" style="font-family:monospace; font-size:11px;">
                        ${failureLines.map(l => `<div>${escapeHtml(l)}</div>`).join('')}
                    </div>
                `;
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
    const sidebarTitle = document.getElementById('voting-sidebar-title');
    const sidebarDisplay = document.getElementById('voting-sidebar-display');
    const mainTitle = document.getElementById('voting-main-title');
    const recapPanel = document.getElementById('voting-recap-panel');
    if (!sidebarTitle || !sidebarDisplay || !mainTitle || !recapPanel) return;

    const date = String(currentActiveDate || '').trim();
    const timeline = globalDateTimelineByDate[date] || '';
    sidebarTitle.textContent = `By Racecourse · ${date || 'No day selected'}`;
    sidebarDisplay.innerHTML = '';
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

    refreshBetEstimatesForDate(date);
    loadOreProSessionStatus();
}

function switchMainView(view) {
    currentMainView = view === 'voting' ? 'voting' : 'races';

    const schedules = document.getElementById('schedules-container');
    const liveView = document.getElementById('live-view-container');
    const racesBtn = document.getElementById('main-view-races');
    const votingBtn = document.getElementById('main-view-voting');
    if (!schedules || !liveView || !racesBtn || !votingBtn) return;

    const isVoting = currentMainView === 'voting';
    schedules.style.display = isVoting ? 'none' : 'block';
    liveView.style.display = isVoting ? 'flex' : 'none';
    const watchlistPanel = document.getElementById('weekend-watchlist-panel');
    if (watchlistPanel) watchlistPanel.style.display = isVoting ? 'none' : '';
    racesBtn.classList.toggle('is-active', !isVoting);
    votingBtn.classList.toggle('is-active', isVoting);
    document.body.classList.toggle('voting-mode', isVoting);

    syncVotingViewAvailability();
    updateLiveViewPopoutAvailability();
    updateWinningVotesFocusButton();

    if (isVoting) {
        renderLiveViewPanel();
    }
}

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
                <span class="day-recap-label">Correct vs Total (any hit type)</span>
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

function applyUmmTheme(enabled) {
    document.body.classList.toggle('umm-mode', enabled);
    // Swap the Pedigree Lists group header emoji
    const summary = document.querySelector('#pedigree-lists-group > summary');
    if (summary) {
        summary.textContent = enabled ? '🎤 Pedigree Lists' : '🎯 Pedigree Lists';
    }
    localStorage.setItem(UMM_STORAGE_KEY, enabled ? '1' : '0');
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
        tvModeSplitPercent: parseClampedPercent('setting-tvModeSplitPercent', Number.isFinite(Number(appConfig.ui?.tvModeSplitPercent)) ? Number(appConfig.ui?.tvModeSplitPercent) : 50),
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

function applySidebarSettings() {
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
    meta.innerHTML = `${raceStatusEmoji(race)} ${info.time} | ${trackName(info.place)} R${info.race_number}: ${localName} ${winBadgesHtml}`;
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
                            ? `<span class="odds-short" title="Shortened from ${prev.toFixed(1)}">↓</span>`
                            : `<span class="odds-drift" title="Drifted from ${prev.toFixed(1)}">↑</span>`;
                    }
                    cell.innerHTML = (e.odds || '') + delta;
                } else if (f === 'fav') {
                    cell.textContent = e.fav || '';
                }
            });
        }

        // Mirror into the in-memory race object so header re-render (and any
        // future evaluateRaceRecap call) sees the fresh finish/odds/fav values.
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
        patchRaceEntries(payload.raceId, payload.entries, ['finish']);
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
        set('setting-orepro-session-cookie',      s.orepro_session_cookie);
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
    try {
        const res = await fetch('/api/orepro/companion/window', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'open' })
        });
        const data = await res.json();
        alert(`${data.status}: ${data.message}`);
    } catch (e) {
        alert(`OrePro probe failed: ${e.message}`);
    }
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