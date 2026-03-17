let globalMarks = {};
let globalRaceMeta = {};
let globalMarksVersion = 2;
let listsData = { favorites: "", watchlist: "" };
let raceLocks = {}; // Per-race lock state for mark interactions
let upcomingRaces = []; // NEW: Stores our parsed race times
let globalRaceEntries = {}; // NEW: Stores local row data for instant sorting
let globalRaceInfo = {}; // NEW: Stores the Racetrack names and numbers
let globalRacesByDate = {}; // All race days organized by date for navigation and jump dropdowns
let globalAllRacesByDate = { upcoming: {}, past: {} }; // Full timeline buckets from API
let globalDateTimelineByDate = {}; // Maps YYYY-MM-DD -> "upcoming" | "past"
let currentTimelineTab = "upcoming";
let currentActiveDate = null;
let currentCalendarMonth = null;
let currentMainView = 'races';
let raceSorts = {}; // NEW: Remembers which column is sorted for each race
let winningVotesFocusEnabled = false;
let searchableHorses = []; // Stores the database for the search bar
let currentSearchSelection = -1; // Tracks keyboard navigation in the dropdown
let appConfig = {}; // NEW: Stores app configuration
let isFirstLoad = true; // NEW: Track if this is the first page load to auto-collapse past races

const DEFAULT_RACE_COLUMNS = ["Shirushi", "BK", "PP", "Horse", "Record", "Sire", "Dam", "BMS", "Odds", "Fav", "Finish"];
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
    Sire: { label: "Sire", sortable: true, sortKey: "Sire", initialAsc: true },
    Dam: { label: "Dam", sortable: true, sortKey: "Dam", initialAsc: true },
    BMS: { label: "BMS", sortable: true, sortKey: "BMS", initialAsc: true },
    Odds: { label: "Odds", sortable: true, sortKey: "Odds", initialAsc: true },
    Fav: { label: "Fav", sortable: true, sortKey: "Fav", initialAsc: true },
    Finish: { label: "Fin", sortable: true, sortKey: "Finish", initialAsc: true }
};

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

function getRaceColumnsLayout() {
    if (!appConfig.ui) appConfig.ui = {};
    appConfig.ui.raceTableColumns = normalizeRaceColumnsLayout(appConfig.ui.raceTableColumns);
    return appConfig.ui.raceTableColumns;
}

function getVisibleRaceColumns() {
    return getRaceColumnsLayout().filter(c => c.visible).map(c => c.key);
}

function isVoteSortingEnabled() {
    return appConfig.ui?.voteSortingTop ?? true;
}

function isAutoFetchPastResultsEnabled() {
    return appConfig.ui?.autoFetchPastResults ?? true;
}

function isAutoLockPastVotesEnabled() {
    return appConfig.ui?.autoLockPastVotes ?? false;
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
    updateRaceHighlighting();
    
    // 4. Restore scroll position seamlessly
    window.scrollTo(0, scrollY);
}

async function refreshListsOnly() {
    // Lightweight refresh that just updates sidebar lists without reloading races
    const listRes = await fetch('/api/lists');
    listsData = await listRes.json();
    renderLists();
    
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
        favorites: parseListIds(listsData?.favorites || ""),
        watchlist: parseListIds(listsData?.watchlist || "")
    };
}

function getTrackedStatus(horseId, trackedSets = null) {
    /**Check if a horse is tracked and on which lists. Returns {fav: bool, watch: bool}*/
    const sets = trackedSets || getTrackedSets();
    const cleanId = String(horseId).split('.')[0].trim();
    return {
        fav: sets.favorites.has(cleanId),
        watch: sets.watchlist.has(cleanId)
    };
}

function calculateWeightedIntensity(horse, sire, dam, bms) {
    // Keep frontend highlight logic aligned with backend scoring rules.
    let fav_weight = horse.fav ? SCORE_TRACKED_HORSE : (sire.fav ? SCORE_TRACKED_SIRE : 0.0);
    fav_weight += (dam.fav ? SCORE_TRACKED_DAM : 0.0) + (bms.fav ? SCORE_TRACKED_BMS : 0.0);

    let watch_weight = horse.watch ? SCORE_WATCHLIST_HORSE : (sire.watch ? SCORE_WATCHLIST_SIRE : 0.0);
    watch_weight += (dam.watch ? SCORE_WATCHLIST_DAM : 0.0) + (bms.watch ? SCORE_WATCHLIST_BMS : 0.0);

    fav_weight = Math.min(fav_weight, SCORE_MAX);
    watch_weight = Math.min(watch_weight, SCORE_MAX);

    return { fav_weight, watch_weight, max: Math.max(fav_weight, watch_weight) };
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
    const isMixed = (weights.fav_weight > 0 && weights.watch_weight > 0);
    
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
            const f_weight = weights.fav_weight;
            const w_weight = weights.watch_weight;
            
            // Update row data
            let icon = "";
            let score = 0;
            let status = "";
            
            if (f_weight > 0) {
                score = Math.min(f_weight, 1.0);
                status = "FAV";
                icon = f_weight >= ICON_THRESHOLD_3STAR ? "⭐⭐⭐" : (f_weight >= ICON_THRESHOLD_2STAR ? "⭐⭐" : "⭐");
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
            header.classList.remove('has-fav', 'has-watch', 'row-mixed', 'intensity-light', 'intensity-medium', 'intensity-strong', 'intensity-very-strong');
            
            // Apply appropriate status class - WATCHLIST COLOR TAKES PRIORITY OVER FAVORITES
            if (hasWatchlist) {
                header.classList.add('has-watch');
            } else if (hasMixed) {
                header.classList.add('row-mixed');
            } else if (hasTracked) {
                header.classList.add('has-fav');
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
}

function updateAllHoverButtons() {
    /**Update all hover buttons to show Add or Remove based on current lists*/
    const tracked_ids = parseListIds(listsData.favorites);
    const watchlist_ids = parseListIds(listsData.watchlist);
    
    document.querySelectorAll('.hover-action-btn').forEach(btn => {
        const horseId = btn.getAttribute('data-horse-id');
        const listType = btn.getAttribute('data-list-type');
        
        if (!horseId || !listType) return;
        
        const isTracked = (listType === 'favorites' && tracked_ids.has(horseId)) ||
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
    
    // NEW: Save slider state to config periodically
    document.getElementById('risk-slider').addEventListener('change', saveConfigToServer);
    document.getElementById('risk-slider').addEventListener('input', updateAllRiskBadges);
    
    // NEW: Load saved slider state from config
    const savedRisk = appConfig.ui?.riskSlider || 50;
    document.getElementById('risk-slider').value = savedRisk;
    updateRiskLabel(savedRisk);
    
    // NEW: Apply sidebar settings
    applySidebarSettings();
    
    await refreshDataAndUI();
    switchMainView('races');
}

// --- HORSE LIST UI LOGIC ---
function renderLists() {
    document.getElementById('list-fav').innerHTML = buildListHTML(listsData.favorites, 'favorites');
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

        const horseData = searchableHorses.find(h => h.h_id === id);
        const parsedName = parts.length >= 2 ? (parts.slice(1).join('#') || '').trim() : '';
        const name = parsedName || (horseData ? horseData.name : '') || id;

        const escapedName = escapeHtml(name);
        const escapedId = escapeHtml(id);

        // Find this horse in searchableHorses to get date and race_id
        if (horseData) {
            html += `
                <div class="horse-item">
                    <span class="horse-item-name" style="cursor: pointer;" onclick="jumpToHorse('${horseData.date}', '${horseData.r_id}', '${horseData.h_id}', '${horseData.timeline || "upcoming"}')" title="Click to view in race">${escapedName}</span>
                    <button class="btn-delete" title="Remove ${escapedName}" onclick="removeHorse('${escapeHtml(listType)}', '${escapedId}')">✖</button>
                </div>`;
        } else {
            // Fallback if not found in searchableHorses
            html += `
                <div class="horse-item">
                    <span class="horse-item-name" style="color: #888;">${escapedName}</span>
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

async function quickAddFromHover(id, listType) {
    const res = await fetch('/api/snipe', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({id: id, list_type: listType})
    });
    const data = await res.json();
    
    if(data.status === "success") {
        // Refresh lists and update highlighting/buttons
        await refreshListsOnly();
    } else {
        alert(data.message);
    }
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
            favorites: listsData.favorites,
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
            favorites: listsData.favorites,
            watchlist: listsData.watchlist
        })
    });
    
    // Refresh only the sidebar lists (keep scroll position)
    await refreshListsOnly();
}

async function snipeHorse() {
    const url = document.getElementById('snipe-url').value;
    const type = document.getElementById('snipe-type').value;
    if (!url) return;
    
    const res = await fetch('/api/snipe', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({url: url, list_type: type})
    });
    const data = await res.json();
    document.getElementById('snipe-url').value = ""; // Clear the input box
    
    if(data.status === "success") await refreshDataAndUI();
    else alert(data.message);
}

// Creates a wrapper with a 500ms delay hover menu
function buildNameWithHover(id, name, listType, trackedStatus, intensity, isMixed) {
    if (!id || id === 'nan' || id === '---' || !name) return escapeHtml(name || "");
    const cleanId = String(id).split('.')[0].trim();
    if (!cleanId) return escapeHtml(name);
    
    // Safety check just in case listsData isn't fully loaded yet
    const isTracked = listsData[listType] && listsData[listType].includes(cleanId);
    
    const escapedId = escapeHtml(cleanId);
    const escapedListType = escapeHtml(listType);
    const escapedName = escapeHtml(name);
    
    let btnHtml = "";
    if (isTracked) {
        btnHtml = `<button class="hover-action-btn remove-btn" data-horse-id="${cleanId}" data-list-type="${listType}" onclick="removeHorseFromHover('${cleanId}', '${listType}')">➖ Remove</button>`;
    } else {
        btnHtml = `<button class="hover-action-btn add-btn" data-horse-id="${cleanId}" data-list-type="${listType}" onclick="quickAddFromHover('${cleanId}', '${listType}')">➕ Add</button>`;
    }
    
    // Generate the link to the English Netkeiba Database!
    const linkHtml = `<a href="https://en.netkeiba.com/db/horse/${escapedId}/" target="_blank" class="hover-link-btn" title="View on Netkeiba DB">🔗 DB</a>`;
    
    // Apply tracking formatting if this horse is tracked
    let nameClass = "name-text";
    if (trackedStatus && (trackedStatus.fav || trackedStatus.watch)) {
        // Determine color based on which list(s) the family member is on
        let colorClass = "";
        if (trackedStatus.fav && trackedStatus.watch) {
            colorClass = "tracked-mixed";
        } else if (trackedStatus.fav) {
            colorClass = "tracked-fav";
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

    entries.sort((a, b) => {
        // Our Custom Default (Votes at top, unmarked middle, X at bottom)
        if (col === 'Default') {
            const keyA = `${r_id}_${String(a.Horse_ID).split('.')[0]}`;
            const keyB = `${r_id}_${String(b.Horse_ID).split('.')[0]}`;
            const ppA = parseRaceNumber(a.PP);
            const ppB = parseRaceNumber(b.PP);
            const voteSortingEnabled = isVoteSortingEnabled();

            if (voteSortingEnabled) {
                const valA = sMap[globalMarks[keyA]] || 50;
                const valB = sMap[globalMarks[keyB]] || 50;

                if (valA !== valB) return valA < valB ? -1 : 1;
            }

            const ppComparison = comparePrimitiveValues(ppA, ppB, true);
            if (ppComparison !== 0) return ppComparison;
            return a.original_index - b.original_index;
        }

        let comparison = 0;
        if (col === 'Shirushi') {
            const keyA = `${r_id}_${String(a.Horse_ID).split('.')[0]}`;
            const keyB = `${r_id}_${String(b.Horse_ID).split('.')[0]}`;
            const valA = sMap[globalMarks[keyA]] || 50;
            const valB = sMap[globalMarks[keyB]] || 50;
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
        } else if (col === 'Sire') {
            comparison = comparePrimitiveValues(normalizeRaceText(a.Sire), normalizeRaceText(b.Sire), asc);
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
            html += `<th class="sortable" id="th-${r_id}-${sortKey}" onclick="setSort('${r_id}', '${sortKey}')">${meta.label} ${getSortIcon(r_id, sortKey)}</th>`;
        } else {
            html += `<th>${meta.label}</th>`;
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

    // Toggle direction or set new column
    if (!raceSorts[r_id]) raceSorts[r_id] = { col: col, asc: initialAsc };
    else if (raceSorts[r_id].col === col) raceSorts[r_id].asc = !raceSorts[r_id].asc;
    else { raceSorts[r_id].col = col; raceSorts[r_id].asc = initialAsc; }

    applySortLogic(r_id, raceSorts[r_id].col, raceSorts[r_id].asc);

    // Instantly replace just the table body and headers for THIS race (Zero flashing!)
    document.getElementById(`tbody-${r_id}`).innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);
    refreshRaceHeaderSortLabels(r_id);
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
        const weights = tracking?.weights || { fav_weight: 0, watch_weight: 0 };
        
        // Determine base status class: mixed takes priority, then FAV/WATCH
        let rowStatusClass = "";
        if (tracking.isMixed) {
            rowStatusClass = "row-mixed";
        } else if (weights.fav_weight > 0) {
            rowStatusClass = "row-fav";
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
        const sireStr = buildNameWithHover(row.Sire_ID, row.Sire, 'favorites', tracking.sire, tracking.intensity, tracking.isMixed);
        const damStr = buildNameWithHover(row.Dam_ID, row.Dam, 'favorites', tracking.dam, tracking.intensity, tracking.isMixed);
        const bmsStr = buildNameWithHover(row.BMS_ID, row.BMS, 'favorites', tracking.bms, tracking.intensity, tracking.isMixed);
        
        // NEW: Added id="row-${r_id}-${h_id}" to the <tr>
        const cellHtmlByCol = {
            Shirushi: `<td style="min-width: 170px;">
                ${createMarkBtn(r_id, h_id, '◎', key)}
                ${createMarkBtn(r_id, h_id, '〇', key)}
                ${createMarkBtn(r_id, h_id, '▲', key)}
                ${createMarkBtn(r_id, h_id, '△', key)}
                ${createMarkBtn(r_id, h_id, 'X', key)}
            </td>`,
            BK: `<td>${row.BK || ""}</td>`,
            PP: `<td>${row.PP || ""}</td>`,
            Horse: `<td style="font-weight: bold;">${horseStr}</td>`,
            Record: `<td>${row.Record || ""}</td>`,
            Sire: `<td>${sireStr}</td>`,
            Dam: `<td>${damStr}</td>`,
            BMS: `<td>${bmsStr}</td>`,
            Odds: `<td>${row.Odds || ""}</td>`,
            Fav: `<td>${row.Fav || ""}</td>`,
            Finish: `<td class="finish-pos finish-pos-${row.Finish || ''}">${row.Finish || ""}</td>`
        };

        const orderedCells = getVisibleRaceColumns().map(col => cellHtmlByCol[col] || "").join("");
        rowsHtml += `<tr id="row-${r_id}-${h_id}" class="${trClass}">${orderedCells}</tr>`;
    });
    return rowsHtml;
}


// --- STRATEGY SLIDER LOGIC ---
function updateRiskLabel(val) {
    const label = document.getElementById('risk-label');
    const slider = document.getElementById('risk-slider');
    let text = "Balanced";
    let color = "#ff9f43"; // Orange
    
    if (val <= 20) { text = "Ultra Safe"; color = "#0abde3"; } // Cyan
    else if (val <= 40) { text = "Chalky"; color = "#1dd1a1"; } // Green
    else if (val <= 60) { text = "Balanced"; color = "#ff9f43"; } // Orange
    else if (val <= 85) { text = "Value Hunter"; color = "#ff4b4b"; } // Red
    else { text = "Maximum Chaos"; color = "#ff0000"; } // Bright Red
    
    label.innerText = `${text} (${val})`;
    label.style.color = color;
    slider.style.color = color; // Changes the thumb color dynamically!
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
    };
}

function calculatePowerScore(row, riskVal) {
    const fw = getFormulaWeights();
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
    if (row.Record) {
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

    // 3. Base Pedigree Score (from Tracked Bloodlines)
    const basePedScore = (parseFloat(row.Score) || 0) * fw.pedigreeMultiplier;

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

// --- AUTO-PICK ALGORITHM ---
async function autoPick(event, r_id, riskOverride = null) {
    event.stopPropagation();
    if (isRaceLocked(r_id)) return;

    const entries = globalRaceEntries[r_id];
    if (!entries || entries.length === 0) return;

    const allSymbols = ["◎", "〇", "▲", "△"];
    let usedSymbols = [];
    let markedHorses = []; 

    for (const [k, v] of Object.entries(globalMarks)) {
        if (k.startsWith(`${r_id}_`) && v) {
            if (allSymbols.includes(v)) usedSymbols.push(v);
            markedHorses.push(k.split('_')[1]); 
        }
    }

    const availableSymbols = allSymbols.filter(s => !usedSymbols.includes(s));
    if (availableSymbols.length === 0) {
        updateRaceActionButtons(r_id);
        return;
    }

    // 2. Calculate Power Score ONLY for unmarked horses using Override OR Slider!
    let currentRisk = parseInt(document.getElementById('risk-slider').value);
    if (isNaN(currentRisk)) currentRisk = 50; // Only fallback to 50 if the slider completely fails to load
    
    if (riskOverride !== null && riskOverride !== 'null' && riskOverride !== undefined) {
        currentRisk = parseInt(riskOverride);
    }

    let scoredHorses = entries
    
        .filter(row => !markedHorses.includes(String(row.Horse_ID).split('.')[0]))
        .map(row => {
            return { h_id: String(row.Horse_ID).split('.')[0], power: calculatePowerScore(row, currentRisk) };
        });

    scoredHorses.sort((a, b) => b.power - a.power);

    // 3. Assign ONLY the missing symbols to the top remaining horses
    for (let i = 0; i < Math.min(availableSymbols.length, scoredHorses.length); i++) {
        const key = `${r_id}_${scoredHorses[i].h_id}`;
        globalMarks[key] = availableSymbols[i];
    }

    // 4. Save and Update UI
    touchRaceMeta(r_id, { markSource: 'auto-pick', riskSlider: currentRisk });
    await saveMarksToServer();

    raceSorts[r_id] = { col: 'Default', asc: true };
    applySortLogic(r_id, 'Default', true);
    document.getElementById(`tbody-${r_id}`).innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);
    refreshRaceHeaderSortLabels(r_id);

    updateRaceActionButtons(r_id);
    updateRiskBadge(r_id);
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
    let currentRisk = parseInt(document.getElementById('risk-slider').value);
    if (isNaN(currentRisk)) currentRisk = 50;

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

function normalizeRacesPayload(data) {
    return {
        upcoming: data.upcoming_races_by_date || data.races_by_date || {},
        past: data.past_races_by_date || {}
    };
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

    if (!dates.length || !months.length) {
        calendar.innerHTML = '<div class="race-calendar-empty-note">No race days loaded.</div>';
        updateActiveDateNavigator();
        return;
    }

    const selectedDate = findNearestAvailableDate(currentActiveDate, dates) || dates[0];
    currentActiveDate = selectedDate;
    currentTimelineTab = globalDateTimelineByDate[selectedDate] || 'upcoming';

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
            cells.push(`<div class="race-calendar-cell"><div class="race-calendar-daynum" style="padding: 8px; color: #4b5565;">${day}</div></div>`);
            continue;
        }

        const timeline = globalDateTimelineByDate[dateStr] || 'upcoming';
        const activeClass = dateStr === currentActiveDate ? ' is-selected' : '';
        cells.push(`
            <button type="button" class="race-calendar-day timeline-${timeline}${activeClass}" onclick="selectCalendarDate('${dateStr}')" title="${dateStr} • ${races.length} races">
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

function renderDayTabsAndSchedules(preferredDate = null, collapseBeforeTime = null, keepOpenRaceId = null) {
    const dates = Object.keys(globalRacesByDate).sort();
    const scheds = document.getElementById('schedules-container');
    scheds.innerHTML = "";

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

    dates.forEach((date) => {
        const isActive = date === activeDate;
        const dateTimeline = globalDateTimelineByDate[date] || 'upcoming';

        let html = `<div id="tab-${date}" class="tab-content ${isActive ? 'active' : ''}">`;

        globalRacesByDate[date].forEach(race => {
            const r_id = race.info.race_id;
            if (isAutoLockPastVotesEnabled() && dateTimeline === 'past') {
                raceLocks[r_id] = true;
            }

            let shouldCollapse = false;
            if (
                dateTimeline === 'upcoming' &&
                isFirstLoad &&
                collapseBeforeTime &&
                race.info.time !== "TBA" &&
                race.info.sort_time
            ) {
                const raceTime = new Date(race.info.sort_time.replace(' ', 'T'));
                if (raceTime < collapseBeforeTime && r_id !== keepOpenRaceId) {
                    shouldCollapse = true;
                }
            }

            const arrow = shouldCollapse ? "▶" : "▼";
            const collapsedClass = shouldCollapse ? "collapsed" : "";

            race.entries.forEach((row, idx) => { row.original_index = idx; });
            globalRaceEntries[r_id] = race.entries;

            if (!raceSorts[r_id]) {
                raceSorts[r_id] = { col: 'Default', asc: true };
            }

            applySortLogic(r_id, raceSorts[r_id].col, raceSorts[r_id].asc);

            let hasFav = false;
            let hasWatch = false;
            let hasMixed = false;
            let maxIntensity = 0;
            globalRaceEntries[r_id].forEach(row => {
                if (!row.familyTracking) {
                    row.familyTracking = calculateFamilyTracking(row.Horse_ID, row.Sire_ID, row.Dam_ID, row.BMS_ID);
                }

                const tracking = row.familyTracking;
                const weights = tracking?.weights || { fav_weight: 0, watch_weight: 0 };

                if (tracking.isMixed) hasMixed = true;
                if (weights.fav_weight > 0) hasFav = true;
                if (weights.watch_weight > 0) hasWatch = true;
                if (tracking.intensity > maxIntensity) maxIntensity = tracking.intensity;
            });

            const rowsHtml = buildTableBody(r_id, globalRaceEntries[r_id]);

            let headerClass = "race-header";
            if (hasWatch) headerClass += " has-watch";
            else if (hasMixed) headerClass += " row-mixed";
            else if (hasFav) headerClass += " has-fav";

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

            const autoStyle = (usedCount >= 4) ? "display: none;" : "display: inline-block;";
            const reorderStyle = (usedCount >= 4) ? "display: inline-block;" : "display: none;";
            const isLocked = isRaceLocked(r_id);
            const lockLabel = isLocked ? "🔓 Unlock Bets" : "🔒 Lock Bets";
            const lockClass = isLocked ? " is-locked" : "";
            const clearStyle = countRaceMarks(r_id) > 0 ? "display: inline-block;" : "display: none;";

            const localName = localizeRaceName(race.info.race_name);
            const winBadgesHtml = buildRaceWinBadgesHtml(race);
            const historyBtnHtml = dateTimeline === 'past' && !raceHasHistoryData(race)
                ? `<button class="btn-history-refresh" onclick="refreshRaceHistory(event, '${r_id}')" title="Fetch finish positions and result data for this race">📜 Update History</button>`
                : "";

            html += `<div id="race-${r_id}" style="margin-bottom: 25px;">
                <h3 id="header-${r_id}" class="${headerClass} ${collapsedClass}" onclick="toggleRace('${r_id}')">
                    <span id="arrow-${r_id}" class="collapse-arrow">${arrow}</span> 🕒 ${race.info.time} | ${race.info.place.toUpperCase()} R${race.info.race_number}: ${localName} ${winBadgesHtml}

                    ${historyBtnHtml}

                    <button class="btn-autopick-safe auto-group-${r_id}" style="${autoStyle}" onclick="autoPick(event, '${r_id}', 20)" title="Force Risk to 20" ${isLocked ? 'disabled' : ''}>🛡️ Safe Bet</button>
                    <button class="btn-autopick auto-group-${r_id}" style="${autoStyle}; margin-left: 8px;" onclick="autoPick(event, '${r_id}', null)" title="Use Sidebar Slider" ${isLocked ? 'disabled' : ''}>🎲 Auto</button>
                    <button class="btn-autopick-lucky auto-group-${r_id}" style="${autoStyle}" onclick="autoPick(event, '${r_id}', 75)" title="Force Risk to 75" ${isLocked ? 'disabled' : ''}>🍀 Lucky</button>
                    <button id="btn-clear-${r_id}" class="btn-clear-bets" style="${clearStyle}" onclick="clearRaceBets(event, '${r_id}')" title="Clear all marks in this race" ${isLocked ? 'disabled' : ''}>🧹 Clear Bets</button>
                    <button id="btn-lock-${r_id}" class="btn-lock-bets${lockClass}" onclick="toggleRaceLock(event, '${r_id}')" title="${isLocked ? 'Unlock to allow mark changes' : 'Lock to prevent any mark changes in this race'}">${lockLabel}</button>

                    <button id="btn-reorder-${r_id}" class="btn-reorder" style="${reorderStyle}" onclick="reorderPicks(event, '${r_id}')" title="Reorder Chosen Picks" ${isLocked ? 'disabled' : ''}>✨ Smart Sort</button>
                    <span id="risk-badge-${r_id}" class="risk-badge" style="display:none;" onclick="event.stopPropagation()"></span>
                </h3>
                <div id="content-${r_id}" class="race-content ${collapsedClass}">
                    <table>
                        <thead id="thead-${r_id}">${buildTableHeaderRow(r_id)}</thead>
                        <tbody id="tbody-${r_id}">${rowsHtml}</tbody>
                    </table>
                </div>
            </div>`;
        });

        html += `</div>`;
        scheds.innerHTML += html;
    });

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
    const racesRes = await fetch('/api/races');
    const data = await racesRes.json();
    const timelineData = normalizeRacesPayload(data);

    // Reset cached structures for a clean rebuild.
    upcomingRaces = [];
    searchableHorses = [];
    globalRaceEntries = {};
    globalRaceInfo = {};
    globalRacesByDate = {};
    globalDateTimelineByDate = {};
    globalAllRacesByDate = {
        upcoming: timelineData.upcoming || {},
        past: timelineData.past || {}
    };

    ["upcoming", "past"].forEach(timeline => {
        Object.keys(globalAllRacesByDate[timeline]).forEach(date => {
            globalRacesByDate[date] = globalAllRacesByDate[timeline][date];
            globalDateTimelineByDate[date] = timeline;

            globalAllRacesByDate[timeline][date].forEach(race => {
                const r_id = race.info.race_id;

                // Preload entries for all timelines so cross-timeline features (export)
                // work immediately without requiring the user to switch tabs first.
                if (!globalRaceEntries[r_id]) {
                    race.entries.forEach((row, idx) => {
                        if (row.original_index === undefined) {
                            row.original_index = idx;
                        }
                    });
                    globalRaceEntries[r_id] = race.entries;
                }

                globalRaceInfo[r_id] = { ...race.info, _timeline: timeline };

                race.entries.forEach(row => {
                    searchableHorses.push({
                        name: row.Horse,
                        date: date,
                        r_id: r_id,
                        h_id: String(row.Horse_ID).split('.')[0],
                        track: race.info.place.toUpperCase(),
                        r_num: race.info.race_number,
                        timeline: timeline
                    });
                });

                if (timeline === "upcoming" && race.info.time !== "TBA" && race.info.sort_time) {
                    upcomingRaces.push({
                        time: new Date(race.info.sort_time.replace(' ', 'T')),
                        name: `${race.info.place.toUpperCase()} R${race.info.race_number}`,
                        r_id: r_id
                    });
                }
            });
        });
    });

    upcomingRaces.sort((a, b) => a.time - b.time);

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

    const tpContainer = document.getElementById('sidebar-weekend-watchlist');
    if (tpContainer) {
        if (data.top_picks && data.top_picks.length > 0) {
            let tpHTML = `<div class="horse-list-container" style="max-height: none;">`;
            data.top_picks.forEach(p => {
                const r_id = p[5];
                const horseName = p[3];
                const raceData = searchableHorses.find(h => h.r_id === r_id && h.name === horseName);

                if (raceData) {
                    tpHTML += `
                    <div class="horse-item" style="flex-direction: column; align-items: flex-start; gap: 4px;">
                        <span style="color: #fafafa; font-weight: bold; font-size: 14px; cursor: pointer;" onclick="jumpToHorse('${raceData.date}', '${raceData.r_id}', '${raceData.h_id}', '${raceData.timeline || "upcoming"}')" title="Click to view in race">${p[4]} ${horseName}</span>
                        <span style="font-size: 11px; color: #888;">${p[0]} | W/S: ${p[1]} | Odds: ${p[2]}</span>
                    </div>`;
                } else {
                    tpHTML += `
                    <div class="horse-item" style="flex-direction: column; align-items: flex-start; gap: 4px;">
                        <span style="color: #888; font-weight: bold; font-size: 14px;">${p[4]} ${horseName}</span>
                        <span style="font-size: 11px; color: #888;">${p[0]} | W/S: ${p[1]} | Odds: ${p[2]}</span>
                    </div>`;
                }
            });
            tpHTML += `</div>`;
            tpContainer.innerHTML = tpHTML;
        } else {
            tpContainer.innerHTML = "<div style='color:#888; font-size:12px; text-align:center; margin-top:10px;'>Run Auto-Pick to generate top picks.</div>";
        }
    }

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
    syncVotingViewAvailability();
    updateLiveViewPopoutAvailability();
    updateAllRiskBadges();

    isFirstLoad = false;
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
    document.querySelectorAll('#schedules-container .tab-content').forEach(c => {
        c.classList.toggle('active', c.id === `tab-${nextDate}`);
    });
    winningVotesFocusEnabled = false;
    syncVotingViewAvailability();
    updateLiveViewPopoutAvailability();
    updateWinningVotesFocusButton();
    renderRaceCalendar();
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

    let changed = false;
    for (const [k, v] of Object.entries(globalMarks)) {
        if (k.startsWith(`${r_id}_`) && v) {
            globalMarks[k] = null;
            changed = true;
        }
    }

    if (!changed) return;

    touchRaceMeta(r_id, { markSource: 'manual', manualAdjustmentsDelta: 1 });
    await saveMarksToServer();

    applySortLogic(r_id, raceSorts[r_id].col, raceSorts[r_id].asc);
    const tbody = document.getElementById(`tbody-${r_id}`);
    if (tbody) tbody.innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);
    refreshRaceHeaderSortLabels(r_id);
    updateRaceActionButtons(r_id);
    updateRiskBadge(r_id);
    updateWinningVotesFocusButton();
    if (winningVotesFocusEnabled) applyWinningVotesFocus();
}

function toggleRaceLock(event, r_id) {
    event.stopPropagation();
    raceLocks[r_id] = !raceLocks[r_id];

    const tbody = document.getElementById(`tbody-${r_id}`);
    if (tbody) tbody.innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);

    updateRaceActionButtons(r_id);
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
    updateWinningVotesFocusButton();
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
    if (appConfig?.ui?.showConsole ?? true) consoleBox.style.display = 'block';
    const prefix = consoleBox.textContent && consoleBox.textContent.trim() ? '\n' : '';
    consoleBox.textContent += `${prefix}${message}`;
    consoleBox.scrollTop = consoleBox.scrollHeight;
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

async function refreshUpcomingRacesLite() {
    const btn = document.getElementById('btn-upcoming-refresh');
    if (btn) btn.disabled = true;

    try {
        const data = await postJson('/api/races/upcoming/refresh', {});
        await refreshDataAndUI();

        const nextUpcomingDate = Object.keys(globalAllRacesByDate.upcoming || {}).sort()[0];
        if (nextUpcomingDate) switchMainTab(nextUpcomingDate);

        const failedCount = Array.isArray(data.failed_races) ? data.failed_races.length : 0;
        alert(`Upcoming refresh complete. Races updated: ${data.updated_races || 0}, rows updated: ${data.updated_rows || 0}, failed races: ${failedCount}.`);
    } catch (err) {
        alert(`Upcoming refresh failed: ${err.message}`);
    } finally {
        if (btn) btn.disabled = false;
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
    if (appConfig?.ui?.showConsole ?? true) consoleBox.style.display = 'block';
    consoleBox.textContent = "Waking up scraper...";
    
    // Start pinging the Python server for console text every 500 milliseconds
    logInterval = setInterval(fetchLogs, 500);
    
    await fetch('/api/scrape', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({mode: mode})
    });
    
    // The scrape is completely finished!
    clearInterval(logInterval);
    await fetchLogs(); // Grab any final lines
    
    document.getElementById('btn-new-race').disabled = false;
    document.getElementById('btn-all-race').disabled = false;
    loadRaces(); 
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
    const raceLabel = `${(info.place || '').toUpperCase()} R${info.race_number || '?'}`.trim();
    const entries = Array.isArray(race?.entries) ? race.entries : [];

    const finishByRank = {};
    entries.forEach(row => {
        const rank = parseFinishRank(row?.Finish);
        if (!rank || rank < 1 || rank > 3) return;

        const horseId = String(row?.Horse_ID ?? '').split('.')[0].trim();
        if (!horseId) return;
        if (!finishByRank[rank]) finishByRank[rank] = horseId;
    });

    const hasCompleteTop3 = !!(finishByRank[1] && finishByRank[2] && finishByRank[3]);
    if (!hasCompleteTop3) {
        return {
            raceId,
            raceLabel,
            hasCompleteTop3: false,
            honmeiHit: false,
            quinellaHit: false,
            trioHit: false
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
        trioHit: pickedSet.has(top1) && pickedSet.has(top2) && pickedSet.has(top3)
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

function buildRaceWinBadgesHtml(race) {
    const recap = evaluateRaceRecap(race);
    if (!recap.hasCompleteTop3) return "";

    const badges = [];
    if (recap.honmeiHit) badges.push('<span class="race-hit-pill race-hit-honmei" title="◎ Honmei hit">◎ Win</span>');
    if (recap.quinellaHit) badges.push('<span class="race-hit-pill race-hit-quinella" title="Quinella Box hit">Q Box</span>');
    if (recap.trioHit) badges.push('<span class="race-hit-pill race-hit-trio" title="Trio Box hit">T Box</span>');

    if (!badges.length) return "";
    return `<span class="race-hit-wrap">${badges.join('')}</span>`;
}

function buildRacecourseCheatHtml(targetDate) {
    const date = String(targetDate || '').trim();
    const timeline = globalDateTimelineByDate[date] || '';
    const races = Array.isArray(globalRacesByDate[date]) ? globalRacesByDate[date] : [];
    const byTrack = {};
    const sMap = { "◎": 1, "〇": 2, "▲": 3, "△": 4, "☆": 5, "消": 6 };
    const oreproRaceMap = getOreProRaceResultMapForActiveDate();

    races.forEach(race => {
        const r_id = String(race?.info?.race_id || '').trim();
        if (!r_id) return;

        const marks = collectRaceMainMarks(r_id);
        if (!Object.keys(marks).length) return;

        const info = race.info || {};
        const track = String(info.place || '').toUpperCase();
        const raceNum = parseInt(info.race_number, 10) || 0;
        const entries = Array.isArray(race.entries) ? race.entries : [];
        const recap = evaluateRaceRecap(race);

        const markRows = Object.entries(marks).map(([symbol, horseId]) => {
            const row = entries.find(r => String(r.Horse_ID).split('.')[0] === String(horseId));
            const finishRank = parseFinishRank(row?.Finish);
            return {
                symbol,
                rank: sMap[symbol] || 99,
                horse: row ? row.Horse : 'Unknown Horse',
                fav: row ? String(row.Fav || '').trim() : '',
                finishRank
            };
        }).sort((a, b) => a.rank - b.rank);

        if (!byTrack[track]) byTrack[track] = [];
        byTrack[track].push({
            r_id,
            raceNum,
            time: String(info.time || 'TBA'),
            raceName: localizeRaceName(info.race_name),
            winBadgesHtml: timeline === 'past' ? buildRaceWinBadgesHtml(race) : '',
            orepro: oreproRaceMap.get(r_id) || null,
            marks: markRows
        });
    });

    const tracks = Object.keys(byTrack).sort();
    if (!tracks.length) {
        return "<p style='text-align:center; color:#888; margin-top:30px;'>No votes for this day yet.</p>";
    }

    let html = '';
    tracks.forEach(track => {
        html += `<div class="export-track-header">${escapeHtml(track)}</div>`;
        html += `<div class="export-track-grid">`;

        byTrack[track].sort((a, b) => a.raceNum - b.raceNum).forEach(raceCard => {
            html += `<div class="export-race-card voting-race-card" data-rid="${escapeHtml(raceCard.r_id)}">`;
            html += `<div class="export-race-title voting-race-title">🕒 ${escapeHtml(raceCard.time)} | Race ${raceCard.raceNum}: ${escapeHtml(raceCard.raceName || '')} ${raceCard.winBadgesHtml}</div>`;
            html += `<div class="voting-race-body">`;

            if (raceCard.orepro) {
                html += `
                <div class="orepro-race-inline">
                    <span class="orepro-inline-chip">Buy ${escapeHtml(raceCard.orepro.purchaseLabel || '-')}</span>
                    <span class="orepro-inline-chip">Pay ${escapeHtml(raceCard.orepro.payoutLabel || '-')}</span>
                    <span class="orepro-inline-chip ${Number(raceCard.orepro.profit) >= 0 ? 'is-positive' : 'is-negative'}">PnL ${escapeHtml(raceCard.orepro.profitLabel || '-')}</span>
                </div>`;
            }

            raceCard.marks.forEach(m => {
                const favBadge = m.fav ? `Fav ${escapeHtml(String(m.fav))}` : 'Fav -';
                const finishBadge = timeline === 'past'
                    ? `<span class="voting-finish-badge${m.finishRank ? ` rank-${m.finishRank}` : ''}">Fin ${m.finishRank || '-'}</span>`
                    : '';

                html += `
                <div class="export-horse-line" style="margin-bottom: 8px;">
                    <span class="export-symbol" style="font-weight:bold; margin-right:8px; width:22px; text-align:center;">${escapeHtml(m.symbol)}</span>
                    <div style="flex: 1; min-width: 0; display:flex; justify-content:space-between; gap:10px;">
                        <span style="font-weight:500;">${escapeHtml(String(m.horse || 'Unknown Horse'))}</span>
                        <div class="voting-line-right-meta">
                            <span style="font-size:11px; color:#ddd; border:1px solid #555; border-radius:4px; padding:2px 6px; white-space:nowrap;">${favBadge}</span>
                            ${finishBadge}
                        </div>
                    </div>
                </div>`;
            });

            html += `</div>`;
            html += `</div>`;
        });

        html += `</div>`;
    });

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

function setVotingOreProCollapsed(collapsed) {
    const container = document.getElementById('live-view-container');
    const toggleBtn = document.getElementById('btn-voting-orepro-toggle');
    if (!container || !toggleBtn) return;

    container.classList.toggle('orepro-collapsed', !!collapsed);
    toggleBtn.textContent = collapsed ? '🌐 Show OrePro' : '🧾 Collapse OrePro';
}

function toggleVotingOrePro() {
    const container = document.getElementById('live-view-container');
    if (!container) return;
    setVotingOreProCollapsed(!container.classList.contains('orepro-collapsed'));
}

let oreproCompanionWindow = null;
const OREPRO_COMPANION_WINDOW_NAME = 'OreProCompanionWindow';
let oreproLastSyncPayload = null;

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

function openOreProCompanion() {
    const win = window.open(OREPRO_URL, OREPRO_COMPANION_WINDOW_NAME, 'width=1320,height=920,menubar=no,toolbar=no,location=yes,status=no');
    if (!win) {
        setOreProSessionStatus('Popup blocked. Allow popups for localhost, then retry Open OrePro.', 'warn');
        return;
    }
    oreproCompanionWindow = win;
    setOreProSessionStatus('OrePro companion opened. Place bets there, then use Sync Results here.', 'ok');
}

function focusOreProCompanion() {
    if (isOreProCompanionOpen()) {
        oreproCompanionWindow.focus();
        setOreProSessionStatus('Focused OrePro companion window.', 'ok');
        return;
    }
    openOreProCompanion();
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

async function syncOreProResults() {
    // Derive date from the calendar's currently selected day
    const kaisai_date = currentActiveDate ? currentActiveDate.replace(/-/g, '') : '';
    const kaisai_id = (document.getElementById('orepro-kaisai-id')?.value || '').trim();
    const yosoka_id = (document.getElementById('orepro-yosoka-id')?.value || '').trim();

    if (!kaisai_date) {
        setOreProSessionStatus('No day selected in the calendar. Select a race day first.', 'warn');
        return;
    }

    const label = ` for ${kaisai_date}`;
    setOreProSessionStatus(`Syncing OrePro results${label}...`, 'warn');
    try {
        const res = await fetch('/api/orepro/results/sync', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ kaisai_date, kaisai_id, yosoka_id }),
        });
        const data = await res.json();
        renderOreProSyncPayload(data);
        if (data.status === 'success') {
            setOreProSessionStatus(`OrePro results synced${label}.`, 'ok');
        } else {
            setOreProSessionStatus(data.message || 'OrePro sync finished with warnings.', 'warn');
        }
    } catch (err) {
        setOreProSessionStatus(`OrePro sync failed: ${err?.message || err}`, 'error');
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
    sidebarDisplay.innerHTML = buildRacecourseCheatHtml(date);
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
    const summaryByDate = {};
    const sMap = {"◎": 1, "〇": 2, "▲": 3, "△": 4, "☆": 5, "消": 6};

    for (const [key, symbol] of Object.entries(globalMarks)) {
        if (!symbol || symbol === 'X') continue;

        const [r_id, h_id] = key.split('_');
        const info = globalRaceInfo[r_id];
        if (!info) continue;

        const dateStr = info.clean_date || "Unknown Date";
        if (!targetDate || dateStr !== targetDate) continue;
        const track = info.place.toUpperCase();
        const raceNum = parseInt(info.race_number);

        if (!summaryByDate[dateStr]) summaryByDate[dateStr] = {};
        if (!summaryByDate[dateStr][track]) summaryByDate[dateStr][track] = {};
        if (!summaryByDate[dateStr][track][raceNum]) summaryByDate[dateStr][track][raceNum] = [];

        const entries = globalRaceEntries[r_id] || [];
        const horseRow = entries.find(r => String(r.Horse_ID).split('.')[0] === h_id);
        const horseName = horseRow ? horseRow.Horse : "Unknown Horse";
        const pp = horseRow ? parseInt(horseRow.PP) || 99 : 99;
        const bk = horseRow ? parseInt(horseRow.BK) || 0 : 0;
        const fav = horseRow ? String(horseRow.Fav || "").trim() : "";

        summaryByDate[dateStr][track][raceNum].push({
            symbol: symbol,
            rank: sMap[symbol] || 99,
            horse: horseName,
            pp: pp,
            bk: bk,
            fav: fav
        });
    }

    let html = "";
    const dates = Object.keys(summaryByDate).sort();

    if (dates.length === 0) {
        html = "<p style='text-align:center; color:#888; margin-top:50px;'>No votes cast yet! Make your selections in the grid first.</p>";
    } else {
        dates.forEach(date => {
            const safeDate = escapeHtml(String(date));
            html += `<h2 style="color: #fff; border-bottom: 2px solid #ff4b4b; padding-bottom: 5px; margin-top: 15px; margin-bottom: 15px;">📅 ${safeDate}</h2>`;
            
            const tracks = Object.keys(summaryByDate[date]).sort();
            tracks.forEach(track => {

                const safeTrackId = `${date}-${track}`.replace(/[^a-zA-Z0-9-]/g, '');

                const safeTrack = escapeHtml(String(track));
                html += `<div class="export-track-header" onclick="toggleExportTrack('${safeTrackId}')" title="Click to collapse/expand track">
                                <span id="arrow-track-${safeTrackId}" style="display:inline-block; width:20px; font-size: 14px; vertical-align: middle;">▼</span>${safeTrack}
                    </div>`;

                html += `<div id="content-track-${safeTrackId}" class="export-track-grid">`;

                const races = Object.keys(summaryByDate[date][track]).map(Number).sort((a,b) => a - b);
                races.forEach(rNum => {
                    html += `<div class="export-race-card">`;

                    const safeId = `${date}-${track}-${rNum}`.replace(/[^a-zA-Z0-9-]/g, '');
                    html += `<div class="export-race-title" onclick="toggleExportRace('${safeId}')" title="Click to collapse/expand">
                                <span id="arrow-${safeId}" style="display:inline-block; width:15px; font-size: 10px; vertical-align: middle;">▼</span> Race ${rNum}
                             </div>`;

                    html += `<div id="content-${safeId}">`;

                    const marks = summaryByDate[date][track][rNum];
                    marks.sort((a, b) => a.rank - b.rank);

                    marks.forEach(m => {
                        let symSize = "16px";
                        if (m.symbol === "◎") {
                            symSize = "19px";
                        }

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
                        
                        const c = bColors[m.bk] || { bg: '#444', color: '#fff', border: '#444' };

                        const ppBadge = m.pp !== 99 
                            ? `<span style="display:inline-block; width:22px; height:22px; line-height:22px; text-align:center; font-size:12px; font-weight:bold; background:${c.bg}; color:${c.color}; border:1px solid ${c.border}; border-radius:4px; margin-right:6px;">${m.pp}</span>` 
                            : `<span style="display:inline-block; width:22px; height:22px; margin-right:6px;"></span>`;

                        const markBadge = `<span style="display:inline-block; width:22px; height:22px; line-height:22px; text-align:center; font-size:${symSize}; font-weight:bold; background:${c.bg}; color:${c.color}; border:1px solid ${c.border}; border-radius:4px; margin-right:8px;">${m.symbol}</span>`;

                        const favBadge = m.fav ? `Fav ${escapeHtml(String(m.fav))}` : "Fav -";
                        const safeHorseName = escapeHtml(String(m.horse || "Unknown Horse"));

                        html += `<div class="export-horse-line" style="margin-bottom: 8px;">
                            ${ppBadge}
                            ${markBadge}
                            <div class="export-horse-main" style="flex: 1; min-width: 0;">
                                <span style="font-weight: 500;">${safeHorseName}</span>
                                <span class="export-fav-badge">${favBadge}</span>
                            </div>
                        </div>`;
                    });
                    html += `</div>`; 
                    html += `</div>`; 
                });
                html += `</div>`;
            });
        });
    }

    const fullHtml = `
    <!DOCTYPE html>
    <html>
    <head>
        <title>🪟 Live View Popout</title>
        <style>
            body { font-family: sans-serif; background-color: #0c0c0c; color: #fafafa; margin: 0; padding: 20px; }
            h2 { color: #fff; border-bottom: 2px solid #ff4b4b; padding-bottom: 5px; margin-top: 15px; margin-bottom: 15px; }
            
            /* NEW: Added hover effects and pointer cursor to Track Header */
            .export-track-header { font-size: 18px; font-weight: bold; color: #1dd1a1; border-bottom: 2px solid #333; padding-bottom: 5px; margin: 20px 0 10px 0; text-transform: uppercase; letter-spacing: 1px; cursor: pointer; user-select: none; transition: 0.2s; }
            .export-track-header:hover { color: #fff; border-color: #555; }
            .export-track-header.collapsed { color: #555; border-bottom-style: dotted; }
            
            .export-track-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 12px; }
            .export-race-card { background: #1a1c23; border: 1px solid #333; border-radius: 6px; padding: 12px; }
            
            .export-race-title { font-size: 14px; font-weight: bold; color: #888; margin-bottom: 10px; border-bottom: 1px dotted #444; padding-bottom: 4px; cursor: pointer; user-select: none; transition: 0.2s; }
            .export-race-title:hover { color: #fff; }
            .export-race-title.collapsed { color: #444; border-bottom-style: solid; border-color: #222; margin-bottom: 0; }
            
            .export-horse-line { display: flex; align-items: center; margin-bottom: 8px; font-size: 14px; }
            .export-horse-main { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
            .export-fav-badge { font-size: 11px; color: #ddd; border: 1px solid #555; border-radius: 4px; padding: 2px 6px; white-space: nowrap; }
        </style>
    </head>
    <body>
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid #333; padding-bottom: 10px;">
            <h3 style="margin: 0; font-size: 20px;">🪟 Live View Popout</h3>
            <a href="https://orepro.netkeiba.com/bet/race_list.html" target="_blank" style="background: #1dd1a1; color: black; padding: 6px 12px; text-decoration: none; border-radius: 4px; font-weight: bold; font-size: 14px;">🔗 Open OrePro</a>
        </div>

        <div id="racecourse-view">
            ${html}
        </div>
        
        <script>
            function toggleExportRace(safeId) {
                const content = document.getElementById('content-' + safeId);
                const title = document.getElementById('arrow-' + safeId).parentElement;
                const arrow = document.getElementById('arrow-' + safeId);
                
                if (content.style.display === 'none') {
                    content.style.display = ''; 
                    title.classList.remove('collapsed');
                    arrow.innerText = '▼';
                } else {
                    content.style.display = 'none';
                    title.classList.add('collapsed');
                    arrow.innerText = '▶';
                }
            }

            function toggleExportTrack(safeId) {
                const content = document.getElementById('content-track-' + safeId);
                const title = document.getElementById('arrow-track-' + safeId).parentElement;
                const arrow = document.getElementById('arrow-track-' + safeId);
                
                if (content.style.display === 'none') {
                    content.style.display = ''; 
                    title.classList.remove('collapsed');
                    arrow.innerText = '▼';
                } else {
                    content.style.display = 'none';
                    title.classList.add('collapsed');
                    arrow.innerText = '▶';
                }
            }
        </script>
    </body>
    </html>
    `;

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

const OREPRO_URL = 'https://orepro.netkeiba.com/bet/race_list.html';

// --- SETTINGS MODAL ---
function showSettingsModal() {
    // Populate checkboxes from current config
    document.getElementById('setting-raceDatabase').checked = appConfig.sidebarTabs?.raceDatabase ?? true;
    document.getElementById('setting-pedigreeLists').checked = appConfig.sidebarTabs?.pedigreeLists ?? true;
    document.getElementById('setting-autoPickStrategy').checked = appConfig.sidebarTabs?.autoPickStrategy ?? true;
    document.getElementById('setting-weekendWatchlist').checked = appConfig.sidebarTabs?.weekendWatchlist ?? true;
    document.getElementById('setting-betSafetyIndicator').checked = appConfig.ui?.betSafetyIndicator ?? true;
    document.getElementById('setting-voteSortingTop').checked = appConfig.ui?.voteSortingTop ?? true;
    document.getElementById('setting-autoFetchPastResults').checked = isAutoFetchPastResultsEnabled();
    document.getElementById('setting-autoLockPastVotes').checked = isAutoLockPastVotesEnabled();
        document.getElementById('setting-showConsole').checked = appConfig.ui?.showConsole ?? true;
    // Populate formula weight inputs
    const fw = getFormulaWeights();
    document.getElementById('fw-oddsCap').value            = fw.oddsCap;
    document.getElementById('fw-formMultiplier').value     = fw.formMultiplier;
    document.getElementById('fw-freshnessBonus').value     = fw.freshnessBonus;
    document.getElementById('fw-freshnessBreakeven').value = fw.freshnessBreakeven;
    document.getElementById('fw-pedigreeMultiplier').value = fw.pedigreeMultiplier;
    renderRaceColumnSettings();
    
    document.getElementById('settings-modal').style.display = 'flex';
}

function closeSettingsModal() {
    document.getElementById('settings-modal').style.display = 'none';
}

function resetFormulaWeights() {
    document.getElementById('fw-oddsCap').value            = 100;
    document.getElementById('fw-formMultiplier').value     = 100;
    document.getElementById('fw-freshnessBonus').value     = 3;
    document.getElementById('fw-freshnessBreakeven').value = 10;
    document.getElementById('fw-pedigreeMultiplier').value = 30;
    updateSidebarSettings();
}

async function updateSidebarSettings() {
    const previousAutoFetchPastResults = isAutoFetchPastResultsEnabled();
    // Update config from checkbox values
    appConfig.sidebarTabs = {
        raceDatabase: document.getElementById('setting-raceDatabase').checked,
        pedigreeLists: document.getElementById('setting-pedigreeLists').checked,
        autoPickStrategy: document.getElementById('setting-autoPickStrategy').checked,
        weekendWatchlist: document.getElementById('setting-weekendWatchlist').checked
    };
    const parseFWInput = (id, def) => { const n = parseFloat(document.getElementById(id).value); return isNaN(n) ? def : n; };
    appConfig.ui = {
        ...appConfig.ui,
        betSafetyIndicator: document.getElementById('setting-betSafetyIndicator').checked,
        voteSortingTop: document.getElementById('setting-voteSortingTop').checked,
        autoFetchPastResults: document.getElementById('setting-autoFetchPastResults').checked,
        autoLockPastVotes: document.getElementById('setting-autoLockPastVotes').checked,
            showConsole: document.getElementById('setting-showConsole').checked,
        formulaWeights: {
            oddsCap:            parseFWInput('fw-oddsCap',            100),
            formMultiplier:     parseFWInput('fw-formMultiplier',     100),
            freshnessBonus:     parseFWInput('fw-freshnessBonus',       3),
            freshnessBreakeven: parseFWInput('fw-freshnessBreakeven',  10),
            pedigreeMultiplier: parseFWInput('fw-pedigreeMultiplier',  30),
        }
    };
    
    // Save to server
    await fetch('/api/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(appConfig)
    });
    
    // Apply settings immediately to sidebar
    applySidebarSettings();
    updateAllRiskBadges();
    applyRaceTableLayoutSettings();

    if (!previousAutoFetchPastResults && isAutoFetchPastResultsEnabled()) {
        await refreshDataAndUI();
    }
}

function applyRaceTableLayoutSettings() {
    Object.keys(globalRaceEntries).forEach(r_id => {
        if (!raceSorts[r_id]) {
            raceSorts[r_id] = { col: 'Default', asc: true };
        }

        const thead = document.getElementById(`thead-${r_id}`);
        if (thead) thead.innerHTML = buildTableHeaderRow(r_id);

        const tbody = document.getElementById(`tbody-${r_id}`);
        if (tbody) tbody.innerHTML = buildTableBody(r_id, globalRaceEntries[r_id]);

        refreshRaceHeaderSortLabels(r_id);
    });
}

function renderRaceColumnSettings() {
    const container = document.getElementById('setting-race-columns');
    if (!container) return;

    const cols = getRaceColumnsLayout();
    container.innerHTML = cols.map((c, idx) => {
        const meta = RACE_COLUMN_META[c.key] || { label: c.key };
        const upDisabled = idx === 0 ? 'disabled' : '';
        const downDisabled = idx === cols.length - 1 ? 'disabled' : '';
        return `<div class="setting-column-row">
            <label class="setting-column-label">
                <input type="checkbox" ${c.visible ? 'checked' : ''} onchange="toggleRaceColumnVisibility('${c.key}', this.checked)">
                <span>${meta.label}</span>
            </label>
            <div class="setting-column-actions">
                <button type="button" ${upDisabled} onclick="moveRaceColumn('${c.key}', -1)">↑</button>
                <button type="button" ${downDisabled} onclick="moveRaceColumn('${c.key}', 1)">↓</button>
            </div>
        </div>`;
    }).join('');
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
}

function applySidebarSettings() {
    const raceDatabaseGroup = document.getElementById('race-database-group');
    const pedigreeListsGroup = document.getElementById('pedigree-lists-group');
    const autoPickGroup = document.getElementById('auto-pick-group');
    const weekendWatchlistGroup = document.getElementById('weekend-watchlist-group');

    if (raceDatabaseGroup) raceDatabaseGroup.open = appConfig.sidebarTabs?.raceDatabase ?? true;
    if (pedigreeListsGroup) pedigreeListsGroup.open = appConfig.sidebarTabs?.pedigreeLists ?? true;
    if (autoPickGroup) autoPickGroup.open = appConfig.sidebarTabs?.autoPickStrategy ?? true;
    if (weekendWatchlistGroup) weekendWatchlistGroup.open = appConfig.sidebarTabs?.weekendWatchlist ?? true;

    const consoleEl = document.getElementById('scrape-console');
    if (consoleEl) {
        consoleEl.style.display = (appConfig.ui?.showConsole ?? true) ? 'block' : 'none';
    }

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
function localizeRaceName(name) {
    if (!name) return "";
    let cleanName = name;

    // 1. Translate Ages (e.g., "4 Toshi Ijou" -> "4yo+", "3 Toshi" -> "3yo")
    cleanName = cleanName.replace(/(\d+)\s*Toshi\s*Ijou/ig, "$1yo+");
    cleanName = cleanName.replace(/(\d+)\s*Toshi/ig, "$1yo");

    // 2. Translate Classes
    cleanName = cleanName.replace(/Mishouri/ig, "Maiden");
    cleanName = cleanName.replace(/Shinba/ig, "Newcomer");
    cleanName = cleanName.replace(/1 Kachi Kurasu/ig, "ALW (1 Win)");
    cleanName = cleanName.replace(/2 Kachi Kurasu/ig, "ALW (2 Wins)");
    cleanName = cleanName.replace(/3 Kachi Kurasu/ig, "ALW (3 Wins)");
    cleanName = cleanName.replace(/Hanshin Supuringu J/ig, "Hanshin Spring Jump");
    
    // 3. Optional: Jump Races
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

// Hide search dropdown if the user clicks anywhere else on the screen
document.addEventListener('click', function(e) {
    const box = document.getElementById('search-suggestions');
    const input = document.getElementById('horse-search');
    if (box && input && e.target !== box && e.target !== input) {
        box.style.display = 'none';
    }
});

init();