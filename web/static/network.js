import { createApiClient } from "./api/client.js";
import { createModalController } from "./ui/modal.js";
import { setActiveTab as setActiveTabUi, initPrimaryTabKeyboardNav } from "./ui/tabs.js";
import { createChip } from "./ui/chip.js";
import { createCard } from "./ui/card.js";
import { enhanceDrawers, attachDrawerResetButton } from "./ui/drawer.js";
import { initTooltips } from "./ui/tooltip.js";
import { createDataTable } from "./ui/datatable.js";

let ws = null;
let wsMatches = null;
let network = null;
let nodes = null;
let edges = null;
let scanning = false;
let matchScraping = false;
let matchProgressTarget = 0;
let continuousScrapeEnabled = false;
let continuousStopRequested = false;
let continuousRestartTimer = null;
let lastMatchRowsScanned = 0;
let visLib = null;
let layoutTick = null;
let currentMatchUsername = "";
let currentStoredUsername = "";
let storedMatchesSource = [];
let storedMatchesCache = [];
let selectedStoredMatchIndex = -1;
let selectedStoredRoundIndex = -1;
let storedVirtualCleanup = null;
const STORED_VIRTUALIZE_THRESHOLD = 220;
const STORED_VIRTUAL_CARD_MIN_WIDTH = 240;
const STORED_VIRTUAL_CARD_GAP = 12;
const STORED_VIRTUAL_ROW_HEIGHT = 154;
const MATCH_RESULTS_VIRTUALIZE_THRESHOLD = 220;
const MATCH_RESULTS_CARD_MIN_WIDTH = 320;
const MATCH_RESULTS_CARD_GAP = 6;
const MATCH_RESULTS_ROW_HEIGHT = 188;
let matchResultsVirtualCleanup = null;
let matchResultsRenderTick = null;
let matchResultsStickToBottom = false;
const matchResultsById = new Map();
const matchResultsOrder = [];
const MAP_IMAGE_BASE = "/map-images";
const OPERATOR_IMAGE_BASE = "/operator-images";
const TRACKER_MAP_IMAGE_BASE = "https://trackercdn.com/cdn/tracker.gg/r6siege/db/images/maps";
const TRACKER_MAP_SLUG_OVERRIDES = {
    "club house": "clubhouse",
    "hereford base": "hereford",
    "theme park": "theme-park",
    "kafe dostoyevsky": "kafe-dostoyevsky",
    "nighthaven labs": "nighthaven-labs",
    "stadium alpha": "stadium-alpha",
    "stadium bravo": "stadium-bravo",
};
let operatorImageFileByKey = {};
let operatorImageIndexLoaded = false;
let operatorImageIndexEnabled = false;
let operatorImageIndexCount = 0;
let filterDrawerCompactMode = null;
const api = createApiClient();
const settingsModalController = createModalController("settings-modal");
let dashboardGraphRenderTimer = null;
let dashboardComputeInFlight = false;
let dashboardLastRequestSignature = "";
let dashboardRefreshQueued = false;
let computeReportState = {
    mode: "overall",
    dashboardView: "insights",
    graphPanel: "threat",
    username: "",
    stats: null,
    round: null,
    chemistry: null,
    lobby: null,
    trade: null,
    team: null,
    enemyThreat: null,
    atkDefHeatmap: null,
    operator: null,
    map: null,
    sorted: null,
    playbookFindings: [],
    evidenceByKey: {},
    selectedEvidenceKey: "",
    sectionVisibility: {
        playbook: true,
        deepStats: true,
        insightCards: true,
        performanceFocus: true,
    },
    workspace: {
        panel: "overview",
        filters: {},
        dataByPanel: {},
        meta: null,
        selection: { type: null },
        teamUi: {
            minMatches: 5,
            minRounds: 30,
            polarity: "all",
            hideNeutral: false,
            neutralThreshold: 1,
            sortBy: "rounds",
            sortDir: "desc",
            selectedPairKey: "",
        },
        evidenceCursor: "",
        evidenceRows: [],
        requestSeq: 0,
    },
};
let workspaceAutoRefreshTimer = null;
const WORKSPACE_REQUEST_TIMEOUT_MS = 45000;
let encounteredPlayersCache = [];
let teamBuilderFriendsCache = [];
let operatorsPlayerListCache = [];
let operatorsLastPayload = null;
let operatorsSelectedMap = "";
const MAP_IMAGE_FILE_BY_KEY = {
    "outback": "r6-maps-outback.avif",
    "oregon": "r6-maps-oregon.avif",
    "coastline": "r6-maps-coastline.avif",
    "favela": "r6-maps-favela__1_.avif",
    "hereford base": "r6-maps-hereford.avif",
    "hereford": "r6-maps-hereford.avif",
    "kanal": "r6-maps-kanal.avif",
    "tower": "r6-maps-tower.avif",
    "villa": "r6-maps-villa.avif",
    "bank": "R6S_Maps_Bank_EXT.avif",
    "border": "R6S_Maps_Border_EXT.avif",
    "chalet": "R6S_Maps_Chalet_EXT.avif",
    "club house": "R6S_Maps_ClubHouse_EXT.avif",
    "clubhouse": "R6S_Maps_ClubHouse_EXT.avif",
    "emerald plains": "r6s_maps_emeraldplains__1_.avif",
    "kafe dostoyevsky": "R6S_Maps_RussianCafe_EXT.avif",
    "kafe": "R6S_Maps_RussianCafe_EXT.avif",
    "skyscraper": "skycraper_modernized_keyart.avif",
    "theme park": "themepark_modernized_keyart.avif",
    "nighthaven labs": "ModernizedMap_Nighthaven_keyart.avif",
    "nighthaven": "ModernizedMap_Nighthaven_keyart.avif",
    "lair": "ModernizedMap_Lair_keyart.avif",
    "consulate": "ModernizedMap_Consulate_keyart.avif",
    "fortress": "fortress-reworked-thumbnail.avif",
    "stadium": "StadiumA_keyart.avif",
    "stadium a": "StadiumA_keyart.avif",
    "stadium b": "stadiumB_keyart.avif",
};
const OPERATOR_KEY_ALIASES = {
    "deimos": "deimos",
    "fenrir": "fenrir",
    "ram": "ram",
    "brava": "brava",
    "solis": "solis",
    "grim": "grim",
    "sens": "sens",
    "azami": "azami",
    "osa": "osa",
    "thunderbird": "thunderbird",
    "flores": "flores",
    "aruni": "aruni",
    "zero": "zero",
    "ace": "ace",
    "melusi": "melusi",
    "iana": "iana",
    "oryx": "oryx",
    "wamai": "wamai",
    "kali": "kali",
    "amaru": "amaru",
    "goyo": "goyo",
    "nokk": "nokk",
    "warden": "warden",
    "mozzie": "mozzie",
    "gridlock": "gridlock",
    "nomad": "nomad",
    "clash": "clash",
    "maverick": "maverick",
    "alibi": "alibi",
    "maestro": "maestro",
    "lion": "lion",
    "finka": "finka",
    "vigil": "vigil",
    "dokkaebi": "dokkaebi",
    "zofia": "zofia",
    "ela": "ela",
    "ying": "ying",
    "lesion": "lesion",
    "jackal": "jackal",
    "mira": "mira",
    "echo": "echo",
    "hibana": "hibana",
    "capitao": "capitao",
    "caveira": "caveira",
    "valkyrie": "valkyrie",
    "blackbeard": "blackbeard",
    "buck": "buck",
    "frost": "frost",
    "mute": "mute",
    "smoke": "smoke",
    "sledge": "sledge",
    "thatcher": "thatcher",
    "ash": "ash",
    "thermite": "thermite",
    "montagne": "montagne",
    "twitch": "twitch",
    "glaz": "glaz",
    "fuze": "fuze",
    "blitz": "blitz",
    "iq": "iq",
    "bandit": "bandit",
    "jager": "jager",
    "jaeger": "jager",
    "rooke": "rook",
    "rook": "rook",
    "doc": "doc",
    "castle": "castle",
    "pulse": "pulse",
    "kapkan": "kapkan",
    "tachanka": "tachanka",
};

function toNumber(value, fallback = 0) {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

function normalizeMapKey(value) {
    return String(value || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim();
}

function resolveMapImageUrl(mapName) {
    const key = normalizeMapKey(mapName);
    if (!key) return "";

    if (MAP_IMAGE_FILE_BY_KEY[key]) {
        return `${MAP_IMAGE_BASE}/${MAP_IMAGE_FILE_BY_KEY[key]}`;
    }

    for (const [candidate, filename] of Object.entries(MAP_IMAGE_FILE_BY_KEY)) {
        if (key.includes(candidate) || candidate.includes(key)) {
            return `${MAP_IMAGE_BASE}/${filename}`;
        }
    }

    return "";
}

function normalizeMapSlug(mapName) {
    const key = normalizeMapKey(mapName);
    if (!key) return "unknown-map";
    const forced = TRACKER_MAP_SLUG_OVERRIDES[key];
    if (forced) return forced;
    return key
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "");
}

function trackerMapImageUrl(mapName) {
    const slug = normalizeMapSlug(mapName);
    return `${TRACKER_MAP_IMAGE_BASE}/${encodeURIComponent(slug)}.png`;
}

function localStaticMapImageUrl(mapName) {
    const slug = normalizeMapSlug(mapName);
    return `/static/maps/${encodeURIComponent(slug)}.jpg`;
}

function attachMatchCardImageFallback(imgEl, placeholderEl, mapName) {
    if (!imgEl) return;
    const chain = [
        trackerMapImageUrl(mapName),
        localStaticMapImageUrl(mapName),
    ];
    let idx = 0;
    const showPlaceholder = () => {
        imgEl.hidden = true;
        if (placeholderEl) {
            placeholderEl.hidden = false;
            placeholderEl.textContent = String(mapName || "Unknown map");
        }
        console.warn(
            `[match-card] Placeholder map image used for "${mapName}" (normalized: "${normalizeMapSlug(mapName)}").`
        );
    };
    const tryNext = () => {
        if (idx >= chain.length) {
            showPlaceholder();
            return;
        }
        const nextUrl = chain[idx];
        idx += 1;
        imgEl.src = nextUrl;
    };
    imgEl.addEventListener("error", tryNext);
    imgEl.addEventListener("load", () => {
        imgEl.hidden = false;
        if (placeholderEl) placeholderEl.hidden = true;
    });
    tryNext();
}

function matchTypeBadgeMeta(modeRaw) {
    const mode = String(modeRaw || "").trim().toLowerCase();
    if (mode.includes("unranked")) return { label: "Unranked", cls: "unranked" };
    if (mode.includes("ranked")) return { label: "Ranked", cls: "ranked" };
    if (mode.includes("quick")) return { label: "Quick", cls: "quick" };
    return { label: modeRaw || "Other", cls: "other" };
}

function extractRpDelta(matchData) {
    const candidates = [
        matchData?.rank_points_delta,
        matchData?.rp_delta,
        matchData?.rp_change,
        matchData?.mmr_delta,
    ];
    for (const value of candidates) {
        if (value == null || value === "") continue;
        const n = Number(value);
        if (Number.isFinite(n)) return n;
    }
    return null;
}

function formatMatchAgeLabel(rawDate) {
    const text = String(rawDate || "").trim();
    if (!text) return "Unknown time";
    let ms = Date.parse(text);
    if (!Number.isFinite(ms)) {
        ms = Date.parse(text.replace(" ", "T"));
    }
    if (!Number.isFinite(ms)) return text;
    const dt = new Date(ms);
    const diff = Date.now() - dt.getTime();
    const minute = 60 * 1000;
    const hour = 60 * minute;
    const day = 24 * hour;
    if (diff >= 0 && diff < hour) {
        return `${Math.max(1, Math.floor(diff / minute))}m ago`;
    }
    if (diff >= 0 && diff < day) {
        return `${Math.max(1, Math.floor(diff / hour))}h ago`;
    }
    if (diff >= 0 && diff < (7 * day)) {
        return `${Math.max(1, Math.floor(diff / day))}d ago`;
    }
    return dt.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function formatPerspectiveScoreLabel(perspective) {
    const mine = toNumber(perspective?.myScore, 0);
    const opp = toNumber(perspective?.oppScore, 0);
    if (perspective?.result === "Win") return `W ${mine}-${opp}`;
    if (perspective?.result === "Loss") return `L ${opp}-${mine}`;
    return mine === opp ? `D ${mine}-${opp}` : `${mine}-${opp}`;
}

function normalizeOperatorKey(value) {
    return String(value || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim();
}

function extractOperatorName(raw) {
    if (!raw) return "";
    if (typeof raw === "string") return raw.trim();
    if (typeof raw === "object") {
        const keys = ["name", "operatorName", "operator", "label", "value", "slug"];
        for (const key of keys) {
            const value = raw[key];
            if (typeof value === "string" && value.trim()) {
                return value.trim();
            }
        }
    }
    return "";
}

async function loadOperatorImageIndex() {
    try {
        const res = await api.getOperatorImageIndex();
        if (!res.ok) return;
        const payload = await res.json();
        operatorImageIndexLoaded = true;
        operatorImageIndexEnabled = Boolean(payload?.enabled);
        operatorImageIndexCount = toNumber(payload?.count, 0);
        if (!payload?.enabled || !payload?.files || typeof payload.files !== "object") return;
        operatorImageFileByKey = payload.files;
    } catch (_) {
        // Optional asset index; ignore failures.
    }
}

function operatorFallbackBadge(operatorName) {
    const clean = String(operatorName || "").trim();
    if (!clean) return "";
    const letters = clean
        .replace(/[^a-z0-9 ]+/gi, " ")
        .trim()
        .split(/\s+/)
        .filter(Boolean)
        .slice(0, 2)
        .map((part) => part[0]?.toUpperCase() || "")
        .join("");
    const badge = letters || clean.slice(0, 2).toUpperCase();
    return `<span class="stored-op-fallback" title="${escapeHtml(clean)}">${escapeHtml(badge)}</span>`;
}

function resolveOperatorImageUrl(operatorName) {
    const key = normalizeOperatorKey(operatorName);
    if (!key) return "";

    const direct = operatorImageFileByKey[key];
    if (direct) return `${OPERATOR_IMAGE_BASE}/${encodeURIComponent(direct)}`;

    const alias = OPERATOR_KEY_ALIASES[key];
    if (alias && operatorImageFileByKey[alias]) {
        return `${OPERATOR_IMAGE_BASE}/${encodeURIComponent(operatorImageFileByKey[alias])}`;
    }

    for (const [candidate, filename] of Object.entries(operatorImageFileByKey)) {
        if (key.includes(candidate) || candidate.includes(key)) {
            return `${OPERATOR_IMAGE_BASE}/${encodeURIComponent(filename)}`;
        }
    }
    // CDN fallback for environments where local icon pack is unavailable.
    const cdnKey = OPERATOR_KEY_ALIASES[key] || key.replace(/\s+/g, "");
    if (cdnKey) {
        return `https://trackercdn.com/cdn/r6.tracker.network/operators/badges/${encodeURIComponent(cdnKey)}.png`;
    }
    return "";
}

function requestLayoutRefresh() {
    if (!network) return;
    if (layoutTick) clearTimeout(layoutTick);
    layoutTick = setTimeout(() => {
        try {
            network.redraw();
            network.startSimulation();
        } catch (err) {
            log(`Layout refresh failed: ${err}`, "error");
        }
    }, 80);
}

function initNetwork() {
    visLib = window.vis || window.visNetwork || null;
    if (!visLib || !visLib.Network || !visLib.DataSet) {
        log("vis-network failed to load. Check /static/vis-network.min.js.", "error");
        return;
    }

    nodes = new visLib.DataSet();
    edges = new visLib.DataSet();

    const container = document.getElementById("network");
    const data = { nodes, edges };
    const options = {
        autoResize: true,
        interaction: {
            hover: true,
            tooltipDelay: 80,
            hideEdgesOnDrag: false,
            hideEdgesOnZoom: false,
            navigationButtons: true,
            keyboard: true,
        },
        nodes: {
            shape: "dot",
            borderWidth: 1,
            font: { size: 14, color: "#f5f5f5" },
            scaling: { min: 8, max: 40 },
        },
        edges: {
            smooth: { type: "dynamic" },
            scaling: { min: 1, max: 12 },
            font: {
                align: "top",
            },
            color: { inherit: false, opacity: 0.35 },
        },
        physics: {
            enabled: true,
            stabilization: { enabled: true, iterations: 250 },
            solver: "barnesHut",
            barnesHut: {
                gravitationalConstant: -8000,
                springLength: 140,
                springConstant: 0.03,
                damping: 0.2,
                avoidOverlap: 0.6,
            },
        },
    };

    network = new visLib.Network(container, data, options);
    log("vis-network initialized successfully.");

    // Seed graph confirms rendering works before live scan data arrives.
    nodes.add([
        { id: "__seed_root__", label: "JAKAL", color: "#4ade80", size: 22, title: "Renderer check" },
        { id: "__seed_peer__", label: "Ready", color: "#3b82f6", size: 16, title: "Renderer check" },
    ]);
    edges.add([{ id: "__seed_edge__", from: "__seed_root__", to: "__seed_peer__", label: "ok", width: 2 }]);
    document.getElementById("node-count").textContent = nodes.length;
    document.getElementById("edge-count").textContent = edges.length;
    network.fit({ animation: { duration: 0 } });
}

function startScan() {
    if (!nodes || !edges || !network) {
        log("Graph is not initialized. Refresh the page and try again.", "error");
        return;
    }

    const username = document.getElementById("username").value.trim();
    const depth = parseInt(document.getElementById("depth").value, 10);
    const debugBrowser = document.getElementById("debug-browser").checked;

    if (!username) {
        alert("Please enter a username");
        return;
    }

    nodes.clear();
    edges.clear();
    document.getElementById("log").innerHTML = "";
    document.getElementById("edge-count").textContent = "0";
    document.getElementById("scan-status").textContent = "Scanning";

    // Add the queried player immediately as a placeholder node.
    nodes.add({
        id: username,
        label: username,
        color: "#666666",
        size: 30,
        font: { color: "#f5f5f5" },
        title: "Pending stats...",
    });
    document.getElementById("node-count").textContent = nodes.length;
    requestLayoutRefresh();

    scanning = true;
    document.getElementById("start-scan").disabled = true;
    document.getElementById("stop-scan").disabled = false;

    ws = api.openScanWebSocket();

    ws.onopen = () => {
        log("Connected to server");
        ws.send(JSON.stringify({ username, max_depth: depth, debug_browser: debugBrowser }));
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        handleMessage(data);
    };

    ws.onclose = () => {
        log("Disconnected from server");
        scanning = false;
        document.getElementById("start-scan").disabled = false;
        document.getElementById("stop-scan").disabled = true;
        if (document.getElementById("scan-status").textContent !== "Complete") {
            document.getElementById("scan-status").textContent = "Idle";
        }
    };

    ws.onerror = (error) => {
        log(`Error: ${error}`, "error");
    };
}

function stopScan() {
    if (ws) {
        ws.close();
    }
    scanning = false;
    document.getElementById("start-scan").disabled = false;
    document.getElementById("stop-scan").disabled = true;
    document.getElementById("scan-status").textContent = "Stopped";
}

function setGlobalScrapeRunning(isRunning) {
    const indicator = document.getElementById("global-scrape-indicator");
    if (!indicator) return;
    indicator.classList.toggle("hidden", !isRunning);
}

function setMatchProgress(processed, total, options = {}) {
    const bar = document.getElementById("match-progress-bar");
    const text = document.getElementById("match-progress-text");
    const wrap = document.getElementById("match-progress-wrap");
    if (!bar || !text) return;
    const animateOpenEnded = options.animateOpenEnded !== false;
    const safeProcessed = Math.max(0, toNumber(processed, 0));
    const rawTotal = toNumber(total, 0);
    const isOpenEnded = continuousScrapeEnabled || rawTotal <= 0;
    if (isOpenEnded) {
        if (animateOpenEnded) {
            wrap?.classList.add("indeterminate");
            bar.style.width = "35%";
        } else {
            wrap?.classList.remove("indeterminate");
            bar.style.transform = "";
            bar.style.width = "100%";
        }
        text.textContent = `${safeProcessed} scanned`;
        return;
    }
    wrap?.classList.remove("indeterminate");
    bar.style.transform = "";
    const safeTotal = Math.max(1, rawTotal);
    const pct = Math.min(100, (safeProcessed / safeTotal) * 100);
    bar.style.width = `${pct.toFixed(1)}%`;
    text.textContent = `${safeProcessed}/${safeTotal} (${pct.toFixed(1)}%)`;
}

function syncContinuousControls() {
    const runForeverEl = document.getElementById("matches-run-forever");
    const newestOnlyEl = document.getElementById("matches-newest-only");
    const fullBackfillEl = document.getElementById("matches-full-backfill");
    const maxMatchesEl = document.getElementById("max-matches");
    if (!runForeverEl || !maxMatchesEl || !newestOnlyEl || !fullBackfillEl) return;
    const isContinuous = runForeverEl.checked;
    const isNewest = newestOnlyEl.checked;
    const isFullBackfill = fullBackfillEl.checked;
    maxMatchesEl.disabled = isContinuous || isNewest || isFullBackfill;
    if (isContinuous) {
        maxMatchesEl.title = "Disabled in continuous mode";
    } else if (isNewest) {
        maxMatchesEl.title = "Disabled in newest mode (open-ended scan)";
    } else if (isFullBackfill) {
        maxMatchesEl.title = "Disabled in full backfill mode (open-ended scan)";
    } else {
        maxMatchesEl.title = "";
    }
}

function syncScrapeModeControls() {
    const newestOnlyEl = document.getElementById("matches-newest-only");
    const fullBackfillEl = document.getElementById("matches-full-backfill");
    const runForeverEl = document.getElementById("matches-run-forever");
    if (!newestOnlyEl || !fullBackfillEl || !runForeverEl) return;
    if (fullBackfillEl.checked) {
        newestOnlyEl.checked = false;
        newestOnlyEl.disabled = true;
        runForeverEl.checked = false;
        runForeverEl.disabled = true;
    } else {
        newestOnlyEl.disabled = false;
        runForeverEl.disabled = false;
    }
    if (newestOnlyEl.checked) {
        fullBackfillEl.checked = false;
    }
    syncContinuousControls();
}

function startMatchScrape(autoRestart = false) {
    const username = document.getElementById("matches-username").value.trim();
    const maxMatches = parseInt(document.getElementById("max-matches").value, 10);
    const debugBrowser = document.getElementById("matches-debug-browser").checked;
    const newestOnly = document.getElementById("matches-newest-only").checked;
    const fullBackfill = document.getElementById("matches-full-backfill").checked;
    const runForever = document.getElementById("matches-run-forever").checked;
    const allowedMatchTypes = Array.from(document.querySelectorAll(".matches-type-filter"))
        .filter((el) => el.checked)
        .map((el) => String(el.value || "").trim().toLowerCase())
        .filter(Boolean);
    if (!username) {
        alert("Please enter a username");
        return;
    }
    if ((newestOnly || fullBackfill) && !allowedMatchTypes.length) {
        alert("Select at least one allowed match type for newest or full backfill mode.");
        return;
    }
    if (!autoRestart) {
        continuousScrapeEnabled = runForever;
        continuousStopRequested = false;
        if (continuousRestartTimer) {
            clearTimeout(continuousRestartTimer);
            continuousRestartTimer = null;
        }
    }
    const openEndedNewest = newestOnly;
    const openEndedBackfill = fullBackfill;
    if (continuousScrapeEnabled && !newestOnly) {
        logMatch("Continuous mode works best with Get Newest Matches enabled.", "info");
    }
    const effectiveMaxMatches = (continuousScrapeEnabled || openEndedNewest || openEndedBackfill) ? 1000000 : maxMatches;

    if (!autoRestart) {
        document.getElementById("match-log").innerHTML = "";
        document.getElementById("match-count").textContent = "0";
        document.getElementById("current-match").textContent = "-";
        document.getElementById("match-status").textContent = "Starting";
        lastMatchRowsScanned = 0;
    } else {
        document.getElementById("match-status").textContent = "Restarting";
        lastMatchRowsScanned = 0;
    }
    matchProgressTarget = (continuousScrapeEnabled || openEndedNewest || openEndedBackfill) ? 0 : Math.max(1, toNumber(maxMatches, 1));
    setMatchProgress(0, matchProgressTarget);
    document.getElementById("start-match-scrape").disabled = true;
    document.getElementById("stop-match-scrape").disabled = false;
    matchScraping = true;
    setGlobalScrapeRunning(true);
    currentMatchUsername = username;

    wsMatches = api.openMatchScrapeWebSocket();
    wsMatches.onopen = () => {
        logMatch("Connected to match scraper");
        if (debugBrowser) {
            logMatch("Debug browser mode enabled (headful Playwright window).");
        }
        if (continuousScrapeEnabled) {
            logMatch("Continuous scrape mode active. Press Stop to end loop.", "info");
        }
        if (openEndedNewest && !continuousScrapeEnabled) {
            logMatch("Newest mode is open-ended: scanning until already-stored boundary is found.", "info");
        }
        if (fullBackfill) {
            logMatch("Full backfill mode: collecting all available matches until Load More is exhausted.", "info");
        }
        if (newestOnly) {
            logMatch(
                `Newest mode enabled. Allowed types: ${allowedMatchTypes.join(", ")}`,
                "info"
            );
        }
        wsMatches.send(
            JSON.stringify({
                username,
                max_matches: effectiveMaxMatches,
                debug_browser: debugBrowser,
                newest_only: newestOnly && !fullBackfill,
                full_backfill: fullBackfill,
                allowed_match_types: allowedMatchTypes,
            })
        );
    };

    wsMatches.onmessage = (event) => {
        const data = JSON.parse(event.data);
        handleMatchMessage(data);
    };

    wsMatches.onclose = () => {
        logMatch("Match scraper disconnected");
        matchScraping = false;
        setMatchProgress(lastMatchRowsScanned, matchProgressTarget, { animateOpenEnded: false });
        if (
            continuousScrapeEnabled &&
            !continuousStopRequested &&
            !document.getElementById("stop-match-scrape").disabled
        ) {
            document.getElementById("match-status").textContent = "Waiting";
            logMatch("Continuous mode: next cycle in 10s...", "info");
            continuousRestartTimer = setTimeout(() => {
                continuousRestartTimer = null;
                if (!continuousStopRequested && continuousScrapeEnabled) {
                    startMatchScrape(true);
                }
            }, 10000);
        } else {
            setGlobalScrapeRunning(false);
            document.getElementById("start-match-scrape").disabled = false;
            document.getElementById("stop-match-scrape").disabled = true;
            if (document.getElementById("match-status").textContent !== "Complete") {
                document.getElementById("match-status").textContent = "Idle";
            }
        }
    };

    wsMatches.onerror = (error) => {
        logMatch(`Error: ${error}`, "error");
    };
}

function stopMatchScrape() {
    continuousStopRequested = true;
    continuousScrapeEnabled = false;
    if (continuousRestartTimer) {
        clearTimeout(continuousRestartTimer);
        continuousRestartTimer = null;
    }
    if (wsMatches && wsMatches.readyState === WebSocket.OPEN) {
        wsMatches.send(JSON.stringify({ action: "stop" }));
        document.getElementById("match-status").textContent = "Stopping...";
        document.getElementById("stop-match-scrape").disabled = true;
        logMatch("Stop requested. Waiting for current match to finish...", "info");
        return;
    }
    if (wsMatches) {
        wsMatches.close();
    }
    matchScraping = false;
    setGlobalScrapeRunning(false);
    setMatchProgress(0, 1, { animateOpenEnded: false });
    document.getElementById("start-match-scrape").disabled = false;
    document.getElementById("stop-match-scrape").disabled = true;
    document.getElementById("match-status").textContent = "Stopped";
}

function handleMessage(data) {
    switch (data.type) {
        case "scan_started":
            log(`Scanning network for ${data.username} (depth: ${data.max_depth})`);
            if (data.debug_browser) {
                log("Debug browser mode enabled (headful Playwright window).");
            }
            document.getElementById("scan-status").textContent = "Running";
            break;
        case "scanning":
            document.getElementById("current-scan").textContent = data.username;
            log(`Scanning ${data.username} (depth ${data.depth})...`);
            break;
        case "node_discovered":
            addNode(data);
            const discoveredRank = rankFromRP(toNumber(data.stats?.rank_points));
            log(
                `Found ${data.username} (${discoveredRank.label}, KD: ${toNumber(data.stats?.kd).toFixed(2)})`
            );
            break;
        case "encounters_found":
            log(`${data.username} has ${data.count} connections`);
            break;
        case "edge_discovered":
            addEdge(data);
            log(`Encounter: ${data.from} -> ${data.to} (${toNumber(data.match_count, 1)} matches)`);
            break;
        case "scan_complete":
            log("Scan complete!", "success");
            document.getElementById("current-scan").textContent = "Complete";
            document.getElementById("scan-status").textContent = "Complete";
            break;
        case "error":
            log(`Error: ${data.message}`, "error");
            break;
        case "warning":
            log(`Warning: ${data.message}`, "info");
            break;
        case "delay":
            log(`Delay ${toNumber(data.seconds, 0).toFixed(1)}s: ${data.reason || "waiting"}`);
            break;
        case "scan_summary":
            log(
                `Scan summary: failures=${toNumber(data.total_failures, 0)} last_error=${data.last_error || "none"}`,
                "info"
            );
            break;
        case "debug":
            log(data.message, "info");
            console.log("[DEBUG]", data.message);
            break;
    }
}

function handleMatchMessage(data) {
    switch (data.type) {
        case "scraping_match":
            const newestOnly = document.getElementById("matches-newest-only")?.checked === true;
            const fullBackfill = document.getElementById("matches-full-backfill")?.checked === true;
            const openEnded = continuousScrapeEnabled || newestOnly || fullBackfill;
            lastMatchRowsScanned = Math.max(lastMatchRowsScanned, toNumber(data.match_number, 0));
            if (openEnded) {
                document.getElementById("current-match").textContent = `${data.match_number}`;
            } else {
                document.getElementById("current-match").textContent = `${data.match_number}/${data.total}`;
            }
            document.getElementById("match-status").textContent = "Running";
            setMatchProgress(data.match_number, openEnded ? 0 : data.total);
            if (openEnded) {
                logMatch(`Scraping match ${data.match_number}...`);
            } else {
                logMatch(`Scraping match ${data.match_number} of ${data.total}...`);
            }
            break;
        case "match_scraped":
            appendMatchResult(data.match_data, "captured");
            document.getElementById("match-count").textContent = `${toNumber(
                document.getElementById("match-count").textContent,
                0
            ) + 1}`;
            if (data.match_data && data.match_data.partial_capture) {
                const reason = data.match_data.partial_reason || "missing round data";
                logMatch(`Partial capture (${reason})`, "info");
            } else {
                logMatch("Match details captured", "success");
            }
            break;
        case "match_seen":
            appendMatchResult(data.match_data, data.status || "captured");
            document.getElementById("match-count").textContent = `${toNumber(
                document.getElementById("match-count").textContent,
                0
            ) + 1}`;
            if (data.status === "filtered") {
                logMatch("Match seen during scan (filtered by allowed types).", "info");
            } else if (data.status === "skipped_complete") {
                logMatch("Match seen during scan (already complete in DB).", "info");
            }
            break;
        case "match_filtered":
            logMatch(
                `Skipped match (${data.mode || "Unknown"}) due to allowed-types filter.`,
                "info"
            );
            break;
        case "match_scraping_complete":
            document.getElementById("match-status").textContent = continuousScrapeEnabled ? "Cycle Complete" : "Complete";
            const newestOnlyDone = document.getElementById("matches-newest-only")?.checked === true;
            const fullBackfillDone = document.getElementById("matches-full-backfill")?.checked === true;
            const openEndedDone = continuousScrapeEnabled || newestOnlyDone || fullBackfillDone;
            const completedScanned = toNumber(data.rows_scanned, toNumber(data.total_matches, 0));
            lastMatchRowsScanned = Math.max(lastMatchRowsScanned, completedScanned);
            setMatchProgress(
                completedScanned,
                openEndedDone ? 0 : (matchProgressTarget || toNumber(data.total_matches, 1)),
                { animateOpenEnded: false }
            );
            logMatch(`Match scraping complete (${data.total_matches} matches)`, "success");
            break;
        case "matches_saved":
            logMatch(`Saved ${toNumber(data.saved_matches, 0)} matches for ${data.username}`, "success");
            break;
        case "matches_unpacked":
            const u = data?.stats || {};
            logMatch(
                `Unpack pass complete for ${data.username}: unpacked=${toNumber(u.unpacked_matches, 0)}, ` +
                `scanned=${toNumber(u.scanned, 0)}, skipped=${toNumber(u.skipped, 0)}, errors=${toNumber(u.errors, 0)}`,
                "success"
            );
            break;
        case "warning":
            logMatch(`Warning: ${data.message}`, "info");
            break;
        case "stop_ack":
            logMatch(`Server: ${data.message}`, "info");
            break;
        case "debug":
            logMatch(`Debug: ${data.message}`, "info");
            break;
        case "error":
            logMatch(`Error: ${data.message}`, "error");
            break;
    }
}

function addNode(data) {
    const rp = toNumber(data.stats?.rank_points);
    const kd = toNumber(data.stats?.kd);
    const winPct = toNumber(data.stats?.win_pct);
    const rank = rankFromRP(rp);
    const color = getRankColor(rp);
    const size = 15 + (data.depth === 0 ? 15 : 0);

    const existing = nodes.get(data.username);
    const nodePayload = {
        id: data.username,
        label: data.username,
        color: color,
        size: size,
        font: { color: "#f5f5f5" },
        title: `Rank: ${rank.label}\nRP: ${rp}\nKD: ${kd.toFixed(2)}\nWin%: ${winPct.toFixed(1)}%`,
    };

    if (existing) {
        nodes.update(nodePayload);
    } else {
        nodes.add(nodePayload);
    }

    document.getElementById("node-count").textContent = nodes.length;
    requestLayoutRefresh();
}

function addEdge(data) {
    const from = (data.from || "").trim();
    const to = (data.to || "").trim();
    const encounterCount = toNumber(data.match_count, 0);
    if (!from || !to || encounterCount <= 0) return;

    // Keep relationships directional so counts from A->B don't overwrite B->A.
    const edgeId = `${from}->${to}`;

    if (!nodes.get(to)) {
        nodes.add({
            id: to,
            label: to,
            color: "#666666",
            size: 15,
            font: { color: "#f5f5f5" },
            title: `${encounterCount} matches with ${from}\nNot scanned yet`,
        });
        document.getElementById("node-count").textContent = nodes.length;
    }
    if (edges.get(edgeId)) return;
    edges.add({
        id: edgeId,
        from: from,
        to: to,
        label: `${encounterCount}`,
        title: `${from} -> ${to}: ${encounterCount} encounters`,
        width: Math.min(1 + encounterCount / 10, 6),
    });

    document.getElementById("edge-count").textContent = edges.length;
    requestLayoutRefresh();
}

function getRankColor(rp) {
    if (rp >= 4000) return "#ff0080";
    if (rp >= 3500) return "#00d4aa";
    if (rp >= 3000) return "#b8b8ff";
    if (rp >= 2500) return "#ffd700";
    if (rp >= 2000) return "#c0c0c0";
    if (rp >= 1500) return "#cd7f32";
    return "#8b4513";
}

function rankFromRP(rp) {
    if (!rp || rp < 1000) {
        return { tier: "Unranked", division: null, label: "Unranked" };
    }

    const tiers = [
        { name: "Copper", floor: 1000 },
        { name: "Bronze", floor: 1500 },
        { name: "Silver", floor: 2000 },
        { name: "Gold", floor: 2500 },
        { name: "Platinum", floor: 3000 },
        { name: "Emerald", floor: 3500 },
        { name: "Diamond", floor: 4000 },
    ];

    if (rp >= 4500) {
        return {
            tier: "Champion",
            division: null,
            label: `Champion (${rp} RP)`,
        };
    }

    let tierIndex = tiers.length - 1;
    for (let i = 0; i < tiers.length; i++) {
        if (rp < tiers[i].floor) {
            tierIndex = i - 1;
            break;
        }
    }

    const tier = tiers[tierIndex];
    const withinTier = rp - tier.floor;
    const divisions = ["V", "IV", "III", "II", "I"];
    const divIndex = Math.floor(withinTier / 100);
    const division = divisions[divIndex];

    return {
        tier: tier.name,
        division: division,
        label: `${tier.name} ${division} (${rp} RP)`,
    };
}

const logBufferByTarget = new Map();
let logFlushTick = null;

function queueLogEntry(targetId, message, type = "info") {
    const key = String(targetId || "");
    if (!key) return;
    if (!logBufferByTarget.has(key)) {
        logBufferByTarget.set(key, []);
    }
    logBufferByTarget.get(key).push({
        text: `[${new Date().toLocaleTimeString()}] ${message}`,
        type: String(type || "info"),
    });
    if (logFlushTick != null) return;
    logFlushTick = window.requestAnimationFrame(() => {
        logFlushTick = null;
        for (const [id, entries] of logBufferByTarget.entries()) {
            const logDiv = document.getElementById(id);
            if (!logDiv || !entries.length) continue;
            const shouldStick = (logDiv.scrollHeight - logDiv.scrollTop - logDiv.clientHeight) < 40;
            const frag = document.createDocumentFragment();
            for (const entryData of entries.splice(0, entries.length)) {
                const entry = document.createElement("div");
                entry.className = `log-entry log-${entryData.type}`;
                entry.textContent = entryData.text;
                frag.appendChild(entry);
            }
            logDiv.appendChild(frag);
            if (shouldStick) {
                logDiv.scrollTop = logDiv.scrollHeight;
            }
        }
    });
}

function log(message, type = "info") {
    queueLogEntry("log", message, type);
}

function logMatch(message, type = "info") {
    queueLogEntry("match-log", message, type);
}

function appendMatchResult(matchData, status = "captured") {
    appendMatchResultRow(matchData, status);
}

function cleanupMatchResultsVirtualization() {
    if (typeof matchResultsVirtualCleanup === "function") {
        matchResultsVirtualCleanup();
    }
    matchResultsVirtualCleanup = null;
}

function buildMatchResultCardElement(entry, cardWidthPx = null) {
    const matchData = entry?.matchData || {};
    const status = String(entry?.status || "captured");
    const matchId = String(matchData?.match_id || "unknown-id");
    const statusClass =
        status === "filtered" ? "match-results-filtered" :
        status === "skipped_complete" ? "match-results-skipped" :
        "match-results-captured";
    const statusLabel =
        status === "filtered" ? "filtered" :
        status === "skipped_complete" ? "skipped" :
        "captured";
    const shortMatchId =
        matchId.length > 14
            ? `${matchId.slice(0, 8)}...${matchId.slice(-4)}`
            : matchId;
    const map = matchData?.map || "Unknown map";
    const perspective = inferTeamPerspective(matchData, currentMatchUsername);
    const result = perspective.result;
    const resultClass =
        result === "Win" ? "stored-result-win" :
        result === "Loss" ? "stored-result-loss" :
        "stored-result-unknown";
    const resultAccentClass =
        result === "Win" ? "match-result-win" :
        result === "Loss" ? "match-result-loss" :
        "match-result-draw";
    const badge = matchTypeBadgeMeta(matchData?.mode);
    const scoreLabel = formatPerspectiveScoreLabel(perspective);
    const rpDelta = extractRpDelta(matchData);
    const rpClass =
        rpDelta == null ? "" :
        rpDelta > 0 ? "stored-rp-pos" :
        rpDelta < 0 ? "stored-rp-neg" :
        "stored-rp-zero";
    const rpText =
        rpDelta == null ? "" :
        `${rpDelta > 0 ? "+" : ""}${Math.round(rpDelta)} RP`;
    const timeLabel = formatMatchAgeLabel(matchData?.date);
    const escapedMap = escapeHtml(String(map));
    const entryEl = document.createElement("div");
    entryEl.className = `stored-match-card match-results-card ${statusClass} ${resultAccentClass}`;
    entryEl.dataset.matchId = matchId;
    if (Number.isFinite(cardWidthPx) && cardWidthPx > 0) {
        entryEl.style.width = `${Math.floor(cardWidthPx)}px`;
    }
    entryEl.innerHTML = `
        <div class="match-card-media">
            <img class="match-card-map-image" alt="${escapedMap}" loading="lazy" decoding="async">
            <div class="match-card-map-placeholder" hidden>${escapedMap}</div>
            <div class="match-card-image-overlay"></div>
            <div class="match-card-map-name">${escapedMap}</div>
            <span class="match-card-type-badge ${badge.cls}">${escapeHtml(String(badge.label || "Other"))}</span>
        </div>
        <div class="match-card-body">
            <div class="match-card-main-line">
                <div class="stored-field-value match-card-result ${resultClass}">${result}</div>
                <div class="stored-field-value match-card-score">${scoreLabel}</div>
            </div>
            ${rpDelta == null ? "" : `<div class="stored-field-value match-card-rp ${rpClass}">${rpText}</div>`}
            <div class="stored-match-meta match-card-meta">
                ${escapeHtml(timeLabel)} | ${escapeHtml(shortMatchId)} | ${escapeHtml(statusLabel)}
            </div>
        </div>
    `;
    const imgEl = entryEl.querySelector(".match-card-map-image");
    const placeholderEl = entryEl.querySelector(".match-card-map-placeholder");
    attachMatchCardImageFallback(imgEl, placeholderEl, map);
    return entryEl;
}

function renderMatchResultsVirtualized(resultsDiv, rows) {
    resultsDiv.classList.add("virtualized");
    resultsDiv.innerHTML = `
        <div class="match-results-virtual-spacer"></div>
        <div class="match-results-virtual-viewport"></div>
    `;
    const spacer = resultsDiv.querySelector(".match-results-virtual-spacer");
    const viewport = resultsDiv.querySelector(".match-results-virtual-viewport");
    if (!spacer || !viewport) return;
    let rafId = null;
    let lastStart = -1;
    let lastEnd = -1;
    let lastCols = -1;
    const rowStride = MATCH_RESULTS_ROW_HEIGHT + MATCH_RESULTS_CARD_GAP;

    const computeLayout = () => {
        const width = Math.max(1, resultsDiv.clientWidth - 2);
        const cols = Math.max(1, Math.floor((width + MATCH_RESULTS_CARD_GAP) / (MATCH_RESULTS_CARD_MIN_WIDTH + MATCH_RESULTS_CARD_GAP)));
        const cardWidth = (width - ((cols - 1) * MATCH_RESULTS_CARD_GAP)) / cols;
        const totalRows = Math.ceil(rows.length / cols);
        const fullHeight = Math.max(0, (totalRows * rowStride) - MATCH_RESULTS_CARD_GAP);
        spacer.style.height = `${Math.ceil(fullHeight)}px`;
        return { cols, cardWidth, totalRows };
    };

    const renderWindow = () => {
        rafId = null;
        const { cols, cardWidth, totalRows } = computeLayout();
        const scrollTop = resultsDiv.scrollTop;
        const viewHeight = resultsDiv.clientHeight;
        const overscanRows = 3;
        const startRow = Math.max(0, Math.floor(scrollTop / rowStride) - overscanRows);
        const endRow = Math.min(totalRows - 1, Math.ceil((scrollTop + viewHeight) / rowStride) + overscanRows);
        const startIdx = Math.max(0, startRow * cols);
        const endIdx = Math.min(rows.length - 1, ((endRow + 1) * cols) - 1);
        if (startIdx === lastStart && endIdx === lastEnd && cols === lastCols) return;
        lastStart = startIdx;
        lastEnd = endIdx;
        lastCols = cols;
        viewport.innerHTML = "";
        const frag = document.createDocumentFragment();
        for (let idx = startIdx; idx <= endIdx; idx += 1) {
            const row = rows[idx];
            if (!row) continue;
            const card = buildMatchResultCardElement(row, cardWidth);
            const gridRow = Math.floor(idx / cols);
            const gridCol = idx % cols;
            card.style.position = "absolute";
            card.style.top = `${Math.floor(gridRow * rowStride)}px`;
            card.style.left = `${Math.floor(gridCol * (cardWidth + MATCH_RESULTS_CARD_GAP))}px`;
            frag.appendChild(card);
        }
        viewport.appendChild(frag);
    };

    const scheduleRender = () => {
        if (rafId != null) return;
        rafId = window.requestAnimationFrame(renderWindow);
    };

    const onScroll = () => scheduleRender();
    const onResize = () => {
        lastStart = -1;
        lastEnd = -1;
        lastCols = -1;
        scheduleRender();
    };

    resultsDiv.addEventListener("scroll", onScroll);
    window.addEventListener("resize", onResize);
    scheduleRender();

    matchResultsVirtualCleanup = () => {
        resultsDiv.removeEventListener("scroll", onScroll);
        window.removeEventListener("resize", onResize);
        if (rafId != null) {
            window.cancelAnimationFrame(rafId);
            rafId = null;
        }
        resultsDiv.classList.remove("virtualized");
    };
}

function renderMatchResults(forceStickToBottom = false) {
    const resultsDiv = document.getElementById("match-results");
    if (!resultsDiv) return;
    const rows = matchResultsOrder.map((id) => matchResultsById.get(id)).filter(Boolean);
    const shouldStick = forceStickToBottom || (resultsDiv.scrollHeight - resultsDiv.scrollTop - resultsDiv.clientHeight) < 30;
    cleanupMatchResultsVirtualization();
    resultsDiv.classList.remove("virtualized");
    if (rows.length >= MATCH_RESULTS_VIRTUALIZE_THRESHOLD) {
        renderMatchResultsVirtualized(resultsDiv, rows);
    } else {
        resultsDiv.innerHTML = "";
        const frag = document.createDocumentFragment();
        for (const row of rows) {
            frag.appendChild(buildMatchResultCardElement(row));
        }
        resultsDiv.appendChild(frag);
    }
    if (shouldStick) {
        resultsDiv.scrollTop = resultsDiv.scrollHeight;
    }
}

function scheduleMatchResultsRender(forceStickToBottom = false) {
    if (forceStickToBottom) {
        matchResultsStickToBottom = true;
    }
    if (matchResultsRenderTick != null) return;
    matchResultsRenderTick = window.requestAnimationFrame(() => {
        const sticky = matchResultsStickToBottom;
        matchResultsRenderTick = null;
        matchResultsStickToBottom = false;
        renderMatchResults(sticky);
    });
}

function appendMatchResultRow(matchData, status = "captured") {
    const matchId = String(matchData?.match_id || "unknown-id");
    const existing = matchResultsById.get(matchId);
    if (existing) {
        existing.matchData = matchData;
        existing.status = status;
    } else {
        matchResultsById.set(matchId, { matchData, status });
        matchResultsOrder.push(matchId);
    }
    scheduleMatchResultsRender(true);
}

function setActiveTab(tabName) {
    setActiveTabUi(tabName, {
        onScannerActivated: () => {
            if (network) network.redraw();
        },
        onStoredActivated: () => {
            loadStoredMatchesView("", true);
        },
        onPlayersActivated: () => {
            loadPlayersTab(false);
        },
        onTeamBuilderActivated: () => {
            loadTeamBuilderFriends(false);
        },
        onOperatorsActivated: () => {
            loadOperatorsTab(false);
        },
        onWorkspaceActivated: () => {
            const workspaceRoot = document.getElementById("graph-panel-workspace");
            if (workspaceRoot) workspaceRoot.hidden = false;
            const panel = computeReportState.workspace.panel || "overview";
            loadWorkspacePanel(panel, false).catch((err) => {
                logCompute(`Workspace load failed: ${err}`, "error");
                renderWorkspacePanelError(panel, err);
            });
        },
        onDashboardActivated: () => {
            triggerDashboardAutoRefresh();
        },
    });
    const params = new URLSearchParams(window.location.search);
    params.set("panel", String(tabName || "scanner"));
    const url = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState({}, "", url);
}

function getFilterInputLabel(el) {
    if (!el) return "";
    const id = String(el.id || "");
    const explicit = String(el.dataset?.filterLabel || "").trim();
    if (explicit) return explicit;
    const label = el.closest("label");
    if (label) {
        const text = String(label.textContent || "").replace(/\s+/g, " ").trim();
        if (text) return text.replace(/^Stack only$/i, "Stack");
    }
    if (id) {
        return id.replace(/[-_]/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
    }
    return el.tagName;
}

function getFilterInputValue(el) {
    if (!el || el.disabled) return "";
    if (el.type === "checkbox") {
        return el.checked ? "On" : "";
    }
    const value = String(el.value || "").trim();
    if (!value) return "";
    if (el.tagName === "SELECT") {
        const option = el.selectedOptions?.[0];
        return String(option?.textContent || value).trim();
    }
    return value;
}

function renderPanelActiveFilterChips(panel) {
    if (!panel) return;
    let host = panel.querySelector(".panel-active-filters");
    if (!host) {
        host = document.createElement("div");
        host.className = "panel-active-filters";
        const topbar = panel.querySelector(".panel-topbar");
        if (topbar && topbar.parentElement) {
            topbar.parentElement.insertBefore(host, topbar.nextSibling);
        } else {
            panel.prepend(host);
        }
    }
    const drawer = panel.querySelector(".filter-drawer");
    if (!drawer) {
        host.replaceChildren();
        return;
    }
    const chips = [];
    drawer.querySelectorAll("input, select").forEach((el) => {
        const label = getFilterInputLabel(el);
        const value = getFilterInputValue(el);
        if (!label || !value) return;
        chips.push(createChip({ label: `${label}: ${value}`, tone: "accent" }));
    });
    host.replaceChildren(...chips);
}

function resetPanelFilters(panel) {
    if (!panel) return;
    panel.querySelectorAll(".filter-drawer input, .filter-drawer select").forEach((el) => {
        if (el.disabled) return;
        if (el.type === "checkbox") {
            el.checked = el.defaultChecked;
        } else {
            el.value = el.defaultValue || "";
        }
        el.dispatchEvent(new Event("change", { bubbles: true }));
    });
    renderPanelActiveFilterChips(panel);
}

function initFilterDrawersAndChips() {
    enhanceDrawers(document);
    document.querySelectorAll(".tab-panel").forEach((panel) => {
        const drawer = panel.querySelector(".filter-drawer");
        if (drawer) {
            attachDrawerResetButton(drawer, () => resetPanelFilters(panel));
            drawer.querySelectorAll("input, select").forEach((el) => {
                const evt = el.tagName === "INPUT" && el.type === "text" ? "input" : "change";
                el.addEventListener(evt, () => renderPanelActiveFilterChips(panel));
            });
        }
        renderPanelActiveFilterChips(panel);
    });
}

function getPrimaryUsernameForPlayers() {
    const explicit = String(document.getElementById("players-primary-username")?.value || "").trim();
    if (explicit) return explicit;
    const compute = String(document.getElementById("compute-username")?.value || "").trim();
    if (compute) return compute;
    const stored = String(document.getElementById("stored-username")?.value || "").trim();
    return stored;
}

function renderPlayersTable(rows) {
    const wrap = document.getElementById("players-table-wrap");
    const chip = document.getElementById("players-count-chip");
    if (!wrap) return;
    const search = String(document.getElementById("players-search")?.value || "").trim().toLowerCase();
    let filtered = Array.isArray(rows) ? rows.slice() : [];
    if (search) {
        filtered = filtered.filter((row) => String(row?.username || "").toLowerCase().includes(search));
    }
    filtered.sort((a, b) => (
        toNumber(b?.is_friend ? 1 : 0, 0) - toNumber(a?.is_friend ? 1 : 0, 0) ||
        toNumber(b?.shared_matches, 0) - toNumber(a?.shared_matches, 0) ||
        String(a?.username || "").localeCompare(String(b?.username || ""))
    ));
    if (chip) chip.textContent = String(filtered.length);
    if (!filtered.length) {
        wrap.innerHTML = `<div class="compute-value">No encountered players found.</div>`;
        return;
    }
    const tableRows = filtered.map((row) => {
        const tags = Array.isArray(row?.tags) ? row.tags : [];
        const isFriend = Boolean(row?.is_friend) || tags.some((t) => String(t).toLowerCase() === "friend");
        return {
            username: String(row?.username || ""),
            shared_matches: toNumber(row?.shared_matches, 0),
            win_rate_together: toNumber(row?.win_rate_together, 0),
            last_seen: formatMatchAgeLabel(row?.last_seen),
            tags: tags.join(", ") || "-",
            friend: isFriend ? "Unfriend" : "Friend",
            friend_enabled: isFriend ? "1" : "0",
        };
    });
    const dt = createDataTable({
        columns: [
            { key: "username", label: "Username", sortable: true },
            { key: "shared_matches", label: "Shared Matches", sortable: true },
            { key: "win_rate_together", label: "Win Rate Together", sortable: true, render: (row) => `${formatFixed(toNumber(row.win_rate_together, 0), 1)}%` },
            { key: "last_seen", label: "Last Seen", sortable: true },
            { key: "tags", label: "Tags", sortable: false },
            {
                key: "friend",
                label: "Friend",
                sortable: false,
                render: (row) => {
                    const tone = row.friend_enabled === "1" ? "primary" : "secondary";
                    return `<button class="ui-button ui-button--${tone} players-friend-toggle" type="button" data-username="${escapeHtml(row.username)}" data-enabled="${row.friend_enabled}">${escapeHtml(row.friend)}</button>`;
                },
            },
        ],
        rows: tableRows,
    });
    wrap.replaceChildren(dt.element);
    wrap.querySelectorAll(".players-friend-toggle").forEach((btn) => {
        btn.addEventListener("click", async () => {
            const username = String(btn.dataset.username || "").trim();
            if (!username) return;
            const currentlyEnabled = btn.dataset.enabled === "1";
            try {
                const res = await api.setPlayerTag(username, "friend", !currentlyEnabled);
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                await loadPlayersTab(true);
                await loadTeamBuilderFriends(true);
            } catch (err) {
                logCompute(`Failed to update friend tag for ${username}: ${err}`, "error");
            }
        });
    });
}

async function loadPlayersTab(silent = false) {
    const username = getPrimaryUsernameForPlayers();
    const wrap = document.getElementById("players-table-wrap");
    if (!wrap) return;
    if (!username) {
        wrap.innerHTML = `<div class="compute-value">Enter a primary username to load encountered players.</div>`;
        const chip = document.getElementById("players-count-chip");
        if (chip) chip.textContent = "0";
        return;
    }
    if (!silent) {
        wrap.innerHTML = `<div class="compute-value">Loading encountered players...</div>`;
    }
    try {
        const res = await api.getEncounteredPlayers(username, "Ranked");
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const payload = await res.json();
        encounteredPlayersCache = Array.isArray(payload?.players) ? payload.players : [];
        renderPlayersTable(encounteredPlayersCache);
    } catch (err) {
        wrap.innerHTML = `<div class="compute-value">Failed to load players: ${escapeHtml(String(err))}</div>`;
    }
}

function renderTeamBuilderFriends(rows) {
    const wrap = document.getElementById("team-builder-friends");
    const chip = document.getElementById("team-builder-friend-count");
    if (!wrap) return;
    const list = Array.isArray(rows) ? rows.slice() : [];
    if (chip) chip.textContent = String(list.length);
    if (!list.length) {
        wrap.innerHTML = `<div class="compute-value">No tagged friends yet.</div>`;
        return;
    }
    wrap.innerHTML = list.map((row) => (
        `<label class="team-friend-item">` +
        `<input type="checkbox" class="team-friend-check" value="${escapeHtml(String(row.username || ""))}">` +
        `<span>${escapeHtml(String(row.username || ""))}</span>` +
        `</label>`
    )).join("");
}

async function loadTeamBuilderFriends(silent = false) {
    const wrap = document.getElementById("team-builder-friends");
    if (!wrap) return;
    if (!silent) {
        wrap.innerHTML = `<div class="compute-value">Loading friends...</div>`;
    }
    try {
        const res = await api.getFriends("friend");
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const payload = await res.json();
        teamBuilderFriendsCache = Array.isArray(payload?.players) ? payload.players : [];
        renderTeamBuilderFriends(teamBuilderFriendsCache);
    } catch (err) {
        wrap.innerHTML = `<div class="compute-value">Failed to load friends: ${escapeHtml(String(err))}</div>`;
    }
}

function renderStackSynergy(analysis) {
    const output = document.getElementById("team-builder-output");
    if (!output) return;
    if (!analysis || analysis.error) {
        output.innerHTML = `<div class="compute-value">${escapeHtml(String(analysis?.error || "No stack data available."))}</div>`;
        return;
    }
    const baselines = analysis.solo_baselines || {};
    const baselineRows = Object.entries(baselines).map(([name, wr]) => (
        `<span class="stack-baseline-chip">${escapeHtml(name)}: <strong>${wr == null ? "N/A" : `${formatFixed(wr, 1)}%`}</strong></span>`
    )).join("");
    const matrixRows = (Array.isArray(analysis.pair_matrix) ? analysis.pair_matrix : []).map((row) => {
        const cells = (Array.isArray(row?.cells) ? row.cells : []).map((cell) => {
            if (cell.win_rate == null) return `<td class="stack-matrix-empty">-</td>`;
            const cls = cell.win_rate >= 50 ? "pos" : "neg";
            return `<td class="${cls}" title="${escapeHtml(String(cell.shared_matches || 0))} shared matches">${formatFixed(cell.win_rate, 1)}%</td>`;
        }).join("");
        return `<tr><th>${escapeHtml(String(row?.player || ""))}</th>${cells}</tr>`;
    }).join("");
    const matrixHeader = (analysis.players || []).map((name) => `<th>${escapeHtml(String(name))}</th>`).join("");
    const mapBars = (Array.isArray(analysis.map_pool) ? analysis.map_pool : []).map((m) => {
        const wr = toNumber(m?.win_rate, 0);
        const width = Math.max(0, Math.min(100, wr));
        return (
            `<div class="stack-map-row">` +
            `<div class="stack-map-label">${escapeHtml(String(m?.map_name || "Unknown"))} <span>(n=${toNumber(m?.matches, 0)})</span></div>` +
            `<div class="stack-map-bar-wrap"><div class="stack-map-bar" style="width:${width.toFixed(1)}%"></div><span>${formatFixed(wr, 1)}%</span></div>` +
            `</div>`
        );
    }).join("");
    const roles = (Array.isArray(analysis.role_coverage) ? analysis.role_coverage : []).map((row) => {
        const sides = row?.side_rounds || {};
        const ops = Array.isArray(row?.top_operators) ? row.top_operators.slice(0, 3) : [];
        const topOps = ops.map((o) => `${o.operator} (${toNumber(o.rounds, 0)})`).join(", ") || "No operator rounds.";
        return (
            `<div class="stack-role-card">` +
            `<div class="stack-role-name">${escapeHtml(String(row?.username || ""))}</div>` +
            `<div class="stack-role-meta">ATK ${toNumber(sides.attacker, 0)} / DEF ${toNumber(sides.defender, 0)}</div>` +
            `<div class="stack-role-meta">${escapeHtml(topOps)}</div>` +
            `</div>`
        );
    }).join("");
    output.innerHTML = (
        `<div class="stack-summary">` +
        `<div class="stack-winrate">${formatFixed(toNumber(analysis.stack_win_rate, 0), 1)}%</div>` +
        `<div class="stack-meta">Stack WR (${toNumber(analysis.stack_wins, 0)}-${Math.max(0, toNumber(analysis.stack_match_count, 0) - toNumber(analysis.stack_wins, 0))}) over ${toNumber(analysis.stack_match_count, 0)} matches</div>` +
        `<div class="stack-baselines">${baselineRows}</div>` +
        `${analysis.warning ? `<div class="stack-warning">${escapeHtml(String(analysis.warning))}</div>` : ""}` +
        `</div>` +
        `<div class="stack-block">` +
        `<div class="compute-label">Chemistry Matrix</div>` +
        `<table class="stack-matrix"><thead><tr><th></th>${matrixHeader}</tr></thead><tbody>${matrixRows}</tbody></table>` +
        `</div>` +
        `<div class="stack-block">` +
        `<div class="compute-label">Map Pool</div>` +
        `<div class="stack-map-pool">${mapBars || `<div class="compute-value">No shared map data.</div>`}</div>` +
        `</div>` +
        `<div class="stack-block">` +
        `<div class="compute-label">Role Coverage</div>` +
        `<div class="stack-role-grid">${roles || `<div class="compute-value">No role data.</div>`}</div>` +
        `</div>`
    );
}

async function analyzeSelectedStack() {
    const checks = Array.from(document.querySelectorAll(".team-friend-check:checked"));
    const selectedFriends = checks.map((el) => String(el.value || "").trim()).filter(Boolean);
    const primary = getPrimaryUsernameForPlayers();
    const output = document.getElementById("team-builder-output");
    if (!output) return;
    if (!primary) {
        output.innerHTML = `<div class="compute-value">Enter a primary username first.</div>`;
        return;
    }
    if (selectedFriends.length < 2 || selectedFriends.length > 4) {
        output.innerHTML = `<div class="compute-value">Select 2-4 friends to analyze with ${escapeHtml(primary)}.</div>`;
        return;
    }
    const players = [primary, ...selectedFriends];
    output.innerHTML = `<div class="compute-value">Analyzing stack...</div>`;
    try {
        const res = await api.getStackSynergy(players, "Ranked");
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const payload = await res.json();
        renderStackSynergy(payload?.analysis || null);
    } catch (err) {
        output.innerHTML = `<div class="compute-value">Failed to analyze stack: ${escapeHtml(String(err))}</div>`;
    }
}

function getOperatorsActiveUsername() {
    const sel = String(document.getElementById("operators-player")?.value || "").trim();
    if (sel) return sel;
    return getPrimaryUsernameForPlayers() || (document.getElementById("compute-username")?.value || "").trim();
}

function operatorSlug(value) {
    return String(value || "")
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "");
}

function operatorTrackerIconUrl(operatorName) {
    const slug = operatorSlug(operatorName);
    if (!slug) return "";
    return `https://trackercdn.com/cdn/tracker.gg/r6siege/db/images/operators/${encodeURIComponent(slug)}.png`;
}

function operatorIconCandidates(operatorName) {
    const op = String(operatorName || "").trim();
    const slug = operatorSlug(op);
    if (!slug) return [];
    const local = resolveOperatorImageUrl(op);
    const candidates = [
        local,
        `https://trackercdn.com/cdn/tracker.gg/r6siege/db/images/operators/${encodeURIComponent(slug)}.png`,
        `https://trackercdn.com/cdn/tracker.gg/r6siege/db/images/operators/${encodeURIComponent(slug)}.webp`,
        `https://trackercdn.com/rainbow6-ubi/assets/images/badge-${encodeURIComponent(slug)}.png`,
        `https://trackercdn.com/rainbow6-ubi/assets/images/badge-${encodeURIComponent(slug)}.webp`,
    ].filter(Boolean);
    return Array.from(new Set(candidates));
}

function handleOperatorDotIconError(imgEl) {
    if (!imgEl) return;
    const raw = String(imgEl.dataset.fallbacks || "");
    const parts = raw ? raw.split("||").filter(Boolean) : [];
    if (parts.length) {
        const [next, ...rest] = parts;
        imgEl.dataset.fallbacks = rest.join("||");
        imgEl.src = next;
        return;
    }
    imgEl.style.display = "none";
    if (imgEl.nextElementSibling) imgEl.nextElementSibling.style.display = "inline-flex";
}

window.handleOperatorDotIconError = handleOperatorDotIconError;

function operatorsConfidenceBand(rounds) {
    const n = Math.max(0, toNumber(rounds, 0));
    if (n >= 20) return "high";
    if (n >= 10) return "med";
    return "low";
}

function operatorsConfidenceLabel(rounds) {
    const band = operatorsConfidenceBand(rounds);
    if (band === "high") return "High confidence";
    if (band === "med") return "Medium confidence";
    return "Low confidence";
}

function renderOperatorDot(item, baseline, bound = 20, yOffset = 0) {
    const delta = toNumber(item?.delta_vs_baseline, 0);
    const pct = Math.max(-bound, Math.min(bound, delta));
    const left = 50 + ((pct / bound) * 50);
    const rounds = Math.max(0, toNumber(item?.rounds, 0));
    const size = Math.max(8, Math.min(16, 8 + Math.sqrt(rounds)));
    const confidence = operatorsConfidenceBand(rounds);
    const alpha = confidence === "high" ? 1 : (confidence === "med" ? 0.88 : 0.7);
    const color = delta >= 0 ? "#4caf50" : "#f44336";
    const op = String(item?.operator || "Unknown");
    const iconCandidates = operatorIconCandidates(op);
    const primaryIcon = iconCandidates[0] || "";
    const fallbackIcons = iconCandidates.slice(1).join("||");
    const tooltip =
        `${op}\n` +
        `Rounds: ${rounds}\n` +
        `Win rate: ${formatFixed(toNumber(item?.win_rate, 0), 1)}% (${delta >= 0 ? "+" : ""}${formatFixed(delta, 1)}% vs map baseline)\n` +
        `Confidence: ${operatorsConfidenceLabel(rounds)}\n` +
        `First kill rate: ${formatFixed(toNumber(item?.fk_rate, 0), 1)}%\n` +
        `First death rate: ${formatFixed(toNumber(item?.fd_rate, 0), 1)}%\n` +
        `KD: ${formatFixed(toNumber(item?.kd, 0), 2)}`;
    const iconHtml = primaryIcon
        ? `<img class="operators-dot-icon" src="${escapeHtml(primaryIcon)}" data-fallbacks="${escapeHtml(fallbackIcons)}" alt="${escapeHtml(op)}" onerror="window.handleOperatorDotIconError(this)">` +
          `<span class="operators-dot-fallback" style="display:none">${escapeHtml(op.slice(0, 2).toUpperCase())}</span>`
        : `<span class="operators-dot-fallback">${escapeHtml(op.slice(0, 2).toUpperCase())}</span>`;
    return (
        `<div class="operators-dot-wrap" style="left:${left.toFixed(2)}%; top:${(34 + yOffset).toFixed(0)}px" title="${escapeHtml(tooltip)}">` +
        `<div class="operators-dot ${confidence}" style="width:${size.toFixed(0)}px;height:${size.toFixed(0)}px;background:${color};opacity:${alpha};"></div>` +
        `<div class="operators-dot-icon-wrap">${iconHtml}</div>` +
        `<div class="operators-dot-label">${escapeHtml(op)}</div>` +
        `</div>`
    );
}

function renderOperatorSideLine(sideKey, baseline, operators) {
    const list = Array.isArray(operators) ? operators : [];
    if (list.length < 3) {
        return `<div class="operators-side-empty">Not enough data</div>`;
    }
    const maxDelta = list.reduce((acc, row) => Math.max(acc, Math.abs(toNumber(row?.delta_vs_baseline, 0))), 0);
    const bound = Math.max(10, Math.min(35, Math.ceil(maxDelta + 2)));
    const bins = new Map();
    const dots = list.map((row) => {
        const delta = toNumber(row?.delta_vs_baseline, 0);
        const binKey = (Math.round(delta * 2) / 2).toFixed(1);
        const used = toNumber(bins.get(binKey), 0);
        bins.set(binKey, used + 1);
        const sign = (used % 2 === 0) ? -1 : 1;
        const layer = Math.floor((used + 1) / 2);
        const yOffset = sign * layer * 18;
        return renderOperatorDot(row, baseline, bound, yOffset);
    }).join("");
    return (
        `<div class="operators-side-line-wrap">` +
        `<div class="operators-axis-caption">Delta win% vs map baseline</div>` +
        `<div class="operators-line-legend"><span class="neg">Negative</span><span class="zero">0 baseline</span><span class="pos">Positive</span></div>` +
        `<div class="operators-side-line">` +
        `<div class="operators-axis-center" aria-hidden="true"></div>` +
        `${dots}` +
        `</div>` +
        `<div class="operators-axis-scale"><span>-${bound}%</span><span>0%</span><span>+${bound}%</span></div>` +
        `</div>`
    );
}

function renderOperatorsTopBottomList(sideKey, operators, topK) {
    const includeLowSample = document.getElementById("operators-include-low-sample")?.checked === true;
    const list = (Array.isArray(operators) ? operators : []).filter((row) => includeLowSample || row?.meets_min_rounds !== false);
    if (!list.length) {
        const minRounds = toNumber(document.getElementById("operators-min-rounds")?.value, 5);
        return `<div class="operators-quick-empty">0 operators meet min_rounds=${minRounds} per map+side+operator on ${sideKey.toUpperCase()}. Try lowering threshold or enable "Include low sample".</div>`;
    }
    const top = list
        .slice()
        .sort((a, b) => toNumber(b?.delta_vs_baseline, 0) - toNumber(a?.delta_vs_baseline, 0))
        .slice(0, topK);
    const bottom = list
        .slice()
        .sort((a, b) => toNumber(a?.delta_vs_baseline, 0) - toNumber(b?.delta_vs_baseline, 0))
        .slice(0, topK);
    const renderRows = (rows, trend) => rows.map((row) => {
        const rounds = toNumber(row?.rounds, 0);
        const delta = toNumber(row?.delta_vs_baseline, 0);
        const confidence = operatorsConfidenceBand(rounds);
        return (
            `<div class="operators-quick-row">` +
            `<span class="operators-quick-op">${escapeHtml(String(row?.operator || "Unknown"))}</span>` +
            `<span class="operators-quick-delta ${trend === "up" ? "pos" : "neg"}">${delta >= 0 ? "+" : ""}${formatFixed(delta, 1)}%</span>` +
            `<span class="operators-quick-n ${confidence}">n=${rounds}</span>` +
            `</div>`
        );
    }).join("");
    return (
        `<div class="operators-quick-grid">` +
        `<div class="operators-quick-col">` +
        `<div class="operators-quick-title">${sideKey.toUpperCase()} Top ${topK}</div>` +
        `${renderRows(top, "up")}` +
        `</div>` +
        `<div class="operators-quick-col">` +
        `<div class="operators-quick-title">${sideKey.toUpperCase()} Bottom ${topK}</div>` +
        `${renderRows(bottom, "down")}` +
        `</div>` +
        `</div>`
    );
}

function renderOperatorsDetailRows(rows, trend, limit) {
    const sorted = rows
        .slice()
        .sort((a, b) => trend === "up"
            ? toNumber(b?.delta_vs_baseline, 0) - toNumber(a?.delta_vs_baseline, 0)
            : toNumber(a?.delta_vs_baseline, 0) - toNumber(b?.delta_vs_baseline, 0))
        .slice(0, limit);
    if (!sorted.length) return `<div class="operators-quick-empty">No rows.</div>`;
    return sorted.map((row) => {
        const rounds = toNumber(row?.rounds, 0);
        const delta = toNumber(row?.delta_vs_baseline, 0);
        const confidence = operatorsConfidenceBand(rounds);
        return (
            `<div class="operators-detail-row">` +
            `<div class="operators-detail-op">` +
            `<span>${escapeHtml(String(row?.operator || "Unknown"))}</span>` +
            `<span class="operators-detail-confidence ${confidence}">${operatorsConfidenceLabel(rounds)}</span>` +
            `</div>` +
            `<div class="operators-detail-metrics">` +
            `<span class="${delta >= 0 ? "pos" : "neg"}">${delta >= 0 ? "+" : ""}${formatFixed(delta, 1)}%</span>` +
            `<span>WR ${formatFixed(toNumber(row?.win_rate, 0), 1)}%</span>` +
            `<span>n=${rounds}</span>` +
            `<span>FK ${formatFixed(toNumber(row?.fk_rate, 0), 1)}%</span>` +
            `<span>FD ${formatFixed(toNumber(row?.fd_rate, 0), 1)}%</span>` +
            `<span>KD ${formatFixed(toNumber(row?.kd, 0), 2)}</span>` +
            `</div>` +
            `</div>`
        );
    }).join("");
}

function renderOperatorsMapDetail(mapNameOverride = "") {
    const panel = document.getElementById("operators-map-detail");
    const body = document.getElementById("operators-map-detail-body");
    const title = document.getElementById("operators-detail-title");
    const sideSel = document.getElementById("operators-detail-side");
    const limitSel = document.getElementById("operators-detail-limit");
    if (!panel || !body || !title || !sideSel || !limitSel) return;
    const maps = Array.isArray(operatorsLastPayload?.maps) ? operatorsLastPayload.maps : [];
    if (!maps.length) {
        panel.classList.add("hidden");
        body.innerHTML = `<div class="compute-value">No map details available.</div>`;
        return;
    }
    const requested = String(mapNameOverride || operatorsSelectedMap || "").trim();
    const active = maps.find((m) => String(m?.map_name || "") === requested) || maps[0];
    operatorsSelectedMap = String(active?.map_name || "");
    const side = String(sideSel.value || "all");
    const limit = Math.max(1, toNumber(limitSel.value, 10));
    const sections = [];
    if (side === "all" || side === "atk") {
        sections.push(
            `<section class="operators-detail-side">` +
            `<h4>ATK Evidence</h4>` +
            `<div class="operators-detail-columns">` +
            `<div><div class="operators-detail-subtitle">Top ${limit}</div>${renderOperatorsDetailRows(Array.isArray(active?.atk) ? active.atk : [], "up", limit)}</div>` +
            `<div><div class="operators-detail-subtitle">Bottom ${limit}</div>${renderOperatorsDetailRows(Array.isArray(active?.atk) ? active.atk : [], "down", limit)}</div>` +
            `</div>` +
            `</section>`
        );
    }
    if (side === "all" || side === "def") {
        sections.push(
            `<section class="operators-detail-side">` +
            `<h4>DEF Evidence</h4>` +
            `<div class="operators-detail-columns">` +
            `<div><div class="operators-detail-subtitle">Top ${limit}</div>${renderOperatorsDetailRows(Array.isArray(active?.def) ? active.def : [], "up", limit)}</div>` +
            `<div><div class="operators-detail-subtitle">Bottom ${limit}</div>${renderOperatorsDetailRows(Array.isArray(active?.def) ? active.def : [], "down", limit)}</div>` +
            `</div>` +
            `</section>`
        );
    }
    title.textContent = `${operatorsSelectedMap} Detail (n=${toNumber(active?.total_rounds, 0)})`;
    body.innerHTML = sections.join("") || `<div class="compute-value">No rows for selected side.</div>`;
    panel.classList.remove("hidden");
}

function renderOperatorsMapCards(payload) {
    const grid = document.getElementById("operators-map-grid");
    const lowWrap = document.getElementById("operators-low-data");
    if (!grid || !lowWrap) return;
    operatorsLastPayload = payload || null;
    const maps = Array.isArray(payload?.maps) ? payload.maps : [];
    const low = Array.isArray(payload?.low_data_maps) ? payload.low_data_maps : [];
    if (!maps.length && !low.length) {
        grid.innerHTML = `<div class="compute-value">No operator map data found for current filters.</div>`;
        lowWrap.innerHTML = "";
        renderOperatorsMapDetail("");
        return;
    }
    const topK = Math.max(1, toNumber(document.getElementById("operators-topk")?.value, 5));
    const advanced = document.getElementById("operators-advanced")?.checked === true;
    const includeLowSample = document.getElementById("operators-include-low-sample")?.checked === true;
    const recommendationBlock = (sideKey, rows) => {
        const list = (Array.isArray(rows) ? rows : []).filter((row) => includeLowSample || row?.meets_min_rounds !== false);
        const top = list
            .slice()
            .sort((a, b) => toNumber(b?.delta_vs_baseline, 0) - toNumber(a?.delta_vs_baseline, 0))
            .slice(0, 3)
            .map((r) => String(r?.operator || "Unknown"));
        const bottom = list
            .slice()
            .sort((a, b) => toNumber(a?.delta_vs_baseline, 0) - toNumber(b?.delta_vs_baseline, 0))
            .slice(0, 2)
            .map((r) => String(r?.operator || "Unknown"));
        return (
            `<div class="operators-reco-card">` +
            `<strong>${sideKey.toUpperCase()} Recommendation</strong>` +
            `<span>Core: ${escapeHtml(top.join(", ") || "N/A")}</span>` +
            `<span>Avoid: ${escapeHtml(bottom.join(", ") || "N/A")}</span>` +
            `</div>`
        );
    };
    if (operatorsSelectedMap && !maps.some((m) => String(m?.map_name || "") === operatorsSelectedMap)) {
        operatorsSelectedMap = "";
    }
    grid.innerHTML = maps.map((m) => (
        `<section class="operators-map-card ${operatorsSelectedMap === String(m?.map_name || "") ? "selected" : ""} ${advanced ? "is-advanced" : ""}" data-operators-map="${escapeHtml(String(m?.map_name || ""))}">` +
        `<header class="operators-map-head">` +
        `<strong>${escapeHtml(String(m?.map_name || "Unknown"))}</strong><span>(n=${toNumber(m?.total_rounds, 0)})</span>` +
        `<button type="button" class="operators-drill-btn" data-operators-map-drill="${escapeHtml(String(m?.map_name || ""))}">Drilldown</button>` +
        `</header>` +
        `<div class="operators-map-bias" data-ui-tooltip="Map baseline split: attacker win% versus defender win%.">` +
        `<span>Map Bias</span>` +
        `<div class="operators-map-bias-bar">` +
        `<div class="operators-map-bias-atk" style="width:${Math.max(0, Math.min(100, toNumber(m?.baseline_atk, 0))).toFixed(2)}%"></div>` +
        `<div class="operators-map-bias-def" style="width:${Math.max(0, Math.min(100, toNumber(m?.baseline_def, 0))).toFixed(2)}%"></div>` +
        `</div>` +
        `<span>ATK ${formatFixed(toNumber(m?.baseline_atk, 0), 1)}% | DEF ${formatFixed(toNumber(m?.baseline_def, 0), 1)}%</span>` +
        `</div>` +
        `<div class="operators-reco-strip">` +
        `${recommendationBlock("atk", m?.atk)}` +
        `${recommendationBlock("def", m?.def)}` +
        `</div>` +
        `<div class="operators-quick-wrap">` +
        `${renderOperatorsTopBottomList("atk", m?.atk, topK)}` +
        `${renderOperatorsTopBottomList("def", m?.def, topK)}` +
        `</div>` +
        `<div class="operators-side-block">` +
        `<div class="operators-side-title">ATK baseline ${formatFixed(toNumber(m?.baseline_atk, 0), 1)}%</div>` +
        `${renderOperatorSideLine("atk", toNumber(m?.baseline_atk, 0), m?.atk)}` +
        `</div>` +
        `<div class="operators-side-block">` +
        `<div class="operators-side-title">DEF baseline ${formatFixed(toNumber(m?.baseline_def, 0), 1)}%</div>` +
        `${renderOperatorSideLine("def", toNumber(m?.baseline_def, 0), m?.def)}` +
        `</div>` +
        `</section>`
    )).join("");
    if (low.length) {
        lowWrap.innerHTML = (
            `<div class="operators-low-title">Low-data maps</div>` +
            low.map((m) => `<div class="operators-low-row">${escapeHtml(String(m?.map_name || "Unknown"))} (n=${toNumber(m?.total_rounds, 0)}) - not enough data</div>`).join("")
        );
    } else {
        lowWrap.innerHTML = "";
    }
    if (operatorsSelectedMap) {
        renderOperatorsMapDetail(operatorsSelectedMap);
    } else {
        document.getElementById("operators-map-detail")?.classList.add("hidden");
    }
    const apiEntryCount = maps.reduce((acc, m) => acc + (Array.isArray(m?.atk) ? m.atk.length : 0) + (Array.isArray(m?.def) ? m.def.length : 0), 0);
    const hiddenByThreshold = maps.reduce((acc, m) => acc + toNumber(m?.filtered_out_by_min_rounds, 0), 0);
    const activeEntryCount = includeLowSample ? apiEntryCount : Math.max(0, apiEntryCount - hiddenByThreshold);
    if (!includeLowSample && apiEntryCount > 0 && activeEntryCount === 0) {
        grid.innerHTML = (
            `<div class="compute-value">0 operators meet the current min-round threshold. ` +
            `Threshold is applied per map+side+operator. Try lowering min rounds or enable Include low sample.</div>` +
            `<button id="operators-lower-threshold" class="ui-button ui-button--secondary" type="button">Lower threshold to 3</button>`
        );
    }
    if (hiddenByThreshold > 0) {
        lowWrap.innerHTML += `<div class="operators-low-row">Why empty? ${hiddenByThreshold} operator rows were hidden by min-round threshold.</div>`;
    }
    console.info(
        `[Operators] API maps=${maps.length} api_entries=${apiEntryCount} hidden_by_min_rounds=${hiddenByThreshold} include_low=${includeLowSample} active_entries=${activeEntryCount}`
    );
}

async function loadOperatorsPlayersList() {
    const sel = document.getElementById("operators-player");
    if (!sel) return;
    const current = String(sel.value || "").trim();
    let names = operatorsPlayerListCache.slice();
    if (!names.length) {
        try {
            const res = await api.getPlayersList();
            if (res.ok) {
                const payload = await res.json();
                names = Array.isArray(payload?.players) ? payload.players : [];
                operatorsPlayerListCache = names.slice();
            }
        } catch (_) {
            // best effort
        }
    }
    const fallback = getPrimaryUsernameForPlayers();
    const merged = new Set(names);
    if (current) merged.add(current);
    if (fallback) merged.add(fallback);
    const finalNames = Array.from(merged).sort((a, b) => a.localeCompare(b));
    sel.innerHTML = finalNames.map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`).join("");
    if (current && finalNames.includes(current)) {
        sel.value = current;
    } else if (fallback && finalNames.includes(fallback)) {
        sel.value = fallback;
    }
}

function buildOperatorsScope() {
    return {
        username: getOperatorsActiveUsername(),
        stack: String(document.getElementById("operators-stack")?.value || "all"),
        matchType: String(document.getElementById("operators-match-type")?.value || "Ranked"),
        minRounds: toNumber(document.getElementById("operators-min-rounds")?.value, 5),
        includeLowSample: document.getElementById("operators-include-low-sample")?.checked === true,
    };
}

async function loadOperatorsTab(silent = false) {
    const grid = document.getElementById("operators-map-grid");
    if (!grid) return;
    await loadOperatorsPlayersList();
    const scope = buildOperatorsScope();
    const username = scope.username;
    const stack = scope.stack;
    const matchType = scope.matchType;
    const minRounds = scope.minRounds;
    if (!username) {
        grid.innerHTML = `<div class="compute-value">Select a player to load operator map breakdown.</div>`;
        return;
    }
    if (!silent) {
        grid.innerHTML = `<div class="compute-value">Loading operator map breakdown...</div>`;
    }
    try {
        const res = await api.getOperatorsMapBreakdown(username, stack, matchType, minRounds);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const payload = await res.json();
        renderOperatorsMapCards(payload);
    } catch (err) {
        grid.innerHTML = `<div class="compute-value">Failed to load operator map breakdown: ${escapeHtml(String(err))}</div>`;
    }
}

function inferResult(matchData) {
    const a = toNumber(matchData?.score_team_a, 0);
    const b = toNumber(matchData?.score_team_b, 0);
    if (a === b) return "Unknown";
    return a > b ? "Win" : "Loss";
}

function readStatValue(stats, key) {
    if (!stats || typeof stats !== "object") return null;
    const raw = stats[key];
    if (typeof raw === "number") return raw;
    if (raw && typeof raw === "object" && typeof raw.value === "number") return raw.value;
    return null;
}

function scoreFromSummarySegments(matchData, username = "") {
    const normalized = String(username || "").trim().toLowerCase();
    if (!normalized) return null;
    const summaryData = matchData?.match_summary?.data;
    const segments = Array.isArray(summaryData?.segments) ? summaryData.segments : [];
    for (const seg of segments) {
        const md = seg?.metadata || {};
        const handle = String(md?.platformUserHandle || md?.name || md?.username || "").trim().toLowerCase();
        if (handle !== normalized) continue;
        const won = readStatValue(seg?.stats, "roundsWon");
        const lost = readStatValue(seg?.stats, "roundsLost");
        if (Number.isFinite(won) && Number.isFinite(lost)) {
            return { myScore: Number(won), oppScore: Number(lost) };
        }
    }
    return null;
}

function inferTeamPerspective(matchData, username = "") {
    const segmentScore = scoreFromSummarySegments(matchData, username);
    if (segmentScore) {
        let result = "Unknown";
        if (segmentScore.myScore !== segmentScore.oppScore) {
            result = segmentScore.myScore > segmentScore.oppScore ? "Win" : "Loss";
        }
        return { myScore: segmentScore.myScore, oppScore: segmentScore.oppScore, result };
    }

    const scoreA = toNumber(matchData?.score_team_a, 0);
    const scoreB = toNumber(matchData?.score_team_b, 0);
    const normalized = String(username || "").trim().toLowerCase();

    let team = "";
    const players = Array.isArray(matchData?.players) ? matchData.players : [];
    if (normalized && players.length) {
        const p = players.find((row) => String(row?.username || "").trim().toLowerCase() === normalized);
        if (p && (p.team === "A" || p.team === "B")) {
            team = p.team;
        }
    }

    let myScore = scoreA;
    let oppScore = scoreB;
    if (team === "B") {
        myScore = scoreB;
        oppScore = scoreA;
    }

    let result = "Unknown";
    if (myScore !== oppScore) {
        result = myScore > oppScore ? "Win" : "Loss";
    }

    return { myScore, oppScore, result };
}

function normalizeTeamLabel(rawTeam) {
    const norm = String(rawTeam ?? "").trim().toLowerCase();
    if (!norm) return "";
    if (norm === "a" || norm === "team_a" || norm === "teama" || norm === "0" || norm.includes("blue")) {
        return "A";
    }
    if (norm === "b" || norm === "team_b" || norm === "teamb" || norm === "1" || norm.includes("orange")) {
        return "B";
    }
    return "";
}

function getMatchTeams(match) {
    const players = Array.isArray(match?.players) ? match.players : [];
    const teamA = [];
    const teamB = [];
    for (const p of players) {
        const rawTeam = normalizeTeamLabel(p?.team);
        const name = String(p?.username || p?.name || p?.nickname || "").trim();
        if (!name) continue;
        if (rawTeam === "A") {
            teamA.push(name);
        } else if (rawTeam === "B") {
            teamB.push(name);
        }
    }
    return { teamA, teamB };
}

function renderMatchOverview(match, username = "") {
    const perspective = inferTeamPerspective(match, username);
    const map = match?.map || "Unknown map";
    const mode = match?.mode || "Unknown mode";
    const players = Array.isArray(match?.players) ? match.players : [];
    const resultClass =
        perspective.result === "Win" ? "stored-result-win" :
        perspective.result === "Loss" ? "stored-result-loss" :
        "stored-result-unknown";
    const rows = players
        .map((p) => ({
            team: normalizeTeamLabel(p?.team),
            username: String(p?.username || p?.name || p?.nickname || "").trim(),
            kills: p?.kills,
            deaths: p?.deaths,
            assists: p?.assists,
            kd: p?.kd,
            hs: p?.hs_percent ?? p?.hsPct,
        }))
        .filter((p) => p.username)
        .sort((a, b) => (a.team || "").localeCompare(b.team || "") || toNumber(b.kills, 0) - toNumber(a.kills, 0));
    const buildTeamTable = (title, titleClass, themeClass, teamRows) => {
        const body = teamRows.length
            ? teamRows.map((p) => (
                `<tr>` +
                `<td>${escapeHtml(p.username)}</td>` +
                `<td>${toNumber(p.kills, 0)}</td>` +
                `<td>${toNumber(p.deaths, 0)}</td>` +
                `<td>${toNumber(p.assists, 0)}</td>` +
                `<td>${toNumber(p.kd, 0).toFixed(2)}</td>` +
                `<td>${toNumber(p.hs, 0).toFixed(1)}%</td>` +
                `</tr>`
            )).join("")
            : `<tr><td colspan="6" class="stored-round-empty">No players captured</td></tr>`;
        return (
            `<div class="stored-team-table-card ${themeClass}">` +
            `<div class="stored-team-table-title ${titleClass}">${title}</div>` +
            `<div class="stored-table-wrap">` +
            `<table class="stored-data-table">` +
            `<thead><tr><th>Player</th><th>K</th><th>D</th><th>A</th><th>KD</th><th>HS%</th></tr></thead>` +
            `<tbody>${body}</tbody>` +
            `</table>` +
            `</div>` +
            `</div>`
        );
    };
    const teamA = rows.filter((p) => p.team === "A");
    const teamB = rows.filter((p) => p.team === "B");
    return (
        `<div class="stored-round-summary">` +
        `Map: ${escapeHtml(map)} | Mode: ${escapeHtml(mode)} | ` +
        `Result: <span class="${resultClass}">${escapeHtml(perspective.result)}</span> | ` +
        `Score: ${perspective.myScore}:${perspective.oppScore}` +
        `</div>` +
        `<div class="stored-round-section">` +
        `<div class="stored-round-section-title">Match Scoreboard</div>` +
        `<div class="stored-team-table-grid">` +
        `${buildTeamTable("Blue Team", "stored-team-blue", "stored-team-card-blue", teamA)}` +
        `${buildTeamTable("Orange Team", "stored-team-orange", "stored-team-card-orange", teamB)}` +
        `</div>` +
        `</div>`
    );
}

function classifyStoredMode(match) {
    const mode = String(match?.mode || "").trim().toLowerCase();
    if (!mode) return "other";
    if (mode.includes("unranked")) return "unranked";
    if (mode.includes("ranked")) return "ranked";
    if (mode.includes("standard")) return "standard";
    if (mode.includes("quick")) return "quick";
    if (mode.includes("event")) return "event";
    if (mode.includes("arcade")) return "arcade";
    return "other";
}

function applyStoredModeFilters(matches) {
    const showRanked = document.getElementById("stored-show-ranked")?.checked !== false;
    const showUnranked = document.getElementById("stored-show-unranked")?.checked !== false;
    return (Array.isArray(matches) ? matches : []).filter((match) => {
        const category = classifyStoredMode(match);
        if (category === "ranked") return showRanked;
        if (category === "unranked") return showUnranked;
        return true;
    });
}

function getDashboardModeFilters() {
    const include = new Set(
        Array.from(document.querySelectorAll(".dashboard-include-type-filter"))
            .filter((el) => el.checked)
            .map((el) => String(el.value || "").trim().toLowerCase())
            .filter(Boolean)
    );
    const exclude = new Set(
        Array.from(document.querySelectorAll(".dashboard-exclude-type-filter"))
            .filter((el) => el.checked)
            .map((el) => String(el.value || "").trim().toLowerCase())
            .filter(Boolean)
    );
    return { include, exclude };
}

function renderDashboardActiveFilterChips() {
    const target = document.getElementById("dashboard-active-filters");
    if (!target) return;
    const username = String(document.getElementById("compute-username")?.value || "").trim();
    const { include, exclude } = getDashboardModeFilters();
    const chips = [];
    if (username) chips.push(`User: ${username}`);
    if (include.size) chips.push(`Include: ${Array.from(include).join(", ")}`);
    if (exclude.size) chips.push(`Exclude: ${Array.from(exclude).join(", ")}`);
    if (!chips.length) {
        target.replaceChildren(createChip({ label: "No active dashboard filters", tone: "info", className: "dashboard-filter-chip empty" }));
        return;
    }
    target.replaceChildren(...chips.map((label) => createChip({ label, tone: "accent", className: "dashboard-filter-chip" })));
}

function renderDashboardNeedsUsernamePrompt() {
    const prompt = "Enter a username above to load dashboard.";
    const compute = document.getElementById("compute-results");
    if (compute) {
        compute.innerHTML = `
            <div class="compute-card">
                <div class="compute-label">Dashboard</div>
                <div class="compute-value">${escapeHtml(prompt)}</div>
            </div>
        `;
    }
    const insightsScroll = document.getElementById("dashboard-insights-scroll");
    if (insightsScroll) {
        insightsScroll.innerHTML = `<div class="compute-value">${escapeHtml(prompt)}</div>`;
    }
    const graphs = document.getElementById("dashboard-graphs-content");
    if (graphs) {
        graphs.innerHTML = `<div class="compute-value">${escapeHtml(prompt)}</div>`;
    }
}

function getDashboardRequestSignature(username) {
    const { include, exclude } = getDashboardModeFilters();
    const includeKey = Array.from(include).sort().join(",");
    const excludeKey = Array.from(exclude).sort().join(",");
    return `${String(username || "").trim().toLowerCase()}|i:${includeKey}|e:${excludeKey}`;
}

function triggerDashboardAutoRefresh({ force = false } = {}) {
    const panel = document.getElementById("panel-dashboard");
    if (!panel || !panel.classList.contains("active")) return;
    const username = (document.getElementById("compute-username")?.value || "").trim();
    if (!username) {
        renderDashboardNeedsUsernamePrompt();
        return;
    }
    const sig = getDashboardRequestSignature(username);
    if (!force && computeReportState.stats && sig === dashboardLastRequestSignature) {
        return;
    }
    if (dashboardComputeInFlight) {
        dashboardRefreshQueued = true;
        return;
    }
    dashboardLastRequestSignature = sig;
    runStatComputation(username);
}

function applyDashboardModeFilters(matches) {
    const { include, exclude } = getDashboardModeFilters();
    return (Array.isArray(matches) ? matches : []).filter((match) => {
        const category = classifyStoredMode(match);
        if (include.size && !include.has(category)) return false;
        if (exclude.has(category)) return false;
        return true;
    });
}

function cleanupStoredVirtualization() {
    if (typeof storedVirtualCleanup === "function") {
        storedVirtualCleanup();
    }
    storedVirtualCleanup = null;
}

function buildStoredMatchCardElement(match, idx, username, cardWidthPx = null) {
    const map = match?.map || "Unknown map";
    const perspective = inferTeamPerspective(match, username);
    const fullMatchId = String(match?.match_id || "No match ID");
    const shortMatchId =
        fullMatchId.length > 14
            ? `${fullMatchId.slice(0, 8)}...${fullMatchId.slice(-4)}`
            : fullMatchId;
    const result = perspective.result;
    const missingRoundData = match?.round_data_missing === true;
    const resultClass =
        result === "Win" ? "stored-result-win" :
        result === "Loss" ? "stored-result-loss" :
        "stored-result-unknown";
    const accentClass =
        result === "Win" ? "stored-card-win" :
        result === "Loss" ? "stored-card-loss" :
        "stored-card-draw";
    const scoreLabel = formatPerspectiveScoreLabel(perspective);
    const badge = matchTypeBadgeMeta(match?.mode);
    const timeLabel = formatMatchAgeLabel(match?.date);
    const rpDelta = extractRpDelta(match);
    const rpClass =
        rpDelta == null ? "" :
        rpDelta > 0 ? "stored-rp-pos" :
        rpDelta < 0 ? "stored-rp-neg" :
        "stored-rp-zero";
    const rpText =
        rpDelta == null ? "" :
        `${rpDelta > 0 ? "+" : ""}${Math.round(rpDelta)} RP`;
    const escapedMap = escapeHtml(String(map));

    const card = document.createElement("div");
    card.className = `stored-match-card ${accentClass}`;
    if (missingRoundData) {
        card.classList.add("stored-match-missing-rounds");
    }
    card.dataset.matchIndex = String(idx);
    if (Number.isFinite(cardWidthPx) && cardWidthPx > 0) {
        card.style.width = `${Math.floor(cardWidthPx)}px`;
    }
    card.innerHTML = `
        <div class="stored-card-media">
            <img class="stored-card-map-image" alt="${escapedMap}" loading="lazy" decoding="async">
            <div class="stored-card-map-placeholder" hidden>${escapedMap}</div>
            <div class="stored-card-map-overlay"></div>
            <div class="stored-card-map-name">${escapedMap}</div>
        </div>
        <div class="stored-card-bottom">
            <div class="stored-card-left">
                <div class="stored-card-score">${escapeHtml(scoreLabel)}</div>
                <div class="stored-card-result ${resultClass}">${escapeHtml(result)}</div>
                ${rpDelta == null ? "" : `<div class="stored-card-rp ${rpClass}">${rpText}</div>`}
            </div>
            <div class="stored-card-right">
                <span class="stored-card-badge ${badge.cls}">${escapeHtml(String(badge.label || "Other"))}</span>
                <div class="stored-card-time">${escapeHtml(timeLabel)}</div>
            </div>
        </div>
        <div class="stored-card-meta">
            ${escapeHtml(shortMatchId)}
            ${missingRoundData ? '<span class="stored-warning-badge">Round Data Missing</span>' : ""}
        </div>
    `;
    const imgEl = card.querySelector(".stored-card-map-image");
    const placeholderEl = card.querySelector(".stored-card-map-placeholder");
    attachStoredCardMapImageFallback(imgEl, placeholderEl, map);
    card.addEventListener("click", () => selectStoredMatch(idx, username));
    return card;
}

function renderStoredLoadingSkeleton() {
    const list = document.getElementById("stored-match-list");
    if (!list) return;
    cleanupStoredVirtualization();
    list.classList.remove("virtualized");
    list.innerHTML = "";
    for (let i = 0; i < 4; i += 1) {
        const card = document.createElement("div");
        card.className = "stored-match-card stored-skeleton-card";
        card.innerHTML = `
            <div class="stored-skeleton-media"></div>
            <div class="stored-skeleton-body">
                <div class="stored-skeleton-line wide"></div>
                <div class="stored-skeleton-line mid"></div>
                <div class="stored-skeleton-line short"></div>
            </div>
        `;
        list.appendChild(card);
    }
}

function renderStoredMatchesVirtualized(list, visibleMatches, username) {
    list.classList.add("virtualized");
    list.innerHTML = `
        <div class="stored-virtual-spacer"></div>
        <div class="stored-virtual-viewport"></div>
    `;
    const spacer = list.querySelector(".stored-virtual-spacer");
    const viewport = list.querySelector(".stored-virtual-viewport");
    if (!spacer || !viewport) return;

    let lastStart = -1;
    let lastEnd = -1;
    let lastCols = -1;
    let rafId = null;
    let rowStride = STORED_VIRTUAL_ROW_HEIGHT + STORED_VIRTUAL_CARD_GAP;

    const computeLayout = () => {
        const width = Math.max(1, list.clientWidth - 16);
        const cols = Math.max(1, Math.floor((width + STORED_VIRTUAL_CARD_GAP) / (STORED_VIRTUAL_CARD_MIN_WIDTH + STORED_VIRTUAL_CARD_GAP)));
        const cardWidth = (width - ((cols - 1) * STORED_VIRTUAL_CARD_GAP)) / cols;
        const totalRows = Math.ceil(visibleMatches.length / cols);
        const fullHeight = Math.max(0, (totalRows * rowStride) - STORED_VIRTUAL_CARD_GAP);
        spacer.style.height = `${Math.ceil(fullHeight)}px`;
        return { cols, cardWidth, totalRows };
    };

    const renderWindow = () => {
        rafId = null;
        const { cols, cardWidth, totalRows } = computeLayout();
        const scrollTop = list.scrollTop;
        const viewHeight = list.clientHeight;
        const overscanRows = 2;
        const startRow = Math.max(0, Math.floor(scrollTop / rowStride) - overscanRows);
        const endRow = Math.min(totalRows - 1, Math.ceil((scrollTop + viewHeight) / rowStride) + overscanRows);
        const startIdx = Math.max(0, startRow * cols);
        const endIdx = Math.min(visibleMatches.length - 1, ((endRow + 1) * cols) - 1);
        if (startIdx === lastStart && endIdx === lastEnd && cols === lastCols) return;
        lastStart = startIdx;
        lastEnd = endIdx;
        lastCols = cols;

        viewport.innerHTML = "";
        const frag = document.createDocumentFragment();
        for (let idx = startIdx; idx <= endIdx; idx += 1) {
            const match = visibleMatches[idx];
            if (!match) continue;
            const row = Math.floor(idx / cols);
            const col = idx % cols;
            const card = buildStoredMatchCardElement(match, idx, username, cardWidth);
            card.style.position = "absolute";
            card.style.top = `${Math.floor(row * rowStride)}px`;
            card.style.left = `${Math.floor(col * (cardWidth + STORED_VIRTUAL_CARD_GAP))}px`;
            frag.appendChild(card);
        }
        viewport.appendChild(frag);
    };

    const scheduleRender = () => {
        if (rafId != null) return;
        rafId = window.requestAnimationFrame(renderWindow);
    };

    const onScroll = () => scheduleRender();
    const onResize = () => {
        lastStart = -1;
        lastEnd = -1;
        lastCols = -1;
        scheduleRender();
    };

    list.addEventListener("scroll", onScroll);
    window.addEventListener("resize", onResize);
    scheduleRender();

    storedVirtualCleanup = () => {
        list.removeEventListener("scroll", onScroll);
        window.removeEventListener("resize", onResize);
        if (rafId != null) {
            window.cancelAnimationFrame(rafId);
            rafId = null;
        }
        list.classList.remove("virtualized");
    };
}

function renderStoredMatches(matches, username) {
    const list = document.getElementById("stored-match-list");
    cleanupStoredVirtualization();
    list.innerHTML = "";
    storedMatchesSource = Array.isArray(matches) ? matches : [];
    const visibleMatches = applyStoredModeFilters(storedMatchesSource);
    storedMatchesCache = visibleMatches;
    selectedStoredMatchIndex = -1;
    selectedStoredRoundIndex = -1;

    let wins = 0;
    let losses = 0;
    let missingRoundDataCount = 0;

    if (!visibleMatches.length) {
        const empty = document.createElement("div");
        empty.className = "stored-empty-state";
        empty.textContent = "No matches stored. Run a scrape first.";
        list.appendChild(empty);
    }

    for (let idx = 0; idx < visibleMatches.length; idx += 1) {
        const match = visibleMatches[idx];
        const perspective = inferTeamPerspective(match, username);
        const result = perspective.result;
        if (match?.round_data_missing === true) missingRoundDataCount += 1;
        if (result === "Win") wins += 1;
        if (result === "Loss") losses += 1;
    }

    if (visibleMatches.length >= STORED_VIRTUALIZE_THRESHOLD) {
        renderStoredMatchesVirtualized(list, visibleMatches, username);
    } else {
        list.classList.remove("virtualized");
        for (let idx = 0; idx < visibleMatches.length; idx += 1) {
            const card = buildStoredMatchCardElement(visibleMatches[idx], idx, username);
            list.appendChild(card);
        }
    }

    document.getElementById("stored-total").textContent = String(visibleMatches.length);
    document.getElementById("stored-wins").textContent = String(wins);
    document.getElementById("stored-losses").textContent = String(losses);
    if (missingRoundDataCount > 0) {
        logMatch(`Flagged ${missingRoundDataCount} stored matches with missing round data.`, "info");
    }

    // Do not auto-open insights on tab load/filter changes.
    // Insights should open only when the user clicks a match card.
    renderStoredDetail(null, username);
}

function getRoundsFromMatch(match) {
    const savedRounds = Array.isArray(match?.rounds) ? match.rounds : [];
    const rd = match?.round_data;
    if (rd && typeof rd === "object" && Array.isArray(rd.rounds) && rd.rounds.length) {
        const players = Array.isArray(rd.players) ? rd.players : [];
        const playerNameById = {};
        const playerTeamById = {};
        const playerOperatorById = {};
        const rawTeams = new Set();
        for (const p of players) {
            const id = p?.id;
            const name = p?.nickname || p?.pseudonym || p?.name || "";
            if (id && name) {
                playerNameById[String(id)] = String(name);
            }
            const team = p?.teamId ?? p?.team ?? p?.side ?? p?.teamName ?? "";
            if (id && team !== undefined && team !== null && String(team).trim()) {
                const teamKey = String(team).trim();
                playerTeamById[String(id)] = teamKey;
                rawTeams.add(teamKey);
            }
            const operator = extractOperatorName(
                p?.operator ?? p?.operatorName ?? p?.operator_name ?? p?.operatorData ?? p?.operator_data
            );
            if (id && operator) {
                playerOperatorById[String(id)] = operator;
            }
        }

        const teamColorByKey = {};
        let hasBlue = false;
        let hasOrange = false;
        for (const key of rawTeams) {
            const norm = String(key).trim().toLowerCase();
            if (norm.includes("blue") || norm === "a" || norm === "team_a" || norm === "teama") {
                teamColorByKey[key] = "blue";
                hasBlue = true;
                continue;
            }
            if (norm.includes("orange") || norm === "b" || norm === "team_b" || norm === "teamb") {
                teamColorByKey[key] = "orange";
                hasOrange = true;
            }
        }
        const unresolvedTeamKeys = [...rawTeams].filter((key) => !teamColorByKey[key]).sort((a, b) => a.localeCompare(b));
        for (const key of unresolvedTeamKeys) {
            if (!hasBlue) {
                teamColorByKey[key] = "blue";
                hasBlue = true;
            } else if (!hasOrange) {
                teamColorByKey[key] = "orange";
                hasOrange = true;
            } else {
                teamColorByKey[key] = "blue";
            }
        }

        const resolveTeamColor = (teamValue) => {
            if (teamValue === undefined || teamValue === null) return "";
            const key = String(teamValue).trim();
            if (!key) return "";
            if (teamColorByKey[key]) return teamColorByKey[key];
            const norm = key.toLowerCase();
            if (norm.includes("blue")) return "blue";
            if (norm.includes("orange")) return "orange";
            return "";
        };

        const extractTeamFromEvent = (ev, role) => {
            if (!ev || typeof ev !== "object") return "";
            if (role === "killer") {
                return (
                    ev?.attackerTeamId ??
                    ev?.attackerTeam ??
                    ev?.killerTeamId ??
                    ev?.killerTeam ??
                    playerTeamById[String(ev?.attackerId)] ??
                    ""
                );
            }
            return (
                ev?.victimTeamId ??
                ev?.victimTeam ??
                playerTeamById[String(ev?.victimId)] ??
                ""
            );
        }

        const killfeed = Array.isArray(rd.killfeed) ? rd.killfeed : [];
        const byRound = new Map();
        for (const ev of killfeed) {
            const rid = ev?.roundId;
            if (rid === undefined || rid === null) continue;
            const key = String(rid);
            const killerOperator =
                extractOperatorName(
                    ev?.attackerOperatorName ??
                    ev?.attackerOperator ??
                    ev?.killerOperatorName ??
                    ev?.killerOperator
                ) ??
                playerOperatorById[String(ev?.attackerId)] ??
                "";
            const victimOperator =
                extractOperatorName(ev?.victimOperatorName ?? ev?.victimOperator) ??
                playerOperatorById[String(ev?.victimId)] ??
                "";
            const out = {
                timestamp: ev?.timestamp,
                killerId: ev?.attackerId,
                victimId: ev?.victimId,
                killerName: playerNameById[String(ev?.attackerId)] || ev?.attackerId || "Unknown",
                victimName: playerNameById[String(ev?.victimId)] || ev?.victimId || "Unknown",
                killerTeam: resolveTeamColor(extractTeamFromEvent(ev, "killer")),
                victimTeam: resolveTeamColor(extractTeamFromEvent(ev, "victim")),
                killerOperator: killerOperator ? String(killerOperator) : "",
                victimOperator: victimOperator ? String(victimOperator) : "",
            };
            if (!byRound.has(key)) byRound.set(key, []);
            byRound.get(key).push(out);
        }

        const parsedFromRoundData = rd.rounds.map((r, idx) => {
            const rid = r?.id ?? idx + 1;
            const num = typeof rid === "number" ? rid : (String(rid).match(/^\d+$/) ? Number(rid) : idx + 1);
            const events = byRound.get(String(num)) || byRound.get(String(rid)) || [];
            const roundPlayers = Array.isArray(r?.players) ? r.players : [];
            const roundOperatorById = {};
            for (const rp of roundPlayers) {
                const pid = rp?.id ?? rp?.playerId ?? rp?.player_id;
                const opName = extractOperatorName(
                    rp?.operator ?? rp?.operatorName ?? rp?.operator_name ?? rp?.operatorData ?? rp?.operator_data
                );
                if (pid && opName) {
                    roundOperatorById[String(pid)] = opName;
                }
            }
            for (const ev of events) {
                if (!ev?.killerOperator && ev?.killerId !== undefined && ev?.killerId !== null) {
                    ev.killerOperator = roundOperatorById[String(ev.killerId)] || "";
                }
                if (!ev?.victimOperator && ev?.victimId !== undefined && ev?.victimId !== null) {
                    ev.victimOperator = roundOperatorById[String(ev.victimId)] || "";
                }
            }
            return {
                round_number: num,
                winner: r?.winner || r?.winningTeam || r?.resultId || "unknown",
                outcome: r?.roundOutcome || r?.outcome || r?.outcomeId || r?.resultId || "unknown",
                kill_events: events,
                players: roundPlayers,
            };
        });

        if (savedRounds.length) {
            for (let i = 0; i < parsedFromRoundData.length; i++) {
                const parsedRound = parsedFromRoundData[i];
                const savedRound = savedRounds[i] || {};
                const parsedEvents = Array.isArray(parsedRound?.kill_events) ? parsedRound.kill_events : [];
                const savedEvents = Array.isArray(savedRound?.kill_events) ? savedRound.kill_events : [];
                for (let j = 0; j < parsedEvents.length; j++) {
                    const pEv = parsedEvents[j] || {};
                    const sEv = savedEvents[j] || {};
                    if (!pEv.killerOperator) {
                        pEv.killerOperator = extractOperatorName(
                            sEv?.killerOperator ?? sEv?.attackerOperator ?? sEv?.killer_operator
                        );
                    }
                    if (!pEv.victimOperator) {
                        pEv.victimOperator = extractOperatorName(
                            sEv?.victimOperator ?? sEv?.victim_operator
                        );
                    }
                }
                if (!Array.isArray(parsedRound?.players) || !parsedRound.players.length) {
                    parsedRound.players = Array.isArray(savedRound?.players) ? savedRound.players : [];
                }
            }
        }
        return parsedFromRoundData;
    }

    if (savedRounds.length) {
        for (const round of savedRounds) {
            const events = Array.isArray(round?.kill_events) ? round.kill_events : [];
            for (const ev of events) {
                if (!ev || typeof ev !== "object") continue;
                if (!ev.killerOperator) {
                    ev.killerOperator = extractOperatorName(
                        ev?.attackerOperator ?? ev?.killer_operator ?? ev?.operatorKiller
                    );
                }
                if (!ev.victimOperator) {
                    ev.victimOperator = extractOperatorName(
                        ev?.victim_operator ?? ev?.operatorVictim
                    );
                }
            }
        }
        return savedRounds;
    }
    const fallback = match?.round_data?.rounds;
    return Array.isArray(fallback) ? fallback : [];
}

function attachStoredCardMapImageFallback(imgEl, placeholderEl, mapName) {
    if (!imgEl) return;
    const chain = [trackerMapImageUrl(mapName)];
    let idx = 0;
    const showPlaceholder = () => {
        imgEl.hidden = true;
        if (placeholderEl) {
            placeholderEl.hidden = false;
            placeholderEl.textContent = String(mapName || "Unknown map");
        }
    };
    const tryNext = () => {
        if (idx >= chain.length) {
            showPlaceholder();
            return;
        }
        imgEl.src = chain[idx];
        idx += 1;
    };
    imgEl.addEventListener("error", tryNext);
    imgEl.addEventListener("load", () => {
        imgEl.hidden = false;
        if (placeholderEl) placeholderEl.hidden = true;
    });
    tryNext();
}

function getRoundEvents(round) {
    if (Array.isArray(round?.kill_events)) return round.kill_events;
    if (Array.isArray(round?.killEvents)) return round.killEvents;
    if (Array.isArray(round?.events)) return round.events;
    return [];
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");
}

function eventTeamClass(ev, role) {
    const raw = role === "killer"
        ? (ev?.killerTeam ?? ev?.killer_team ?? ev?.attackerTeam ?? ev?.attacker_team ?? "")
        : (ev?.victimTeam ?? ev?.victim_team ?? "");
    const norm = String(raw || "").trim().toLowerCase();
    if (norm === "blue") return "stored-team-blue";
    if (norm === "orange") return "stored-team-orange";
    return "";
}

function buildRoundScoreRows(round) {
    const roundPlayers = Array.isArray(round?.players) ? round.players : [];
    let rows = roundPlayers
        .map((p) => {
            const name = String(
                p?.nickname || p?.pseudonym || p?.name || p?.username || p?.playerName || ""
            ).trim();
            return {
                team: normalizeTeamLabel(p?.team ?? p?.teamId ?? p?.side ?? p?.teamName),
                username: name,
                kills: p?.kills,
                deaths: p?.deaths,
                assists: p?.assists,
                kd: p?.kd ?? p?.kdRatio,
                hs: p?.hs_percent ?? p?.hsPct ?? p?.headshotPct,
                operator: extractOperatorName(
                    p?.operator ?? p?.operatorName ?? p?.operator_name ?? p?.operatorData ?? p?.operator_data
                ),
            };
        })
        .filter((p) => p.username)
        .sort((a, b) => (a.team || "").localeCompare(b.team || "") || toNumber(b.kills, 0) - toNumber(a.kills, 0));

    // Fallback: derive a minimal per-round scoreboard from kill events when round.players is empty.
    if (!rows.length) {
        const events = getRoundEvents(round);
        const byName = {};
        for (const ev of events) {
            const killer = String(
                ev?.killerName || ev?.killer || ev?.killerUsername || ev?.attacker || ev?.from || ""
            ).trim();
            const victim = String(
                ev?.victimName || ev?.victim || ev?.victimUsername || ev?.target || ev?.to || ""
            ).trim();
            const killerTeamRaw = String(ev?.killerTeam || ev?.killer_team || ev?.attackerTeam || "").toLowerCase();
            const victimTeamRaw = String(ev?.victimTeam || ev?.victim_team || "").toLowerCase();
            const killerTeam = killerTeamRaw.includes("blue") ? "A" : killerTeamRaw.includes("orange") ? "B" : "";
            const victimTeam = victimTeamRaw.includes("blue") ? "A" : victimTeamRaw.includes("orange") ? "B" : "";
            const killerOp = extractOperatorName(ev?.killerOperator || ev?.attackerOperator || ev?.killer_operator);
            const victimOp = extractOperatorName(ev?.victimOperator || ev?.victim_operator);

            if (killer) {
                if (!byName[killer]) {
                    byName[killer] = { team: killerTeam, username: killer, kills: 0, deaths: 0, assists: 0, kd: 0, hs: 0, operator: killerOp || "" };
                }
                byName[killer].kills += 1;
                if (!byName[killer].team && killerTeam) byName[killer].team = killerTeam;
                if (!byName[killer].operator && killerOp) byName[killer].operator = killerOp;
            }
            if (victim) {
                if (!byName[victim]) {
                    byName[victim] = { team: victimTeam, username: victim, kills: 0, deaths: 0, assists: 0, kd: 0, hs: 0, operator: victimOp || "" };
                }
                byName[victim].deaths += 1;
                if (!byName[victim].team && victimTeam) byName[victim].team = victimTeam;
                if (!byName[victim].operator && victimOp) byName[victim].operator = victimOp;
            }
        }
        rows = Object.values(byName).map((p) => ({
            ...p,
            kd: p.deaths ? p.kills / p.deaths : p.kills,
            hs: 0,
        }));
        rows.sort((a, b) => (a.team || "").localeCompare(b.team || "") || toNumber(b.kills, 0) - toNumber(a.kills, 0));
    }
    return rows;
}

function renderRoundPlayersTable(round) {
    const rows = buildRoundScoreRows(round);
    if (!rows.length) {
        return "<div class=\"stored-round-empty\">No round player rows or kill events captured for this round.</div>";
    }
    const events = getRoundEvents(round);
    const first = events.length ? (events[0] || {}) : {};
    const firstKiller = String(first?.killerName || first?.killer || first?.killerUsername || first?.attacker || first?.from || "").trim().toLowerCase();
    const firstVictim = String(first?.victimName || first?.victim || first?.victimUsername || first?.target || first?.to || "").trim().toLowerCase();
    const body = rows.map((p) => {
        const rowClass = p.team === "A" ? "stored-row-blue" : p.team === "B" ? "stored-row-orange" : "";
        const nameNorm = String(p.username || "").trim().toLowerCase();
        const fk = nameNorm && nameNorm === firstKiller ? "1" : "";
        const fd = nameNorm && nameNorm === firstVictim ? "1" : "";
        return (
            `<tr class="${rowClass}">` +
            `<td>${escapeHtml(p.username)}</td>` +
            `<td>${escapeHtml(p.operator || "-")}</td>` +
            `<td>${toNumber(p.kills, 0)}</td>` +
            `<td>${toNumber(p.deaths, 0)}</td>` +
            `<td>${toNumber(p.assists, 0)}</td>` +
            `<td>${toNumber(p.kd, 0).toFixed(2)}</td>` +
            `<td>${toNumber(p.hs, 0).toFixed(1)}%</td>` +
            `<td>${fk}</td>` +
            `<td>${fd}</td>` +
            `</tr>`
        );
    }).join("");
    return (
        `<div class="stored-table-wrap">` +
        `<table class="stored-data-table">` +
        `<thead><tr><th>Player</th><th>Operator</th><th>K</th><th>D</th><th>A</th><th>KD</th><th>HS%</th><th>FK</th><th>FD</th></tr></thead>` +
        `<tbody>${body}</tbody>` +
        `</table>` +
        `</div>`
    );
}

function renderRoundEvents(round) {
    const events = getRoundEvents(round);
    if (!events.length) {
        return "<div class=\"stored-round-empty\">No round events captured for this round.</div>";
    }

    const operatorByName = {};
    const roundPlayers = Array.isArray(round?.players) ? round.players : [];
    for (const p of roundPlayers) {
        const name = String(
            p?.nickname || p?.pseudonym || p?.name || p?.username || p?.playerName || ""
        ).trim().toLowerCase();
        if (!name) continue;
        const opName = extractOperatorName(
            p?.operator ?? p?.operatorName ?? p?.operator_name ?? p?.operatorData ?? p?.operator_data
        );
        if (opName && !operatorByName[name]) {
            operatorByName[name] = opName;
        }
    }

    const formatEventTime = (value) => {
        const n = Number(value);
        if (!Number.isFinite(n) || n <= 0) return value ? String(value) : "-";
        const ms = n > 1e12 ? n : n * 1000;
        try {
            const dt = new Date(ms);
            if (Number.isNaN(dt.getTime())) return String(value);
            return dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
        } catch (_) {
            return String(value);
        }
    };
    const rows = [];
    for (let i = 0; i < events.length; i++) {
        const ev = events[i] || {};
        const killer =
            ev.killerName || ev.killer || ev.killerUsername || ev.attacker || ev.from || "Unknown";
        const victim =
            ev.victimName || ev.victim || ev.victimUsername || ev.target || ev.to || "Unknown";
        const weapon = ev.weapon || ev.weaponName || ev.gun || "";
        const when = ev.time || ev.timestamp || ev.eventTime || "";
        const killerOperator =
            extractOperatorName(ev.killerOperator || ev.attackerOperator || ev.killer_operator) ||
            operatorByName[String(killer).trim().toLowerCase()] ||
            "";
        const victimOperator =
            extractOperatorName(ev.victimOperator || ev.victim_operator) ||
            operatorByName[String(victim).trim().toLowerCase()] ||
            "";
        const killerClass = eventTeamClass(ev, "killer");
        const victimClass = eventTeamClass(ev, "victim");
        const killerIconUrl = resolveOperatorImageUrl(killerOperator);
        const victimIconUrl = resolveOperatorImageUrl(victimOperator);
        const killerIconHtml = killerIconUrl
            ? `<img class="stored-op-icon" src="${killerIconUrl}" alt="${escapeHtml(killerOperator || "Operator")}" onerror="this.style.display='none'">`
            : operatorFallbackBadge(killerOperator);
        const victimIconHtml = victimIconUrl
            ? `<img class="stored-op-icon" src="${victimIconUrl}" alt="${escapeHtml(victimOperator || "Operator")}" onerror="this.style.display='none'">`
            : operatorFallbackBadge(victimOperator);
        const killerHtml = killerClass
            ? `<span class="${killerClass}">${escapeHtml(killer)}</span>`
            : escapeHtml(killer);
        const victimHtml = victimClass
            ? `<span class="${victimClass}">${escapeHtml(victim)}</span>`
            : escapeHtml(victim);
        rows.push(
            `<tr class="${i === 0 ? "stored-first-event" : ""}">` +
            `<td>${i + 1}</td>` +
                `<td>${killerIconHtml}${killerHtml}${i === 0 ? ' <span class="stored-first-tag fk">FK</span>' : ""}</td>` +
                `<td>${victimIconHtml}${victimHtml}${i === 0 ? ' <span class="stored-first-tag fd">FD</span>' : ""}</td>` +
                `<td>${escapeHtml(weapon || "-")}</td>` +
                `<td>${escapeHtml(formatEventTime(when))}</td>` +
                `</tr>`
        );
    }
    return (
        `<div class="stored-table-wrap">` +
        `<table class="stored-data-table">` +
        `<thead><tr><th>#</th><th>Killer</th><th>Victim</th><th>Weapon</th><th>Time</th></tr></thead>` +
        `<tbody>${rows.join("")}</tbody>` +
        `</table>` +
        `</div>`
    );
}

function renderStoredDetail(match, username) {
    const detailEl = document.getElementById("stored-detail");
    const titleEl = document.getElementById("stored-detail-title");
    const metaEl = document.getElementById("stored-detail-meta");
    const insightsEl = document.getElementById("stored-insights");
    const roundListEl = document.getElementById("stored-round-list");
    const roundLabelEl = document.getElementById("stored-round-label");
    const roundBodyEl = document.getElementById("stored-round-body");
    const iconHintEl = document.getElementById("stored-operator-icon-hint");

    if (!match) {
        detailEl.classList.add("hidden");
        titleEl.textContent = "Match Insights";
        metaEl.textContent = "";
        insightsEl.textContent = "Select a stored match card to view insights and rounds.";
        roundListEl.innerHTML = "";
        roundLabelEl.textContent = "Round -/-";
        roundBodyEl.textContent = "No round data available.";
        return;
    }
    detailEl.classList.remove("hidden");

    if (iconHintEl) {
        if (operatorImageIndexLoaded && (!operatorImageIndexEnabled || operatorImageIndexCount <= 0)) {
            iconHintEl.textContent = "Local operator pack not detected. Using online/fallback icons.";
            iconHintEl.classList.remove("hidden");
        } else {
            iconHintEl.textContent = "";
            iconHintEl.classList.add("hidden");
        }
    }

    const map = match?.map || "Unknown map";
    const perspective = inferTeamPerspective(match, username);
    const result = perspective.result;
    const rounds = getRoundsFromMatch(match);

    titleEl.textContent = `Match Insights: ${map}`;
    metaEl.textContent = `${match?.date || "No date"} | ${match?.match_id || "No match ID"}`;
    insightsEl.textContent =
        `Placeholder insights: ${result} (${perspective.myScore}:${perspective.oppScore}) in ` +
        `${match?.mode || "Unknown mode"}. Advanced computational insights will be added next.`;

    const totalRounds = rounds.length;
    roundListEl.innerHTML = "";
    const overviewChip = document.createElement("button");
    overviewChip.type = "button";
    overviewChip.className = `stored-round-chip${selectedStoredRoundIndex < 0 ? " active" : ""}`;
    overviewChip.textContent = "Overview";
    overviewChip.addEventListener("click", () => {
        selectedStoredRoundIndex = -1;
        renderStoredDetail(match, username);
    });
    roundListEl.appendChild(overviewChip);

    for (let i = 0; i < totalRounds; i++) {
        const chip = document.createElement("button");
        chip.type = "button";
        chip.className = `stored-round-chip${i === selectedStoredRoundIndex ? " active" : ""}`;
        chip.textContent = `Round ${i + 1}`;
        chip.addEventListener("click", () => {
            selectedStoredRoundIndex = i;
            renderStoredDetail(match, username);
        });
        roundListEl.appendChild(chip);
    }

    if (!totalRounds) {
        roundLabelEl.textContent = "Match Overview";
        roundBodyEl.innerHTML =
            `${renderMatchOverview(match, username)}` +
            `<div class="stored-round-empty">No round-level data captured for this match.</div>`;
        return;
    }

    if (selectedStoredRoundIndex >= totalRounds) selectedStoredRoundIndex = totalRounds - 1;
    if (selectedStoredRoundIndex < 0) {
        roundLabelEl.textContent = "Match Overview";
        roundBodyEl.innerHTML = renderMatchOverview(match, username);
        return;
    }

    const round = rounds[selectedStoredRoundIndex] || {};
    const roundNum = Number(round?.round_number || round?.roundNumber || selectedStoredRoundIndex + 1);
    const winner = round?.winner || round?.winner_side || round?.winningTeam || "Unknown";
    const outcome = round?.outcome || round?.end_reason || "Unknown";
    const kills = getRoundEvents(round).length;
    const players = buildRoundScoreRows(round).length;

    roundLabelEl.textContent = `Round ${selectedStoredRoundIndex + 1}/${totalRounds}`;
    roundBodyEl.innerHTML =
        `<div class="stored-round-summary">` +
        `Round #${roundNum} | Winner: ${winner} | Outcome: ${outcome} | ` +
        `Events: ${kills} | Players tracked: ${players}` +
        `</div>` +
        `<div class="stored-round-section">` +
        `<div class="stored-round-section-title">Round Scoreboard</div>` +
        `${renderRoundPlayersTable(round)}` +
        `</div>` +
        `<div class="stored-round-section">` +
        `<div class="stored-round-section-title">Round Eliminations</div>` +
        `${renderRoundEvents(round)}` +
        `</div>`;
}

function selectStoredMatch(index, username) {
    if (!Array.isArray(storedMatchesCache) || index < 0 || index >= storedMatchesCache.length) {
        return;
    }
    selectedStoredMatchIndex = index;
    selectedStoredRoundIndex = -1;
    renderStoredDetail(storedMatchesCache[index], username);
}

function closeStoredDetail() {
    document.getElementById("stored-detail").classList.add("hidden");
}

async function unpackStoredMatches(explicitUsername = "") {
    const username = (explicitUsername || document.getElementById("stored-username").value || "").trim();
    if (!username) {
        logMatch("Enter a username before unpacking stored matches.", "error");
        return;
    }
    const btn = document.getElementById("unpack-stored-matches");
    if (btn) btn.disabled = true;
    try {
        const resp = await api.postUnpackScrapedMatches(username, 5000);
        if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}`);
        }
        const payload = await resp.json();
        const stats = payload?.stats || {};
        logMatch(
            `Unpack complete for ${username}: scanned=${toNumber(stats.scanned, 0)}, ` +
            `unpacked=${toNumber(stats.unpacked_matches, 0)}, skipped=${toNumber(stats.skipped, 0)}, ` +
            `errors=${toNumber(stats.errors, 0)}`,
            "success"
        );
        await loadStoredMatchesView(username, true);
    } catch (err) {
        logMatch(`Failed to unpack stored matches: ${err}`, "error");
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function deleteBadStoredMatches(explicitUsername = "") {
    const username = (explicitUsername || document.getElementById("stored-username").value || "").trim();
    if (!username) {
        logMatch("Enter a username before deleting bad stored matches.", "error");
        return;
    }
    const confirmed = window.confirm(
        `Delete all flagged bad matches for ${username}? This removes those cards and normalized rows.`
    );
    if (!confirmed) return;

    const btn = document.getElementById("delete-bad-stored-matches");
    if (btn) btn.disabled = true;
    try {
        const resp = await api.postDeleteBadScrapedMatches(username);
        if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}`);
        }
        const payload = await resp.json();
        const stats = payload?.stats || {};
        logMatch(
            `Deleted bad matches for ${username}: cards=${toNumber(stats.deleted_cards, 0)}, ` +
            `match_ids=${toNumber(stats.deleted_match_ids, 0)}, detail_rows=${toNumber(stats.deleted_detail_rows, 0)}, ` +
            `round_rows=${toNumber(stats.deleted_round_rows, 0)}, player_round_rows=${toNumber(stats.deleted_player_round_rows, 0)}`,
            "success"
        );
        await loadStoredMatchesView(username, true);
    } catch (err) {
        logMatch(`Failed to delete bad stored matches: ${err}`, "error");
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function loadStoredMatchesView(explicitUsername = "", silent = false) {
    const username = (explicitUsername || document.getElementById("stored-username").value || "").trim();
    if (!username) {
        if (!silent) {
            logMatch("Enter a username before loading stored matches.", "error");
        }
        return;
    }

    currentStoredUsername = username;
    renderStoredLoadingSkeleton();
    try {
        const res = await api.getScrapedMatches(username, 10000);
        if (!res.ok) {
            throw new Error(`HTTP ${res.status}`);
        }
        const payload = await res.json();
        const matches = Array.isArray(payload.matches) ? payload.matches : [];
        renderStoredMatches(matches, username);
        if (!silent) {
            logMatch(`Loaded stored view for ${username} (${matches.length} matches)`, "success");
        }
    } catch (err) {
        if (!silent) {
            logMatch(`Failed to load stored view: ${err}`, "error");
        }
    }
}

function logCompute(message, level = "info") {
    queueLogEntry("compute-log", message, level);
}

function openSettingsModal() {
    settingsModalController.open();
}

function closeSettingsModal() {
    settingsModalController.close();
}

function renderStandardizerReport(payload) {
    const report = payload?.report || {};
    const flags = Array.isArray(report.data_quality_flags) ? report.data_quality_flags : [];
    const lines = [
        `DB: ${report.db_path || "-"}`,
        `Run at: ${report.run_at || "-"}`,
        `Mode: ${payload?.dry_run ? "Dry Run" : "Write Changes"}`,
        "",
        `Total matches: ${toNumber(report.total_matches, 0)}`,
        `Total player_rounds: ${toNumber(report.total_player_rounds, 0)}`,
        `Total round_outcomes: ${toNumber(report.total_round_outcomes, 0)}`,
        "",
        `Null usernames: found=${toNumber(report.null_usernames_found, 0)} fixed=${toNumber(report.null_usernames_fixed, 0)}`,
        `Match type normalization: found=${toNumber(report.bad_match_types_found, 0)} fixed=${toNumber(report.bad_match_types_fixed, 0)}`,
        `Summary kill reconstruction: missing=${toNumber(report.summary_kills_missing, 0)} reconstructed=${toNumber(report.summary_kills_reconstructed, 0)}`,
        `OW-ingest stats backfill: missing=${toNumber(report.owingest_stats_missing, 0)} fixed=${toNumber(report.owingest_stats_fixed, 0)}`,
        `Killer operator backfill: missing=${toNumber(report.killed_by_op_missing, 0)} fixed=${toNumber(report.killed_by_op_fixed, 0)}`,
    ];
    if (flags.length) {
        lines.push("", "Data quality flags:");
        for (const flag of flags) lines.push(`- ${String(flag)}`);
    }
    return lines.join("\n");
}

async function runDbStandardizerFromSettings() {
    const dryRunEl = document.getElementById("settings-dry-run");
    const verboseEl = document.getElementById("settings-verbose");
    const runBtn = document.getElementById("run-db-standardizer");
    const output = document.getElementById("settings-output");
    if (!dryRunEl || !verboseEl || !runBtn || !output) return;

    runBtn.disabled = true;
    output.textContent = "Running DB standardizer...";
    try {
        const res = await api.postDbStandardize(dryRunEl.checked, verboseEl.checked);
        if (!res.ok) {
            const detail = await res.text();
            throw new Error(`HTTP ${res.status}: ${detail}`);
        }
        const payload = await res.json();
        output.textContent = renderStandardizerReport(payload);
        logCompute("DB standardizer finished via Settings.", "success");
    } catch (err) {
        output.textContent = `Failed to run DB standardizer:\n${String(err)}`;
        logCompute(`DB standardizer failed: ${err}`, "error");
    } finally {
        runBtn.disabled = false;
    }
}

function renderComputeCards(stats) {
    computeReportState.stats = stats;
    renderComputeReport();
}

function formatFixed(value, digits = 1) {
    const n = Number(value);
    return Number.isFinite(n) ? n.toFixed(digits) : Number(0).toFixed(digits);
}

function modeSummaryCard(label, key, s, isActive) {
    return `
        <button class="compute-mode-card ${isActive ? "active" : ""}" data-mode="${key}">
            <div class="compute-mode-card-label">${label}</div>
            <div class="compute-mode-card-value">${formatFixed(s.winRate, 1)}%</div>
            <div class="compute-mode-card-sub">${toNumber(s.wins, 0)}-${toNumber(s.losses, 0)} • ${toNumber(s.matches, 0)} matches</div>
            <div class="compute-mode-card-sub">Avg K/D ${formatFixed(s.avgKd, 2)}</div>
            <div class="compute-mode-card-sub">Avg K/D/A ${formatFixed(s.avgKills, 1)} / ${formatFixed(s.avgDeaths, 1)} / ${formatFixed(s.avgAssists, 1)}</div>
            <div class="compute-mode-card-sub">Tracked ${toNumber(s.trackedRows, 0)}/${toNumber(s.matches, 0)}</div>
        </button>
    `;
}

function tooltipIcon(text) {
    return `<span class="compute-info" tabindex="0" title="${escapeHtml(text)}" aria-label="${escapeHtml(text)}">ⓘ</span>`;
}

function statTile(label, value, sample, tooltip = "") {
    return `
        <div class="compute-tile">
            <div class="compute-tile-label">${escapeHtml(label)} ${tooltip ? tooltipIcon(tooltip) : ""}</div>
            <div class="compute-tile-value">${value}</div>
            <div class="compute-tile-sub">${escapeHtml(sample)}</div>
        </div>
    `;
}

function findingPriority(sev) {
    if (sev === "critical") return 0;
    if (sev === "warning") return 1;
    return 2;
}

function splitFindingMessage(message) {
    const text = String(message || "").trim();
    if (!text) return { headline: "No finding text", impact: "" };
    const sentences = text.split(".").map((s) => s.trim()).filter(Boolean);
    return {
        headline: sentences[0] || text,
        impact: sentences.length > 1 ? (sentences[1] || "").trim() : "",
    };
}

function buildFilteredPlaybookFindings(filteredMatches, stats) {
    const rows = Array.isArray(filteredMatches) ? filteredMatches : [];
    const overall = stats?.overall || {};
    const total = toNumber(overall.matches, 0);
    const wins = toNumber(overall.wins, 0);
    const losses = toNumber(overall.losses, 0);
    const winRate = toNumber(overall.winRate, 0);
    const avgKd = toNumber(overall.avgKd, 0);
    const findings = [];
    const modeCounts = new Map();

    for (const match of rows) {
        const mode = classifyStoredMode(match);
        modeCounts.set(mode, toNumber(modeCounts.get(mode), 0) + 1);
    }
    const modeCitations = Array.from(modeCounts.entries())
        .sort((a, b) => b[1] - a[1])
        .map(([mode, count]) => `${mode}: ${count} matches`);
    const { include, exclude } = getDashboardModeFilters();
    const includeText = include.size ? Array.from(include).sort().join(", ") : "all";
    const excludeText = exclude.size ? Array.from(exclude).sort().join(", ") : "none";

    findings.push({
        severity: "info",
        message:
            `Filtered set recalculated: ${total} matches (${wins}-${losses}, ${formatFixed(winRate, 1)}% WR). ` +
            `Include=${includeText}; Exclude=${excludeText}.`,
        citations: modeCitations,
    });

    if (total > 0 && total < 8) {
        findings.push({
            severity: "warning",
            message: `Small filtered sample (${total} matches). Treat this playbook as directional.`,
        });
    }
    if (total >= 8 && winRate >= 55) {
        findings.push({
            severity: "info",
            message: `Current filtered trend is strong (${formatFixed(winRate, 1)}% WR). Keep this queue profile.`,
        });
    } else if (total >= 8 && winRate <= 45) {
        findings.push({
            severity: "warning",
            message: `Current filtered trend is weak (${formatFixed(winRate, 1)}% WR). Adjust comp/pace for these modes.`,
        });
    }
    if (total >= 8 && avgKd < 1.0) {
        findings.push({
            severity: "warning",
            message: `Average K/D is ${formatFixed(avgKd, 2)} in the filtered set. Prioritize survivability and trade spacing.`,
        });
    }

    return findings;
}

function normalizeCitation(citation) {
    if (typeof citation === "string") {
        return citation.trim();
    }
    if (citation && typeof citation === "object") {
        const preferred = ["message", "text", "label", "match_id", "round_id", "map_name"];
        for (const key of preferred) {
            const value = citation[key];
            if (typeof value === "string" && value.trim()) return value.trim();
            if (typeof value === "number" && Number.isFinite(value)) return String(value);
        }
        try {
            return JSON.stringify(citation);
        } catch (_) {
            return String(citation);
        }
    }
    return String(citation ?? "").trim();
}

function formatCitationText(citations) {
    const rows = (Array.isArray(citations) ? citations : [])
        .map(normalizeCitation)
        .filter(Boolean);
    return rows.length ? rows.join("\n") : "No citation provided.";
}

function flattenSortedFindings() {
    const sources = [
        { label: "Filtered Match Set", data: { findings: computeReportState.playbookFindings || [] } },
        { label: "Round Analysis", data: computeReportState.round },
        { label: "Teammate Chemistry", data: computeReportState.chemistry },
        { label: "Lobby Quality", data: computeReportState.lobby },
        { label: "Trade Analysis", data: computeReportState.trade },
        { label: "Team Analysis", data: computeReportState.team },
        { label: "Operator Stats", data: computeReportState.operator },
        { label: "Map Stats", data: computeReportState.map },
    ];
    const combined = [];
    for (const src of sources) {
        const findings = Array.isArray(src.data?.findings) ? src.data.findings : [];
        for (const finding of findings) {
            const severity = normalizeFindingSeverity(finding?.severity);
            combined.push({
                ...finding,
                severity,
                source: src.label,
            });
        }
    }
    return combined.sort((a, b) => findingPriority(a.severity) - findingPriority(b.severity));
}

function renderEvidencePanel(key, html) {
    const panel = document.getElementById("dashboard-evidence");
    if (!panel) return;
    computeReportState.selectedEvidenceKey = key;
    panel.innerHTML = html;
}

function renderPlaybook() {
    const container = document.getElementById("compute-playbook");
    if (!container) return;
    const findings = flattenSortedFindings();
    computeReportState.evidenceByKey = {};
    if (!findings.length) {
        container.innerHTML = `<div class="compute-value">No findings generated for this player.</div>`;
        return;
    }
    container.innerHTML = findings
        .map((f, idx) => {
            const sev = normalizeFindingSeverity(f?.severity);
            const parts = splitFindingMessage(f?.message || "");
            const sevLabel = sev === "critical" ? "Critical" : sev === "warning" ? "Warning" : "Info";
            const key = `playbook-${idx}`;
            computeReportState.evidenceByKey[key] = {
                title: parts.headline || "Finding",
                source: f.source || "Insight",
                text: formatCitationText(f?.citations),
            };
            return `
                <article class="playbook-card playbook-${sev}" data-playbook-index="${idx}">
                    <div class="playbook-top">
                        <span class="severity-pill sev-${sev}">${sevLabel}</span>
                        <span class="playbook-source">${escapeHtml(f.source || "Insight")}</span>
                    </div>
                    <h4 class="playbook-headline">${escapeHtml(parts.headline)}</h4>
                    ${parts.impact ? `<div class="playbook-line"><strong>Impact:</strong> ${escapeHtml(parts.impact)}</div>` : ""}
                    <button
                        type="button"
                        class="playbook-evidence"
                        data-evidence-key="${key}"
                    >
                        Evidence
                    </button>
                </article>
            `;
        })
        .join("");

    container.querySelectorAll(".playbook-evidence").forEach((btn) => {
        btn.addEventListener("click", () => {
            const key = btn.dataset.evidenceKey || "";
            const evidence = computeReportState.evidenceByKey[key] || {};
            const title = evidence.title || "Finding";
            const source = evidence.source || "Insight";
            const text = evidence.text || "No citation provided.";
            const body = escapeHtml(text).replace(/\n/g, "<br>");
            renderEvidencePanel(
                key,
                `<div class="dashboard-evidence-title">${escapeHtml(title)}</div>` +
                `<div class="dashboard-evidence-meta">${escapeHtml(source)}</div>` +
                `<div class="dashboard-evidence-body">${body}</div>`
            );
        });
    });
}

function renderDeepStats(round) {
    const wrap = document.getElementById("dashboard-deep-stats");
    const panel = document.getElementById("dashboard-evidence");
    if (!wrap || !panel) return;
    const data = round || {};
    const fb = toNumber(data.fb_impact_delta, 0);
    const fd = toNumber(data.fd_rate, 0);
    const atk = toNumber(data.atk_win_rate, 0);
    const def = toNumber(data.def_win_rate, 0);
    const clutch = toNumber(data.clutch_win_rate, 0);
    const roundWr = toNumber(data.overall_round_win_rate, 0);
    const totalRounds = toNumber(data.total_rounds, 0);
    const clutchAttempts = toNumber(data.clutch_attempts, 0);
    wrap.innerHTML = [
        `<button class="dashboard-stat-chip" type="button" data-stat-key="fb_delta">FB Delta ${fb >= 0 ? "+" : ""}${formatFixed(fb, 1)}%</button>`,
        `<button class="dashboard-stat-chip" type="button" data-stat-key="fd_rate">FD Rate ${formatFixed(fd, 1)}%</button>`,
        `<button class="dashboard-stat-chip" type="button" data-stat-key="atk_def">ATK ${formatFixed(atk, 1)}% / DEF ${formatFixed(def, 1)}%</button>`,
        `<button class="dashboard-stat-chip" type="button" data-stat-key="clutch">Clutch ${formatFixed(clutch, 1)}%</button>`,
        `<button class="dashboard-stat-chip" type="button" data-stat-key="round_wr">Round WR ${formatFixed(roundWr, 1)}%</button>`,
    ].join('<span class="dashboard-stat-sep">·</span>');

    const evidenceByKey = {
        fb_delta: `Round sample ${totalRounds}. This is the win-rate delta between rounds where you secured first blood and rounds where you did not.`,
        fd_rate: `Round sample ${totalRounds}. Use this to track opening-risk consistency, not just final round outcomes.`,
        atk_def: `ATK ${toNumber(data.atk_rounds, 0)} rounds at ${formatFixed(atk, 1)}%; DEF ${toNumber(data.def_rounds, 0)} rounds at ${formatFixed(def, 1)}%. Weak side: ${String(data.weak_side || "even")}.`,
        clutch: `Clutch attempts: ${clutchAttempts}. Primary win condition from plugin: ${String(data.primary_win_condition || "mixed")}.`,
        round_wr: `Overall: ${formatFixed(roundWr, 1)}% over ${totalRounds} rounds. Data quality status: ${String(data.data_quality || "unknown")}.`,
    };
    wrap.querySelectorAll(".dashboard-stat-chip").forEach((chip) => {
        chip.addEventListener("click", () => {
            const key = chip.dataset.statKey || "";
            const body = evidenceByKey[key] || "No evidence available.";
            wrap.querySelectorAll(".dashboard-stat-chip").forEach((el) => el.classList.remove("active"));
            chip.classList.add("active");
            renderEvidencePanel(
                `stat-${key}`,
                `<div class="dashboard-evidence-title">${escapeHtml(chip.textContent || "Stat")}</div>` +
                `<div class="dashboard-evidence-meta">Round Analysis Evidence</div>` +
                `<div class="dashboard-evidence-body">${body}</div>`
            );
        });
    });
}

function computeDashboardSortedData() {
    const seededMapRows = Array.isArray(computeReportState.sorted?.mapRows) ? computeReportState.sorted.mapRows : [];
    const operatorRows = (Array.isArray(computeReportState.operator?.operators) ? computeReportState.operator.operators : [])
        .filter((row) => String(row?.operator || "").trim() && toNumber(row?.rounds, 0) > 0)
        .slice()
        .sort((a, b) => (
            toNumber(b?.win_pct, 0) - toNumber(a?.win_pct, 0) ||
            toNumber(b?.rounds, 0) - toNumber(a?.rounds, 0)
        ));

    const mapRows = seededMapRows.length
        ? seededMapRows
        : (Array.isArray(computeReportState.map?.maps) ? computeReportState.map.maps : [])
            .filter((row) => String(row?.map_name || "").trim() && toNumber(row?.matches, 0) > 0)
            .slice()
            .sort((a, b) => (
                toNumber(b?.win_pct, 0) - toNumber(a?.win_pct, 0) ||
                toNumber(b?.matches, 0) - toNumber(a?.matches, 0)
            ));

    const sorted = { operatorRows, mapRows };
    computeReportState.sorted = sorted;
    return sorted;
}

function computeSortedMapsFromMatches(matches, username) {
    const rows = Array.isArray(matches) ? matches : [];
    const agg = new Map();
    for (const match of rows) {
        const mapName = String(match?.map || "").trim() || "Unknown";
        const perspective = inferTeamPerspective(match, username);
        const entry = agg.get(mapName) || { map_name: mapName, matches: 0, wins: 0, losses: 0, win_pct: 0 };
        entry.matches += 1;
        if (perspective.result === "Win") entry.wins += 1;
        if (perspective.result === "Loss") entry.losses += 1;
        agg.set(mapName, entry);
    }
    return Array.from(agg.values())
        .map((row) => ({
            ...row,
            win_pct: row.matches ? Number(((row.wins / row.matches) * 100).toFixed(1)) : 0,
        }))
        .sort((a, b) => (
            toNumber(b?.win_pct, 0) - toNumber(a?.win_pct, 0) ||
            toNumber(b?.matches, 0) - toNumber(a?.matches, 0)
        ));
}

function renderDashboardBestWorstSummary() {
    const operator = computeReportState.operator || {};
    const map = computeReportState.map || {};
    const sorted = computeReportState.sorted || computeDashboardSortedData();
    const bestOperator = operator.best_operator || sorted.operatorRows[0] || null;
    const worstOperator = operator.worst_operator || sorted.operatorRows[sorted.operatorRows.length - 1] || null;
    const bestMap = map.best_map || sorted.mapRows[0] || null;
    const worstMap = map.worst_map || map.ban_recommendation || sorted.mapRows[sorted.mapRows.length - 1] || null;

    const writeSummary = (valueId, metaId, label, winPct, matches, extra = "") => {
        const valueEl = document.getElementById(valueId);
        const metaEl = document.getElementById(metaId);
        if (valueEl) valueEl.textContent = label || "N/A";
        if (!metaEl) return;
        if (!label) {
            metaEl.textContent = "No data available.";
            return;
        }
        const pctText = Number.isFinite(Number(winPct)) ? `${formatFixed(winPct, 1)}% WR` : "WR N/A";
        const matchesText = Number.isFinite(Number(matches)) ? `${toNumber(matches, 0)} matches` : "match count N/A";
        metaEl.textContent = `${pctText} • ${matchesText}${extra ? ` • ${extra}` : ""}`;
    };

    writeSummary(
        "dashboard-best-operator-value",
        "dashboard-best-operator-meta",
        bestOperator?.operator || "",
        bestOperator?.win_pct,
        bestOperator?.rounds,
        bestOperator?.side ? String(bestOperator.side) : ""
    );
    writeSummary(
        "dashboard-worst-operator-value",
        "dashboard-worst-operator-meta",
        worstOperator?.operator || "",
        worstOperator?.win_pct,
        worstOperator?.rounds,
        worstOperator?.side ? String(worstOperator.side) : ""
    );
    writeSummary(
        "dashboard-best-map-value",
        "dashboard-best-map-meta",
        bestMap?.map_name || "",
        bestMap?.win_pct,
        bestMap?.matches
    );
    writeSummary(
        "dashboard-worst-map-value",
        "dashboard-worst-map-meta",
        worstMap?.map_name || "",
        worstMap?.win_pct,
        worstMap?.matches
    );
}

function renderDashboardInsightCards() {
    const wrap = document.getElementById("dashboard-insights-scroll");
    if (!wrap) return;
    const lastUpdated = new Date().toLocaleString();
    wrap.innerHTML = [
        renderRoundReportCard(computeReportState.round, lastUpdated),
        renderChemistryReportCard(computeReportState.chemistry, lastUpdated),
        renderLobbyReportCard(computeReportState.lobby, lastUpdated),
        renderTradeReportCard(computeReportState.trade, lastUpdated),
        renderTeamReportCard(computeReportState.team, lastUpdated),
        renderOperatorReportCard(computeReportState.operator, lastUpdated),
        renderMapReportCard(computeReportState.map, lastUpdated),
    ].join("");
    renderDashboardBestWorstSummary();
}

function heatmapCellColor(lift, nRounds, mode = "percent_delta", absBound = 15) {
    const liftNum = toNumber(lift, 0);
    const bound = Math.max(0.000001, toNumber(absBound, mode === "percent_delta" ? 15 : 1.25));
    const normalized = Math.max(-1, Math.min(1, liftNum / bound));
    const intensity = Math.abs(normalized);
    const alpha = 0.22 + Math.min(0.78, toNumber(nRounds, 0) / 40);
    if (intensity < 0.05) {
        return `rgba(71, 85, 105, ${alpha.toFixed(3)})`;
    }
    const hue = normalized >= 0 ? 146 : 8;
    const sat = 72;
    const light = 38 - (intensity * 12);
    return `hsla(${hue}, ${sat}%, ${light}%, ${alpha.toFixed(3)})`;
}

function computeHeatmapBound(values, mode = "percent_delta") {
    const nums = values
        .map((v) => Math.abs(toNumber(v, 0)))
        .filter((v) => Number.isFinite(v) && v > 0);
    if (!nums.length) {
        return mode === "percent_delta" ? 15 : 1.25;
    }
    nums.sort((a, b) => a - b);
    const idx = Math.min(nums.length - 1, Math.floor(nums.length * 0.9));
    const p90 = nums[idx];
    if (mode === "percent_delta") {
        return Math.min(30, Math.max(8, p90));
    }
    return Math.min(3, Math.max(0.5, p90));
}

function heatmapDistance(v1, v2) {
    let sum = 0;
    let seen = 0;
    for (let i = 0; i < v1.length; i += 1) {
        const a = v1[i];
        const b = v2[i];
        if (!Number.isFinite(a) || !Number.isFinite(b)) continue;
        const d = a - b;
        sum += d * d;
        seen += 1;
    }
    if (!seen) return Number.POSITIVE_INFINITY;
    return Math.sqrt(sum / seen);
}

function clusterColumnsGreedy(defenders, attackers, byKey, minN) {
    if (defenders.length < 3 || attackers.length < 2) return defenders.slice();
    const vectors = new Map();
    for (const d of defenders) {
        vectors.set(d, attackers.map((a) => {
            const c = byKey.get(`${a}|||${d}`);
            if (!c) return NaN;
            const n = toNumber(c.n_rounds, 0);
            if (n < minN) return NaN;
            return toNumber(c.lift, 0);
        }));
    }
    const sampleVolume = new Map(defenders.map((d) => [d, attackers.reduce((acc, a) => {
        const c = byKey.get(`${a}|||${d}`);
        return acc + (c ? toNumber(c.n_rounds, 0) : 0);
    }, 0)]));
    const remaining = new Set(defenders);
    let current = defenders
        .slice()
        .sort((a, b) => toNumber(sampleVolume.get(b), 0) - toNumber(sampleVolume.get(a), 0))[0];
    const ordered = [current];
    remaining.delete(current);
    while (remaining.size) {
        let best = null;
        let bestDist = Number.POSITIVE_INFINITY;
        for (const candidate of remaining) {
            const dist = heatmapDistance(vectors.get(current) || [], vectors.get(candidate) || []);
            if (dist < bestDist) {
                bestDist = dist;
                best = candidate;
            }
        }
        if (!best) {
            const fallback = Array.from(remaining)[0];
            ordered.push(fallback);
            remaining.delete(fallback);
            current = fallback;
            continue;
        }
        ordered.push(best);
        remaining.delete(best);
        current = best;
    }
    return ordered;
}

function initHeatmapInteractions() {
    const grid = document.getElementById("atk-def-heatmap-grid");
    if (!grid) return;
    let tooltip = document.getElementById("heatmap-tooltip-overlay");
    if (!tooltip) {
        tooltip = document.createElement("div");
        tooltip.id = "heatmap-tooltip-overlay";
        tooltip.className = "heatmap-tooltip hidden";
        document.body.appendChild(tooltip);
    }
    let locked = false;
    let lockedRow = null;
    let lockedCol = null;
    const cells = Array.from(grid.querySelectorAll(".heatmap-cell"));
    const clear = () => {
        for (const el of cells) {
            el.classList.remove("is-row-highlight", "is-col-highlight", "is-focus");
        }
    };
    const apply = (row, col) => {
        clear();
        if (row == null && col == null) return;
        for (const el of cells) {
            const cellRow = Number.parseInt(el.dataset.row || "-999", 10);
            const cellCol = Number.parseInt(el.dataset.col || "-999", 10);
            if (row != null && cellRow === row) el.classList.add("is-row-highlight");
            if (col != null && cellCol === col) el.classList.add("is-col-highlight");
            if (row != null && col != null && cellRow === row && cellCol === col) el.classList.add("is-focus");
        }
    };
    const hideTooltip = () => {
        tooltip?.classList.add("hidden");
    };
    const showTooltip = (cell, event) => {
        if (!tooltip || !cell || !event) return;
        const raw = String(cell.dataset.tooltip || "").trim();
        if (!raw) {
            hideTooltip();
            return;
        }
        tooltip.textContent = raw;
        tooltip.classList.remove("hidden");
        const margin = 16;
        const rect = tooltip.getBoundingClientRect();
        let left = event.clientX + margin;
        let top = event.clientY + margin;
        if (left + rect.width > window.innerWidth - 8) {
            left = Math.max(8, event.clientX - rect.width - margin);
        }
        if (top + rect.height > window.innerHeight - 8) {
            top = Math.max(8, event.clientY - rect.height - margin);
        }
        tooltip.style.left = `${Math.round(left)}px`;
        tooltip.style.top = `${Math.round(top)}px`;
    };
    grid.addEventListener("mouseover", (event) => {
        if (locked) return;
        const cell = event.target?.closest(".heatmap-cell");
        if (!cell) return;
        const row = Number.parseInt(cell.dataset.row || "-999", 10);
        const col = Number.parseInt(cell.dataset.col || "-999", 10);
        apply(Number.isNaN(row) || row < 0 ? null : row, Number.isNaN(col) || col < 0 ? null : col);
        showTooltip(cell, event);
    });
    grid.addEventListener("mousemove", (event) => {
        if (locked) return;
        const cell = event.target?.closest(".heatmap-cell");
        if (!cell) {
            hideTooltip();
            return;
        }
        showTooltip(cell, event);
    });
    grid.addEventListener("mouseleave", () => {
        if (!locked) clear();
        hideTooltip();
    });
    grid.addEventListener("click", (event) => {
        const cell = event.target?.closest(".heatmap-cell");
        if (!cell) return;
        const row = Number.parseInt(cell.dataset.row || "-999", 10);
        const col = Number.parseInt(cell.dataset.col || "-999", 10);
        if (locked && lockedRow === row && lockedCol === col) {
            locked = false;
            lockedRow = null;
            lockedCol = null;
            clear();
            return;
        }
        locked = true;
        lockedRow = Number.isNaN(row) || row < 0 ? null : row;
        lockedCol = Number.isNaN(col) || col < 0 ? null : col;
        apply(lockedRow, lockedCol);
        hideTooltip();
    });
}

function renderAtkDefHeatmap(analysis) {
    if (!analysis || analysis.error) {
        return `<div class="insights-empty">${escapeHtml(analysis?.error || "No ATK/DEF matchup data available.")}</div>`;
    }
    let attackers = Array.isArray(analysis.attackers) ? analysis.attackers.slice() : [];
    let defenders = Array.isArray(analysis.defenders) ? analysis.defenders.slice() : [];
    const cells = Array.isArray(analysis.cells) ? analysis.cells : [];
    const metricMode = String(analysis.lift_mode || "percent_delta");
    if (!attackers.length || !defenders.length) {
        return `<div class="insights-empty">Not enough operator overlap to build heatmap.</div>`;
    }
    const minN = toNumber(analysis?.filters?.min_n, 0);
    const topN = toNumber(document.getElementById("heatmap-top-n")?.value, 20);
    const rowSort = String(document.getElementById("heatmap-sort-rows")?.value || "threat");
    const colSort = String(document.getElementById("heatmap-sort-cols")?.value || "threat");
    const clusterCols = Boolean(document.getElementById("heatmap-cluster-cols")?.checked);
    const searchRaw = String(document.getElementById("heatmap-search-op")?.value || "").trim().toLowerCase();
    const byKey = new Map(cells.map((c) => [`${c.attacker}|||${c.defender}`, c]));

    if (searchRaw) {
        attackers = attackers.filter((op) => String(op).toLowerCase().includes(searchRaw));
        defenders = defenders.filter((op) => String(op).toLowerCase().includes(searchRaw));
    }

    const rowSample = (a) => defenders.reduce((acc, d) => {
        const c = byKey.get(`${a}|||${d}`);
        return acc + (c ? toNumber(c.n_rounds, 0) : 0);
    }, 0);
    const colSample = (d) => attackers.reduce((acc, a) => {
        const c = byKey.get(`${a}|||${d}`);
        return acc + (c ? toNumber(c.n_rounds, 0) : 0);
    }, 0);
    const rowMeanLift = (a) => {
        let w = 0;
        let s = 0;
        for (const d of defenders) {
            const c = byKey.get(`${a}|||${d}`);
            if (!c) continue;
            const n = toNumber(c.n_rounds, 0);
            if (n < minN) continue;
            w += n;
            s += toNumber(c.lift, 0) * n;
        }
        return w > 0 ? s / w : -9999;
    };
    const colThreat = (d) => {
        let w = 0;
        let s = 0;
        for (const a of attackers) {
            const c = byKey.get(`${a}|||${d}`);
            if (!c) continue;
            const n = toNumber(c.n_rounds, 0);
            if (n < minN) continue;
            w += n;
            s += (-toNumber(c.lift, 0)) * n;
        }
        return w > 0 ? s / w : -9999;
    };

    if (rowSort === "alpha") attackers.sort((a, b) => String(a).localeCompare(String(b)));
    else if (rowSort === "sample") attackers.sort((a, b) => rowSample(b) - rowSample(a));
    else attackers.sort((a, b) => rowMeanLift(b) - rowMeanLift(a));

    if (colSort === "alpha") defenders.sort((a, b) => String(a).localeCompare(String(b)));
    else if (colSort === "sample") defenders.sort((a, b) => colSample(b) - colSample(a));
    else defenders.sort((a, b) => colThreat(b) - colThreat(a));

    if (topN > 0) {
        attackers = attackers.slice(0, topN);
        defenders = defenders.slice(0, topN);
    }
    if (clusterCols) {
        defenders = clusterColumnsGreedy(defenders, attackers, byKey, minN);
    }
    if (!attackers.length || !defenders.length) {
        return `<div class="insights-empty">No operators match the current filter set.</div>`;
    }

    const visibleLifts = [];
    for (const a of attackers) {
        for (const d of defenders) {
            const c = byKey.get(`${a}|||${d}`);
            if (!c) continue;
            if (toNumber(c.n_rounds, 0) < minN) continue;
            const lift = toNumber(c.lift, NaN);
            if (Number.isFinite(lift)) visibleLifts.push(lift);
        }
    }
    const colorBound = computeHeatmapBound(visibleLifts, metricMode);
    const tickFormat = (v) => metricMode === "percent_delta" ? `${v.toFixed(0)}%` : v.toFixed(2);
    const leftTick = tickFormat(-colorBound);
    const midTick = metricMode === "percent_delta" ? "0%" : "0.00";
    const rightTick = tickFormat(colorBound);

    const header = [
        `<div class="heatmap-cell heatmap-head heatmap-corner" data-row="-1" data-col="-1">ATK \\ DEF</div>`,
        ...defenders.map((d, colIdx) => `<div class="heatmap-cell heatmap-head" data-row="-1" data-col="${colIdx}">${escapeHtml(d)}</div>`),
    ];
    const rows = attackers.flatMap((a, rowIdx) => {
        const out = [`<div class="heatmap-cell heatmap-rowhead" data-row="${rowIdx}" data-col="-1">${escapeHtml(a)}</div>`];
        for (let colIdx = 0; colIdx < defenders.length; colIdx += 1) {
            const d = defenders[colIdx];
            const cell = byKey.get(`${a}|||${d}`);
            if (!cell) {
                out.push(`<div class="heatmap-cell heatmap-data-cell" data-row="${rowIdx}" data-col="${colIdx}" data-tooltip="No shared rounds">-</div>`);
                continue;
            }
            const n = toNumber(cell.n_rounds, 0);
            if (n < minN) {
                out.push(`<div class="heatmap-cell heatmap-data-cell heatmap-hidden" data-row="${rowIdx}" data-col="${colIdx}" data-tooltip="Hidden by sample-size filter (n=${n} < ${minN})">n&lt;${minN}</div>`);
                continue;
            }
            const winPct = toNumber(cell.win_pct, 0);
            const baselineWr = toNumber(cell.baseline_wr, 0);
            const winCiLow = toNumber(cell.win_ci_low, 0);
            const winCiHigh = toNumber(cell.win_ci_high, 0);
            const lift = toNumber(cell.lift, 0);
            const ciLow = toNumber(cell.ci_low, 0);
            const ciHigh = toNumber(cell.ci_high, 0);
            const sign = lift >= 0 ? "+" : "";
            const metricLabel = metricMode === "percent_delta" ? `${sign}${lift.toFixed(1)}%` : `${sign}${lift.toFixed(2)}`;
            const intervalLabel = metricMode === "percent_delta"
                ? `[${ciLow.toFixed(3)}, ${ciHigh.toFixed(3)}]`
                : `[${ciLow.toFixed(4)}, ${ciHigh.toFixed(4)}]`;
            const ciMethod = String(analysis.interval_method || "wilson").toLowerCase();
            const title = `${a} vs ${d}\n` +
                `n=${n}\n` +
                `ATK win%=${winPct.toFixed(3)}\n` +
                `95% CI for ATK win% (${ciMethod})=[${winCiLow.toFixed(3)}, ${winCiHigh.toFixed(3)}]\n` +
                `Baseline WR=${baselineWr.toFixed(3)}\n` +
                `lift=${lift.toFixed(metricMode === "percent_delta" ? 3 : 4)}${metricMode === "percent_delta" ? "%" : ""}\n` +
                `${metricMode === "log_odds_ratio" ? "95% CI for lift (log OR, normal approx)" : "95% approx CI for lift"}=${intervalLabel}`;
            out.push(
                `<div class="heatmap-cell heatmap-data-cell" data-row="${rowIdx}" data-col="${colIdx}" data-tooltip="${escapeHtml(title.replace(/\n/g, " | "))}" style="background:${heatmapCellColor(lift, n, metricMode, colorBound)};">${metricLabel}</div>`
            );
        }
        return out;
    });
    const stackInfo = analysis.stack_context?.enabled
        ? (
            analysis.stack_context?.applied
                ? `Stack filter enabled (${toNumber((analysis.stack_context?.matched_teammates || []).length, 0)} teammate matches found)`
                : `Stack filter requested (${analysis.stack_context?.reason || "not applied"})`
        )
        : "Stack filter disabled";
    const normLabel = analysis.normalization === "attacker" ? "attacker-row baseline" : "global ATK baseline";
    const ciLabel = String(analysis.interval_method || "wilson").toLowerCase();
    const metricLabel = metricMode === "percent_delta" ? "percent delta" : (metricMode === "logit_lift" ? "logit lift" : "log-odds ratio");
    const diagnostics = analysis.diagnostics;
    const hideLabel = minN > 0 ? `n<${toNumber(minN, 0)}` : "none";
    let diagnosticsSummary = "";
    if (diagnostics) {
        const nanInf = toNumber(diagnostics?.pathology_counters?.nan_or_inf_cells_count, 0);
        const rowChecks = Array.isArray(diagnostics?.row_checks) ? diagnostics.row_checks : [];
        const rowsOutsideTol = rowChecks.filter((r) => Math.abs(toNumber(r?.weighted_mean_lift, 0)) > 0.5).length;
        const pass = nanInf === 0 && rowsOutsideTol === 0;
        diagnosticsSummary = `
            <div class="heatmap-debug-summary ${pass ? "ok" : "warn"}">
                Debug ${pass ? "PASS" : "WARN"} • nan/inf=${nanInf} • rows_outside_tol=${rowsOutsideTol}
            </div>
        `;
    }
    const diagnosticsHtml = diagnostics
        ? `<details class="heatmap-diagnostics"><summary>Diagnostics</summary><pre>${escapeHtml(JSON.stringify(diagnostics, null, 2))}</pre></details>`
        : "";
    return `
        <div class="dashboard-graph-summary">
            <span>ATK baseline <strong>${formatPct(analysis.baseline_atk_win_rate)}</strong></span>
            <span>Normalization <strong>${escapeHtml(normLabel)}</strong></span>
            <span>Metric <strong>${escapeHtml(metricLabel)}</strong></span>
            <span>Interval <strong>${escapeHtml(ciLabel)} 95%</strong></span>
            <span>Rounds <strong>${toNumber(analysis.total_rounds, 0)}</strong></span>
            <span>Cells <strong>${toNumber(cells.length, 0)}</strong></span>
            <span>${escapeHtml(stackInfo)}</span>
        </div>
        <div class="heatmap-method-badge">Lift: ${escapeHtml(metricLabel)} | CI: ${escapeHtml(ciLabel)} | Norm: ${escapeHtml(normLabel)} | Hide: ${hideLabel}</div>
        <div class="heatmap-legend">
            <span class="heatmap-legend-main">Color = lift at 0-centered baseline. Hatching = hidden low-sample cells. Opacity = sample size.</span>
            <span class="heatmap-legend-main">Hover for details, click to lock crosshair.</span>
            <span class="heatmap-opacity-scale">
                <span class="heatmap-opacity-chip o10">n=10</span>
                <span class="heatmap-opacity-chip o50">n=50</span>
                <span class="heatmap-opacity-chip o200">n=200</span>
            </span>
        </div>
        <div class="heatmap-color-scale">
            <div class="heatmap-color-bar"></div>
            <div class="heatmap-color-ticks">
                <span>${escapeHtml(leftTick)}</span>
                <span>${escapeHtml(midTick)}</span>
                <span>${escapeHtml(rightTick)}</span>
            </div>
        </div>
        ${diagnosticsSummary}
        <div class="heatmap-wrap">
            <div id="atk-def-heatmap-grid" class="heatmap-grid" style="grid-template-columns: 180px repeat(${defenders.length}, minmax(72px, 1fr));">
                ${header.join("")}
                ${rows.join("")}
            </div>
        </div>
        ${diagnosticsHtml}
    `;
}

function updateHeatmapMapOptions(analysis) {
    const select = document.getElementById("heatmap-map");
    if (!select) return;
    const maps = Array.isArray(analysis?.available_maps) ? analysis.available_maps : [];
    const current = String(select.value || "");
    const options = [`<option value="">All Maps</option>`]
        .concat(maps.map((m) => `<option value="${escapeHtml(String(m))}">${escapeHtml(String(m))}</option>`));
    select.innerHTML = options.join("");
    if (maps.includes(current)) {
        select.value = current;
    }
}

async function loadAtkDefHeatmap(username) {
    const heatmapMode = document.getElementById("heatmap-mode");
    const heatmapDays = document.getElementById("heatmap-days");
    const heatmapNormalization = document.getElementById("heatmap-normalization");
    const heatmapLiftMode = document.getElementById("heatmap-lift-mode");
    const heatmapMinN = document.getElementById("heatmap-min-n");
    const heatmapMap = document.getElementById("heatmap-map");
    const heatmapStackOnly = document.getElementById("heatmap-stack-only");
    const heatmapDebug = document.getElementById("heatmap-debug");
    const mode = heatmapMode ? heatmapMode.value : "ranked";
    const days = heatmapDays ? heatmapDays.value : "90";
    const normalization = heatmapNormalization ? heatmapNormalization.value : "global";
    const liftMode = heatmapLiftMode ? heatmapLiftMode.value : "percent_delta";
    const minN = heatmapMinN ? heatmapMinN.value : "0";
    const mapName = heatmapMap ? heatmapMap.value : "";
    const stackOnly = heatmapStackOnly ? heatmapStackOnly.checked : false;
    const debug = heatmapDebug ? heatmapDebug.checked : false;
    const qs = new URLSearchParams({
        mode,
        days,
        normalization,
        lift_mode: liftMode,
        interval_method: "wilson",
        min_n: minN,
        map_name: mapName,
        stack_only: stackOnly ? "true" : "false",
        debug: debug ? "true" : "false",
    });
    const res = await api.getAtkDefHeatmap(username, qs);
    if (!res.ok) {
        throw new Error(`ATK/DEF heatmap HTTP ${res.status}`);
    }
    const payload = await res.json();
    computeReportState.atkDefHeatmap = payload?.analysis || null;
    updateHeatmapMapOptions(computeReportState.atkDefHeatmap);
}

function renderDashboardGraphs() {
    const wrap = document.getElementById("dashboard-graphs-content");
    const heatWrap = document.getElementById("dashboard-heatmap-content");
    const mapWrap = document.getElementById("dashboard-mapperf-content");
    const flagshipWrap = document.getElementById("dashboard-flagship-content");
    if (!wrap) return;
    if (mapWrap) {
        mapWrap.innerHTML = renderMapPerformanceGraph(computeReportState.map);
    }
    if (flagshipWrap) {
        const round = computeReportState.round || {};
        const stats = computeReportState.stats || {};
        const openingDelta = toNumber(round?.first_blood_win_delta, toNumber(round?.fb_delta, 0));
        const endReasons = Array.isArray(round?.top_end_reasons) ? round.top_end_reasons : [];
        const endReasonLabel = endReasons.length ? `${String(endReasons[0]?.reason || "Unknown")} (${toNumber(endReasons[0]?.rounds, 0)})` : "No end-reason data";
        const rollingRecent = toNumber(stats?.recent_window?.win_pct, toNumber(stats?.recent_win_pct, 0));
        const card = createCard({
            title: "Trajectory / Maps / Operators / Sessions / Rounds / Context",
            bodyHtml:
                `<div class="dashboard-flagship-grid">` +
                `<div class="dashboard-flagship-item"><div class="label">Opening Impact</div><div class="value">${openingDelta >= 0 ? "+" : ""}${formatFixed(openingDelta, 1)}%</div></div>` +
                `<div class="dashboard-flagship-item"><div class="label">End Reasons</div><div class="value">${escapeHtml(endReasonLabel)}</div></div>` +
                `<div class="dashboard-flagship-item"><div class="label">Rolling Win%</div><div class="value">${formatFixed(rollingRecent, 1)}%</div></div>` +
                `</div>`,
        });
        flagshipWrap.replaceChildren(card);
    }
    if (heatWrap) {
        heatWrap.innerHTML = renderAtkDefHeatmap(computeReportState.atkDefHeatmap);
        initHeatmapInteractions();
    }
    const analysis = computeReportState.enemyThreat;
    if (!analysis || analysis.error) {
        wrap.innerHTML = `<div class="insights-empty">${escapeHtml(analysis?.error || "No graph data available. Run Dashboard first.")}</div>`;
        return;
    }
    wrap.innerHTML = `
        <div class="dashboard-graph-summary">
            <span>Baseline WR <strong>${formatPct(analysis.baseline_win_rate)}</strong></span>
            <span>Total rounds <strong>${toNumber(analysis.total_rounds, 0)}</strong></span>
            <span>Death rounds <strong>${toNumber(analysis.total_death_rounds, 0)}</strong></span>
            <span>Operators tracked <strong>${toNumber(Array.isArray(analysis.threats) ? analysis.threats.length : 0, 0)}</strong></span>
        </div>
        ${renderEnemyThreatScatter(analysis)}
        <div class="insights-findings">${renderInsightsFindings(analysis.findings)}</div>
    `;
}

function mapPerfRowTotalRounds(row) {
    const atk = toNumber(row?.atk_rounds, 0);
    const def = toNumber(row?.def_rounds, 0);
    const sum = atk + def;
    if (sum > 0) return sum;
    const rounds = toNumber(row?.rounds, 0);
    if (rounds > 0) return rounds;
    const matches = toNumber(row?.matches, 0);
    if (matches > 0) return matches;
    return 0;
}

function mapPerfBarTone(side, winPct) {
    const pct = toNumber(winPct, 0);
    const isAtk = side === "atk";
    let tone = "neutral";
    if (pct < 50) tone = "low";
    if (pct > 60) tone = "high";
    return `${isAtk ? "atk" : "def"} ${tone}`;
}

function renderMapPerformanceGraph(mapAnalysis) {
    const empty = `<div class="compute-value">No map data available. Run Dashboard first.</div>`;
    const rowsRaw = Array.isArray(mapAnalysis?.maps) ? mapAnalysis.maps : [];
    if (!rowsRaw.length) return empty;

    const minSample = toNumber(document.getElementById("map-perf-min-sample")?.value, 0);
    const sortMode = String(document.getElementById("map-perf-sort")?.value || "overall");

    const rows = rowsRaw
        .map((row) => {
            const atk = row?.atk_win_pct == null ? null : toNumber(row.atk_win_pct, 0);
            const def = row?.def_win_pct == null ? null : toNumber(row.def_win_pct, 0);
            const sideOverall =
                atk != null && def != null ? ((atk + def) / 2) :
                (atk != null ? atk : (def != null ? def : null));
            const overall = toNumber(sideOverall, toNumber(row?.win_pct, 0));
            const rounds = mapPerfRowTotalRounds(row);
            const gap = (atk == null || def == null) ? null : Math.abs(atk - def);
            const weakSide = (atk == null || def == null) ? "" : (atk < def ? "ATK weak" : "DEF weak");
            return {
                map_name: String(row?.map_name || "Unknown"),
                atk_win_pct: atk,
                def_win_pct: def,
                win_pct: overall,
                rounds,
                gap,
                weak_side: weakSide,
                matches: toNumber(row?.matches, 0),
            };
        })
        .filter(Boolean)
        .filter((row) => row.rounds >= Math.max(0, minSample));

    if (!rows.length) {
        return `<div class="compute-value">No map data matches current sample filter.</div>`;
    }

    rows.sort((a, b) => {
        if (sortMode === "atk") return toNumber(b.atk_win_pct, -1) - toNumber(a.atk_win_pct, -1);
        if (sortMode === "def") return toNumber(b.def_win_pct, -1) - toNumber(a.def_win_pct, -1);
        if (sortMode === "rounds") return b.rounds - a.rounds;
        return toNumber(b.win_pct, -1) - toNumber(a.win_pct, -1);
    });

    const best = rows[0] || null;
    const worst = rows[rows.length - 1] || null;
    const ban = rows
        .filter((row) => row.rounds >= 20)
        .slice()
        .sort((a, b) => toNumber(a.win_pct, 999) - toNumber(b.win_pct, 999))[0] || null;
    const sideGap = rows
        .filter((row) => row.gap != null)
        .slice()
        .sort((a, b) => toNumber(b.gap, -1) - toNumber(a.gap, -1))[0] || null;

    const renderSummary = (label, row, extra = "") => {
        if (!row) return `<div class="map-perf-summary-item"><span>${label}</span><strong>N/A</strong></div>`;
        const atkTxt = row.atk_win_pct == null ? "N/A" : `${formatFixed(row.atk_win_pct, 1)}%`;
        const defTxt = row.def_win_pct == null ? "N/A" : `${formatFixed(row.def_win_pct, 1)}%`;
        return `<div class="map-perf-summary-item"><span>${label}</span><strong>${escapeHtml(row.map_name)} — ${formatFixed(row.win_pct, 1)}% overall (ATK: ${atkTxt} / DEF: ${defTxt})${extra}</strong></div>`;
    };

    const chartRows = rows.map((row) => {
        const atk = row.atk_win_pct;
        const def = row.def_win_pct;
        const atkWidth = Math.max(0, Math.min(100, toNumber(atk, 0)));
        const defWidth = Math.max(0, Math.min(100, toNumber(def, 0)));
        const atkTone = mapPerfBarTone("atk", atk);
        const defTone = mapPerfBarTone("def", def);
        const atkLabel = atk == null ? "N/A" : `${formatFixed(atk, 1)}%`;
        const defLabel = def == null ? "N/A" : `${formatFixed(def, 1)}%`;
        return `
            <div class="map-perf-row">
                <div class="map-perf-label"><span class="map-perf-map-name">${escapeHtml(row.map_name)}</span><span class="map-perf-rounds">n=${toNumber(row.rounds, 0)}</span></div>
                <div class="map-perf-bars">
                    <div class="map-perf-midline" aria-hidden="true"></div>
                    <div class="map-perf-bar-lane atk">
                        <span class="map-perf-side-tag atk">ATK</span>
                        <div class="map-perf-bar ${atkTone}" style="width:${atkWidth.toFixed(2)}%"></div>
                        <span class="map-perf-bar-value">${atkLabel}</span>
                    </div>
                    <div class="map-perf-bar-lane def">
                        <span class="map-perf-side-tag def">DEF</span>
                        <div class="map-perf-bar ${defTone}" style="width:${defWidth.toFixed(2)}%"></div>
                        <span class="map-perf-bar-value">${defLabel}</span>
                    </div>
                </div>
            </div>
        `;
    }).join("");

    return `
        <div class="map-perf-legend" aria-label="Map performance legend">
            <span class="map-perf-legend-item"><span class="map-perf-legend-swatch atk" aria-hidden="true"></span>ATK</span>
            <span class="map-perf-legend-item"><span class="map-perf-legend-swatch def" aria-hidden="true"></span>DEF</span>
            <span class="map-perf-legend-item"><span class="map-perf-legend-baseline" aria-hidden="true"></span>50% baseline</span>
        </div>
        <div class="map-perf-summary">
            ${renderSummary("Best map", best)}
            ${renderSummary("Worst map", worst)}
            ${renderSummary("Recommended ban", ban, " (min 20 rounds)")}
            ${sideGap ? `<div class="map-perf-summary-item"><span>Biggest side gap</span><strong>${escapeHtml(sideGap.map_name)} — ${formatFixed(sideGap.gap, 1)}% gap (${escapeHtml(sideGap.weak_side)})</strong></div>` : `<div class="map-perf-summary-item"><span>Biggest side gap</span><strong>N/A</strong></div>`}
        </div>
        <div class="map-perf-chart">
            ${chartRows}
        </div>
    `;
}

function getWorkspaceFiltersFromUI() {
    const f = {
        days: document.getElementById("ws-days")?.value || "90",
        queue: document.getElementById("ws-queue")?.value || "all",
        playlist: document.getElementById("ws-playlist")?.value || "",
        map_name: (document.getElementById("ws-map-name")?.value || "").trim(),
        stack_only: document.getElementById("ws-stack-only")?.checked ? "true" : "false",
        search: (document.getElementById("ws-search")?.value || "").trim(),
        normalization: document.getElementById("ws-normalization")?.value || "global",
        lift_mode: document.getElementById("ws-lift-mode")?.value || "percent_delta",
        interval_method: document.getElementById("ws-interval-method")?.value || "wilson",
        min_n: document.getElementById("ws-min-n")?.value || "25",
        weighting: document.getElementById("ws-weighting")?.value || "rounds",
        clamp_mode: document.getElementById("ws-clamp-mode")?.value || "percentile",
        clamp_abs: document.getElementById("ws-clamp-abs")?.value || "15",
        clamp_p_low: "5",
        clamp_p_high: "95",
        debug: document.getElementById("ws-debug")?.checked ? "true" : "false",
    };
    computeReportState.workspace.filters = f;
    return f;
}

function persistWorkspaceState() {
    const filters = getWorkspaceFiltersFromUI();
    const params = new URLSearchParams(window.location.search);
    params.set("ws_panel", computeReportState.workspace.panel || "overview");
    params.set("ws_view", computeReportState.workspace.panel || "overview");
    ["days", "queue", "playlist", "map_name", "stack_only", "search"].forEach((k) => {
        if (filters[k] && String(filters[k]).trim() !== "") params.set(`ws_${k}`, filters[k]);
        else params.delete(`ws_${k}`);
    });
    const url = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState({}, "", url);
    const prefs = {
        normalization: filters.normalization,
        lift_mode: filters.lift_mode,
        interval_method: filters.interval_method,
        min_n: filters.min_n,
        weighting: filters.weighting,
        clamp_mode: filters.clamp_mode,
        clamp_abs: filters.clamp_abs,
        labels: document.getElementById("ws-labels")?.checked ? "1" : "0",
    };
    localStorage.setItem("jakal_workspace_prefs_v1", JSON.stringify(prefs));
}

function mountWorkspaceTopLevel() {
    const source = document.getElementById("graph-panel-workspace");
    const host = document.getElementById("workspace-top-level-host");
    if (!source || !host) return;
    if (host.contains(source)) return;
    host.innerHTML = "";
    host.appendChild(source);
    source.hidden = false;
    const workspaceGraphTab = document.getElementById("graph-tab-workspace");
    if (workspaceGraphTab) {
        workspaceGraphTab.hidden = true;
        workspaceGraphTab.setAttribute("aria-hidden", "true");
        workspaceGraphTab.classList.remove("active");
        workspaceGraphTab.setAttribute("aria-selected", "false");
    }
    const threatGraphTab = document.getElementById("graph-tab-threat");
    if (threatGraphTab) {
        threatGraphTab.classList.add("active");
        threatGraphTab.setAttribute("aria-selected", "true");
    }
    computeReportState.graphPanel = "threat";
}

function isWorkspaceMountedTopLevel() {
    const source = document.getElementById("graph-panel-workspace");
    const host = document.getElementById("workspace-top-level-host");
    if (!source || !host) return false;
    return host.contains(source);
}

function restoreWorkspaceState() {
    const params = new URLSearchParams(window.location.search);
    const panel = params.get("ws_panel") || params.get("ws_view") || "overview";
    computeReportState.workspace.panel = panel;
    const map = {
        ws_days: "ws-days",
        ws_queue: "ws-queue",
        ws_playlist: "ws-playlist",
        ws_map_name: "ws-map-name",
        ws_stack_only: "ws-stack-only",
        ws_search: "ws-search",
    };
    Object.entries(map).forEach(([p, id]) => {
        const v = params.get(p);
        const el = document.getElementById(id);
        if (!el || v == null) return;
        if (el.type === "checkbox") el.checked = v === "true";
        else el.value = v;
    });
    try {
        const prefs = JSON.parse(localStorage.getItem("jakal_workspace_prefs_v1") || "{}");
        const prefMap = {
            normalization: "ws-normalization",
            lift_mode: "ws-lift-mode",
            interval_method: "ws-interval-method",
            min_n: "ws-min-n",
            weighting: "ws-weighting",
            clamp_mode: "ws-clamp-mode",
            clamp_abs: "ws-clamp-abs",
        };
        Object.entries(prefMap).forEach(([k, id]) => {
            const el = document.getElementById(id);
            if (!el) return;
            if (prefs[k] != null) el.value = String(prefs[k]);
        });
        if (prefs.labels != null && document.getElementById("ws-labels")) {
            document.getElementById("ws-labels").checked = String(prefs.labels) === "1";
        }
    } catch (_) {
        // ignore persisted view parse failures
    }
}

function restorePrimaryTabFromUrl() {
    const params = new URLSearchParams(window.location.search);
    const panel = String(params.get("panel") || "").trim().toLowerCase();
    const allowed = new Set(["scanner", "matches", "stored", "players", "team-builder", "operators", "workspace", "dashboard"]);
    return allowed.has(panel) ? panel : "scanner";
}

function setWorkspacePanel(panel) {
    const next = ["overview", "operators", "matchups", "team"].includes(panel) ? panel : "overview";
    computeReportState.workspace.panel = next;
    document.querySelectorAll("[data-ws-panel]").forEach((btn) => {
        const isActive = btn.dataset.wsPanel === next;
        btn.classList.toggle("active", isActive);
        btn.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    document.querySelectorAll(".workspace-panel").forEach((p) => p.classList.remove("active"));
    const active = document.getElementById(`workspace-panel-${next}`);
    if (active) active.classList.add("active");
    persistWorkspaceState();
}

function renderWorkspaceOverview(payload) {
    const meta = payload?.meta || {};
    const diag = payload?.diagnostics?.integrity || {};
    return `
        <div class="dashboard-graph-summary">
            <span>API v<strong>${toNumber(meta.api_version, 1)}</strong></span>
            <span>Ordering <strong>${escapeHtml(meta.ordering_mode || "-")}</strong></span>
            <span>DB Rev <strong>${escapeHtml(meta.db_rev || "-")}</strong></span>
            <span>Hash <strong>${escapeHtml((meta.hash || "").slice(0, 10))}</strong></span>
        </div>
        <div class="insights-findings">
            <div class="insights-finding info"><strong>Workspace summary</strong> rows integrity counters are available when debug is on.</div>
            <div class="insights-finding info">rounds_total=${toNumber(diag.rounds_total, 0)} missing_players=${toNumber(diag.rounds_missing_players, 0)} not_5v5=${toNumber(diag.rounds_not_5v5, 0)}</div>
        </div>
    `;
}

function renderWorkspaceOperatorScatter(operators) {
    const rawPoints = Array.isArray(operators?.scatter?.points) ? operators.scatter.points : [];
    const points = rawPoints.filter((p) => {
        const name = String(p?.operator || "").trim().toLowerCase();
        return name && !["unknown", "n/a", "none", "null", "-", "operator", "undefined"].includes(name);
    });
    if (!points.length) return `<div class="compute-value">No operator points for current filters.</div>`;
    const maxX = Math.max(1, ...points.map((p) => toNumber(p.presence_pct, 0)));
    const minY = Math.min(0, ...points.map((p) => toNumber(p.win_delta, 0)));
    const maxY = Math.max(0, ...points.map((p) => toNumber(p.win_delta, 0)));
    const yPad = Math.max(2, (maxY - minY) * 0.12);
    const yLo = minY - yPad;
    const yHi = maxY + yPad;
    const zeroY = ((0 - yLo) / (yHi - yLo || 1)) * 100;
    const labelsOn = document.getElementById("ws-labels")?.checked;
    const labelOps = new Set(
        points
            .slice()
            .sort((a, b) => toNumber(b.n_rounds, 0) - toNumber(a.n_rounds, 0))
            .slice(0, 12)
            .map((p) => String(p.operator || ""))
    );
    const dots = points.map((p) => {
        const x = (toNumber(p.presence_pct, 0) / maxX) * 100;
        const y = ((toNumber(p.win_delta, 0) - yLo) / (yHi - yLo || 1)) * 100;
        const color = p.side === "attacker" ? "#22c55e" : "#fb7185";
        const alpha = Math.min(1, 0.3 + (toNumber(p.n_rounds, 0) / 120));
        const op = String(p.operator || "");
        const icon = resolveOperatorImageUrl(op);
        const fallback = operatorFallbackBadge(op);
        const marker = icon
            ? `<img src="${icon}" alt="${escapeHtml(op)}" loading="lazy" />`
            : fallback;
        const label = labelsOn && labelOps.has(op) ? `<span class="ws-scatter-label">${escapeHtml(op)}</span>` : "";
        return `<button class="ws-scatter-dot ${p.side === "attacker" ? "atk" : "def"}" data-op="${escapeHtml(op)}" style="left:${x.toFixed(3)}%;top:${(100-y).toFixed(3)}%;opacity:${alpha.toFixed(3)}" title="${escapeHtml(`${op} • ${p.side} • n=${p.n_rounds} • win%=${toNumber(p.win_pct, 0).toFixed(2)} • delta=${toNumber(p.win_delta, 0).toFixed(2)}`)}">${marker}${label}</button>`;
    }).join("");
    return `
        <div class="ws-scatter-wrap">
            <div class="ws-scatter-y">Win Delta</div>
            <div class="ws-scatter-plot">
                <div class="ws-scatter-axis-x"></div>
                <div class="ws-scatter-axis-y"></div>
                <div class="ws-scatter-axis-zero" style="bottom:${Math.max(0, Math.min(100, zeroY)).toFixed(2)}%"></div>
                ${dots}
            </div>
            <div class="ws-scatter-x">Presence % (0 - ${maxX.toFixed(1)}%)</div>
        </div>
    `;
}

function renderThreatBars(matchups) {
    const block = matchups?.threat_index || {};
    const defBars = Array.isArray(block.defender_threat) ? block.defender_threat.slice(0, 10) : [];
    const atkBars = Array.isArray(block.attacker_vulnerability) ? block.attacker_vulnerability.slice(0, 10) : [];
    const renderBars = (rows, type) => {
        if (!rows.length) return `<div class="compute-value">No ${type} threat bars.</div>`;
        const maxVal = Math.max(0.0001, ...rows.map((r) => Math.abs(toNumber(r.index, 0))));
        return rows.map((r) => {
            const pct = (Math.abs(toNumber(r.index, 0)) / maxVal) * 100;
            const cov = toNumber(r.coverage_pct_visible, 0).toFixed(1);
            return `<button class="ws-threat-row" data-type="${type}" data-op="${escapeHtml(String(r.operator || ""))}">
                <span>${escapeHtml(String(r.operator || ""))}</span>
                <span class="ws-threat-bar"><span style="width:${pct.toFixed(2)}%"></span></span>
                <span>${toNumber(r.index, 0).toFixed(2)}</span>
                <span>${cov}%</span>
            </button>`;
        }).join("");
    };
    return `
        <div class="ws-threat-grid">
            <div>
                <div class="compute-label">Defender Threat Index</div>
                ${renderBars(defBars, "matchup_col")}
            </div>
            <div>
                <div class="compute-label">Attacker Vulnerability Index</div>
                ${renderBars(atkBars, "matchup_row")}
            </div>
        </div>
    `;
}

function renderWorkspaceMatchups(payload) {
    const m = payload?.matchups || {};
    if (m.error) return `<div class="compute-value">${escapeHtml(m.error)}</div>`;
    return `${renderAtkDefHeatmap(m)}${renderThreatBars(m)}`;
}

function getWorkspaceTeamUiState() {
    const ws = computeReportState.workspace || {};
    if (!ws.teamUi) ws.teamUi = {};
    ws.teamUi.minMatches = Math.max(1, toNumber(ws.teamUi.minMatches, 5));
    ws.teamUi.minRounds = Math.max(1, toNumber(ws.teamUi.minRounds, 30));
    ws.teamUi.polarity = String(ws.teamUi.polarity || "all");
    ws.teamUi.hideNeutral = ws.teamUi.hideNeutral === true;
    ws.teamUi.neutralThreshold = Math.max(0.1, toNumber(ws.teamUi.neutralThreshold, 1));
    ws.teamUi.sortBy = String(ws.teamUi.sortBy || "rounds");
    ws.teamUi.sortDir = String(ws.teamUi.sortDir || "desc") === "asc" ? "asc" : "desc";
    ws.teamUi.selectedPairKey = String(ws.teamUi.selectedPairKey || "");
    return ws.teamUi;
}

function workspaceTeamPairKey(row) {
    const a = String(row?.teammate_a || "").trim().toLowerCase();
    const b = String(row?.teammate_b || "").trim().toLowerCase();
    if (a && b) return [a, b].sort().join("||");
    const raw = String(row?.pair || "").trim().toLowerCase();
    if (!raw) return "";
    const bits = raw.split("+").map((p) => p.trim()).filter(Boolean);
    if (bits.length >= 2) return [bits[0], bits[1]].sort().join("||");
    return raw;
}

function workspaceTeamConfidence(rounds) {
    const n = toNumber(rounds, 0);
    if (n >= 150) return { label: "HIGH", cls: "high" };
    if (n >= 80) return { label: "MED", cls: "med" };
    if (n >= 30) return { label: "LOW", cls: "low" };
    return { label: "VERY LOW", cls: "verylow" };
}

function workspaceTeamDeltaBucket(delta) {
    if (delta >= 8) return "strong-pos";
    if (delta >= 2) return "mild-pos";
    if (delta <= -8) return "strong-neg";
    if (delta <= -2) return "mild-neg";
    return "neutral";
}

function workspaceTeamSortRows(rows, sortBy, sortDir) {
    const dir = sortDir === "asc" ? 1 : -1;
    const cmpNumber = (a, b, k) => (toNumber(a?.[k], 0) - toNumber(b?.[k], 0));
    const cmpString = (a, b, k) => String(a?.[k] || "").localeCompare(String(b?.[k] || ""));
    const cmp = (a, b) => {
        let primary = 0;
        if (sortBy === "pair") primary = cmpString(a, b, "pair");
        else if (sortBy === "abs_delta") primary = Math.abs(toNumber(a?.delta_vs_user_baseline, 0)) - Math.abs(toNumber(b?.delta_vs_user_baseline, 0));
        else primary = cmpNumber(a, b, sortBy);
        if (primary !== 0) return primary * dir;
        const t1 = cmpNumber(a, b, "rounds_n");
        if (t1 !== 0) return -t1;
        const t2 = Math.abs(toNumber(a?.delta_vs_user_baseline, 0)) - Math.abs(toNumber(b?.delta_vs_user_baseline, 0));
        if (t2 !== 0) return -t2;
        const t3 = cmpNumber(a, b, "matches_n");
        if (t3 !== 0) return -t3;
        return cmpString(a, b, "pair");
    };
    return rows.slice().sort(cmp);
}

function renderWorkspaceTeamInspector(row, baseline, clampAbs = 15) {
    const ins = document.getElementById("workspace-inspector");
    if (!ins) return;
    if (!row) {
        ins.innerHTML = `<div class="compute-label">Inspector</div><div class="compute-value">Click a heatmap cell or table row to inspect a teammate pair.</div>`;
        return;
    }
    const delta = toNumber(row?.delta_vs_user_baseline, 0);
    const c = workspaceTeamConfidence(toNumber(row?.rounds_n, 0));
    const pair = String(row?.pair || "").trim();
    const pairKey = workspaceTeamPairKey(row);
    const leftPct = delta < 0 ? (Math.min(clampAbs, Math.abs(delta)) / clampAbs) * 50 : 0;
    const rightPct = delta > 0 ? (Math.min(clampAbs, Math.abs(delta)) / clampAbs) * 50 : 0;
    const deltaTooltip = `Δ vs baseline: ${delta >= 0 ? "+" : ""}${formatFixed(delta, 2)} percentage points`;
    ins.innerHTML = `
        <div class="compute-label">Inspector: ${escapeHtml(pair)}</div>
        <div class="dashboard-graph-summary">
            <span>Win% <strong>${formatFixed(toNumber(row?.win_rate, 0), 2)}%</strong></span>
            <span>User baseline (scope win%) <strong>${formatFixed(baseline, 2)}%</strong></span>
            <span>Matches <strong>${toNumber(row?.matches_n, 0)}</strong></span>
            <span>Rounds <strong>${toNumber(row?.rounds_n, 0)}</strong></span>
            <span>Confidence <strong>${c.label}</strong></span>
        </div>
        <div class="workspace-team-delta-center">
            <span class="workspace-team-delta-value ${delta >= 0 ? "pos" : "neg"}" title="${escapeHtml(deltaTooltip)}">${delta >= 0 ? "+" : ""}${formatFixed(delta, 2)}pp</span>
            <div class="workspace-team-zero-bar" title="${escapeHtml(deltaTooltip)}">
                <span class="workspace-team-zero-center"></span>
                <span class="workspace-team-zero-fill neg" style="left:calc(50% - ${leftPct.toFixed(2)}%);width:${leftPct.toFixed(2)}%"></span>
                <span class="workspace-team-zero-fill pos" style="left:50%;width:${rightPct.toFixed(2)}%"></span>
            </div>
        </div>
        <div class="dashboard-graph-summary">
            <button id="ws-team-filter-selected" type="button" data-pair-key="${escapeHtml(pairKey)}">Filter to this pair</button>
            <button id="ws-team-clear-selected" type="button">Clear selection</button>
        </div>
    `;
}

function renderWorkspaceTeam(payload) {
    const pairs = Array.isArray(payload?.pairs) ? payload.pairs : [];
    const teamUi = getWorkspaceTeamUiState();
    const isPartial = payload?.is_partial === true;
    const reason = String(payload?.reason || "").trim();
    const baseline = toNumber(payload?.baseline_win_rate, 0);
    const scopeMatches = toNumber(payload?.scope?.match_ids, 0);
    const clampAbs = Math.max(5, toNumber(document.getElementById("ws-clamp-abs")?.value, 15));

    const withKey = pairs.map((row) => ({ ...row, __pairKey: workspaceTeamPairKey(row) }));
    const counts = { minMatches: 0, minRounds: 0, polarity: 0, neutral: 0, selected: 0 };
    const filtered = withKey.filter((row) => {
        const matchesN = toNumber(row?.matches_n, 0);
        const roundsN = toNumber(row?.rounds_n, 0);
        const delta = toNumber(row?.delta_vs_user_baseline, 0);
        if (matchesN < teamUi.minMatches) {
            counts.minMatches += 1;
            return false;
        }
        if (roundsN < teamUi.minRounds) {
            counts.minRounds += 1;
            return false;
        }
        if (teamUi.polarity === "positive" && delta < 0) {
            counts.polarity += 1;
            return false;
        }
        if (teamUi.polarity === "negative" && delta > 0) {
            counts.polarity += 1;
            return false;
        }
        if (teamUi.hideNeutral && Math.abs(delta) < teamUi.neutralThreshold) {
            counts.neutral += 1;
            return false;
        }
        if (teamUi.selectedPairKey && teamUi.selectedPairKey !== row.__pairKey) {
            counts.selected += 1;
            return false;
        }
        return true;
    });
    const sortedRows = workspaceTeamSortRows(filtered, teamUi.sortBy, teamUi.sortDir);
    const pairCount = sortedRows.length;
    const reliableCount = sortedRows.filter((p) => toNumber(p?.matches_n, 0) >= 5).length;
    const avgPairDelta = pairCount
        ? sortedRows.reduce((acc, p) => acc + toNumber(p?.delta_vs_user_baseline, 0), 0) / pairCount
        : 0;

    const topPositive = sortedRows
        .filter((p) => toNumber(p?.matches_n, 0) >= 3)
        .slice()
        .sort((a, b) => toNumber(b?.delta_vs_user_baseline, 0) - toNumber(a?.delta_vs_user_baseline, 0))
        .slice(0, 5);
    const topNegative = sortedRows
        .filter((p) => toNumber(p?.matches_n, 0) >= 3)
        .slice()
        .sort((a, b) => toNumber(a?.delta_vs_user_baseline, 0) - toNumber(b?.delta_vs_user_baseline, 0))
        .slice(0, 5);

    const renderQuickList = (rows, tone) => {
        if (!rows.length) return `<div class="workspace-team-empty">Not enough pairs after current filters.</div>`;
        return rows.map((row) => {
            const d = toNumber(row?.delta_vs_user_baseline, 0);
            return (
                `<button class="workspace-team-quick-row ${tone}" type="button" data-pair-key="${escapeHtml(String(row.__pairKey || ""))}">` +
                `<span class="workspace-team-quick-pair">${escapeHtml(String(row?.pair || ""))}</span>` +
                `<span class="workspace-team-quick-meta">` +
                `${formatFixed(toNumber(row?.win_rate, 0), 1)}% WR (${d >= 0 ? "+" : ""}${formatFixed(d, 1)}pp) · ${toNumber(row?.matches_n, 0)} matches · ${toNumber(row?.rounds_n, 0)} rounds` +
                `</span>` +
                `</button>`
            );
        }).join("");
    };

    const teammateTotals = new Map();
    sortedRows.forEach((row) => {
        const roundsN = toNumber(row?.rounds_n, 0);
        const a = String(row?.teammate_a || "").trim();
        const b = String(row?.teammate_b || "").trim();
        if (a) teammateTotals.set(a, toNumber(teammateTotals.get(a), 0) + roundsN);
        if (b) teammateTotals.set(b, toNumber(teammateTotals.get(b), 0) + roundsN);
    });
    const heatNames = [...teammateTotals.entries()]
        .sort((x, y) => toNumber(y[1], 0) - toNumber(x[1], 0))
        .slice(0, 12)
        .map(([name]) => name);
    const heatNameSet = new Set(heatNames.map((n) => n.toLowerCase()));
    const pairByKey = new Map(sortedRows.map((r) => [String(r.__pairKey || ""), r]));
    const heatRows = sortedRows.filter((row) => heatNameSet.has(String(row?.teammate_a || "").toLowerCase()) && heatNameSet.has(String(row?.teammate_b || "").toLowerCase()));
    heatRows.forEach((row) => pairByKey.set(String(row.__pairKey || ""), row));
    const heatmap = heatNames.length > 1 ? `
        <div class="workspace-team-heat-wrap">
            <div class="workspace-team-heat-head">
                <h4>Synergy Matrix (delta vs baseline)</h4>
                <button id="ws-team-clear-selection" type="button">Clear selection</button>
            </div>
            <div class="workspace-team-heat-grid" style="grid-template-columns: 130px repeat(${heatNames.length}, minmax(52px, 1fr));">
                <span class="workspace-team-heat-corner"></span>
                ${heatNames.map((n) => `<span class="workspace-team-heat-col">${escapeHtml(n)}</span>`).join("")}
                ${heatNames.map((rowName) => {
                    const rowKey = rowName.toLowerCase();
                    const cells = heatNames.map((colName) => {
                        const colKey = colName.toLowerCase();
                        if (rowKey === colKey) return `<span class="workspace-team-heat-cell diag">-</span>`;
                        const pKey = [rowKey, colKey].sort().join("||");
                        const rec = pairByKey.get(pKey);
                        if (!rec) return `<button class="workspace-team-heat-cell nodata" type="button" disabled title="No data">-</button>`;
                        const delta = toNumber(rec?.delta_vs_user_baseline, 0);
                        const c = workspaceTeamConfidence(toNumber(rec?.rounds_n, 0));
                        const bucket = workspaceTeamDeltaBucket(delta);
                        const lowSample = toNumber(rec?.rounds_n, 0) < 80 ? " low-sample" : "";
                        const tip = `${rec.pair}\n${toNumber(rec.matches_n, 0)} matches · ${toNumber(rec.rounds_n, 0)} rounds\n${formatFixed(toNumber(rec.win_rate, 0), 2)}% WR\nΔ vs baseline: ${delta >= 0 ? "+" : ""}${formatFixed(delta, 2)}pp\nConfidence: ${c.label}`;
                        return `<button class="workspace-team-heat-cell ${bucket}${lowSample}${teamUi.selectedPairKey === pKey ? " selected" : ""}" type="button" data-pair-key="${escapeHtml(pKey)}" title="${escapeHtml(tip)}">${delta >= 0 ? "+" : ""}${formatFixed(delta, 1)}</button>`;
                    }).join("");
                    return `<span class="workspace-team-heat-rowname">${escapeHtml(rowName)}</span>${cells}`;
                }).join("")}
            </div>
        </div>
    ` : `<div class="workspace-team-empty">Need at least 2 teammates after filters to render synergy matrix.</div>`;

    const sortIcon = (key) => teamUi.sortBy === key ? (teamUi.sortDir === "desc" ? "↓" : "↑") : "";
    const tableRows = sortedRows.slice(0, 120).map((row, idx) => {
        const delta = toNumber(row?.delta_vs_user_baseline, 0);
        const c = workspaceTeamConfidence(toNumber(row?.rounds_n, 0));
        const absPct = (Math.min(clampAbs, Math.abs(delta)) / clampAbs) * 50;
        const leftPct = delta < 0 ? absPct : 0;
        const rightPct = delta > 0 ? absPct : 0;
        const deltaTip = `Δ vs baseline: ${delta >= 0 ? "+" : ""}${formatFixed(delta, 2)} percentage points`;
        return (
            `<button class="workspace-team-row${teamUi.selectedPairKey === row.__pairKey ? " selected" : ""}" type="button" data-pair-key="${escapeHtml(String(row.__pairKey || ""))}">` +
            `<span class="workspace-team-rank">${idx + 1}</span>` +
            `<span class="workspace-team-pair">${escapeHtml(String(row?.pair || ""))}</span>` +
            `<span>${toNumber(row?.matches_n, 0)}</span>` +
            `<span>${toNumber(row?.rounds_n, 0)}</span>` +
            `<span>${toNumber(row?.wins_n, 0)}</span>` +
            `<span>${formatFixed(toNumber(row?.win_rate, 0), 2)}%</span>` +
            `<span class="workspace-team-delta-center">` +
            `<span class="workspace-team-delta-value ${delta >= 0 ? "pos" : "neg"}" title="${escapeHtml(deltaTip)}">${delta >= 0 ? "+" : ""}${formatFixed(delta, 2)}pp</span>` +
            `<span class="workspace-team-zero-bar" title="${escapeHtml(deltaTip)}">` +
            `<span class="workspace-team-zero-center"></span>` +
            `<span class="workspace-team-zero-fill neg" style="left:calc(50% - ${leftPct.toFixed(2)}%);width:${leftPct.toFixed(2)}%"></span>` +
            `<span class="workspace-team-zero-fill pos" style="left:50%;width:${rightPct.toFixed(2)}%"></span>` +
            `</span>` +
            `</span>` +
            `<span class="workspace-team-conf ${c.cls}">${c.label}</span>` +
            `</button>`
        );
    }).join("");

    const meta = `
        <div class="dashboard-graph-summary">
            <span>Scope matches <strong>${scopeMatches}</strong></span>
            <span>User baseline (scope win%) <strong>${formatFixed(baseline, 2)}%</strong></span>
            <span>Cache <strong>${payload?.cache_hit ? "hit" : "miss"}</strong></span>
            <span>Compute <strong>${toNumber(payload?.compute_ms, 0)}ms</strong></span>
        </div>
    `;
    const filterBar = `
        <div class="workspace-team-filters">
            <label>Min matches
                <select id="ws-team-min-matches">
                    <option value="5" ${teamUi.minMatches === 5 ? "selected" : ""}>5</option>
                    <option value="10" ${teamUi.minMatches === 10 ? "selected" : ""}>10</option>
                    <option value="20" ${teamUi.minMatches === 20 ? "selected" : ""}>20</option>
                </select>
            </label>
            <label>Min rounds
                <select id="ws-team-min-rounds">
                    <option value="30" ${teamUi.minRounds === 30 ? "selected" : ""}>30</option>
                    <option value="60" ${teamUi.minRounds === 60 ? "selected" : ""}>60</option>
                    <option value="120" ${teamUi.minRounds === 120 ? "selected" : ""}>120</option>
                </select>
            </label>
            <label>Polarity
                <select id="ws-team-polarity">
                    <option value="all" ${teamUi.polarity === "all" ? "selected" : ""}>All</option>
                    <option value="positive" ${teamUi.polarity === "positive" ? "selected" : ""}>Positive</option>
                    <option value="negative" ${teamUi.polarity === "negative" ? "selected" : ""}>Negative</option>
                </select>
            </label>
            <label class="settings-check"><input id="ws-team-hide-neutral" type="checkbox" ${teamUi.hideNeutral ? "checked" : ""}>Hide near-neutral (|delta| &lt; 1pp)</label>
        </div>
    `;
    const partialBanner = isPartial
        ? `<div class="insights-finding warning"><strong>Partial result</strong> ${escapeHtml(reason || "Scope trimmed for responsiveness.")}<br>` +
          `<button id="ws-team-quick-30d" type="button">Narrow to 30d</button> ` +
          `<button id="ws-team-quick-ranked" type="button">Ranked only</button> ` +
          `<button id="ws-team-force-refresh" type="button">Refresh</button></div>`
        : "";
    if (!pairs.length) {
        return `${meta}${filterBar}${partialBanner}<div class="compute-value">No teammate pairs found for current scope.</div>`;
    }
    const emptyReasons = [
        counts.minMatches ? `${counts.minMatches} removed by min matches` : "",
        counts.minRounds ? `${counts.minRounds} removed by min rounds` : "",
        counts.polarity ? `${counts.polarity} removed by polarity` : "",
        counts.neutral ? `${counts.neutral} removed as near-neutral` : "",
        counts.selected ? `${counts.selected} hidden by pair selection` : "",
    ].filter(Boolean).join(" · ");
    const emptyState = pairCount === 0
        ? `<div class="insights-finding warning"><strong>Why empty?</strong> ${escapeHtml(emptyReasons || "All rows were filtered out.")}<br>` +
          `<button id="ws-team-reset-filters" type="button">Reset filters</button> ` +
          `<button id="ws-team-clear-selection-quick" type="button">Clear selection</button>` +
          `</div>`
        : "";
    return (
        `${meta}${filterBar}${partialBanner}${emptyState}` +
        `<div class="workspace-team-kpis">` +
        `<div class="workspace-team-kpi"><span>Visible pairs</span><strong>${pairCount}</strong></div>` +
        `<div class="workspace-team-kpi"><span>Reliable pairs (>=5 matches)</span><strong>${reliableCount}</strong></div>` +
        `<div class="workspace-team-kpi"><span>Avg pair delta</span><strong class="${avgPairDelta >= 0 ? "pos" : "neg"}">${avgPairDelta >= 0 ? "+" : ""}${formatFixed(avgPairDelta, 2)}pp</strong></div>` +
        `</div>` +
        `<div class="workspace-team-quick-grid">` +
        `<section><h4>Top Synergy</h4>${renderQuickList(topPositive, "positive")}</section>` +
        `<section><h4>Risk Pairs</h4>${renderQuickList(topNegative, "negative")}</section>` +
        `</div>` +
        `${heatmap}` +
        `<div class="workspace-team-table">` +
        `<div class="workspace-team-header">` +
        `<strong>#</strong>` +
        `<button type="button" class="workspace-team-sort" data-sort="pair">Pair ${sortIcon("pair")}</button>` +
        `<button type="button" class="workspace-team-sort" data-sort="matches_n">Matches ${sortIcon("matches_n")}</button>` +
        `<button type="button" class="workspace-team-sort" data-sort="rounds_n">Rounds ${sortIcon("rounds_n")}</button>` +
        `<button type="button" class="workspace-team-sort" data-sort="wins_n">Wins ${sortIcon("wins_n")}</button>` +
        `<button type="button" class="workspace-team-sort" data-sort="win_rate">Win% ${sortIcon("win_rate")}</button>` +
        `<button type="button" class="workspace-team-sort" data-sort="delta_vs_user_baseline">Delta vs Baseline ${sortIcon("delta_vs_user_baseline")}</button>` +
        `<strong>Conf</strong>` +
        `</div>` +
        tableRows +
        `</div>`
    );
}

function syncWorkspaceTeamInspectorFromState(payload) {
    const teamUi = getWorkspaceTeamUiState();
    const pairs = Array.isArray(payload?.pairs) ? payload.pairs : [];
    const withKey = pairs.map((row) => ({ ...row, __pairKey: workspaceTeamPairKey(row) }));
    const selected = withKey.find((row) => String(row.__pairKey || "") === String(teamUi.selectedPairKey || ""));
    const clampAbs = Math.max(5, toNumber(document.getElementById("ws-clamp-abs")?.value, 15));
    renderWorkspaceTeamInspector(selected || null, toNumber(payload?.baseline_win_rate, 0), clampAbs);
}

function renderWorkspacePanel(panel, payload) {
    const el = document.getElementById(`workspace-panel-${panel}`);
    if (!el) return;
    if (panel === "overview") el.innerHTML = renderWorkspaceOverview(payload);
    if (panel === "operators") el.innerHTML = renderWorkspaceOperatorScatter(payload?.operators);
    if (panel === "matchups") el.innerHTML = renderWorkspaceMatchups(payload);
    if (panel === "team") el.innerHTML = renderWorkspaceTeam(payload);
    if (panel === "operators") {
        el.querySelectorAll(".ws-scatter-dot").forEach((btn) => {
            btn.addEventListener("click", async () => {
                const op = btn.dataset.op || "";
                const sel = { type: "operator", operator: op };
                setWorkspaceSelection(sel);
                try {
                    await loadWorkspaceOperatorInspector(op);
                    computeReportState.workspace.evidenceCursor = "";
                    computeReportState.workspace.evidenceRows = [];
                    await loadWorkspaceEvidence(true);
                } catch (err) {
                    logCompute(`Workspace operator inspector failed: ${err}`, "error");
                }
            });
        });
    }
    if (panel === "matchups") {
        el.querySelectorAll(".ws-threat-row").forEach((btn) => {
            btn.addEventListener("click", async () => {
                const type = btn.dataset.type || "";
                const op = btn.dataset.op || "";
                const sel = type === "matchup_col"
                    ? { type, col_def_op: op }
                    : { type, row_atk_op: op };
                setWorkspaceSelection(sel);
                computeReportState.workspace.evidenceCursor = "";
                computeReportState.workspace.evidenceRows = [];
                try {
                    await loadWorkspaceEvidence(true);
                } catch (err) {
                    logCompute(`Workspace evidence failed: ${err}`, "error");
                }
            });
        });
    }
    if (panel === "team") {
        const rerenderTeam = () => {
            const cached = computeReportState.workspace.dataByPanel?.team || payload;
            if (cached) renderWorkspacePanel("team", cached);
        };
        el.querySelector("#ws-team-min-matches")?.addEventListener("change", (ev) => {
            getWorkspaceTeamUiState().minMatches = Math.max(1, toNumber(ev?.target?.value, 5));
            rerenderTeam();
        });
        el.querySelector("#ws-team-min-rounds")?.addEventListener("change", (ev) => {
            getWorkspaceTeamUiState().minRounds = Math.max(1, toNumber(ev?.target?.value, 30));
            rerenderTeam();
        });
        el.querySelector("#ws-team-polarity")?.addEventListener("change", (ev) => {
            getWorkspaceTeamUiState().polarity = String(ev?.target?.value || "all");
            rerenderTeam();
        });
        el.querySelector("#ws-team-hide-neutral")?.addEventListener("change", (ev) => {
            getWorkspaceTeamUiState().hideNeutral = ev?.target?.checked === true;
            rerenderTeam();
        });
        el.querySelectorAll(".workspace-team-sort").forEach((btn) => {
            btn.addEventListener("click", () => {
                const sort = String(btn.dataset.sort || "rounds_n");
                const ui = getWorkspaceTeamUiState();
                if (ui.sortBy === sort) ui.sortDir = ui.sortDir === "desc" ? "asc" : "desc";
                else {
                    ui.sortBy = sort;
                    ui.sortDir = "desc";
                }
                rerenderTeam();
            });
        });
        const selectPairKey = (pairKey) => {
            const ui = getWorkspaceTeamUiState();
            ui.selectedPairKey = String(pairKey || "").trim();
            rerenderTeam();
        };
        el.querySelectorAll("[data-pair-key]").forEach((btn) => {
            btn.addEventListener("click", () => selectPairKey(String(btn.dataset.pairKey || "")));
        });
        el.querySelector("#ws-team-clear-selection")?.addEventListener("click", () => selectPairKey(""));
        el.querySelector("#ws-team-clear-selection-quick")?.addEventListener("click", () => selectPairKey(""));
        el.querySelector("#ws-team-reset-filters")?.addEventListener("click", () => {
            const ui = getWorkspaceTeamUiState();
            ui.minMatches = 5;
            ui.minRounds = 30;
            ui.polarity = "all";
            ui.hideNeutral = false;
            rerenderTeam();
        });
        el.querySelector("#ws-team-quick-30d")?.addEventListener("click", async () => {
            const daysEl = document.getElementById("ws-days");
            if (daysEl) daysEl.value = "30";
            computeReportState.workspace.dataByPanel = {};
            await loadWorkspacePanel("team", true);
        });
        el.querySelector("#ws-team-quick-ranked")?.addEventListener("click", async () => {
            const queueEl = document.getElementById("ws-queue");
            if (queueEl) queueEl.value = "ranked";
            computeReportState.workspace.dataByPanel = {};
            await loadWorkspacePanel("team", true);
        });
        el.querySelector("#ws-team-force-refresh")?.addEventListener("click", async () => {
            computeReportState.workspace.dataByPanel = {};
            await loadWorkspacePanel("team", true);
        });
        syncWorkspaceTeamInspectorFromState(payload);
        document.getElementById("ws-team-filter-selected")?.addEventListener("click", () => {
            const key = String(document.getElementById("ws-team-filter-selected")?.dataset?.pairKey || "");
            getWorkspaceTeamUiState().selectedPairKey = key;
            rerenderTeam();
        });
        document.getElementById("ws-team-clear-selected")?.addEventListener("click", () => {
            getWorkspaceTeamUiState().selectedPairKey = "";
            rerenderTeam();
        });
    }
}

function renderWorkspacePanelError(panel, err) {
    const el = document.getElementById(`workspace-panel-${panel}`);
    if (!el) return;
    el.innerHTML = `<div class="compute-value">Workspace ${escapeHtml(panel)} failed: ${escapeHtml(String(err))}</div>`;
}

function setWorkspaceSelection(nextSelection) {
    computeReportState.workspace.selection = nextSelection || { type: null };
    const ins = document.getElementById("workspace-inspector");
    if (!ins) return;
    if (!nextSelection || !nextSelection.type) {
        ins.innerHTML = `<div class="compute-label">Inspector</div><div class="compute-value">Click a chart item to inspect.</div>`;
        return;
    }
    if (nextSelection.type === "operator") {
        ins.innerHTML = `<div class="compute-label">Inspector</div><div class="compute-value">Loading operator ${escapeHtml(nextSelection.operator)}...</div>`;
    } else {
        ins.innerHTML = `<div class="compute-label">Inspector</div><div class="compute-value">Selection: ${escapeHtml(nextSelection.type)}</div>`;
    }
}

async function loadWorkspaceOperatorInspector(operatorName) {
    const username = computeReportState.username || (document.getElementById("compute-username")?.value || "").trim();
    if (!username || !operatorName) return;
    const f = getWorkspaceFiltersFromUI();
    const qs = new URLSearchParams({
        days: f.days,
        queue: f.queue,
        playlist: f.playlist,
        map_name: f.map_name,
        stack_only: f.stack_only,
        search: f.search,
        weighting: f.weighting,
        side: "all",
        recent_n_rounds: "300",
        previous_window: "true",
    });
    const res = await api.getDashboardWorkspaceOperator(username, operatorName, qs);
    if (!res.ok) throw new Error(`Operator inspector HTTP ${res.status}`);
    const payload = await res.json();
    const ins = document.getElementById("workspace-inspector");
    if (!ins) return;
    const s = payload.summary || {};
    const i = payload.impact_metrics || {};
    const rw = payload.recent_windows || {};
    ins.innerHTML = `
        <div class="compute-label">Inspector: ${escapeHtml(operatorName)}</div>
        <div class="dashboard-graph-summary">
            <span>Rounds <strong>${toNumber(s.n_rounds, 0)}</strong></span>
            <span>Win% <strong>${toNumber(s.win_pct, 0).toFixed(2)}</strong></span>
            <span>Presence <strong>${toNumber(s.presence_pct, 0).toFixed(2)}%</strong></span>
            <span>CI <strong>[${toNumber(s.win_ci_low, 0).toFixed(2)}, ${toNumber(s.win_ci_high, 0).toFixed(2)}]</strong></span>
        </div>
        <div class="dashboard-graph-summary">
            <span>OKR <strong>${toNumber(i.opening_kill_rate, 0).toFixed(2)}%</strong></span>
            <span>ODR <strong>${toNumber(i.opening_death_rate, 0).toFixed(2)}%</strong></span>
            <span>Survival <strong>${toNumber(i.survival_rate, 0).toFixed(2)}%</strong></span>
            <span>Clutch <strong>${toNumber(i.clutch_rate, 0).toFixed(2)}%</strong></span>
        </div>
        <div class="dashboard-graph-summary">
            <span>Recent ${toNumber(rw.recent_n_rounds, 0)} <strong>${toNumber(rw.recent?.win_pct, 0).toFixed(2)}%</strong></span>
            <span>Prev window <strong>${toNumber(rw.previous?.win_pct, 0).toFixed(2)}%</strong></span>
        </div>
    `;
}

function renderWorkspaceEvidenceTable() {
    const table = document.getElementById("workspace-evidence-table");
    const meta = document.getElementById("workspace-evidence-meta");
    if (!table || !meta) return;
    const rows = computeReportState.workspace.evidenceRows || [];
    if (!rows.length) {
        table.innerHTML = `<div class="compute-value">No evidence rows.</div>`;
        meta.textContent = "No evidence loaded.";
        return;
    }
    meta.textContent = `Rows loaded: ${rows.length}`;
    const header = `<div class="stored-match-row"><strong>Match</strong><strong>Round</strong><strong>Map</strong><strong>User</strong><strong>Side</strong><strong>Op</strong><strong>Result</strong><strong>K/D/A</strong></div>`;
    const body = rows.map((r) => `<div class="stored-match-row"><span>${escapeHtml(String(r.match_id || ""))}</span><span>${toNumber(r.round_id, 0)}</span><span>${escapeHtml(String(r.map_name || ""))}</span><span>${escapeHtml(String(r.username || ""))}</span><span>${escapeHtml(String(r.side || ""))}</span><span>${escapeHtml(String(r.operator || ""))}</span><span>${escapeHtml(String(r.result || ""))}</span><span>${toNumber(r.kills, 0)}/${toNumber(r.deaths, 0)}/${toNumber(r.assists, 0)}</span></div>`).join("");
    table.innerHTML = header + body;
}

async function loadWorkspaceEvidence(reset = true) {
    const selection = computeReportState.workspace.selection || { type: null };
    if (!selection.type) return;
    const username = computeReportState.username || (document.getElementById("compute-username")?.value || "").trim();
    if (!username) return;
    const f = getWorkspaceFiltersFromUI();
    const qs = new URLSearchParams({
        days: f.days,
        queue: f.queue,
        playlist: f.playlist,
        map_name: f.map_name,
        stack_only: f.stack_only,
        search: f.search,
        selection_version: "1",
        selection_type: selection.type,
        evidence_limit: "200",
    });
    if (selection.operator) qs.set("operator", selection.operator);
    if (selection.atk_op) qs.set("atk_op", selection.atk_op);
    if (selection.def_op) qs.set("def_op", selection.def_op);
    if (selection.row_atk_op) qs.set("row_atk_op", selection.row_atk_op);
    if (selection.col_def_op) qs.set("col_def_op", selection.col_def_op);
    if (!reset && computeReportState.workspace.evidenceCursor) qs.set("evidence_cursor", computeReportState.workspace.evidenceCursor);
    const res = await api.getDashboardWorkspaceEvidence(username, qs);
    if (!res.ok) throw new Error(`Evidence HTTP ${res.status}`);
    const payload = await res.json();
    computeReportState.workspace.evidenceCursor = payload.next_cursor || "";
    if (reset) computeReportState.workspace.evidenceRows = Array.isArray(payload.rows) ? payload.rows : [];
    else computeReportState.workspace.evidenceRows = (computeReportState.workspace.evidenceRows || []).concat(Array.isArray(payload.rows) ? payload.rows : []);
    renderWorkspaceEvidenceTable();
    const more = document.getElementById("workspace-evidence-more");
    if (more) more.disabled = !payload.has_more;
}

async function loadWorkspacePanel(panel, force = false) {
    const username = computeReportState.username || (document.getElementById("compute-username")?.value || "").trim();
    if (!username) return;
    const panelKey = panel || computeReportState.workspace.panel || "overview";
    const target = document.getElementById(`workspace-panel-${panelKey}`);
    if (target) {
        target.innerHTML = `<div class="compute-value">Loading workspace ${escapeHtml(panelKey)}...</div>`;
    }
    if (!force && computeReportState.workspace.dataByPanel[panelKey]) {
        renderWorkspacePanel(panelKey, computeReportState.workspace.dataByPanel[panelKey]);
        return;
    }
    const requestSeq = (toNumber(computeReportState.workspace.requestSeq, 0) + 1);
    computeReportState.workspace.requestSeq = requestSeq;
    const f = getWorkspaceFiltersFromUI();
    persistWorkspaceState();
    if (panelKey === "team") {
        if (target) target.innerHTML = `<div class="compute-value">Building scope...</div>`;
        const teamQs = new URLSearchParams({
            ws_days: f.days,
            ws_queue: f.queue,
            ws_playlist: f.playlist,
            ws_map_name: f.map_name,
            ws_stack_only: f.stack_only,
            ws_search: f.search,
            force_refresh: force ? "true" : "false",
        });
        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), 10000);
        let res;
        try {
            if (target) target.innerHTML = `<div class="compute-value">Loading cached results...</div>`;
            res = await api.getWorkspaceTeam(username, teamQs, { signal: ctrl.signal });
        } catch (err) {
            if (String(err?.name || "").toLowerCase() === "aborterror") {
                throw new Error("Team request timed out after 10s");
            }
            throw err;
        } finally {
            clearTimeout(timer);
        }
        if (!res.ok) {
            const text = await res.text();
            throw new Error(`Workspace Team HTTP ${res.status}: ${text}`);
        }
        const payload = await res.json();
        if (requestSeq !== computeReportState.workspace.requestSeq) return;
        computeReportState.workspace.dataByPanel[panelKey] = payload;
        renderWorkspacePanel(panelKey, payload);
        return;
    }
    const qs = new URLSearchParams({ ...f, panel: panelKey });
    const fetchWorkspace = async (timeoutMs) => {
        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), timeoutMs);
        try {
            return await api.getDashboardWorkspace(username, qs, { signal: ctrl.signal });
        } catch (err) {
            if (err?.name === "AbortError") {
                throw new Error(`Workspace request timed out after ${Math.round(timeoutMs / 1000)}s`);
            }
            throw err;
        } finally {
            clearTimeout(timer);
        }
    };
    let res;
    try {
        res = await fetchWorkspace(WORKSPACE_REQUEST_TIMEOUT_MS);
    } catch (err) {
        const isTimeout = String(err).toLowerCase().includes("timed out");
        if (!isTimeout) throw err;
        logCompute("Workspace request timed out; retrying once...", "info");
        res = await fetchWorkspace(WORKSPACE_REQUEST_TIMEOUT_MS + 30000);
    }
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`Workspace HTTP ${res.status}: ${text}`);
    }
    const payload = await res.json();
    if (requestSeq !== computeReportState.workspace.requestSeq) return;
    computeReportState.workspace.dataByPanel[panelKey] = payload;
    computeReportState.workspace.meta = payload.meta || null;
    renderWorkspacePanel(panelKey, payload);
}

function scheduleWorkspaceAutoRefresh(delayMs = 250) {
    if (workspaceAutoRefreshTimer) {
        clearTimeout(workspaceAutoRefreshTimer);
        workspaceAutoRefreshTimer = null;
    }
    workspaceAutoRefreshTimer = setTimeout(async () => {
        workspaceAutoRefreshTimer = null;
        const username = computeReportState.username || (document.getElementById("compute-username")?.value || "").trim();
        if (!username) return;
        const panel = computeReportState.workspace.panel || "overview";
        try {
            await loadWorkspacePanel(panel, true);
        } catch (err) {
            logCompute(`Workspace refresh failed: ${err}`, "error");
            renderWorkspacePanelError(panel, err);
        }
    }, Math.max(0, toNumber(delayMs, 250)));
}

function scheduleDashboardGraphRender(delayMs = 80) {
    if (dashboardGraphRenderTimer) {
        clearTimeout(dashboardGraphRenderTimer);
        dashboardGraphRenderTimer = null;
    }
    dashboardGraphRenderTimer = setTimeout(() => {
        dashboardGraphRenderTimer = null;
        renderDashboardGraphs();
    }, Math.max(0, toNumber(delayMs, 80)));
}

function setDashboardView(view) {
    const next = view === "graphs" ? "graphs" : "insights";
    computeReportState.dashboardView = next;
    const insightsPanel = document.getElementById("dashboard-insights-panel");
    const graphsPanel = document.getElementById("dashboard-graphs-panel");
    if (insightsPanel) insightsPanel.classList.toggle("active", next === "insights");
    if (graphsPanel) graphsPanel.classList.toggle("active", next === "graphs");
    // Only toggle top-level Dashboard subtabs; workspace subtabs are managed separately.
    document.querySelectorAll(".dashboard-subtab[data-dashboard-view]").forEach((btn) => {
        const isActive = btn.dataset.dashboardView === next;
        btn.classList.toggle("active", isActive);
        btn.setAttribute("aria-selected", isActive ? "true" : "false");
    });
}

function setGraphPanel(panel) {
    const next = panel === "threat" ? "threat" : "workspace";
    computeReportState.graphPanel = next;
    document.querySelectorAll("[data-graph-panel]").forEach((btn) => {
        const isActive = btn.dataset.graphPanel === next;
        btn.classList.toggle("active", isActive);
        btn.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    const workspacePanel = document.getElementById("graph-panel-workspace");
    const threatPanel = document.getElementById("graph-panel-threat");
    if (workspacePanel) {
        if (isWorkspaceMountedTopLevel()) {
            // Top-level Workspace must stay visible independent of dashboard graph tab state.
            workspacePanel.hidden = false;
        } else {
            workspacePanel.hidden = next !== "workspace";
        }
    }
    if (threatPanel) threatPanel.hidden = next !== "threat";
}

function initStatusChipMirrors() {
    const mirrors = [
        { sourceId: "scan-status", targetId: "scan-status-chip" },
        { sourceId: "match-status", targetId: "match-status-chip" },
        { sourceId: "stored-total", targetId: "stored-list-chip", prefix: "Items " },
        { sourceId: "scan-status", targetId: "chip-websocket" },
        { sourceId: "match-status", targetId: "chip-scraper" },
    ];
    mirrors.forEach(({ sourceId, targetId, prefix = "" }) => {
        const source = document.getElementById(sourceId);
        const target = document.getElementById(targetId);
        if (!source || !target) return;
        const sync = () => {
            const next = `${prefix}${String(source.textContent || "").trim() || "-"}`;
            target.textContent = next;
        };
        sync();
        const observer = new MutationObserver(sync);
        observer.observe(source, { childList: true, characterData: true, subtree: true });
    });
    const dbChip = document.getElementById("chip-db");
    if (dbChip) dbChip.textContent = "Ready";
}

function syncFilterDrawerDefaults() {
    const compact = window.matchMedia("(max-width: 900px)").matches;
    if (filterDrawerCompactMode === compact) return;
    filterDrawerCompactMode = compact;
    document.querySelectorAll(".filter-drawer").forEach((drawer) => {
        if (!(drawer instanceof HTMLDetailsElement)) return;
        drawer.open = !compact;
    });
}

function refreshDashboardSectionLayout() {
    const layout = document.querySelector(".dashboard-insights-layout");
    const left = document.getElementById("dashboard-section-insight-cards");
    const right = document.getElementById("dashboard-section-performance-focus");
    if (!layout || !left || !right) return;
    const leftVisible = !left.classList.contains("hidden");
    const rightVisible = !right.classList.contains("hidden");
    layout.classList.toggle("single-column", leftVisible !== rightVisible);
}

function initDashboardSectionToggles() {
    const map = {
        "dashboard-section-playbook": "playbook",
        "dashboard-section-deep-stats": "deepStats",
        "dashboard-section-insight-cards": "insightCards",
        "dashboard-section-performance-focus": "performanceFocus",
    };
    document.querySelectorAll(".dashboard-toggle-chip").forEach((btn) => {
        btn.addEventListener("click", () => {
            const targetId = btn.dataset.targetId || "";
            const section = document.getElementById(targetId);
            if (!section) return;
            const nowVisible = section.classList.contains("hidden");
            section.classList.toggle("hidden", !nowVisible);
            btn.classList.toggle("active", nowVisible);
            const key = map[targetId];
            if (key) computeReportState.sectionVisibility[key] = nowVisible;
            refreshDashboardSectionLayout();
        });
    });
}

function applyDashboardSectionVisibility() {
    const visibility = computeReportState.sectionVisibility || {};
    const sections = [
        ["dashboard-section-playbook", "toggle-playbook", visibility.playbook !== false],
        ["dashboard-section-deep-stats", "toggle-deep-stats", visibility.deepStats !== false],
        ["dashboard-section-insight-cards", "toggle-insight-cards", visibility.insightCards !== false],
        ["dashboard-section-performance-focus", "toggle-performance-focus", visibility.performanceFocus !== false],
    ];
    for (const [sectionId, toggleId, isVisible] of sections) {
        const section = document.getElementById(sectionId);
        const toggle = document.getElementById(toggleId);
        if (section) section.classList.toggle("hidden", !isVisible);
        if (toggle) toggle.classList.toggle("active", isVisible);
    }
    refreshDashboardSectionLayout();
}

function renderComputeReport() {
    const stats = computeReportState.stats;
    if (!stats) return;
    const mode = computeReportState.mode || "overall";
    const current = stats[mode] || stats.overall;
    const modeCardsEl = document.getElementById("compute-mode-cards");
    if (modeCardsEl) {
        modeCardsEl.innerHTML = [
            modeSummaryCard("Overall", "overall", stats.overall, mode === "overall"),
            modeSummaryCard("Ranked", "ranked", stats.ranked, mode === "ranked"),
            modeSummaryCard("Unranked", "unranked", stats.unranked, mode === "unranked"),
        ].join("");
        modeCardsEl.querySelectorAll(".compute-mode-card").forEach((el) => {
            el.addEventListener("click", () => {
                computeReportState.mode = el.dataset.mode || "overall";
                renderComputeReport();
            });
        });
    }

    const winRateEl = document.getElementById("compute-kpi-winrate");
    const recordEl = document.getElementById("compute-kpi-record");
    if (winRateEl) winRateEl.textContent = `${formatFixed(current.winRate, 1)}%`;
    if (recordEl) {
        recordEl.textContent = `Record ${toNumber(current.wins, 0)}-${toNumber(current.losses, 0)} • ${toNumber(current.matches, 0)} matches`;
    }

    document.querySelectorAll(".compute-mode-toggle .compute-chip").forEach((chip) => {
        chip.classList.toggle("active", chip.dataset.mode === mode);
    });

    const round = computeReportState.round || {};
    const atk = toNumber(round.atk_win_rate, 0);
    const def = toNumber(round.def_win_rate, 0);
    const fd = toNumber(round.fd_rate, 0);
    const fb = toNumber(round.fb_impact_delta, 0);
    const atkEl = document.getElementById("compute-kpi-atk");
    const defEl = document.getElementById("compute-kpi-def");
    const fdEl = document.getElementById("compute-kpi-fd");
    const fbEl = document.getElementById("compute-kpi-fb");
    if (atkEl) atkEl.style.width = `${Math.max(0, Math.min(100, atk))}%`;
    if (defEl) defEl.style.width = `${Math.max(0, Math.min(100, def))}%`;
    if (fdEl) fdEl.style.width = `${Math.max(0, Math.min(100, fd))}%`;
    if (fbEl) fbEl.style.width = `${Math.max(0, Math.min(100, Math.abs(fb)))}%`;
    const atkDefText = document.getElementById("compute-kpi-atkdef-text");
    const fdText = document.getElementById("compute-kpi-fd-text");
    const fbText = document.getElementById("compute-kpi-fb-text");
    if (atkDefText) atkDefText.textContent = `${formatFixed(atk, 1)}% / ${formatFixed(def, 1)}%`;
    if (fdText) fdText.textContent = `${formatFixed(fd, 1)}%`;
    if (fbText) fbText.textContent = `${fb >= 0 ? "+" : ""}${formatFixed(fb, 1)}%`;

    computeDashboardSortedData();
    applyDashboardSectionVisibility();
    setDashboardView(computeReportState.dashboardView || "insights");
    renderPlaybook();
    renderDeepStats(round);
    renderDashboardInsightCards();
    renderDashboardGraphs();
}

function formatPct(value) {
    const n = Number(value);
    return Number.isFinite(n) ? `${n.toFixed(1)}%` : "0.0%";
}

function renderRoundAnalysisCard(analysis) {
    if (!analysis || analysis.error) {
        const reason = analysis?.error || "No round analysis data available.";
        return `
            <div class="compute-card compute-round-card">
                <div class="compute-label">Round Analysis (V3)</div>
                <div class="compute-value">${escapeHtml(reason)}</div>
            </div>
        `;
    }

    const findings = Array.isArray(analysis.findings) ? analysis.findings : [];
    const findingsHtml = findings.length
        ? findings
              .map((f) => {
                  const sev = String(f?.severity || "info").toLowerCase();
                  const sevClass = sev === "critical" ? "compute-sev-critical" : sev === "warning" ? "compute-sev-warning" : "compute-sev-info";
                  const label = sev === "critical" ? "Critical" : sev === "warning" ? "Warning" : "Info";
                  return `<li class="compute-finding ${sevClass}"><strong>${label}:</strong> ${escapeHtml(f?.message || "")}</li>`;
              })
              .join("")
        : '<li class="compute-finding compute-sev-info"><strong>Info:</strong> No findings generated.</li>';

    return `
        <div class="compute-card compute-round-card">
            <div class="compute-label">Round Analysis (V3)</div>
            <div class="compute-metric-row"><span>Total Rounds</span><strong>${toNumber(analysis.total_rounds, 0)}</strong></div>
            <div class="compute-metric-row"><span>Data Quality</span><strong>${escapeHtml(String(analysis.data_quality || "unknown"))}</strong></div>
            <div class="compute-metric-row"><span>FB Impact Delta</span><strong>${formatPct(analysis.fb_impact_delta)}</strong></div>
            <div class="compute-metric-row"><span>First Death Rate</span><strong>${formatPct(analysis.fd_rate)}</strong></div>
            <div class="compute-metric-row"><span>Attack Win Rate</span><strong>${formatPct(analysis.atk_win_rate)}</strong></div>
            <div class="compute-metric-row"><span>Defense Win Rate</span><strong>${formatPct(analysis.def_win_rate)}</strong></div>
            <div class="compute-metric-row"><span>Clutch Win Rate</span><strong>${formatPct(analysis.clutch_win_rate)}</strong></div>
            <div class="compute-metric-row"><span>Primary Win Condition</span><strong>${escapeHtml(String(analysis.primary_win_condition || "mixed"))}</strong></div>
            <div class="compute-findings-wrap">
                <div class="compute-findings-title">Findings</div>
                <ul class="compute-findings-list">${findingsHtml}</ul>
            </div>
        </div>
    `;
}

function renderRoundAnalysisCardInCompute(analysis) {
    const el = document.getElementById("compute-results");
    if (!el) return;
    const existing = el.querySelector(".compute-round-card");
    if (existing) existing.remove();
    el.insertAdjacentHTML("beforeend", renderRoundAnalysisCard(analysis));
}

function formatSignedPct(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return "0.0%";
    return `${n >= 0 ? "+" : ""}${n.toFixed(1)}%`;
}

function renderTeammateChemistryCard(analysis) {
    if (!analysis || analysis.error) {
        const reason = analysis?.error || "No teammate chemistry data available.";
        return `
            <div class="compute-card compute-chem-card">
                <div class="compute-label">Teammate Chemistry (V3)</div>
                <div class="compute-value">${escapeHtml(reason)}</div>
            </div>
        `;
    }

    const best = analysis.best_teammate || null;
    const worst = analysis.worst_teammate || null;
    const mostPlayed = analysis.most_played_with || null;
    const findings = Array.isArray(analysis.findings) ? analysis.findings : [];
    const findingsHtml = findings.length
        ? findings
              .map((f) => {
                  const sev = String(f?.severity || "info").toLowerCase();
                  const sevClass = sev === "critical" ? "compute-sev-critical" : sev === "warning" ? "compute-sev-warning" : "compute-sev-info";
                  const label = sev === "critical" ? "Critical" : sev === "warning" ? "Warning" : "Info";
                  return `<li class="compute-finding ${sevClass}"><strong>${label}:</strong> ${escapeHtml(f?.message || "")}</li>`;
              })
              .join("")
        : '<li class="compute-finding compute-sev-info"><strong>Info:</strong> No findings generated.</li>';

    const teammateLine = (label, row, deltaSigned = true) => {
        if (!row) {
            return `<div class="compute-metric-row"><span>${label}</span><strong>N/A</strong></div>`;
        }
        const delta = deltaSigned ? formatSignedPct(row.chemistry_delta) : formatPct(row.chemistry_delta);
        return `<div class="compute-metric-row"><span>${label}</span><strong>${escapeHtml(row.teammate)} (${toNumber(row.shared_matches, 0)} | ${delta})</strong></div>`;
    };

    return `
        <div class="compute-card compute-chem-card">
            <div class="compute-label">Teammate Chemistry (V3)</div>
            <div class="compute-metric-row"><span>Baseline Win Rate</span><strong>${formatPct(analysis.baseline_win_rate)}</strong></div>
            <div class="compute-metric-row"><span>Matches Analyzed</span><strong>${toNumber(analysis.total_matches_analyzed, 0)}</strong></div>
            <div class="compute-metric-row"><span>Unique Teammates</span><strong>${toNumber(analysis.unique_teammates_seen, 0)}</strong></div>
            <div class="compute-metric-row"><span>Reliable Teammates</span><strong>${toNumber(analysis.reliable_teammate_count, 0)}</strong></div>
            ${teammateLine("Best Teammate", best)}
            ${teammateLine("Toughest Queue", worst)}
            ${teammateLine("Most Played With", mostPlayed)}
            <div class="compute-findings-wrap">
                <div class="compute-findings-title">Findings</div>
                <ul class="compute-findings-list">${findingsHtml}</ul>
            </div>
        </div>
    `;
}

function renderTeammateChemistryCardInCompute(analysis) {
    const el = document.getElementById("compute-results");
    if (!el) return;
    const existing = el.querySelector(".compute-chem-card");
    if (existing) existing.remove();
    el.insertAdjacentHTML("beforeend", renderTeammateChemistryCard(analysis));
}

function renderLobbyQualityCard(analysis) {
    if (!analysis || analysis.error) {
        const reason = analysis?.error || "No lobby quality data available.";
        return `
            <div class="compute-card compute-lobby-card">
                <div class="compute-label">Lobby Quality (V3)</div>
                <div class="compute-value">${escapeHtml(reason)}</div>
            </div>
        `;
    }

    const findings = Array.isArray(analysis.findings) ? analysis.findings : [];
    const findingsHtml = findings.length
        ? findings
              .map((f) => {
                  const sev = String(f?.severity || "info").toLowerCase();
                  const sevClass = sev === "critical" ? "compute-sev-critical" : sev === "warning" ? "compute-sev-warning" : "compute-sev-info";
                  const label = sev === "critical" ? "Critical" : sev === "warning" ? "Warning" : "Info";
                  return `<li class="compute-finding ${sevClass}"><strong>${label}:</strong> ${escapeHtml(f?.message || "")}</li>`;
              })
              .join("")
        : '<li class="compute-finding compute-sev-info"><strong>Info:</strong> No findings generated.</li>';

    return `
        <div class="compute-card compute-lobby-card">
            <div class="compute-label">Lobby Quality (V3)</div>
            <div class="compute-metric-row"><span>Matches Analyzed</span><strong>${toNumber(analysis.matches_analyzed, 0)}</strong></div>
            <div class="compute-metric-row"><span>Your Avg RP</span><strong>${toNumber(analysis.avg_my_rp, 0)}</strong></div>
            <div class="compute-metric-row"><span>Enemy Avg RP</span><strong>${toNumber(analysis.avg_enemy_rp, 0)}</strong></div>
            <div class="compute-metric-row"><span>RP Diff (You-Enemy)</span><strong>${toNumber(analysis.avg_rp_diff, 0)}</strong></div>
            <div class="compute-metric-row"><span>Overall Win Rate</span><strong>${formatPct(analysis.overall_win_rate)}</strong></div>
            <div class="compute-metric-row"><span>Vs Higher RP</span><strong>${formatPct(analysis.win_rate_vs_higher)}</strong></div>
            <div class="compute-metric-row"><span>Vs Even RP</span><strong>${formatPct(analysis.win_rate_vs_even)}</strong></div>
            <div class="compute-metric-row"><span>Vs Lower RP</span><strong>${formatPct(analysis.win_rate_vs_lower)}</strong></div>
            <div class="compute-findings-wrap">
                <div class="compute-findings-title">Findings</div>
                <ul class="compute-findings-list">${findingsHtml}</ul>
            </div>
        </div>
    `;
}

function renderTradeAnalysisCard(analysis) {
    if (!analysis || analysis.error) {
        const reason = analysis?.error || "No trade analysis data available.";
        return `
            <div class="compute-card compute-trade-card">
                <div class="compute-label">Trade Analysis (V3)</div>
                <div class="compute-value">${escapeHtml(reason)}</div>
            </div>
        `;
    }

    const findings = Array.isArray(analysis.findings) ? analysis.findings : [];
    const findingsHtml = findings.length
        ? findings
              .map((f) => {
                  const sev = String(f?.severity || "info").toLowerCase();
                  const sevClass = sev === "critical" ? "compute-sev-critical" : sev === "warning" ? "compute-sev-warning" : "compute-sev-info";
                  const label = sev === "critical" ? "Critical" : sev === "warning" ? "Warning" : "Info";
                  return `<li class="compute-finding ${sevClass}"><strong>${label}:</strong> ${escapeHtml(f?.message || "")}</li>`;
              })
              .join("")
        : '<li class="compute-finding compute-sev-info"><strong>Info:</strong> No findings generated.</li>';

    const citations = Array.isArray(analysis.citations) ? analysis.citations : [];
    const citationHtml = citations.length
        ? `<div class="compute-findings-wrap"><div class="compute-findings-title">Examples</div><ul class="compute-findings-list">${citations.map((c) => `<li class=\"compute-finding compute-sev-info\">${escapeHtml(c)}</li>`).join("")}</ul></div>`
        : "";

    return `
        <div class="compute-card compute-trade-card">
            <div class="compute-label">Trade Analysis (V3)</div>
            <div class="compute-metric-row"><span>Window</span><strong>${toNumber(analysis.window_seconds, 5)}s</strong></div>
            <div class="compute-metric-row"><span>Matches Analyzed</span><strong>${toNumber(analysis.matches_analyzed, 0)}</strong></div>
            <div class="compute-metric-row"><span>Total Deaths</span><strong>${toNumber(analysis.total_deaths, 0)}</strong></div>
            <div class="compute-metric-row"><span>Traded Deaths</span><strong>${toNumber(analysis.traded_deaths, 0)}</strong></div>
            <div class="compute-metric-row"><span>Trade Rate</span><strong>${formatPct(analysis.trade_rate)}</strong></div>
            <div class="compute-metric-row"><span>Direct Refrags</span><strong>${toNumber(analysis.direct_refrags, 0)}</strong></div>
            <div class="compute-metric-row"><span>Direct Refrag Rate</span><strong>${formatPct(analysis.direct_refrag_rate)}</strong></div>
            <div class="compute-metric-row"><span>Avg Trade Time</span><strong>${toNumber(analysis.avg_trade_time_seconds, 0).toFixed(2)}s</strong></div>
            <div class="compute-findings-wrap">
                <div class="compute-findings-title">Findings</div>
                <ul class="compute-findings-list">${findingsHtml}</ul>
            </div>
            ${citationHtml}
        </div>
    `;
}

function logInsights(message, level = "info") {
    const logEl = document.getElementById("insights-log");
    if (!logEl) return;
    const entry = document.createElement("div");
    entry.className = `log-entry log-${level}`;
    entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    logEl.appendChild(entry);
    logEl.scrollTop = logEl.scrollHeight;
}

function normalizeFindingSeverity(value) {
    const sev = String(value || "info").toLowerCase();
    if (sev === "critical" || sev === "warning" || sev === "info") return sev;
    return "info";
}

function findingSeverityIcon(severity) {
    if (severity === "critical") return "✗";
    if (severity === "warning") return "⚠";
    return "✓";
}

function toPctValue(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 0;
    return Math.max(0, Math.min(100, n));
}

function renderInsightsFindings(findings) {
    const list = Array.isArray(findings) ? findings : [];
    const maxVisible = 10;
    if (!list.length) {
        return `
            <div class="insights-finding-chip insights-sev-info">
                <div class="insights-finding-main">
                    <span class="insights-finding-icon">✓</span>
                    <span class="insights-finding-message">No findings generated.</span>
                </div>
            </div>
        `;
    }

    const renderFinding = (finding) => {
            const severity = normalizeFindingSeverity(finding?.severity);
            const citations = Array.isArray(finding?.citations) ? finding.citations : [];
            const citesHtml = citations.length
                ? `<div class="insights-finding-cites">${citations.map((c) => `<div class="insights-finding-cite">${escapeHtml(String(c))}</div>`).join("")}</div>`
                : "";
            return `
                <div class="insights-finding-chip insights-sev-${severity}">
                    <div class="insights-finding-main">
                        <span class="insights-finding-icon">${findingSeverityIcon(severity)}</span>
                        <span class="insights-finding-message">${escapeHtml(String(finding?.message || ""))}</span>
                    </div>
                    ${citesHtml}
                </div>
            `;
        };

    const visible = list.slice(0, maxVisible).map(renderFinding).join("");
    const hidden = list.slice(maxVisible).map(renderFinding).join("");
    if (list.length <= maxVisible) {
        return visible;
    }
    return `
        ${visible}
        <details class="insights-findings-more">
            <summary>Show ${list.length - maxVisible} more findings</summary>
            <div class="insights-findings-more-body">${hidden}</div>
        </details>
    `;
}

function renderRoundReportCard(analysis, lastUpdated) {
    if (!analysis || analysis.error) {
        return `
            <section class="insights-card">
                <header class="insights-card-head">
                    <div>
                        <div class="insights-card-title">Round Analysis</div>
                        <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                    </div>
                </header>
                <div class="insights-empty">${escapeHtml(analysis?.error || "No round analysis data available.")}</div>
            </section>
        `;
    }
    const atk = toPctValue(analysis.atk_win_rate);
    const def = toPctValue(analysis.def_win_rate);
    const fbDelta = Number(analysis.fb_impact_delta) || 0;
    const fbDeltaClass = fbDelta < 0 ? "insights-negative" : "insights-positive";
    const roundWin = toPctValue(analysis.overall_round_win_rate);
    const clutchWin = toPctValue(analysis.clutch_win_rate);
    return `
        <section class="insights-card">
            <header class="insights-card-head">
                <div>
                    <div class="insights-card-title">Round Analysis</div>
                    <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                </div>
                <div class="insights-stat-strip">
                    <span>Total rounds <strong>${toNumber(analysis.total_rounds, 0)}</strong></span>
                    <span>Data quality <strong>${escapeHtml(String(analysis.data_quality || "unknown"))}</strong></span>
                </div>
            </header>
            <div class="insights-side-bars">
                <div class="insights-side-col">
                    <div class="insights-side-label">ATK ${formatPct(atk)}</div>
                    <div class="insights-bar-track"><div class="insights-bar-fill insights-bar-atk" style="width:${atk.toFixed(1)}%"></div></div>
                </div>
                <div class="insights-side-col">
                    <div class="insights-side-label">DEF ${formatPct(def)}</div>
                    <div class="insights-bar-track"><div class="insights-bar-fill insights-bar-def" style="width:${def.toFixed(1)}%"></div></div>
                </div>
            </div>
            <div class="insights-callout ${fbDeltaClass}">
                <span class="insights-callout-label">FB Impact Delta</span>
                <strong>${formatSignedPct(fbDelta)}</strong>
            </div>
            <div class="insights-gauges">
                <div class="insights-gauge">
                    <div class="insights-gauge-ring" style="--pct:${roundWin.toFixed(1)}"><span>${formatPct(roundWin)}</span></div>
                    <div class="insights-gauge-label">Round Win Rate</div>
                </div>
                <div class="insights-gauge">
                    <div class="insights-gauge-ring" style="--pct:${clutchWin.toFixed(1)}"><span>${formatPct(clutchWin)}</span></div>
                    <div class="insights-gauge-label">Clutch Win Rate</div>
                </div>
            </div>
            <div class="insights-findings">${renderInsightsFindings(analysis.findings)}</div>
        </section>
    `;
}

function renderChemistryReportCard(analysis, lastUpdated) {
    if (!analysis || analysis.error) {
        return `
            <section class="insights-card">
                <header class="insights-card-head">
                    <div>
                        <div class="insights-card-title">Teammate Chemistry</div>
                        <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                    </div>
                </header>
                <div class="insights-empty">${escapeHtml(analysis?.error || "No teammate chemistry data available.")}</div>
            </section>
        `;
    }
    const all = Array.isArray(analysis.all_teammates) ? analysis.all_teammates : [];
    const ranked = all.slice(0, 8);
    const best = analysis.best_teammate?.teammate || "";
    const worst = analysis.worst_teammate?.teammate || "";
    const reliable = all.filter((t) => t && t.reliable).sort((a, b) => toNumber(b.win_rate, 0) - toNumber(a.win_rate, 0));
    const spread = reliable.length >= 2 ? Math.max(0, toNumber(reliable[0]?.win_rate, 0) - toNumber(reliable[reliable.length - 1]?.win_rate, 0)) : 0;
    const listHtml = ranked.length
        ? ranked
              .map((row) => {
                  const name = String(row?.teammate || "Unknown");
                  const delta = toNumber(row?.chemistry_delta, 0);
                  const tag = name === best ? "insights-teammate-best" : (name === worst ? "insights-teammate-worst" : "");
                  const deltaClass = delta >= 0 ? "insights-delta-pos" : "insights-delta-neg";
                  return `
                      <div class="insights-teammate-row ${tag}">
                          <div class="insights-teammate-name">${escapeHtml(name)}</div>
                          <div class="insights-teammate-matches">${toNumber(row?.shared_matches, 0)} matches</div>
                          <div class="insights-teammate-win">${formatPct(row?.win_rate)}</div>
                          <div class="insights-teammate-delta ${deltaClass}">${formatSignedPct(delta)}</div>
                      </div>
                  `;
              })
              .join("")
        : `<div class="insights-empty">No teammate ranking data available.</div>`;

    return `
        <section class="insights-card">
            <header class="insights-card-head">
                <div>
                    <div class="insights-card-title">Teammate Chemistry</div>
                    <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                </div>
                <div class="insights-stat-strip">
                    <span>Baseline <strong>${formatPct(analysis.baseline_win_rate)}</strong></span>
                    <span>Reliable teammates <strong>${toNumber(analysis.reliable_teammate_count, 0)}</strong></span>
                </div>
            </header>
            <div class="insights-callout ${spread >= 25 ? "insights-negative" : "insights-neutral"}">
                <span class="insights-callout-label">Queue Impact Swing</span>
                <strong>${formatPct(spread)} depending on who you queue with</strong>
            </div>
            <div class="insights-teammate-table">${listHtml}</div>
            <div class="insights-findings">${renderInsightsFindings(analysis.findings)}</div>
        </section>
    `;
}

function renderLobbyReportCard(analysis, lastUpdated) {
    if (!analysis || analysis.error) {
        return `
            <section class="insights-card">
                <header class="insights-card-head">
                    <div>
                        <div class="insights-card-title">Lobby Quality</div>
                        <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                    </div>
                </header>
                <div class="insights-empty">${escapeHtml(analysis?.error || "No lobby quality data available.")}</div>
            </section>
        `;
    }
    const brackets = Array.isArray(analysis.bracket_data) ? analysis.bracket_data : [];
    const bars = brackets.length
        ? brackets
              .map((b) => {
                  const wr = toPctValue(b?.win_rate);
                  return `
                      <div class="insights-bracket-row">
                          <div class="insights-bracket-label">${escapeHtml(String(b?.label || "Unknown"))}</div>
                          <div class="insights-bar-track"><div class="insights-bar-fill insights-bar-lobby" style="width:${wr.toFixed(1)}%"></div></div>
                          <div class="insights-bracket-value">${formatPct(wr)}</div>
                      </div>
                  `;
              })
              .join("")
        : `<div class="insights-empty">No bracket breakdown data available.</div>`;
    const evenWr = toNumber(analysis.win_rate_vs_even, 0);
    return `
        <section class="insights-card">
            <header class="insights-card-head">
                <div>
                    <div class="insights-card-title">Lobby Quality</div>
                    <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                </div>
                <div class="insights-stat-strip">
                    <span>Matches analyzed <strong>${toNumber(analysis.matches_analyzed, 0)}</strong></span>
                    <span>RP diff <strong>${toNumber(analysis.avg_rp_diff, 0)}</strong></span>
                </div>
            </header>
            <div class="insights-callout ${evenWr <= 20 ? "insights-critical" : "insights-neutral"}">
                <span class="insights-callout-label">You vs Even RP</span>
                <strong>${formatPct(evenWr)}</strong>
            </div>
            <div class="insights-brackets">${bars}</div>
            <div class="insights-findings">${renderInsightsFindings(analysis.findings)}</div>
        </section>
    `;
}

function renderTradeReportCard(analysis, lastUpdated) {
    if (!analysis || analysis.error) {
        return `
            <section class="insights-card">
                <header class="insights-card-head">
                    <div>
                        <div class="insights-card-title">Trade Analysis</div>
                        <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                    </div>
                </header>
                <div class="insights-empty">${escapeHtml(analysis?.error || "No trade analysis data available.")}</div>
            </section>
        `;
    }
    return `
        <section class="insights-card">
            <header class="insights-card-head">
                <div>
                    <div class="insights-card-title">Trade Analysis</div>
                    <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                </div>
                <div class="insights-stat-strip">
                    <span>Trade rate <strong>${formatPct(analysis.trade_rate)}</strong></span>
                    <span>Avg trade time <strong>${toNumber(analysis.avg_trade_time_seconds, 0).toFixed(2)}s</strong></span>
                </div>
            </header>
            <div class="insights-findings">${renderInsightsFindings(analysis.findings)}</div>
        </section>
    `;
}

function renderTeamReportCard(analysis, lastUpdated) {
    if (!analysis || analysis.error) {
        return `
            <section class="insights-card">
                <header class="insights-card-head">
                    <div>
                        <div class="insights-card-title">Team Analysis</div>
                        <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                    </div>
                </header>
                <div class="insights-empty">${escapeHtml(analysis?.error || "No team analysis data available.")}</div>
            </section>
        `;
    }
    const best = analysis.best_partner?.username || "N/A";
    const stack = analysis.best_stack_size?.label || "N/A";
    return `
        <section class="insights-card">
            <header class="insights-card-head">
                <div>
                    <div class="insights-card-title">Team Analysis</div>
                    <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                </div>
                <div class="insights-stat-strip">
                    <span>Matches analyzed <strong>${toNumber(analysis.total_matches, 0)}</strong></span>
                    <span>Baseline <strong>${formatPct(analysis.baseline_win_rate)}</strong></span>
                </div>
            </header>
            <div class="insights-callout insights-neutral">
                <span class="insights-callout-label">Best Queue Partner</span>
                <strong>${escapeHtml(best)}</strong>
            </div>
            <div class="insights-callout insights-neutral">
                <span class="insights-callout-label">Best Stack</span>
                <strong>${escapeHtml(stack)}</strong>
            </div>
            <div class="insights-findings">${renderInsightsFindings(analysis.findings)}</div>
        </section>
    `;
}

function renderEnemyThreatScatter(analysis) {
    const points = (Array.isArray(analysis?.scatter?.points) ? analysis.scatter.points : []).filter((p) => {
        const name = String(extractOperatorName(p?.operator) || "").trim().toLowerCase();
        return name && !["unknown", "n/a", "none", "null", "-", "operator", "undefined"].includes(name);
    });
    if (!points.length) {
        return `<div class="insights-empty">No threat points to plot.</div>`;
    }
    const rawMaxPresence = Math.max(...points.map((p) => toNumber(p.presence_pct, 0)), 1);
    const minDelta = Math.min(...points.map((p) => toNumber(p.win_delta, 0)));
    const maxDelta = Math.max(...points.map((p) => toNumber(p.win_delta, 0)));
    const xMin = 0;
    const xMax = Math.max(1, rawMaxPresence * 1.1);
    const yPadding = 5;
    const yMin = minDelta - yPadding;
    const yMax = maxDelta + yPadding;
    const spanX = Math.max(xMax - xMin, 1);
    const spanY = Math.max(yMax - yMin, 1);
    const zeroYRaw = ((yMax - 0) / spanY) * 100;
    const zeroY = Math.max(0, Math.min(100, zeroYRaw));
    const chartHeightPx = Math.max(420, Math.min(700, Math.round(420 + spanY * 6)));

    const markers = points
        .slice()
        .sort((a, b) => toNumber(b.times_killed_by, 0) - toNumber(a.times_killed_by, 0))
        .slice(0, 35)
        .map((point) => {
        const operator = extractOperatorName(point.operator) || "Unknown";
        const rawX = ((toNumber(point.presence_pct, 0) - xMin) / spanX) * 100;
        const rawY = ((yMax - toNumber(point.win_delta, 0)) / spanY) * 100;
        const x = Math.max(2, Math.min(98, rawX));
        const y = Math.max(2, Math.min(98, rawY));
        const icon = resolveOperatorImageUrl(operator);
        const tooltip = `${operator}: presence ${toNumber(point.presence_pct, 0).toFixed(1)}%, delta ${toNumber(point.win_delta, 0).toFixed(1)}%, killed by ${toNumber(point.times_killed_by, 0)}`;
        const fallback = operatorFallbackBadge(operator);
        const marker = icon
            ? `<img src="${icon}" alt="${escapeHtml(operator)}" loading="lazy" />`
            : fallback;
        return `
            <div class="threat-scatter-point" title="${escapeHtml(tooltip)}" style="left:${x.toFixed(1)}%;top:${y.toFixed(1)}%;">
                ${marker}
            </div>
        `;
    }).join("");

    return `
        <div class="threat-scatter-wrap" style="--threat-chart-height:${chartHeightPx}px;">
            <div class="threat-scatter-y-label">${escapeHtml(analysis?.scatter?.y_label || "Win delta vs baseline (%)")}</div>
            <div class="threat-scatter">
                <div class="threat-scatter-plot">
                    <div class="threat-scatter-axis threat-scatter-axis-x"></div>
                    <div class="threat-scatter-axis threat-scatter-axis-y"></div>
                    <div class="threat-scatter-axis threat-scatter-axis-zero" style="top:${zeroY.toFixed(1)}%;"></div>
                    ${markers}
                </div>
            </div>
            <div class="threat-scatter-x-label">
                ${escapeHtml(analysis?.scatter?.x_label || "Presence (% of rounds)")} 0.0-${xMax.toFixed(1)}%
            </div>
        </div>
    `;
}

function renderEnemyThreatReportCard(analysis, lastUpdated) {
    if (!analysis || analysis.error) {
        return `
            <section class="insights-card">
                <header class="insights-card-head">
                    <div>
                        <div class="insights-card-title">Enemy Operator Threat</div>
                        <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                    </div>
                </header>
                <div class="insights-empty">${escapeHtml(analysis?.error || "No enemy operator threat data available.")}</div>
            </section>
        `;
    }
    const top = analysis.threats?.[0]?.operator || "N/A";
    return `
        <section class="insights-card">
            <header class="insights-card-head">
                <div>
                    <div class="insights-card-title">Enemy Operator Threat</div>
                    <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                </div>
                <div class="insights-stat-strip">
                    <span>Baseline WR <strong>${formatPct(analysis.baseline_win_rate)}</strong></span>
                    <span>Death rounds <strong>${toNumber(analysis.total_death_rounds, 0)}</strong></span>
                </div>
            </header>
            <div class="insights-callout insights-negative">
                <span class="insights-callout-label">Top Killer Operator</span>
                <strong>${escapeHtml(top)}</strong>
            </div>
            ${renderEnemyThreatScatter(analysis)}
            <div class="insights-findings">${renderInsightsFindings(analysis.findings)}</div>
        </section>
    `;
}

function renderOperatorReportCard(analysis, lastUpdated) {
    if (!analysis || analysis.error) {
        return `
            <section class="insights-card">
                <header class="insights-card-head">
                    <div>
                        <div class="insights-card-title">Operator Stats</div>
                        <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                    </div>
                </header>
                <div class="insights-empty">${escapeHtml(analysis?.error || "No operator stats data available.")}</div>
            </section>
        `;
    }
    const best = analysis.best_operator?.operator || "N/A";
    const diversity = toNumber(analysis.diversity_score, 0);
    return `
        <section class="insights-card">
            <header class="insights-card-head">
                <div>
                    <div class="insights-card-title">Operator Stats</div>
                    <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                </div>
                <div class="insights-stat-strip">
                    <span>Rounds analyzed <strong>${toNumber(analysis.total_rounds_analyzed, 0)}</strong></span>
                    <span>Diversity <strong>${diversity}</strong></span>
                </div>
            </header>
            <div class="insights-callout insights-neutral">
                <span class="insights-callout-label">Best Operator</span>
                <strong>${escapeHtml(best)}</strong>
            </div>
            <div class="insights-findings">${renderInsightsFindings(analysis.findings)}</div>
        </section>
    `;
}

function renderMapReportCard(analysis, lastUpdated) {
    if (!analysis || analysis.error) {
        return `
            <section class="insights-card">
                <header class="insights-card-head">
                    <div>
                        <div class="insights-card-title">Map Stats</div>
                        <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                    </div>
                </header>
                <div class="insights-empty">${escapeHtml(analysis?.error || "No map stats data available.")}</div>
            </section>
        `;
    }
    const best = analysis.best_map?.map_name || "N/A";
    const ban = analysis.ban_recommendation?.map_name || "N/A";
    return `
        <section class="insights-card">
            <header class="insights-card-head">
                <div>
                    <div class="insights-card-title">Map Stats</div>
                    <div class="insights-card-updated">Last updated ${escapeHtml(lastUpdated)}</div>
                </div>
                <div class="insights-stat-strip">
                    <span>Matches analyzed <strong>${toNumber(analysis.total_matches_analyzed, 0)}</strong></span>
                    <span>Maps tracked <strong>${toNumber(Array.isArray(analysis.maps) ? analysis.maps.length : 0, 0)}</strong></span>
                </div>
            </header>
            <div class="insights-callout insights-neutral">
                <span class="insights-callout-label">Best Map</span>
                <strong>${escapeHtml(best)}</strong>
            </div>
            <div class="insights-callout insights-negative">
                <span class="insights-callout-label">Ban Recommendation</span>
                <strong>${escapeHtml(ban)}</strong>
            </div>
            <div class="insights-findings">${renderInsightsFindings(analysis.findings)}</div>
        </section>
    `;
}

function renderInsightsCards(roundAnalysis, teammateChemistry, lobbyQuality, tradeAnalysis, teamAnalysis, enemyThreat, operatorStats, mapStats) {
    const el = document.getElementById("insights-results");
    if (!el) return;
    const lastUpdated = new Date().toLocaleString();
    const allFindings = [
        ...(Array.isArray(roundAnalysis?.findings) ? roundAnalysis.findings : []),
        ...(Array.isArray(teammateChemistry?.findings) ? teammateChemistry.findings : []),
        ...(Array.isArray(lobbyQuality?.findings) ? lobbyQuality.findings : []),
        ...(Array.isArray(tradeAnalysis?.findings) ? tradeAnalysis.findings : []),
        ...(Array.isArray(teamAnalysis?.findings) ? teamAnalysis.findings : []),
        ...(Array.isArray(enemyThreat?.findings) ? enemyThreat.findings : []),
        ...(Array.isArray(operatorStats?.findings) ? operatorStats.findings : []),
        ...(Array.isArray(mapStats?.findings) ? mapStats.findings : []),
    ];
    const severityCounts = { critical: 0, warning: 0, info: 0 };
    for (const finding of allFindings) {
        const sev = normalizeFindingSeverity(finding?.severity);
        severityCounts[sev] += 1;
    }
    el.innerHTML = `
        <div class="insights-summary-banner">
            <span><strong>${severityCounts.warning}</strong> warnings</span>
            <span><strong>${severityCounts.critical}</strong> critical</span>
            <span><strong>${severityCounts.info}</strong> info</span>
        </div>
        <div class="insights-grid">
            ${renderRoundReportCard(roundAnalysis, lastUpdated)}
            ${renderChemistryReportCard(teammateChemistry, lastUpdated)}
            ${renderLobbyReportCard(lobbyQuality, lastUpdated)}
            ${renderTradeReportCard(tradeAnalysis, lastUpdated)}
            ${renderTeamReportCard(teamAnalysis, lastUpdated)}
            ${renderEnemyThreatReportCard(enemyThreat, lastUpdated)}
            ${renderOperatorReportCard(operatorStats, lastUpdated)}
            ${renderMapReportCard(mapStats, lastUpdated)}
        </div>
    `;
}

async function runInsights(explicitUsername = "") {
    const username = (explicitUsername || document.getElementById("insights-username")?.value || "").trim();
    if (!username) {
        logInsights("Enter a username before running insights.", "error");
        return;
    }
    try {
        const [roundRes, chemistryRes, lobbyRes, tradeRes, teamRes, enemyThreatRes, operatorRes, mapRes] = await Promise.all([
            api.getRoundAnalysis(username),
            api.getTeammateChemistry(username),
            api.getLobbyQuality(username),
            api.getTradeAnalysis(username, 5),
            api.getTeamAnalysis(username),
            api.getEnemyOperatorThreat(username),
            api.getOperatorStats(username),
            api.getMapStats(username),
        ]);

        let roundAnalysis = null;
        let teammateChemistry = null;
        let lobbyQuality = null;
        let tradeAnalysis = null;
        let teamAnalysis = null;
        let enemyThreat = null;
        let operatorStats = null;
        let mapStats = null;
        if (roundRes.ok) {
            roundAnalysis = (await roundRes.json())?.analysis || null;
        }
        if (chemistryRes.ok) {
            teammateChemistry = (await chemistryRes.json())?.analysis || null;
        }
        if (lobbyRes.ok) {
            lobbyQuality = (await lobbyRes.json())?.analysis || null;
        }
        if (tradeRes.ok) {
            tradeAnalysis = (await tradeRes.json())?.analysis || null;
        }
        if (teamRes.ok) {
            teamAnalysis = (await teamRes.json())?.analysis || null;
        }
        if (enemyThreatRes.ok) {
            enemyThreat = (await enemyThreatRes.json())?.analysis || null;
        }
        if (operatorRes.ok) {
            operatorStats = (await operatorRes.json())?.analysis || null;
        }
        if (mapRes.ok) {
            mapStats = (await mapRes.json())?.analysis || null;
        }

        renderInsightsCards(roundAnalysis, teammateChemistry, lobbyQuality, tradeAnalysis, teamAnalysis, enemyThreat, operatorStats, mapStats);
        logInsights(`Ran insight plugins for ${username}.`, "success");
        if (!roundRes.ok) logInsights(`Round analysis unavailable (HTTP ${roundRes.status}).`, "error");
        if (!chemistryRes.ok) logInsights(`Teammate chemistry unavailable (HTTP ${chemistryRes.status}).`, "error");
        if (!lobbyRes.ok) logInsights(`Lobby quality unavailable (HTTP ${lobbyRes.status}).`, "error");
        if (!tradeRes.ok) logInsights(`Trade analysis unavailable (HTTP ${tradeRes.status}).`, "error");
        if (!teamRes.ok) logInsights(`Team analysis unavailable (HTTP ${teamRes.status}).`, "error");
        if (!enemyThreatRes.ok) logInsights(`Enemy operator threat unavailable (HTTP ${enemyThreatRes.status}).`, "error");
        if (!operatorRes.ok) logInsights(`Operator stats unavailable (HTTP ${operatorRes.status}).`, "error");
        if (!mapRes.ok) logInsights(`Map stats unavailable (HTTP ${mapRes.status}).`, "error");
    } catch (err) {
        logInsights(`Failed to run insights: ${err}`, "error");
    }
}

async function runStatComputation(explicitUsername = "") {
    const username = (explicitUsername || document.getElementById("compute-username")?.value || "").trim();
    if (!username) {
        renderDashboardNeedsUsernamePrompt();
        return;
    }
    if (dashboardComputeInFlight) return;
    dashboardComputeInFlight = true;
    computeReportState.username = username;
    try {
        const [matchesRes, roundRes, chemistryRes, lobbyRes, tradeRes, teamRes, enemyThreatRes, operatorRes, mapRes] = await Promise.all([
            api.getScrapedMatches(username, 2000),
            api.getRoundAnalysis(username),
            api.getTeammateChemistry(username),
            api.getLobbyQuality(username),
            api.getTradeAnalysis(username, 5),
            api.getTeamAnalysis(username),
            api.getEnemyOperatorThreat(username),
            api.getOperatorStats(username),
            api.getMapStats(username),
        ]);
        if (!matchesRes.ok) {
            throw new Error(`HTTP ${matchesRes.status}`);
        }
        const payload = await matchesRes.json();
        let roundAnalysis = null;
        if (roundRes.ok) {
            const roundPayload = await roundRes.json();
            roundAnalysis = roundPayload?.analysis || null;
        }
        let teammateChemistry = null;
        if (chemistryRes.ok) {
            const chemistryPayload = await chemistryRes.json();
            teammateChemistry = chemistryPayload?.analysis || null;
        }
        let lobbyQuality = null;
        if (lobbyRes.ok) {
            lobbyQuality = (await lobbyRes.json())?.analysis || null;
        }
        let tradeAnalysis = null;
        if (tradeRes.ok) {
            tradeAnalysis = (await tradeRes.json())?.analysis || null;
        }
        let teamAnalysis = null;
        if (teamRes.ok) {
            teamAnalysis = (await teamRes.json())?.analysis || null;
        }
        let enemyThreat = null;
        if (enemyThreatRes.ok) {
            enemyThreat = (await enemyThreatRes.json())?.analysis || null;
        }
        let atkDefHeatmap = null;
        try {
            await loadAtkDefHeatmap(username);
            atkDefHeatmap = computeReportState.atkDefHeatmap;
        } catch (err) {
            atkDefHeatmap = { error: String(err) };
        }
        let operatorStats = null;
        if (operatorRes.ok) {
            operatorStats = (await operatorRes.json())?.analysis || null;
        }
        let mapStats = null;
        if (mapRes.ok) {
            mapStats = (await mapRes.json())?.analysis || null;
        }
        const matches = Array.isArray(payload.matches) ? payload.matches : [];
        const filteredMatches = applyDashboardModeFilters(matches);
        const emptyBucket = () => ({
            matches: 0,
            wins: 0,
            losses: 0,
            trackedRows: 0,
            sumKills: 0,
            sumDeaths: 0,
            sumAssists: 0,
            sumKd: 0,
        });
        const buckets = {
            overall: emptyBucket(),
            ranked: emptyBucket(),
            unranked: emptyBucket(),
        };
        const finalize = (b) => {
            const denom = b.trackedRows || 1;
            return {
                matches: b.matches,
                wins: b.wins,
                losses: b.losses,
                winRate: b.matches ? ((b.wins / b.matches) * 100).toFixed(1) : "0.0",
                avgKd: (b.sumKd / denom).toFixed(2),
                avgKills: (b.sumKills / denom).toFixed(1),
                avgDeaths: (b.sumDeaths / denom).toFixed(1),
                avgAssists: (b.sumAssists / denom).toFixed(1),
                trackedRows: b.trackedRows,
            };
        };
        if (!matches.length) {
            const stats = {
                overall: finalize(buckets.overall),
                ranked: finalize(buckets.ranked),
                unranked: finalize(buckets.unranked),
            };
            computeReportState = {
                ...computeReportState,
                stats,
                round: roundAnalysis,
                chemistry: teammateChemistry,
                lobby: lobbyQuality,
                trade: tradeAnalysis,
                team: teamAnalysis,
                enemyThreat,
                atkDefHeatmap,
                operator: operatorStats,
                map: mapStats,
                sorted: { operatorRows: [], mapRows: [] },
                playbookFindings: [],
            };
            renderComputeCards(stats);
            try {
                await loadWorkspacePanel(computeReportState.workspace.panel || "overview", true);
            } catch (err) {
                logCompute(`Workspace load failed: ${err}`, "error");
                renderWorkspacePanelError(computeReportState.workspace.panel || "overview", err);
            }
            logCompute(`No stored matches found for ${username}.`, "info");
            return;
        }
        if (!filteredMatches.length) {
            const stats = {
                overall: finalize(buckets.overall),
                ranked: finalize(buckets.ranked),
                unranked: finalize(buckets.unranked),
            };
            computeReportState = {
                ...computeReportState,
                stats,
                round: roundAnalysis,
                chemistry: teammateChemistry,
                lobby: lobbyQuality,
                trade: tradeAnalysis,
                team: teamAnalysis,
                enemyThreat,
                atkDefHeatmap,
                operator: operatorStats,
                map: mapStats,
                sorted: { operatorRows: [], mapRows: [] },
                playbookFindings: [],
            };
            renderComputeCards(stats);
            try {
                await loadWorkspacePanel(computeReportState.workspace.panel || "overview", true);
            } catch (err) {
                logCompute(`Workspace load failed: ${err}`, "error");
                renderWorkspacePanelError(computeReportState.workspace.panel || "overview", err);
            }
            logCompute("Dashboard filters excluded all matches for this username.", "info");
            return;
        }
        const sortedMapRows = computeSortedMapsFromMatches(filteredMatches, username);

        const normalized = username.toLowerCase();
        for (const match of filteredMatches) {
            const category = classifyStoredMode(match);
            const group =
                category === "ranked" ? buckets.ranked :
                category === "unranked" ? buckets.unranked :
                null;

            const perspective = inferTeamPerspective(match, username);
            buckets.overall.matches += 1;
            if (group) group.matches += 1;
            if (perspective.result === "Win") {
                buckets.overall.wins += 1;
                if (group) group.wins += 1;
            }
            if (perspective.result === "Loss") {
                buckets.overall.losses += 1;
                if (group) group.losses += 1;
            }

            const players = Array.isArray(match?.players) ? match.players : [];
            const player = players.find(
                (p) => String(p?.username || p?.name || "").trim().toLowerCase() === normalized
            );
            if (!player) continue;

            const kills = toNumber(player?.kills, 0);
            const deaths = toNumber(player?.deaths, 0);
            const assists = toNumber(player?.assists, 0);
            const kd = toNumber(player?.kd, 0);
            buckets.overall.trackedRows += 1;
            buckets.overall.sumKills += kills;
            buckets.overall.sumDeaths += deaths;
            buckets.overall.sumAssists += assists;
            buckets.overall.sumKd += kd;
            if (group) {
                group.trackedRows += 1;
                group.sumKills += kills;
                group.sumDeaths += deaths;
                group.sumAssists += assists;
                group.sumKd += kd;
            }
        }

        const stats = {
            overall: finalize(buckets.overall),
            ranked: finalize(buckets.ranked),
            unranked: finalize(buckets.unranked),
        };
        computeReportState = {
            ...computeReportState,
            stats,
            round: roundAnalysis,
            chemistry: teammateChemistry,
            lobby: lobbyQuality,
            trade: tradeAnalysis,
            team: teamAnalysis,
            enemyThreat,
            atkDefHeatmap,
            operator: operatorStats,
            map: mapStats,
            sorted: {
                operatorRows: Array.isArray(computeReportState.sorted?.operatorRows) ? computeReportState.sorted.operatorRows : [],
                mapRows: sortedMapRows,
            },
            playbookFindings: buildFilteredPlaybookFindings(filteredMatches, stats),
        };
        renderComputeCards(stats);
        try {
            await loadWorkspacePanel(computeReportState.workspace.panel || "overview", true);
        } catch (err) {
            logCompute(`Workspace load failed: ${err}`, "error");
            renderWorkspacePanelError(computeReportState.workspace.panel || "overview", err);
        }
        logCompute(
            `Computed stats for ${username} (after dashboard type filters): overall ${buckets.overall.matches}, ` +
            `ranked ${buckets.ranked.matches}, unranked ${buckets.unranked.matches}. ` +
            `Sorted map dataset ready (${sortedMapRows.length} maps).`,
            "success"
        );
        if (!roundRes.ok) {
            logCompute(`Round analysis unavailable (HTTP ${roundRes.status}).`, "error");
        }
        if (!chemistryRes.ok) {
            logCompute(`Teammate chemistry unavailable (HTTP ${chemistryRes.status}).`, "error");
        }
        if (!lobbyRes.ok) {
            logCompute(`Lobby quality unavailable (HTTP ${lobbyRes.status}).`, "error");
        }
        if (!tradeRes.ok) {
            logCompute(`Trade analysis unavailable (HTTP ${tradeRes.status}).`, "error");
        }
        if (!teamRes.ok) {
            logCompute(`Team analysis unavailable (HTTP ${teamRes.status}).`, "error");
        }
        if (!enemyThreatRes.ok) {
            logCompute(`Enemy operator threat unavailable (HTTP ${enemyThreatRes.status}).`, "error");
        }
        if (!operatorRes.ok) {
            logCompute(`Operator stats unavailable (HTTP ${operatorRes.status}).`, "error");
        }
        if (!mapRes.ok) {
            logCompute(`Map stats unavailable (HTTP ${mapRes.status}).`, "error");
        }
    } catch (err) {
        dashboardLastRequestSignature = "";
        logCompute(`Failed stat computation: ${err}`, "error");
    } finally {
        dashboardComputeInFlight = false;
        if (dashboardRefreshQueued) {
            dashboardRefreshQueued = false;
            setTimeout(() => triggerDashboardAutoRefresh({ force: true }), 0);
        }
    }
}

document.getElementById("start-scan")?.addEventListener("click", startScan);
document.getElementById("stop-scan")?.addEventListener("click", stopScan);
document.getElementById("start-match-scrape")?.addEventListener("click", startMatchScrape);
document.getElementById("stop-match-scrape")?.addEventListener("click", stopMatchScrape);
document.getElementById("load-stored-matches")?.addEventListener("click", () => loadStoredMatchesView("", false));
document.getElementById("unpack-stored-matches")?.addEventListener("click", () => unpackStoredMatches(""));
document.getElementById("delete-bad-stored-matches")?.addEventListener("click", () => deleteBadStoredMatches(""));
document.getElementById("stored-detail-close")?.addEventListener("click", closeStoredDetail);
document.getElementById("stored-show-ranked")?.addEventListener("change", () => {
    renderStoredMatches(storedMatchesSource, currentStoredUsername);
});
document.getElementById("stored-show-unranked")?.addEventListener("change", () => {
    renderStoredMatches(storedMatchesSource, currentStoredUsername);
});
document.getElementById("tab-scanner")?.addEventListener("click", () => setActiveTab("scanner"));
document.getElementById("tab-matches")?.addEventListener("click", () => setActiveTab("matches"));
document.getElementById("tab-stored")?.addEventListener("click", () => setActiveTab("stored"));
document.getElementById("tab-players")?.addEventListener("click", () => setActiveTab("players"));
document.getElementById("tab-team-builder")?.addEventListener("click", () => setActiveTab("team-builder"));
document.getElementById("tab-operators")?.addEventListener("click", () => setActiveTab("operators"));
document.getElementById("tab-workspace")?.addEventListener("click", () => setActiveTab("workspace"));
document.getElementById("tab-dashboard")?.addEventListener("click", () => setActiveTab("dashboard"));
document.getElementById("dashboard-tab-insights")?.addEventListener("click", () => setDashboardView("insights"));
document.getElementById("dashboard-tab-graphs")?.addEventListener("click", () => setDashboardView("graphs"));
["graph-tab-workspace", "graph-tab-threat"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("click", () => {
        setGraphPanel(el.dataset.graphPanel || "workspace");
    });
});
document.getElementById("open-settings")?.addEventListener("click", openSettingsModal);
document.getElementById("close-settings")?.addEventListener("click", closeSettingsModal);
document.getElementById("run-db-standardizer")?.addEventListener("click", runDbStandardizerFromSettings);
document.getElementById("run-stat-compute")?.addEventListener("click", () => triggerDashboardAutoRefresh({ force: true }));
const computeUsernameInput = document.getElementById("compute-username");
computeUsernameInput?.addEventListener("input", renderDashboardActiveFilterChips);
computeUsernameInput?.addEventListener("blur", () => triggerDashboardAutoRefresh());
computeUsernameInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
        event.preventDefault();
        triggerDashboardAutoRefresh({ force: true });
    }
});
document.querySelectorAll(".dashboard-include-type-filter, .dashboard-exclude-type-filter").forEach((el) => {
    el.addEventListener("change", () => {
        renderDashboardActiveFilterChips();
        triggerDashboardAutoRefresh();
    });
});
const heatmapRefresh = document.getElementById("heatmap-refresh");
if (heatmapRefresh) {
    heatmapRefresh.addEventListener("click", async () => {
        const username = computeReportState.username || (document.getElementById("compute-username")?.value || "").trim();
        if (!username) return;
        try {
            await loadAtkDefHeatmap(username);
            scheduleDashboardGraphRender(30);
        } catch (err) {
            computeReportState.atkDefHeatmap = { error: String(err) };
            scheduleDashboardGraphRender(30);
            logCompute(`Heatmap refresh failed: ${err}`, "error");
        }
    });
}
["heatmap-mode", "heatmap-days", "heatmap-normalization", "heatmap-lift-mode", "heatmap-map", "heatmap-stack-only", "heatmap-min-n", "heatmap-debug"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("change", async () => {
        const username = computeReportState.username || (document.getElementById("compute-username")?.value || "").trim();
        if (!username) return;
        try {
            await loadAtkDefHeatmap(username);
        } catch (err) {
            computeReportState.atkDefHeatmap = { error: String(err) };
        }
        scheduleDashboardGraphRender(50);
    });
});
["heatmap-top-n", "heatmap-sort-rows", "heatmap-sort-cols", "heatmap-cluster-cols"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("change", () => {
        scheduleDashboardGraphRender(40);
    });
});
["map-perf-sort", "map-perf-min-sample"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("change", () => {
        scheduleDashboardGraphRender(40);
    });
});
const heatmapSearch = document.getElementById("heatmap-search-op");
if (heatmapSearch) {
    heatmapSearch.addEventListener("input", () => {
        scheduleDashboardGraphRender(120);
    });
}
["ws-tab-overview", "ws-tab-operators", "ws-tab-matchups", "ws-tab-team"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("click", async () => {
        const panel = el.dataset.wsPanel || "overview";
        setWorkspacePanel(panel);
        try {
            await loadWorkspacePanel(panel, false);
        } catch (err) {
            logCompute(`Workspace load failed: ${err}`, "error");
            renderWorkspacePanelError(panel, err);
        }
    });
});
const wsRefresh = document.getElementById("ws-refresh");
if (wsRefresh) {
    wsRefresh.addEventListener("click", async () => {
        try {
            await loadWorkspacePanel(computeReportState.workspace.panel || "overview", true);
        } catch (err) {
            logCompute(`Workspace refresh failed: ${err}`, "error");
            renderWorkspacePanelError(computeReportState.workspace.panel || "overview", err);
        }
    });
}
["ws-queue", "ws-playlist", "ws-days", "ws-map-name", "ws-search", "ws-stack-only", "ws-normalization", "ws-lift-mode", "ws-interval-method", "ws-min-n", "ws-weighting", "ws-debug", "ws-clamp-mode", "ws-clamp-abs", "ws-labels"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    const evt = el.tagName === "INPUT" && el.type === "text" ? "input" : "change";
    el.addEventListener(evt, () => {
        persistWorkspaceState();
        if (id === "ws-labels") {
            const panel = computeReportState.workspace.panel || "overview";
            const payload = computeReportState.workspace.dataByPanel[panel];
            if (payload) renderWorkspacePanel(panel, payload);
            return;
        }
        computeReportState.workspace.dataByPanel = {};
        scheduleWorkspaceAutoRefresh(id === "ws-map-name" || id === "ws-search" ? 350 : 150);
    });
});
const wsEvidenceMore = document.getElementById("workspace-evidence-more");
if (wsEvidenceMore) {
    wsEvidenceMore.addEventListener("click", async () => {
        try {
            await loadWorkspaceEvidence(false);
        } catch (err) {
            logCompute(`Workspace evidence paging failed: ${err}`, "error");
        }
    });
}
document.getElementById("matches-run-forever")?.addEventListener("change", syncContinuousControls);
document.getElementById("matches-newest-only")?.addEventListener("change", syncScrapeModeControls);
document.getElementById("matches-full-backfill")?.addEventListener("change", syncScrapeModeControls);
document.querySelectorAll(".compute-mode-toggle .compute-chip").forEach((chip) => {
    chip.addEventListener("click", () => {
        computeReportState.mode = chip.dataset.mode || "overall";
        renderComputeReport();
    });
});

document.getElementById("players-refresh")?.addEventListener("click", () => loadPlayersTab(false));
document.getElementById("players-search")?.addEventListener("input", () => renderPlayersTable(encounteredPlayersCache));
document.getElementById("players-primary-username")?.addEventListener("blur", () => {
    const val = String(document.getElementById("players-primary-username")?.value || "").trim();
    if (val) {
        const computeInput = document.getElementById("compute-username");
        if (computeInput && !String(computeInput.value || "").trim()) computeInput.value = val;
    }
    loadPlayersTab(false);
});
document.getElementById("team-builder-analyze")?.addEventListener("click", analyzeSelectedStack);
document.getElementById("operators-refresh")?.addEventListener("click", () => loadOperatorsTab(false));
["operators-player", "operators-stack", "operators-match-type", "operators-min-rounds"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("change", () => loadOperatorsTab(true));
});
document.getElementById("operators-topk")?.addEventListener("change", () => {
    if (operatorsLastPayload) {
        renderOperatorsMapCards(operatorsLastPayload);
        return;
    }
    loadOperatorsTab(true);
});
document.getElementById("operators-advanced")?.addEventListener("change", () => {
    if (operatorsLastPayload) {
        renderOperatorsMapCards(operatorsLastPayload);
    }
});
document.getElementById("operators-include-low-sample")?.addEventListener("change", () => {
    if (operatorsLastPayload) {
        renderOperatorsMapCards(operatorsLastPayload);
    }
});
document.getElementById("operators-detail-side")?.addEventListener("change", () => renderOperatorsMapDetail(operatorsSelectedMap));
document.getElementById("operators-detail-limit")?.addEventListener("change", () => renderOperatorsMapDetail(operatorsSelectedMap));
document.getElementById("operators-detail-close")?.addEventListener("click", () => {
    operatorsSelectedMap = "";
    document.getElementById("operators-map-detail")?.classList.add("hidden");
    if (operatorsLastPayload) renderOperatorsMapCards(operatorsLastPayload);
});
document.getElementById("operators-map-grid")?.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) return;
    if (target.id === "operators-lower-threshold") {
        const minEl = document.getElementById("operators-min-rounds");
        if (minEl) minEl.value = "3";
        loadOperatorsTab(true);
        return;
    }
    const card = target.closest(".operators-map-card");
    if (!card) return;
    const mapName = String(card.getAttribute("data-operators-map") || "").trim();
    if (!mapName) return;
    operatorsSelectedMap = mapName;
    if (operatorsLastPayload) renderOperatorsMapCards(operatorsLastPayload);
});

window.addEventListener("error", (event) => {
    log(`JS error: ${event.message}`, "error");
});

window.addEventListener("unhandledrejection", (event) => {
    log(`Promise error: ${event.reason}`, "error");
});

loadOperatorImageIndex();
mountWorkspaceTopLevel();
restoreWorkspaceState();
setWorkspacePanel(computeReportState.workspace.panel || "overview");
const initialPrimaryTab = restorePrimaryTabFromUrl();
setActiveTab(initialPrimaryTab);
initDashboardSectionToggles();
applyDashboardSectionVisibility();
setDashboardView("insights");
setGraphPanel(computeReportState.graphPanel || "threat");
syncScrapeModeControls();
initPrimaryTabKeyboardNav();
initStatusChipMirrors();
initFilterDrawersAndChips();
initTooltips(document);
renderDashboardActiveFilterChips();
settingsModalController.bindDismissHandlers();
syncFilterDrawerDefaults();
window.addEventListener("resize", syncFilterDrawerDefaults);
initNetwork();
log("Ready to scan. Enter a username and click Start Scan.");
logMatch("Ready to scrape matches. Enter a username and click Scrape Matches.");
