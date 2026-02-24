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
let visLib = null;
let layoutTick = null;
let currentMatchUsername = "";
let currentStoredUsername = "";
let storedMatchesSource = [];
let storedMatchesCache = [];
let selectedStoredMatchIndex = -1;
let selectedStoredRoundIndex = -1;
const MAP_IMAGE_BASE = "/map-images";
const OPERATOR_IMAGE_BASE = "/operator-images";
let operatorImageFileByKey = {};
let operatorImageIndexLoaded = false;
let operatorImageIndexEnabled = false;
let operatorImageIndexCount = 0;
let computeReportState = {
    mode: "overall",
    stats: null,
    round: null,
    chemistry: null,
    lobby: null,
    trade: null,
    selectedEvidenceKey: "",
};
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
        const res = await fetch("/api/operator-image-index");
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

    ws = new WebSocket("ws://localhost:5000/ws/scan");

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

function setMatchProgress(processed, total) {
    const bar = document.getElementById("match-progress-bar");
    const text = document.getElementById("match-progress-text");
    const wrap = document.getElementById("match-progress-wrap");
    if (!bar || !text) return;
    const safeProcessed = Math.max(0, toNumber(processed, 0));
    const rawTotal = toNumber(total, 0);
    const isOpenEnded = continuousScrapeEnabled || rawTotal <= 0;
    if (isOpenEnded) {
        wrap?.classList.add("indeterminate");
        bar.style.width = "35%";
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
    } else {
        document.getElementById("match-status").textContent = "Restarting";
    }
    matchProgressTarget = (continuousScrapeEnabled || openEndedNewest || openEndedBackfill) ? 0 : Math.max(1, toNumber(maxMatches, 1));
    setMatchProgress(0, matchProgressTarget);
    document.getElementById("start-match-scrape").disabled = true;
    document.getElementById("stop-match-scrape").disabled = false;
    matchScraping = true;
    setGlobalScrapeRunning(true);
    currentMatchUsername = username;

    wsMatches = new WebSocket("ws://localhost:5000/ws/scrape-matches");
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
            setMatchProgress(
                completedScanned,
                openEndedDone ? 0 : (matchProgressTarget || toNumber(data.total_matches, 1))
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

function log(message, type = "info") {
    const logDiv = document.getElementById("log");
    const entry = document.createElement("div");
    entry.className = `log-entry log-${type}`;
    entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    logDiv.appendChild(entry);
    logDiv.scrollTop = logDiv.scrollHeight;
}

function logMatch(message, type = "info") {
    const logDiv = document.getElementById("match-log");
    const entry = document.createElement("div");
    entry.className = `log-entry log-${type}`;
    entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    logDiv.appendChild(entry);
    logDiv.scrollTop = logDiv.scrollHeight;
}

function appendMatchResult(matchData, status = "captured") {
    appendMatchResultRow(matchData, status);
}

function appendMatchResultRow(matchData, status = "captured") {
    const resultsDiv = document.getElementById("match-results");
    const matchId = String(matchData?.match_id || "unknown-id");
    const existing = resultsDiv.querySelector(`[data-match-id="${matchId}"]`);
    const statusClass =
        status === "filtered" ? "match-results-filtered" :
        status === "skipped_complete" ? "match-results-skipped" :
        "match-results-captured";
    const statusLabel =
        status === "filtered" ? "filtered" :
        status === "skipped_complete" ? "skipped" :
        "captured";
    if (existing) {
        existing.classList.remove("match-results-captured", "match-results-skipped", "match-results-filtered");
        existing.classList.add(statusClass);
        const meta = existing.querySelector(".stored-match-meta");
        if (meta) {
            const shortExistingId =
                matchId.length > 14
                    ? `${matchId.slice(0, 8)}...${matchId.slice(-4)}`
                    : matchId;
            meta.textContent = `${matchData?.date || "No date"} | ${shortExistingId} | ${statusLabel}`;
        }
        return;
    }
    const entry = document.createElement("div");
    entry.className = `stored-match-card match-results-card ${statusClass}`;
    entry.dataset.matchId = matchId;
    const shortMatchId =
        matchId.length > 14
            ? `${matchId.slice(0, 8)}...${matchId.slice(-4)}`
            : matchId;
    const map = matchData?.map || "Unknown map";
    const perspective = inferTeamPerspective(matchData, currentMatchUsername);
    const scoreA = perspective.myScore;
    const scoreB = perspective.oppScore;
    const result = perspective.result;
    const resultClass =
        result === "Win" ? "stored-result-win" :
        result === "Loss" ? "stored-result-loss" :
        "stored-result-unknown";
    const bgUrl = resolveMapImageUrl(map);
    if (bgUrl) {
        entry.style.backgroundImage = `linear-gradient(rgba(8,8,8,0.72), rgba(8,8,8,0.82)), url('${bgUrl}')`;
    }
    entry.innerHTML = `
        <div class="stored-match-top">
            <div>
                <div class="stored-field-label">Map</div>
                <div class="stored-field-value">${map}</div>
            </div>
            <div>
                <div class="stored-field-label">Score</div>
                <div class="stored-field-value">${scoreA}:${scoreB}</div>
            </div>
            <div>
                <div class="stored-field-label">Result</div>
                <div class="stored-field-value ${resultClass}">${result}</div>
            </div>
            <div>
                <div class="stored-field-label">Mode</div>
                <div class="stored-field-value">${matchData?.mode || "Unknown"}</div>
            </div>
        </div>
        <div class="stored-match-meta">
            ${matchData?.date || "No date"} | ${shortMatchId} | ${statusLabel}
        </div>
    `;
    resultsDiv.appendChild(entry);
    resultsDiv.scrollTop = resultsDiv.scrollHeight;
}

function setActiveTab(tabName) {
    const isScanner = tabName === "scanner";
    const isMatches = tabName === "matches";
    const isStored = tabName === "stored";
    const isDashboard = tabName === "dashboard";
    document.getElementById("tab-scanner").classList.toggle("active", isScanner);
    document.getElementById("tab-matches").classList.toggle("active", isMatches);
    document.getElementById("tab-stored").classList.toggle("active", isStored);
    document.getElementById("tab-dashboard").classList.toggle("active", isDashboard);
    document.getElementById("panel-scanner").classList.toggle("active", isScanner);
    document.getElementById("panel-matches").classList.toggle("active", isMatches);
    document.getElementById("panel-stored").classList.toggle("active", isStored);
    document.getElementById("panel-dashboard").classList.toggle("active", isDashboard);
    if (isScanner && network) {
        setTimeout(() => network.redraw(), 10);
    }
    if (isStored) {
        loadStoredMatchesView("", true);
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

function renderStoredMatches(matches, username) {
    const list = document.getElementById("stored-match-list");
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
        empty.className = "log-entry log-info";
        empty.textContent = "No stored matches found for this username/filter.";
        list.appendChild(empty);
    }

    for (let idx = 0; idx < visibleMatches.length; idx++) {
        const match = visibleMatches[idx];
        const map = match?.map || "Unknown map";
        const perspective = inferTeamPerspective(match, username);
        const scoreA = perspective.myScore;
        const scoreB = perspective.oppScore;
        const fullMatchId = String(match?.match_id || "No match ID");
        const shortMatchId =
            fullMatchId.length > 14
                ? `${fullMatchId.slice(0, 8)}...${fullMatchId.slice(-4)}`
                : fullMatchId;
        const result = perspective.result;
        const missingRoundData = match?.round_data_missing === true;
        if (missingRoundData) missingRoundDataCount += 1;

        if (result === "Win") wins += 1;
        if (result === "Loss") losses += 1;

        const resultClass =
            result === "Win" ? "stored-result-win" :
            result === "Loss" ? "stored-result-loss" :
            "stored-result-unknown";

        const card = document.createElement("div");
        card.className = "stored-match-card";
        if (missingRoundData) {
            card.classList.add("stored-match-missing-rounds");
        }
        card.dataset.matchIndex = String(idx);
        const bgUrl = resolveMapImageUrl(map);
        if (bgUrl) {
            card.style.backgroundImage = `linear-gradient(rgba(8,8,8,0.72), rgba(8,8,8,0.82)), url('${bgUrl}')`;
        }
        card.innerHTML = `
            <div class="stored-match-top">
                <div>
                    <div class="stored-field-label">Map</div>
                    <div class="stored-field-value">${map}</div>
                </div>
                <div>
                    <div class="stored-field-label">Score</div>
                    <div class="stored-field-value">${scoreA}:${scoreB}</div>
                </div>
                <div>
                    <div class="stored-field-label">Result</div>
                    <div class="stored-field-value ${resultClass}">${result}</div>
                </div>
                <div>
                    <div class="stored-field-label">Mode</div>
                    <div class="stored-field-value">${match?.mode || "Unknown"}</div>
                </div>
            </div>
            <div class="stored-match-meta">
                ${match?.date || "No date"} | ${shortMatchId}
                ${missingRoundData ? '<span class="stored-warning-badge">Round Data Missing</span>' : ""}
            </div>
        `;
        card.addEventListener("click", () => selectStoredMatch(idx, username));
        list.appendChild(card);
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
        const resp = await fetch(`/api/unpack-scraped-matches/${encodeURIComponent(username)}?limit=5000`, {
            method: "POST",
        });
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
        const resp = await fetch(`/api/delete-bad-scraped-matches/${encodeURIComponent(username)}`, {
            method: "POST",
        });
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
    try {
        const res = await fetch(`/api/scraped-matches/${encodeURIComponent(username)}?limit=100`);
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
    const logEl = document.getElementById("compute-log");
    if (!logEl) return;
    const entry = document.createElement("div");
    entry.className = `log-entry log-${level}`;
    entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    logEl.appendChild(entry);
    logEl.scrollTop = logEl.scrollHeight;
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

function flattenSortedFindings() {
    const sources = [
        { label: "Round Analysis", data: computeReportState.round },
        { label: "Teammate Chemistry", data: computeReportState.chemistry },
        { label: "Lobby Quality", data: computeReportState.lobby },
        { label: "Trade Analysis", data: computeReportState.trade },
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
    if (!findings.length) {
        container.innerHTML = `<div class="compute-value">No findings generated for this player.</div>`;
        return;
    }
    container.innerHTML = findings
        .map((f, idx) => {
            const sev = normalizeFindingSeverity(f?.severity);
            const parts = splitFindingMessage(f?.message || "");
            const sevLabel = sev === "critical" ? "Critical" : sev === "warning" ? "Warning" : "Info";
            const citations = Array.isArray(f?.citations) ? f.citations : [];
            const evidenceText = citations.length ? citations.join(" | ") : "No citation provided.";
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
                        data-evidence-key="playbook-${idx}"
                        data-evidence-title="${escapeHtml(parts.headline)}"
                        data-evidence-source="${escapeHtml(f.source || "Insight")}"
                        data-evidence-text="${escapeHtml(evidenceText)}"
                    >
                        Evidence
                    </button>
                </article>
            `;
        })
        .join("");

    container.querySelectorAll(".playbook-evidence").forEach((btn) => {
        btn.addEventListener("click", () => {
            const title = btn.dataset.evidenceTitle || "Finding";
            const source = btn.dataset.evidenceSource || "Insight";
            const text = btn.dataset.evidenceText || "No citation provided.";
            const key = btn.dataset.evidenceKey || "";
            renderEvidencePanel(
                key,
                `<div class="dashboard-evidence-title">${escapeHtml(title)}</div>` +
                `<div class="dashboard-evidence-meta">${escapeHtml(source)}</div>` +
                `<div class="dashboard-evidence-body">${escapeHtml(text)}</div>`
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

    renderPlaybook();
    renderDeepStats(round);
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
    return list
        .map((finding) => {
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
        })
        .join("");
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

function renderInsightsCards(roundAnalysis, teammateChemistry, lobbyQuality, tradeAnalysis) {
    const el = document.getElementById("insights-results");
    if (!el) return;
    const lastUpdated = new Date().toLocaleString();
    const allFindings = [
        ...(Array.isArray(roundAnalysis?.findings) ? roundAnalysis.findings : []),
        ...(Array.isArray(teammateChemistry?.findings) ? teammateChemistry.findings : []),
        ...(Array.isArray(lobbyQuality?.findings) ? lobbyQuality.findings : []),
        ...(Array.isArray(tradeAnalysis?.findings) ? tradeAnalysis.findings : []),
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
        const [roundRes, chemistryRes, lobbyRes, tradeRes] = await Promise.all([
            fetch(`/api/round-analysis/${encodeURIComponent(username)}`),
            fetch(`/api/teammate-chemistry/${encodeURIComponent(username)}`),
            fetch(`/api/lobby-quality/${encodeURIComponent(username)}`),
            fetch(`/api/trade-analysis/${encodeURIComponent(username)}?window_seconds=5`),
        ]);

        let roundAnalysis = null;
        let teammateChemistry = null;
        let lobbyQuality = null;
        let tradeAnalysis = null;
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

        renderInsightsCards(roundAnalysis, teammateChemistry, lobbyQuality, tradeAnalysis);
        logInsights(`Ran insight plugins for ${username}.`, "success");
        if (!roundRes.ok) logInsights(`Round analysis unavailable (HTTP ${roundRes.status}).`, "error");
        if (!chemistryRes.ok) logInsights(`Teammate chemistry unavailable (HTTP ${chemistryRes.status}).`, "error");
        if (!lobbyRes.ok) logInsights(`Lobby quality unavailable (HTTP ${lobbyRes.status}).`, "error");
        if (!tradeRes.ok) logInsights(`Trade analysis unavailable (HTTP ${tradeRes.status}).`, "error");
    } catch (err) {
        logInsights(`Failed to run insights: ${err}`, "error");
    }
}

async function runStatComputation(explicitUsername = "") {
    const username = (explicitUsername || document.getElementById("compute-username")?.value || "").trim();
    if (!username) {
        logCompute("Enter a username before computing stats.", "error");
        return;
    }
    try {
        const [matchesRes, roundRes, chemistryRes, lobbyRes, tradeRes] = await Promise.all([
            fetch(`/api/scraped-matches/${encodeURIComponent(username)}?limit=100`),
            fetch(`/api/round-analysis/${encodeURIComponent(username)}`),
            fetch(`/api/teammate-chemistry/${encodeURIComponent(username)}`),
            fetch(`/api/lobby-quality/${encodeURIComponent(username)}`),
            fetch(`/api/trade-analysis/${encodeURIComponent(username)}?window_seconds=5`),
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
        const matches = Array.isArray(payload.matches) ? payload.matches : [];
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
            };
            renderComputeCards(stats);
            logCompute(`No stored matches found for ${username}.`, "info");
            return;
        }

        const normalized = username.toLowerCase();
        for (const match of matches) {
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
        };
        renderComputeCards(stats);
        logCompute(
            `Computed stats for ${username}: overall ${buckets.overall.matches}, ` +
            `ranked ${buckets.ranked.matches}, unranked ${buckets.unranked.matches}.`,
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
    } catch (err) {
        logCompute(`Failed stat computation: ${err}`, "error");
    }
}

document.getElementById("start-scan").addEventListener("click", startScan);
document.getElementById("stop-scan").addEventListener("click", stopScan);
document.getElementById("start-match-scrape").addEventListener("click", startMatchScrape);
document.getElementById("stop-match-scrape").addEventListener("click", stopMatchScrape);
document.getElementById("load-stored-matches").addEventListener("click", () => loadStoredMatchesView("", false));
document.getElementById("unpack-stored-matches").addEventListener("click", () => unpackStoredMatches(""));
document.getElementById("delete-bad-stored-matches").addEventListener("click", () => deleteBadStoredMatches(""));
document.getElementById("stored-detail-close").addEventListener("click", closeStoredDetail);
document.getElementById("stored-show-ranked").addEventListener("change", () => {
    renderStoredMatches(storedMatchesSource, currentStoredUsername);
});
document.getElementById("stored-show-unranked").addEventListener("change", () => {
    renderStoredMatches(storedMatchesSource, currentStoredUsername);
});
document.getElementById("tab-scanner").addEventListener("click", () => setActiveTab("scanner"));
document.getElementById("tab-matches").addEventListener("click", () => setActiveTab("matches"));
document.getElementById("tab-stored").addEventListener("click", () => setActiveTab("stored"));
document.getElementById("tab-dashboard").addEventListener("click", () => setActiveTab("dashboard"));
document.getElementById("run-stat-compute").addEventListener("click", () => runStatComputation(""));
document.getElementById("matches-run-forever").addEventListener("change", syncContinuousControls);
document.getElementById("matches-newest-only").addEventListener("change", syncScrapeModeControls);
document.getElementById("matches-full-backfill").addEventListener("change", syncScrapeModeControls);
document.querySelectorAll(".compute-mode-toggle .compute-chip").forEach((chip) => {
    chip.addEventListener("click", () => {
        computeReportState.mode = chip.dataset.mode || "overall";
        renderComputeReport();
    });
});

window.addEventListener("error", (event) => {
    log(`JS error: ${event.message}`, "error");
});

window.addEventListener("unhandledrejection", (event) => {
    log(`Promise error: ${event.reason}`, "error");
});

loadOperatorImageIndex();
syncScrapeModeControls();
initNetwork();
log("Ready to scan. Enter a username and click Start Scan.");
logMatch("Ready to scrape matches. Enter a username and click Scrape Matches.");
