let ws = null;
let wsMatches = null;
let network = null;
let nodes = null;
let edges = null;
let scanning = false;
let matchScraping = false;
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

function startMatchScrape() {
    const username = document.getElementById("matches-username").value.trim();
    const maxMatches = parseInt(document.getElementById("max-matches").value, 10);
    const debugBrowser = document.getElementById("matches-debug-browser").checked;
    if (!username) {
        alert("Please enter a username");
        return;
    }

    document.getElementById("match-log").innerHTML = "";
    document.getElementById("match-results").innerHTML = "";
    document.getElementById("match-count").textContent = "0";
    document.getElementById("current-match").textContent = "-";
    document.getElementById("match-status").textContent = "Starting";
    document.getElementById("start-match-scrape").disabled = true;
    document.getElementById("stop-match-scrape").disabled = false;
    matchScraping = true;
    currentMatchUsername = username;

    wsMatches = new WebSocket("ws://localhost:5000/ws/scrape-matches");
    wsMatches.onopen = () => {
        logMatch("Connected to match scraper");
        if (debugBrowser) {
            logMatch("Debug browser mode enabled (headful Playwright window).");
        }
        wsMatches.send(JSON.stringify({ username, max_matches: maxMatches, debug_browser: debugBrowser }));
    };

    wsMatches.onmessage = (event) => {
        const data = JSON.parse(event.data);
        handleMatchMessage(data);
    };

    wsMatches.onclose = () => {
        logMatch("Match scraper disconnected");
        matchScraping = false;
        document.getElementById("start-match-scrape").disabled = false;
        document.getElementById("stop-match-scrape").disabled = true;
        if (document.getElementById("match-status").textContent !== "Complete") {
            document.getElementById("match-status").textContent = "Idle";
        }
    };

    wsMatches.onerror = (error) => {
        logMatch(`Error: ${error}`, "error");
    };
}

function stopMatchScrape() {
    if (wsMatches) {
        wsMatches.close();
    }
    matchScraping = false;
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
            document.getElementById("current-match").textContent = `${data.match_number}/${data.total}`;
            document.getElementById("match-status").textContent = "Running";
            logMatch(`Scraping match ${data.match_number} of ${data.total}...`);
            break;
        case "match_scraped":
            appendMatchResult(data.match_data);
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
        case "match_scraping_complete":
            document.getElementById("match-status").textContent = "Complete";
            logMatch(`Match scraping complete (${data.total_matches} matches)`, "success");
            break;
        case "matches_saved":
            logMatch(`Saved ${toNumber(data.saved_matches, 0)} matches for ${data.username}`, "success");
            loadSavedMatches(data.username, true);
            break;
        case "warning":
            logMatch(`Warning: ${data.message}`, "info");
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

function appendMatchResult(matchData) {
    appendMatchResultRow(matchData, "live");
}

function appendMatchResultRow(matchData, source = "live") {
    const resultsDiv = document.getElementById("match-results");
    const entry = document.createElement("div");
    entry.className = "stored-match-card";
    const matchId = String(matchData?.match_id || "unknown-id");
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
    const sourceLabel = source === "saved" ? "saved" : "live";
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
            ${matchData?.date || "No date"} | ${shortMatchId} | ${sourceLabel}
        </div>
    `;
    resultsDiv.appendChild(entry);
    resultsDiv.scrollTop = resultsDiv.scrollHeight;
}

async function loadSavedMatches(explicitUsername = "", replaceResults = true) {
    const username = (explicitUsername || document.getElementById("matches-username").value || "").trim();
    if (!username) {
        logMatch("Enter a username before loading saved matches.", "error");
        return;
    }

    try {
        const res = await fetch(`/api/scraped-matches/${encodeURIComponent(username)}?limit=50`);
        if (!res.ok) {
            throw new Error(`HTTP ${res.status}`);
        }
        const payload = await res.json();
        const matches = Array.isArray(payload.matches) ? payload.matches : [];

        if (replaceResults) {
            document.getElementById("match-results").innerHTML = "";
        }

        for (const match of matches) {
            appendMatchResultRow(match, "saved");
        }

        document.getElementById("match-count").textContent = `${matches.length}`;
        document.getElementById("current-match").textContent = "-";
        currentMatchUsername = username;
        logMatch(`Loaded ${matches.length} saved matches for ${username}`, "success");
    } catch (err) {
        logMatch(`Failed to load saved matches: ${err}`, "error");
    }
}

function setActiveTab(tabName) {
    const isScanner = tabName === "scanner";
    const isMatches = tabName === "matches";
    const isStored = tabName === "stored";
    const isCompute = tabName === "compute";
    document.getElementById("tab-scanner").classList.toggle("active", isScanner);
    document.getElementById("tab-matches").classList.toggle("active", isMatches);
    document.getElementById("tab-stored").classList.toggle("active", isStored);
    document.getElementById("tab-compute").classList.toggle("active", isCompute);
    document.getElementById("panel-scanner").classList.toggle("active", isScanner);
    document.getElementById("panel-matches").classList.toggle("active", isMatches);
    document.getElementById("panel-stored").classList.toggle("active", isStored);
    document.getElementById("panel-compute").classList.toggle("active", isCompute);
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

        if (result === "Win") wins += 1;
        if (result === "Loss") losses += 1;

        const resultClass =
            result === "Win" ? "stored-result-win" :
            result === "Loss" ? "stored-result-loss" :
            "stored-result-unknown";

        const card = document.createElement("div");
        card.className = "stored-match-card";
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
            </div>
        `;
        card.addEventListener("click", () => selectStoredMatch(idx, username));
        list.appendChild(card);
    }

    document.getElementById("stored-total").textContent = String(visibleMatches.length);
    document.getElementById("stored-wins").textContent = String(wins);
    document.getElementById("stored-losses").textContent = String(losses);

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
    const el = document.getElementById("compute-results");
    if (!el) return;
    const section = (label, s) => `
        <div class="compute-card">
            <div class="compute-label">${label}</div>
            <div class="compute-metric-row"><span>Matches</span><strong>${s.matches}</strong></div>
            <div class="compute-metric-row"><span>Record</span><strong>${s.wins}-${s.losses}</strong></div>
            <div class="compute-metric-row"><span>Win Rate</span><strong>${s.winRate}%</strong></div>
            <div class="compute-metric-row"><span>Avg K/D</span><strong>${s.avgKd}</strong></div>
            <div class="compute-metric-row"><span>Avg K / D / A</span><strong>${s.avgKills} / ${s.avgDeaths} / ${s.avgAssists}</strong></div>
            <div class="compute-metric-row"><span>Tracked Rows</span><strong>${s.trackedRows}/${s.matches}</strong></div>
        </div>
    `;
    el.innerHTML = `
        ${section("Overall", stats.overall)}
        ${section("Ranked", stats.ranked)}
        ${section("Unranked", stats.unranked)}
    `;
}

async function runStatComputation(explicitUsername = "") {
    const username = (explicitUsername || document.getElementById("compute-username")?.value || "").trim();
    if (!username) {
        logCompute("Enter a username before computing stats.", "error");
        return;
    }
    try {
        const res = await fetch(`/api/scraped-matches/${encodeURIComponent(username)}?limit=100`);
        if (!res.ok) {
            throw new Error(`HTTP ${res.status}`);
        }
        const payload = await res.json();
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
            renderComputeCards({
                overall: finalize(buckets.overall),
                ranked: finalize(buckets.ranked),
                unranked: finalize(buckets.unranked),
            });
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

        renderComputeCards({
            overall: finalize(buckets.overall),
            ranked: finalize(buckets.ranked),
            unranked: finalize(buckets.unranked),
        });
        logCompute(
            `Computed stats for ${username}: overall ${buckets.overall.matches}, ` +
            `ranked ${buckets.ranked.matches}, unranked ${buckets.unranked.matches}.`,
            "success"
        );
    } catch (err) {
        logCompute(`Failed stat computation: ${err}`, "error");
    }
}

document.getElementById("start-scan").addEventListener("click", startScan);
document.getElementById("stop-scan").addEventListener("click", stopScan);
document.getElementById("start-match-scrape").addEventListener("click", startMatchScrape);
document.getElementById("stop-match-scrape").addEventListener("click", stopMatchScrape);
document.getElementById("load-saved-matches").addEventListener("click", () => loadSavedMatches("", true));
document.getElementById("load-stored-matches").addEventListener("click", () => loadStoredMatchesView("", false));
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
document.getElementById("tab-compute").addEventListener("click", () => setActiveTab("compute"));
document.getElementById("run-stat-compute").addEventListener("click", () => runStatComputation(""));

window.addEventListener("error", (event) => {
    log(`JS error: ${event.message}`, "error");
});

window.addEventListener("unhandledrejection", (event) => {
    log(`Promise error: ${event.reason}`, "error");
});

loadOperatorImageIndex();
initNetwork();
log("Ready to scan. Enter a username and click Start Scan.");
logMatch("Ready to scrape matches. Enter a username and click Scrape Matches.");
