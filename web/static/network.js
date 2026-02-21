let ws = null;
let wsMatches = null;
let network = null;
let nodes = null;
let edges = null;
let scanning = false;
let matchScraping = false;
let visLib = null;
let layoutTick = null;

function toNumber(value, fallback = 0) {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
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
            logMatch("Match details captured", "success");
            break;
        case "match_scraping_complete":
            document.getElementById("match-status").textContent = "Complete";
            logMatch(`Match scraping complete (${data.total_matches} matches)`, "success");
            break;
        case "warning":
            logMatch(`Warning: ${data.message}`, "info");
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
    const resultsDiv = document.getElementById("match-results");
    const entry = document.createElement("div");
    entry.className = "log-entry log-info";
    const map = matchData?.map || "Unknown map";
    const scoreA = toNumber(matchData?.score_team_a, 0);
    const scoreB = toNumber(matchData?.score_team_b, 0);
    const players = Array.isArray(matchData?.players) ? matchData.players.length : 0;
    entry.textContent = `${map} | ${scoreA}:${scoreB} | ${players} players`;
    resultsDiv.appendChild(entry);
    resultsDiv.scrollTop = resultsDiv.scrollHeight;
}

function setActiveTab(tabName) {
    const isScanner = tabName === "scanner";
    document.getElementById("tab-scanner").classList.toggle("active", isScanner);
    document.getElementById("tab-matches").classList.toggle("active", !isScanner);
    document.getElementById("panel-scanner").classList.toggle("active", isScanner);
    document.getElementById("panel-matches").classList.toggle("active", !isScanner);
    if (isScanner && network) {
        setTimeout(() => network.redraw(), 10);
    }
}

document.getElementById("start-scan").addEventListener("click", startScan);
document.getElementById("stop-scan").addEventListener("click", stopScan);
document.getElementById("start-match-scrape").addEventListener("click", startMatchScrape);
document.getElementById("stop-match-scrape").addEventListener("click", stopMatchScrape);
document.getElementById("tab-scanner").addEventListener("click", () => setActiveTab("scanner"));
document.getElementById("tab-matches").addEventListener("click", () => setActiveTab("matches"));

window.addEventListener("error", (event) => {
    log(`JS error: ${event.message}`, "error");
});

window.addEventListener("unhandledrejection", (event) => {
    log(`Promise error: ${event.reason}`, "error");
});

initNetwork();
log("Ready to scan. Enter a username and click Start Scan.");
logMatch("Ready to scrape matches. Enter a username and click Scrape Matches.");
