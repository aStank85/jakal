function resolveHttpUrl(path) {
    const value = String(path || "");
    if (!value) return window.location.origin;
    if (/^https?:\/\//i.test(value)) return value;
    if (value.startsWith("/")) return `${window.location.origin}${value}`;
    return `${window.location.origin}/${value}`;
}

function resolveWsUrl(path) {
    const value = String(path || "");
    if (!value) {
        const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
        return `${proto}//${window.location.host}/`;
    }
    if (/^wss?:\/\//i.test(value)) return value;
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    if (value.startsWith("/")) return `${proto}//${window.location.host}${value}`;
    return `${proto}//${window.location.host}/${value}`;
}

function encodeSegment(value) {
    return encodeURIComponent(String(value || ""));
}

function queryString(params) {
    if (!params) return "";
    if (typeof params === "string") return params.replace(/^\?/, "");
    if (params instanceof URLSearchParams) return params.toString();
    return new URLSearchParams(params).toString();
}

export function createApiClient() {
    return {
        request(path, options = {}) {
            return fetch(resolveHttpUrl(path), options);
        },
        openWebSocket(path) {
            return new WebSocket(resolveWsUrl(path));
        },
        getOperatorImageIndex() {
            return this.request("/api/operator-image-index");
        },
        openScanWebSocket() {
            return this.openWebSocket("/ws/scan");
        },
        openMatchScrapeWebSocket() {
            return this.openWebSocket("/ws/scrape-matches");
        },
        postUnpackScrapedMatches(username, limit = 5000) {
            return this.request(`/api/unpack-scraped-matches/${encodeSegment(username)}?limit=${encodeSegment(limit)}`, {
                method: "POST",
            });
        },
        postDeleteBadScrapedMatches(username) {
            return this.request(`/api/delete-bad-scraped-matches/${encodeSegment(username)}`, {
                method: "POST",
            });
        },
        getScrapedMatches(username, limit = 10000) {
            return this.request(`/api/scraped-matches/${encodeSegment(username)}?limit=${encodeSegment(limit)}`);
        },
        postDbStandardize(dryRun, verbose) {
            const qs = queryString({
                dry_run: dryRun ? "true" : "false",
                verbose: verbose ? "true" : "false",
            });
            return this.request(`/api/settings/db-standardize?${qs}`, { method: "POST" });
        },
        getAtkDefHeatmap(username, params) {
            const qs = queryString(params);
            return this.request(`/api/atk-def-heatmap/${encodeSegment(username)}?${qs}`);
        },
        getDashboardWorkspace(username, params, options = {}) {
            const qs = queryString(params);
            return this.request(`/api/dashboard-workspace/${encodeSegment(username)}?${qs}`, options);
        },
        getDashboardWorkspaceOperator(username, operatorName, params) {
            const qs = queryString(params);
            return this.request(`/api/dashboard-workspace/${encodeSegment(username)}/operator/${encodeSegment(operatorName)}?${qs}`);
        },
        getDashboardWorkspaceEvidence(username, params) {
            const qs = queryString(params);
            return this.request(`/api/dashboard-workspace/${encodeSegment(username)}/evidence?${qs}`);
        },
        getRoundAnalysis(username) {
            return this.request(`/api/round-analysis/${encodeSegment(username)}`);
        },
        getTeammateChemistry(username) {
            return this.request(`/api/teammate-chemistry/${encodeSegment(username)}`);
        },
        getLobbyQuality(username) {
            return this.request(`/api/lobby-quality/${encodeSegment(username)}`);
        },
        getTradeAnalysis(username, windowSeconds = 5) {
            return this.request(`/api/trade-analysis/${encodeSegment(username)}?window_seconds=${encodeSegment(windowSeconds)}`);
        },
        getTeamAnalysis(username) {
            return this.request(`/api/team-analysis/${encodeSegment(username)}`);
        },
        getEnemyOperatorThreat(username) {
            return this.request(`/api/enemy-operator-threat/${encodeSegment(username)}`);
        },
        getOperatorStats(username) {
            return this.request(`/api/operator-stats/${encodeSegment(username)}`);
        },
        getMapStats(username) {
            return this.request(`/api/map-stats/${encodeSegment(username)}`);
        },
        getEncounteredPlayers(username, matchType = "Ranked") {
            const qs = queryString({ username, match_type: matchType });
            return this.request(`/api/players/encountered?${qs}`);
        },
        setPlayerTag(username, tag = "friend", enabled = true) {
            return this.request("/api/players/tag", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ username, tag, enabled }),
            });
        },
        getFriends(tag = "friend") {
            return this.request(`/api/players/friends?tag=${encodeSegment(tag)}`);
        },
        getStackSynergy(players, matchType = "Ranked") {
            const qs = queryString({
                players: Array.isArray(players) ? players.join(",") : String(players || ""),
                match_type: matchType,
            });
            return this.request(`/api/stack/synergy?${qs}`);
        },
        getPlayersList() {
            return this.request("/api/players/list");
        },
        getOperatorsMapBreakdown(username, stack = "solo", matchType = "Ranked") {
            const qs = queryString({ username, stack, match_type: matchType });
            return this.request(`/api/operators/map-breakdown?${qs}`);
        },
    };
}
