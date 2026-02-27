export function setActiveTab(tabName, options = {}) {
    const {
        onScannerActivated = null,
        onStoredActivated = null,
        onPlayersActivated = null,
        onTeamBuilderActivated = null,
        onOperatorsActivated = null,
        onDashboardActivated = null,
    } = options;
    const isScanner = tabName === "scanner";
    const isMatches = tabName === "matches";
    const isStored = tabName === "stored";
    const isPlayers = tabName === "players";
    const isTeamBuilder = tabName === "team-builder";
    const isOperators = tabName === "operators";
    const isDashboard = tabName === "dashboard";

    const tabs = [
        { tab: "tab-scanner", panel: "panel-scanner", active: isScanner },
        { tab: "tab-matches", panel: "panel-matches", active: isMatches },
        { tab: "tab-stored", panel: "panel-stored", active: isStored },
        { tab: "tab-players", panel: "panel-players", active: isPlayers },
        { tab: "tab-team-builder", panel: "panel-team-builder", active: isTeamBuilder },
        { tab: "tab-operators", panel: "panel-operators", active: isOperators },
        { tab: "tab-dashboard", panel: "panel-dashboard", active: isDashboard },
    ];

    tabs.forEach(({ tab, panel, active }) => {
        const tabEl = document.getElementById(tab);
        const panelEl = document.getElementById(panel);
        if (tabEl) {
            tabEl.classList.toggle("active", active);
            tabEl.setAttribute("aria-selected", active ? "true" : "false");
            tabEl.tabIndex = active ? 0 : -1;
        }
        if (panelEl) {
            panelEl.classList.toggle("active", active);
            panelEl.hidden = !active;
        }
    });

    if (isScanner && typeof onScannerActivated === "function") {
        setTimeout(() => onScannerActivated(), 10);
    }
    if (isStored && typeof onStoredActivated === "function") {
        onStoredActivated();
    }
    if (isPlayers && typeof onPlayersActivated === "function") {
        onPlayersActivated();
    }
    if (isTeamBuilder && typeof onTeamBuilderActivated === "function") {
        onTeamBuilderActivated();
    }
    if (isOperators && typeof onOperatorsActivated === "function") {
        onOperatorsActivated();
    }
    if (isDashboard && typeof onDashboardActivated === "function") {
        onDashboardActivated();
    }
}

export function initPrimaryTabKeyboardNav(selector = ".tabs .tab-btn") {
    const tabButtons = Array.from(document.querySelectorAll(selector));
    if (!tabButtons.length) return;
    tabButtons.forEach((btn, index) => {
        btn.addEventListener("keydown", (event) => {
            if (event.key !== "ArrowLeft" && event.key !== "ArrowRight" && event.key !== "Home" && event.key !== "End") {
                return;
            }
            event.preventDefault();
            let nextIndex = index;
            if (event.key === "ArrowRight") nextIndex = (index + 1) % tabButtons.length;
            if (event.key === "ArrowLeft") nextIndex = (index - 1 + tabButtons.length) % tabButtons.length;
            if (event.key === "Home") nextIndex = 0;
            if (event.key === "End") nextIndex = tabButtons.length - 1;
            const target = tabButtons[nextIndex];
            target?.focus();
            target?.click();
        });
    });
}
