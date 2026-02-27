let tooltipEl = null;

function ensureTooltip() {
    if (tooltipEl) return tooltipEl;
    tooltipEl = document.createElement("div");
    tooltipEl.className = "ui-tooltip hidden";
    document.body.appendChild(tooltipEl);
    return tooltipEl;
}

export function initTooltips(root = document) {
    const tip = ensureTooltip();
    const hide = () => tip.classList.add("hidden");
    root.addEventListener("mouseover", (event) => {
        const target = event.target?.closest?.("[data-ui-tooltip]");
        if (!target) return;
        tip.textContent = String(target.getAttribute("data-ui-tooltip") || "");
        const rect = target.getBoundingClientRect();
        tip.style.left = `${rect.left + rect.width / 2}px`;
        tip.style.top = `${rect.top - 10}px`;
        tip.classList.remove("hidden");
    });
    root.addEventListener("mouseout", (event) => {
        const target = event.target?.closest?.("[data-ui-tooltip]");
        if (!target) return;
        hide();
    });
    root.addEventListener("scroll", hide, true);
}
