import { createButton } from "./button.js";

export function enhanceDrawers(root = document) {
    root.querySelectorAll("details.filter-drawer").forEach((drawer) => {
        drawer.classList.add("ui-drawer");
        const summary = drawer.querySelector("summary");
        if (summary) summary.classList.add("ui-drawer__summary");
    });
}

export function attachDrawerResetButton(drawer, onReset) {
    if (!drawer || drawer.querySelector(".ui-drawer__reset")) return null;
    const controls = document.createElement("div");
    controls.className = "ui-drawer__controls";
    const btn = createButton({
        label: "Reset Filters",
        variant: "ghost",
        className: "ui-drawer__reset",
        onClick: onReset,
    });
    controls.appendChild(btn);
    drawer.appendChild(controls);
    return btn;
}
