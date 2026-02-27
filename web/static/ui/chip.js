export function createChip({
    label = "",
    tone = "info",
    className = "",
    removable = false,
    onRemove = null,
} = {}) {
    const chip = document.createElement("span");
    chip.className = `ui-chip ui-chip--${tone}${className ? ` ${className}` : ""}`;
    chip.textContent = label;
    if (removable) {
        const remove = document.createElement("button");
        remove.type = "button";
        remove.className = "ui-chip__remove";
        remove.setAttribute("aria-label", `Remove ${label}`);
        remove.textContent = "x";
        if (typeof onRemove === "function") remove.addEventListener("click", onRemove);
        chip.appendChild(remove);
    }
    return chip;
}
