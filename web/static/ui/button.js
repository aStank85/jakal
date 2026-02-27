export function createButton({
    label = "",
    variant = "secondary",
    className = "",
    attrs = {},
    onClick = null,
} = {}) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = `ui-button ui-button--${variant}${className ? ` ${className}` : ""}`;
    btn.textContent = label;
    Object.entries(attrs || {}).forEach(([key, value]) => {
        if (value == null) return;
        btn.setAttribute(key, String(value));
    });
    if (typeof onClick === "function") btn.addEventListener("click", onClick);
    return btn;
}
