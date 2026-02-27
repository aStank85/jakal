export function createCard({
    title = "",
    className = "",
    bodyHtml = "",
} = {}) {
    const card = document.createElement("section");
    card.className = `ui-card${className ? ` ${className}` : ""}`;
    if (title) {
        const head = document.createElement("header");
        head.className = "ui-card__head";
        head.textContent = title;
        card.appendChild(head);
    }
    const body = document.createElement("div");
    body.className = "ui-card__body";
    body.innerHTML = bodyHtml;
    card.appendChild(body);
    return card;
}
