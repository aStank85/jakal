export function createDataTable({ columns = [], rows = [] } = {}) {
    let sortKey = columns.find((c) => c.sortable)?.key || "";
    let sortDir = "asc";
    const table = document.createElement("table");
    table.className = "ui-datatable";
    const thead = document.createElement("thead");
    const tbody = document.createElement("tbody");

    function compare(a, b) {
        const left = a?.[sortKey];
        const right = b?.[sortKey];
        if (typeof left === "number" || typeof right === "number") {
            return (Number(left) || 0) - (Number(right) || 0);
        }
        return String(left || "").localeCompare(String(right || ""));
    }

    function renderHead() {
        const tr = document.createElement("tr");
        columns.forEach((col) => {
            const th = document.createElement("th");
            th.textContent = col.label || col.key;
            if (col.sortable) {
                th.classList.add("is-sortable");
                th.tabIndex = 0;
                th.addEventListener("click", () => {
                    if (sortKey === col.key) {
                        sortDir = sortDir === "asc" ? "desc" : "asc";
                    } else {
                        sortKey = col.key;
                        sortDir = "asc";
                    }
                    renderRows();
                });
            }
            tr.appendChild(th);
        });
        thead.replaceChildren(tr);
    }

    function renderRows(nextRows = rows) {
        rows = Array.isArray(nextRows) ? nextRows.slice() : [];
        const data = rows.slice();
        if (sortKey) {
            data.sort(compare);
            if (sortDir === "desc") data.reverse();
        }
        const fragment = document.createDocumentFragment();
        data.forEach((row) => {
            const tr = document.createElement("tr");
            columns.forEach((col) => {
                const td = document.createElement("td");
                if (typeof col.render === "function") {
                    td.innerHTML = String(col.render(row) || "");
                } else {
                    td.textContent = String(row?.[col.key] ?? "");
                }
                tr.appendChild(td);
            });
            fragment.appendChild(tr);
        });
        tbody.replaceChildren(fragment);
    }

    renderHead();
    renderRows(rows);
    table.appendChild(thead);
    table.appendChild(tbody);
    return { element: table, setRows: renderRows };
}
