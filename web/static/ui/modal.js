export function createModalController(modalId, options = {}) {
    const { hiddenClass = "hidden" } = options;

    function getModal() {
        return document.getElementById(modalId);
    }

    function open() {
        const modal = getModal();
        if (!modal) return;
        modal.classList.remove(hiddenClass);
    }

    function close() {
        const modal = getModal();
        if (!modal) return;
        modal.classList.add(hiddenClass);
    }

    function isOpen() {
        const modal = getModal();
        if (!modal) return false;
        return !modal.classList.contains(hiddenClass);
    }

    function bindDismissHandlers() {
        window.addEventListener("keydown", (event) => {
            if (event.key !== "Escape") return;
            if (!isOpen()) return;
            close();
        });

        const modal = getModal();
        if (!modal) return;
        modal.addEventListener("click", (event) => {
            if (event.target?.id === modalId) {
                close();
            }
        });
    }

    return {
        open,
        close,
        isOpen,
        bindDismissHandlers,
    };
}

