window.toggleTheme = () => {
    if (document.documentElement.classList.contains('dark')) {
        document.documentElement.classList.remove('dark');
        localStorage.setItem('theme', 'light');
    } else {
        document.documentElement.classList.add('dark');
        localStorage.setItem('theme', 'dark');
    }
};

function copyText(value) {
    if (!value) return Promise.reject();
    if (navigator.clipboard && navigator.clipboard.writeText) {
        return navigator.clipboard.writeText(value);
    }
    const area = document.createElement('textarea');
    area.value = value;
    area.setAttribute('readonly', '');
    area.style.position = 'absolute';
    area.style.left = '-9999px';
    document.body.appendChild(area);
    area.select();
    const ok = document.execCommand('copy');
    document.body.removeChild(area);
    return ok ? Promise.resolve() : Promise.reject();
}

function flashCopied(el) {
    el.classList.add('is-copied');
    window.setTimeout(() => el.classList.remove('is-copied'), 1200);
}

document.body.addEventListener('click', (event) => {
    const el = event.target.closest('[data-copy]');
    if (!el) return;
    event.preventDefault();
    copyText(el.getAttribute('data-copy') || '')
        .then(() => flashCopied(el))
        .catch(() => {});
});
