import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@latest/+esm';

const PAGE_SIZE = 24;
const PARQUET_BASE = ['localhost', '127.0.0.1'].includes(window.location.hostname)
    ? new URL('.', window.location.href).href
    : 'https://htbrowser-parquet.s3.us-west-1.amazonaws.com/';

const PARQUET_FILES = Array.from({length: 10}, (_, i) =>
    `${PARQUET_BASE}collection_${String(i + 1).padStart(2, '0')}.parquet`
);

let conn = null;
let currentPage = 0;
let searchTimer = null;
let isLoading = false;

// Cache for the current search result set.
// { key: string, total: number, rows: object[]|null }
// rows is null when the result set exceeds ROW_CACHE_LIMIT (use OFFSET instead).
let queryCache = null;
const ROW_CACHE_LIMIT = 500;

function searchKey() {
    return JSON.stringify({
        q: document.getElementById('search').value.trim(),
        lang: document.getElementById('lang-filter').value,
    });
}

function sortRows(rows) {
    const sort = document.getElementById('sort').value;
    if (!sort) return rows;
    const copy = rows.slice();
    if (sort === 'title ASC') {
        copy.sort((a, b) => (a.title ?? '').localeCompare(b.title ?? ''));
    } else if (sort === 'title DESC') {
        copy.sort((a, b) => (b.title ?? '').localeCompare(a.title ?? ''));
    } else if (sort === 'rights_date_used ASC NULLS LAST') {
        copy.sort((a, b) => {
            const ya = (!a.rights_date_used || a.rights_date_used === 9999) ? Infinity : a.rights_date_used;
            const yb = (!b.rights_date_used || b.rights_date_used === 9999) ? Infinity : b.rights_date_used;
            return ya - yb;
        });
    } else {
        // Year newest first
        copy.sort((a, b) => {
            const ya = (!a.rights_date_used || a.rights_date_used === 9999) ? -Infinity : a.rights_date_used;
            const yb = (!b.rights_date_used || b.rights_date_used === 9999) ? -Infinity : b.rights_date_used;
            return yb - ya;
        });
    }
    return copy;
}

// ── DuckDB init ──────────────────────────────────────────────────────────────

async function initDuckDB() {
    setStatus('');

    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);

    const workerUrl = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
    );
    const worker = new Worker(workerUrl);
    const logger = new duckdb.VoidLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);

    conn = await db.connect();

    await conn.query(`
PRAGMA enable_object_cache=true;
`)
}

// ── Query helpers ─────────────────────────────────────────────────────────────

function buildWhere() {
    const search = document.getElementById('search').value.trim();
    const lang   = document.getElementById('lang-filter').value;
    const parts  = [];

    if (search) {
        const q = search.replace(/'/g, "''");
        parts.push(`(title ILIKE '%${q}%' OR author ILIKE '%${q}%')`);
    }
    if (lang) parts.push(`lang = '${lang.replace(/'/g,"''")}'`);

    return parts.length ? 'WHERE ' + parts.join(' AND ') : '';
}

function buildOrderBy() {
    const sort = document.getElementById('sort').value;
    return sort ? `ORDER BY ${sort}` : '';
}

function fromParquet() {
    const list = PARQUET_FILES.map(u => `'${u}'`).join(', ');
    return `FROM read_parquet([${list}])`;
}

async function runQuery(sql) {
    const table = await conn.query(sql);
    return table.toArray().map(row => {
        const obj = {};
        for (const f of table.schema.fields) {
            const v = row[f.name];
            obj[f.name] = typeof v === 'bigint' ? Number(v) : v;
        }
        return obj;
    });
}

async function fetchBooks(page) {
    if (queryCache?.rows) {
        console.debug('[cache] fetchBooks page=%d via row slice (no DuckDB query)', page);
        return sortRows(queryCache.rows).slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
    }
    // Large result set: OFFSET/LIMIT (count already cached)
    console.debug('[cache] fetchBooks page=%d via OFFSET/LIMIT (result set too large to cache)', page);
    return runQuery(`
        SELECT htid, title, author, rights_date_used, lang
        ${fromParquet()}
        ${buildWhere()}
        ${buildOrderBy()}
        LIMIT ${PAGE_SIZE} OFFSET ${page * PAGE_SIZE}
    `);
}

async function ensureCache() {
    const key = searchKey();
    if (queryCache?.key === key) {
        console.debug('[cache] HIT — total=%d, rows cached=%s',
            queryCache.total, queryCache.rows ? queryCache.rows.length : 'no (large set)');
        return;
    }

    console.debug('[cache] MISS — fetching rows for key:', key);
    setLoadingMsg('Searching…');

    // Fetch up to ROW_CACHE_LIMIT rows in one query.
    // If fewer come back than the limit, that count is the exact total.
    // Only run a separate COUNT when the set is too large to cache.
    const rows = await runQuery(`
        SELECT htid, title, author, rights_date_used, lang
        ${fromParquet()}
        ${buildWhere()}
        ${buildOrderBy()}
        LIMIT ${ROW_CACHE_LIMIT}
    `);

    if (rows.length < ROW_CACHE_LIMIT) {
        console.debug('[cache] cached %d rows (single query)', rows.length);
        setLoadingMsg(`Found ${rows.length.toLocaleString()} results…`);
        queryCache = { key, total: rows.length, rows };
    } else {
        setLoadingMsg('Large result set, counting…');
        const total = (await runQuery(`SELECT COUNT(*) AS n ${fromParquet()} ${buildWhere()}`))[0].n;
        setLoadingMsg(`Found ${total.toLocaleString()} results, loading page…`);
        console.debug('[cache] result set too large (%d), falling back to OFFSET/LIMIT', total);
        queryCache = { key, total, rows: null };
    }
}

async function fetchLanguages() {
    const sql = `
    SELECT lang, COUNT(*) AS n
    ${fromParquet()}
    WHERE lang IS NOT NULL AND lang != ''
    GROUP BY lang
    ORDER BY n DESC
    LIMIT 60
  `;
    return runQuery(sql);
}

// ── Rendering ────────────────────────────────────────────────────────────────

function esc(s) {
    return String(s ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function coverUrl(htid) {
    return `https://babel.hathitrust.org/cgi/imgsrv/image?id=${encodeURIComponent(htid)};orient=0;size=25;seq=1`;
}

function embedUrl(htid) {
    return `https://babel.hathitrust.org/cgi/pt?id=${encodeURIComponent(htid)}&ui=embed`;
}

function cleanTitle(t) {
    if (!t) return 'Untitled';
    const slash = t.indexOf(' / ');
    return (slash !== -1 ? t.slice(0, slash) : t).trim();
}

function formatYear(y) {
    if (!y || y === 9999 || y === 0) return null;
    return String(y);
}

function renderGrid(books) {
    const grid = document.getElementById('grid');

    if (!books.length) {
        grid.innerHTML = `<div class="state-message" style="grid-column:1/-1"><p>No books found for this search.</p></div>`;
        return;
    }

    grid.innerHTML = books.map(book => {
        const year  = formatYear(book.rights_date_used);
        const title = cleanTitle(book.title);

        return `<a class="card" href="${esc(embedUrl(book.htid))}"
         data-htid="${esc(book.htid)}"
         data-title="${esc(title)}"
         data-author="${esc(book.author || '')}">
      <div class="cover">
        <img src="${esc(coverUrl(book.htid))}" loading="lazy" alt=""
             onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">
        <div class="cover-fallback">
          <div class="fallback-title">${esc(title)}</div>
        </div>
      </div>
      <div class="card-body">
        <div class="card-title">${esc(title)}</div>
        ${book.author ? `<div class="card-author">${esc(book.author)}</div>` : ''}
        <div class="card-meta">${[year, book.lang].filter(Boolean).map(esc).join(' · ')}</div>
      </div>
    </a>`;
    }).join('');
}

function renderPagination(page, total) {
    const pages = Math.ceil(total / PAGE_SIZE);

    if (pages <= 1) {
        document.getElementById('pagination-top').innerHTML = '';
        document.getElementById('pagination').innerHTML = '';
        return;
    }

    const window_size = 5;
    let lo = Math.max(0, page - Math.floor(window_size / 2));
    let hi = Math.min(pages - 1, lo + window_size - 1);
    if (hi - lo < window_size - 1) lo = Math.max(0, hi - window_size + 1);

    let btns = `<button class="pag-prev" data-p="${page - 1}" ${page === 0 ? 'disabled' : ''}>&larr;</button>`;
    if (lo > 0) {
        btns += `<button data-p="0">1</button>`;
        if (lo > 1) btns += `<button class="ellipsis" data-ellipsis>…</button>`;
    }
    for (let i = lo; i <= hi; i++) {
        btns += `<button data-p="${i}" class="${i === page ? 'current' : ''}">${(i + 1).toLocaleString()}</button>`;
    }
    if (hi < pages - 1) {
        if (hi < pages - 2) btns += `<button class="ellipsis" data-ellipsis>…</button>`;
        btns += `<button data-p="${pages - 1}">${pages.toLocaleString()}</button>`;
    }
    btns += `<button class="pag-next" data-p="${page + 1}" ${page >= pages - 1 ? 'disabled' : ''}>&rarr;</button>`;

    const pagHTML = () => `<div class="pag-buttons">${btns}</div>`;

    document.getElementById('pagination-top').innerHTML = pagHTML();
    document.getElementById('pagination').innerHTML = pagHTML();

    for (const id of ['pagination-top', 'pagination']) {
        const container = document.getElementById(id);

        container.querySelectorAll('button[data-ellipsis]').forEach(btn => {
            btn.addEventListener('click', () => {
                const input = document.createElement('input');
                input.type = 'number';
                input.min = 1;
                input.max = pages;
                input.placeholder = '…';
                input.className = 'ellipsis-input';
                btn.replaceWith(input);
                input.focus();
                input.addEventListener('keydown', e => {
                    if (e.key === 'Enter') {
                        const p = parseInt(input.value, 10) - 1;
                        if (!isNaN(p) && p >= 0 && p < pages) { currentPage = p; refresh(); }
                    }
                    if (e.key === 'Escape') { input.replaceWith(btn); }
                });
                input.addEventListener('blur', () => { input.replaceWith(btn); });
            });
        });
    }
}

// ── State display ─────────────────────────────────────────────────────────────

function setStatus(msg) {
    document.getElementById('page-info').textContent = msg;
}

function showLoading() {
    document.getElementById('grid').innerHTML =
        `<div class="state-message" style="grid-column:1/-1">
       <div class="spinner"></div>
       <div id="loading-msg"></div>
     </div>`;
    document.getElementById('pagination-top').innerHTML = '';
    document.getElementById('pagination').innerHTML = '';
    setStatus('');
}

function setLoadingMsg(msg) {
    const el = document.getElementById('loading-msg');
    if (el) el.textContent = msg;
}

function showError(err) {
    document.getElementById('grid').innerHTML =
        `<div class="state-message" style="grid-column:1/-1">
       <p class="error-msg">Error: ${esc(err.message)}</p>
       <p class="error-hint">Make sure you're serving this over HTTP, not file://</p>
     </div>`;
    document.getElementById('pagination-top').innerHTML = '';
    document.getElementById('pagination').innerHTML = '';
    setStatus('Error');
    console.error(err);
}

// ── Main refresh ──────────────────────────────────────────────────────────────

async function refresh() {
    if (!conn || isLoading) return;
    isLoading = true;

    showLoading();

    try {
        const q    = document.getElementById('search').value.trim();
        const lang = document.getElementById('lang-filter').value;

        let books, total;

        if (q || lang) {
            // Search/filter mode: cache all results, page turns are free
            await ensureCache();
            total = queryCache.total;
            books = await fetchBooks(currentPage);
        } else {
            // Browse mode: no search, go straight to OFFSET/LIMIT + COUNT in parallel
            const offset = currentPage * PAGE_SIZE;
            setLoadingMsg('Loading…');
            [books, total] = await Promise.all([
                runQuery(`
                    SELECT htid, title, author, rights_date_used, lang
                    ${fromParquet()}
                    ${buildWhere()}
                    ${buildOrderBy()}
                    LIMIT ${PAGE_SIZE} OFFSET ${offset}
                `),
                runQuery(`SELECT COUNT(*) AS n ${fromParquet()} ${buildWhere()}`).then(r => r[0].n),
            ]);
        }

        const start = currentPage * PAGE_SIZE + 1;
        const end   = Math.min((currentPage + 1) * PAGE_SIZE, total);

        document.getElementById('page-info').textContent =
            total > 0 ? `${start.toLocaleString()}–${end.toLocaleString()} of ${total.toLocaleString()}` : '';

        renderGrid(books);
        renderPagination(currentPage, total);

        requestAnimationFrame(() => window.applyVanDeGraafPadding?.());
        window.scrollTo({ top: 0, behavior: 'smooth' });
    } catch (err) {
        showError(err);
    } finally {
        isLoading = false;
    }
}

// ── Language dropdown ─────────────────────────────────────────────────────────

async function populateLanguages() {
    try {
        const langs = await fetchLanguages();
        const sel = document.getElementById('lang-filter');
        for (const { lang, n } of langs) {
            const opt = document.createElement('option');
            opt.value = lang;
            opt.textContent = `${lang}  (${Number(n).toLocaleString()})`;
            sel.appendChild(opt);
        }
    } catch (_) { /* non-fatal */ }
}

// ── Bootstrap ────────────────────────────────────────────────────────────────

(async () => {
    showLoading();

    try {
        await initDuckDB();
    } catch (err) {
        showError(err);
        return;
    }

    await refresh();
    populateLanguages(); // runs concurrently in background
})();

// ── Event listeners ───────────────────────────────────────────────────────────

document.getElementById('search').addEventListener('input', () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => { currentPage = 0; refresh(); }, 400);
});

for (const id of ['lang-filter', 'sort']) {
    document.getElementById(id).addEventListener('change', () => {
        currentPage = 0;
        refresh();
    });
}

// ── Viewer modal ──────────────────────────────────────────────────────────────

const modal      = document.getElementById('viewer-modal');
const viewerFrame = document.getElementById('viewer-frame');

function openViewer(htid, title, author) {
    document.getElementById('viewer-title').textContent = title;
    document.getElementById('viewer-author').textContent = author;
    viewerFrame.src = embedUrl(htid);
    modal.hidden = false;
    document.body.style.overflow = 'hidden';
}

function closeViewer() {
    modal.hidden = true;
    viewerFrame.src = '';
    document.body.style.overflow = '';
}

document.getElementById('viewer-close').addEventListener('click', closeViewer);
document.getElementById('viewer-overlay').addEventListener('click', closeViewer);
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeViewer(); });

document.getElementById('grid').addEventListener('click', e => {
    const card = e.target.closest('a.card[data-htid]');
    if (!card) return;
    e.preventDefault();
    openViewer(card.dataset.htid, card.dataset.title, card.dataset.author);
});

for (const id of ['pagination-top', 'pagination']) {
    document.getElementById(id).addEventListener('click', e => {
        const btn = e.target.closest('button[data-p]');
        if (!btn) return;
        const p = parseInt(btn.dataset.p, 10);
        if (!isNaN(p)) { currentPage = p; refresh(); }
    });
}
