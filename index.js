require('dotenv').config?.();
const express = require('express');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const rateLimit = require('express-rate-limit');
const LRU = require('lru-cache');
const pino = require('pino');

const LRUCache = LRU.LRUCache || LRU;
puppeteer.use(StealthPlugin());

// ═══════════════════════════════════════════════════════
// 1. CONFIG
// ═══════════════════════════════════════════════════════
const CONFIG = {
    PORT: parseInt(process.env.PORT) || 3000,
    POOL_SIZE: parseInt(process.env.POOL_SIZE) || 3,
    PAGES_PER_BROWSER: parseInt(process.env.PAGES_PER_BROWSER) || 2,
    QUEUE_CONCURRENCY: parseInt(process.env.QUEUE_CONCURRENCY) || 5,
    MAX_RESULTS: 10,
    NAV_TIMEOUT: 25000,
    MAX_RETRIES: 2,
    CACHE_TTL_MS: 5 * 60 * 1000,
    CACHE_MAX: 500,
    LOG_LEVEL: process.env.LOG_LEVEL || 'info',
    BLOCK_RESOURCES: process.env.BLOCK_RESOURCES !== 'false',
    PROXIES: (process.env.PROXIES || '').split(',').map(s => s.trim()).filter(Boolean),
};

// ═══════════════════════════════════════════════════════
// 2. LOGGER (Pino)
// ═══════════════════════════════════════════════════════
const logger = pino({
    level: CONFIG.LOG_LEVEL,
    transport: process.env.NODE_ENV !== 'production'
        ? { target: 'pino-pretty', options: { colorize: true, translateTime: 'SYS:HH:MM:ss' } }
        : undefined,
});

// ═══════════════════════════════════════════════════════
// 3. PROXY MANAGER (Rotation + Health + Cooldown)
// ═══════════════════════════════════════════════════════
class ProxyManager {
    constructor(proxies) {
        this.proxies = proxies.map(url => ({ url, fails: 0, cooldownUntil: 0 }));
        this.index = 0;
    }
    hasProxies() { return this.proxies.length > 0; }
    getNext() {
        if (!this.proxies.length) return null;
        const now = Date.now();
        for (let i = 0; i < this.proxies.length; i++) {
            this.index = (this.index + 1) % this.proxies.length;
            const p = this.proxies[this.index];
            if (p.cooldownUntil < now) return p.url;
        }
        return null;
    }
    markBad(url) {
        const p = this.proxies.find(x => x.url === url);
        if (!p) return;
        p.fails++;
        p.cooldownUntil = Date.now() + Math.min(60000 * p.fails, 600000);
        logger.warn({ proxy: this._mask(url), fails: p.fails }, 'Proxy marked bad');
    }
    markGood(url) {
        const p = this.proxies.find(x => x.url === url);
        if (p) { p.fails = 0; p.cooldownUntil = 0; }
    }
    _mask(u) { return u.replace(/:[^:@]*@/, ':***@'); }
    stats() { return this.proxies.map(p => ({ proxy: this._mask(p.url), fails: p.fails, cooldown: p.cooldownUntil > Date.now() })); }
}
const proxyManager = new ProxyManager(CONFIG.PROXIES);

// ═══════════════════════════════════════════════════════
// 4. CACHE (LRU + TTL)
// ═══════════════════════════════════════════════════════
const cache = new LRUCache({ max: CONFIG.CACHE_MAX, ttl: CONFIG.CACHE_TTL_MS });
const cacheKey = (q, n) => `${q.toLowerCase().trim()}::${n}`;

// ═══════════════════════════════════════════════════════
// 5. LIGHTWEIGHT ASYNC QUEUE
// ═══════════════════════════════════════════════════════
class AsyncQueue {
    constructor(concurrency) {
        this.concurrency = concurrency;
        this.running = 0;
        this.queue = [];
    }
    stats() { return { running: this.running, waiting: this.queue.length, concurrency: this.concurrency }; }
    add(fn) {
        return new Promise((resolve, reject) => {
            this.queue.push({ fn, resolve, reject });
            this._next();
        });
    }
    _next() {
        while (this.running < this.concurrency && this.queue.length) {
            const { fn, resolve, reject } = this.queue.shift();
            this.running++;
            Promise.resolve().then(fn).then(resolve, reject)
                .finally(() => { this.running--; this._next(); });
        }
    }
}
const queue = new AsyncQueue(CONFIG.QUEUE_CONCURRENCY);

// ═══════════════════════════════════════════════════════
// 6. BROWSER POOL (Auto-restart + Page reuse)
// ═══════════════════════════════════════════════════════
class BrowserPool {
    constructor(size, pagesPerBrowser) {
        this.size = size;
        this.ppb = pagesPerBrowser;
        this.browsers = new Map();
        this.freePages = [];
        this.waiters = [];
        this.nextId = 1;
        this.closed = false;
    }

    async launchBrowser() {
        if (this.closed) return;
        const proxy = proxyManager.hasProxies() ? proxyManager.getNext() : null;
        const args = [
            '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
            '--disable-blink-features=AutomationControlled',
            '--window-size=1366,768', '--lang=en-US,en',
        ];
        if (proxy) args.push(`--proxy-server=${proxy}`);

        const browser = await puppeteer.launch({
            headless: 'true', args, ignoreDefaultArgs: ['--enable-automation'],
        });
        const id = this.nextId++;
        const entry = { id, browser, proxy, pages: new Set() };

        for (let i = 0; i < this.ppb; i++) {
            const page = await browser.newPage();
            await this._setupPage(page);
            entry.pages.add(page);
        }

        this.browsers.set(id, entry);

        // pages ko pool me daalo
        for (const page of entry.pages) this._givePage({ page, entry });

        // ── Browser Auto-Restart ──
        browser.on('disconnected', () => {
            logger.warn({ browserId: id }, 'Browser disconnected — auto restart');
            this.browsers.delete(id);
            this.freePages = this.freePages.filter(fp => fp.entry.id !== id);
            if (!this.closed && this.browsers.size < this.size) {
                this.launchBrowser().catch(e => logger.error({ err: e.message }, 'Relaunch fail'));
            }
        });

        logger.info({ browserId: id, proxy: proxy ? proxy.replace(/:[^:@]*@/, ':***@') : 'direct' }, 'Browser up ✅');
    }

    // ── Request Interception (bandwidth + speed) ──
    async _setupPage(page) {
        await page.setViewport({ width: 1366, height: 768 });
        await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });
        page.setDefaultNavigationTimeout(CONFIG.NAV_TIMEOUT);

        

        if (CONFIG.BLOCK_RESOURCES) {
            await page.setRequestInterception(true);
            const blockTypes = new Set(['image', 'media', 'font', 'stylesheet']);
            const blockHosts = /(google-analytics|googletagmanager|doubleclick|facebook\.com\/tr|hotjar|clarity|sentry)/i;
            page.on('request', req => {
                try {
                    if (blockTypes.has(req.resourceType()) || blockHosts.test(req.url())) {
                        req.abort().catch(() => {});
                    } else {
                        req.continue().catch(() => {});
                    }
                } catch (_) {}
            });
        }
    }

    async init() {
        const jobs = [];
        for (let i = 0; i < this.size; i++) jobs.push(this.launchBrowser());
        await Promise.all(jobs);
        logger.info({ poolSize: this.size, ppb: this.ppb }, 'Browser Pool ready 🚀');
    }

    _givePage(item) {
        if (item.page.isClosed()) return;
        if (this.waiters.length) this.waiters.shift()(item);
        else this.freePages.push(item);
    }

    async acquire() {
        while (this.freePages.length) {
            const item = this.freePages.pop();
            if (!item.page.isClosed() && item.entry.browser.isConnected()) return item;
        }
        return new Promise(r => this.waiters.push(r));
    }

    release(item) {
        if (this.closed) return;
        this._givePage(item);
    }

    async close() {
        this.closed = true;
        for (const entry of this.browsers.values()) {
            try { await entry.browser.close(); } catch (_) {}
        }
        this.browsers.clear();
        this.freePages = [];
    }

    stats() {
        return {
            browsers: [...this.browsers.values()].map(e => ({
                id: e.id,
                proxy: e.proxy ? e.proxy.replace(/:[^:@]*@/, ':***@') : 'direct',
                pages: e.pages.size,
                connected: e.browser.isConnected(),
            })),
            freePages: this.freePages.length,
            waiting: this.waiters.length,
        };
    }
}
const pool = new BrowserPool(CONFIG.POOL_SIZE, CONFIG.PAGES_PER_BROWSER);

// ═══════════════════════════════════════════════════════
// 7. SCRAPER HELPERS
// ═══════════════════════════════════════════════════════
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
];
const randomUA = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
const sleep = ms => new Promise(r => setTimeout(r, ms));

function isCaptchaPage(url, html) {
    if (url.includes('/sorry/') || url.includes('captcha')) return true;
    const l = html.toLowerCase();
    return l.includes('recaptcha') || l.includes("i'm not a robot")
        || l.includes('unusual traffic') || l.includes('our systems have detected');
}

async function handleConsent(page) {
    try {
        const btns = await page.$$('button, div[role="button"]');
        for (const btn of btns) {
            const txt = (await page.evaluate(el => el.innerText, btn)) || '';
            if (/accept all|i agree|accept/i.test(txt)) {
                await btn.click();
                await sleep(800);
                return true;
            }
        }
    } catch (_) {}
    return false;
}

async function humanType(page, selector, text) {
    await page.waitForSelector(selector, { timeout: 10000 });
    await page.click(selector, { clickCount: 3 });
    for (const ch of text) {
        await page.type(selector, ch, { delay: 60 + Math.random() * 100 });
        if (Math.random() < 0.05) await sleep(100 + Math.random() * 200);
    }
}

async function extractResults(page, max) {
    return page.evaluate((max) => {
        const out = [];
        const seen = new Set();
        const anchors = document.querySelectorAll('a[href^="http"] > h3, a[href^="http"] h3');
        for (const h3 of anchors) {
            if (out.length >= max) break;
            const linkEl = h3.closest('a');
            if (!linkEl) continue;
            const url = linkEl.href;
            if (seen.has(url) || url.includes('google.com')) continue;
            seen.add(url);
            const title = h3.innerText.trim();
            if (!title) continue;

            let snippet = '';
            const container = h3.closest('div.g, div[data-hveid], div');
            if (container) {
                const snipEl = container.querySelector('.VwiC3b, .yXK7lf, .MUxGbd, div[style*="-webkit-line-clamp"]');
                if (snipEl) snippet = snipEl.innerText.trim();
            }
            out.push({ position: out.length + 1, title, link: url, snippet: snippet || '(no snippet)' });
        }
        return out;
    }, max);
}

// ═══════════════════════════════════════════════════════
// 8. CORE SEARCH (uses pool + queue)
// ═══════════════════════════════════════════════════════
async function performSearch(query, maxResults) {
    const { page, entry } = await pool.acquire();
    try {
        await page.setUserAgent(randomUA());

        await page.goto('https://www.google.com', { waitUntil: 'domcontentloaded' });
        await handleConsent(page);

        const searchBox = 'textarea[name="q"], input[name="q"]';
        await humanType(page, searchBox, query);

        await Promise.all([
            page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: CONFIG.NAV_TIMEOUT }).catch(() => {}),
            page.keyboard.press('Enter'),
        ]);
        await sleep(1200);

        const finalUrl = page.url();
        const html = await page.content();

        if (isCaptchaPage(finalUrl, html)) {
            if (entry.proxy) proxyManager.markBad(entry.proxy);
            return { captcha: true, results: [] };
        }
        if (entry.proxy) proxyManager.markGood(entry.proxy);

        const results = await extractResults(page, maxResults);
        return { captcha: false, results };
    } catch (err) {
        if (entry.proxy) proxyManager.markBad(entry.proxy);
        throw err;
    } finally {
        try { await page.goto('about:blank', { timeout: 5000 }); } catch (_) {}
        pool.release({ page, entry });
    }
}

async function searchWithRetry(query, maxResults) {
    for (let attempt = 0; attempt <= CONFIG.MAX_RETRIES; attempt++) {
        try {
            const r = await performSearch(query, maxResults);
            if (r.captcha) {
                logger.warn({ attempt: attempt + 1, query }, 'CAPTCHA detected');
                if (attempt === CONFIG.MAX_RETRIES) return { success: false, error: 'CAPTCHA_DETECTED' };
                await sleep(1500 * (attempt + 1));
                continue;
            }
            return { success: true, data: r.results };
        } catch (err) {
            logger.error({ err: err.message, attempt: attempt + 1 }, 'Search attempt failed');
            if (attempt === CONFIG.MAX_RETRIES) return { success: false, error: err.message };
            await sleep(1000 * (attempt + 1));
        }
    }
}

// ═══════════════════════════════════════════════════════
// 9. EXPRESS APP
// ═══════════════════════════════════════════════════════
const app = express();
app.set('trust proxy', 1);
app.use(express.json({ limit: '10kb' }));

app.use('/search', rateLimit({
    windowMs: 60 * 1000,
    max: 30,
    standardHeaders: true,
    legacyHeaders: false,
    message: { success: false, error: 'Rate limit exceeded. Thoda ruk ke try karo.' },
}));

app.get('/health', (_req, res) => {
    res.json({
        status: 'ok',
        uptime: process.uptime(),
        memory: process.memoryUsage().rss,
        poolReady: pool.browsers.size > 0,
    });
});

app.get('/stats', (_req, res) => {
    res.json({
        uptime: process.uptime(),
        pool: pool.stats(),
        queue: queue.stats(),
        cache: { size: cache.size, max: cache.max },
        proxies: proxyManager.stats(),
    });
});

app.post('/search', async (req, res) => {
    const query = (req.body?.query || '').toString().trim();
    const maxResults = Math.min(parseInt(req.body?.maxResults) || 5, CONFIG.MAX_RESULTS);

    if (!query) return res.status(400).json({ success: false, error: 'Query required' });
    if (query.length > 300) return res.status(400).json({ success: false, error: 'Query too long' });

    // ── CACHE HIT ──
    const key = cacheKey(query, maxResults);
    const cached = cache.get(key);
    if (cached) {
        logger.info({ query, cached: true }, 'Cache HIT 🎯');
        return res.json({ ...cached, cached: true });
    }

    logger.info({ query, maxResults, queue: queue.stats() }, 'New search request');

    // ── QUEUE + EXECUTE ──
    const start = Date.now();
    try {
        const result = await queue.add(() => searchWithRetry(query, maxResults));
        result.tookMs = Date.now() - start;
        result.query = query;

        if (result.success) cache.set(key, { success: true, data: result.data });

        res.json(result);
    } catch (err) {
        logger.error({ err: err.message }, 'Unhandled search error');
        res.status(500).json({ success: false, error: 'Internal error' });
    }
});

// ═══════════════════════════════════════════════════════
// 10. GRACEFUL SHUTDOWN
// ═══════════════════════════════════════════════════════
async function shutdown(signal) {
    logger.info({ signal }, 'Shutting down...');
    try { await pool.close(); } catch (_) {}
    process.exit(0);
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('uncaughtException', err => logger.fatal({ err: err.message }, 'Uncaught exception'));
process.on('unhandledRejection', err => logger.error({ err: String(err) }, 'Unhandled rejection'));

// ═══════════════════════════════════════════════════════
// 11. BOOT
// ═══════════════════════════════════════════════════════
(async () => {
    await pool.init();
    app.listen(CONFIG.PORT, () => {
        logger.info({ port: CONFIG.PORT }, `[NEXUS ENGINE] 🚀 http://localhost:${CONFIG.PORT}`);
    });
})();
