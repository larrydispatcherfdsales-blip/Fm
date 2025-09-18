// ===== extractor.js =====
// FMCSA scraping headlessly (Node >= 18, global fetch).
// Reads MCs from batch.txt (if present), otherwise mc_list.txt.
// Outputs CSV to ./output/fmcsa_batch_<batchIndex>_<timestamp>.csv
// Config via env vars: CONCURRENCY, DELAY, BATCH_SIZE, WAIT_SECONDS, MODE, BATCH_INDEX

import fs from 'fs';
import path from 'path';

// ---- Config ----
const CONCURRENCY = Number(process.env.CONCURRENCY || 4);
const DELAY = Number(process.env.DELAY || 1000); // ms between waves
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 250);
const WAIT_SECONDS = Number(process.env.WAIT_SECONDS || 0);
const MODE = String(process.env.MODE || 'both'); // 'both' or 'urls'
const BATCH_INDEX = Number(process.env.BATCH_INDEX || 0);

const EXTRACT_TIMEOUT_MS = 20000;
const FETCH_TIMEOUT_MS = 20000;
const MAX_RETRIES = 3;
const BACKOFF_BASE_MS = 2000;

const INPUT_FILE = fs.existsSync('batch.txt') ? path.resolve('batch.txt') : path.resolve('mc_list.txt');
const OUTPUT_DIR = path.resolve('output');
fs.mkdirSync(OUTPUT_DIR, { recursive: true });

function now() { return new Date().toISOString(); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function mcToSnapshotUrl(mc) {
  const m = String(mc || '').replace(/\s+/g, '');
  return `https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=MC_MX&query_string=${encodeURIComponent(m)}`;
}

function absoluteUrl(base, href) {
  try { return new URL(href, base).href; } catch { return href; }
}

async function fetchWithTimeout(url, ms, opts = {}) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try {
    return await fetch(url, { ...opts, signal: ctrl.signal });
  } finally { clearTimeout(id); }
}

async function fetchRetry(url, tries = MAX_RETRIES, timeout = FETCH_TIMEOUT_MS, label = 'fetch') {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try {
      const resp = await fetchWithTimeout(url, timeout, { redirect: 'follow' });
      if (!resp.ok) throw new Error(`${label} HTTP ${resp.status}`);
      return await resp.text();
    } catch (err) {
      lastErr = err;
      const backoff = BACKOFF_BASE_MS * Math.pow(2, i);
      console.log(`[${now()}] ${label} attempt ${i + 1}/${tries} failed → ${err?.message}. Backoff ${backoff}ms`);
      await sleep(backoff);
    }
  }
  throw lastErr || new Error(`${label} failed`);
}

function htmlToText(s) {
  if (!s) return '';
  return s.replace(/<[^>]*>/g, ' ')
          .replace(/&nbsp;/g, ' ')
          .replace(/&amp;/g, '&')
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>')
          .replace(/\s+/g, ' ')
          .trim();
}

function extractPhoneAnywhere(html) {
  const m = html.match(/\(?\d{3}\)?[\s\-.]*\d{3}[\s\-.]*\d{4}/);
  return m ? m[0] : '';
}

async function extractOne(url) {
  const timer = setTimeout(() => { throw new Error('Extraction timed out'); }, EXTRACT_TIMEOUT_MS);
  try {
    const html = await fetchRetry(url, MAX_RETRIES, FETCH_TIMEOUT_MS, 'snapshot');

    // MC number
    let mcNumber = '';
    const pats = [
      /MC[-\s]?(\d{3,7})/i,
      /MC\/MX\/FF Number\(s\):\s*MC[-\s]?(\d{3,7})/i,
      /MC\/MX Number:\s*MC[-\s]?(\d{3,7})/i,
      /MC\/MX Number:\s*(\d{3,7})/i,
    ];
    for (const p of pats) {
      const m = html.match(p);
      if (m && m[1]) { mcNumber = 'MC-' + m[1]; break; }
    }
    if (!mcNumber) {
      const any = html.match(/MC[-\s]?(\d{3,7})/i);
      if (any && any[1]) mcNumber = 'MC-' + any[1];
    }

    // Phone guess
    let phone = extractPhoneAnywhere(html);

    // SMS / Registration for email
    let email = '';
    let smsLink = '';
    const hrefRe = /href=["']([^"']*(safer_xfr\.aspx|\/SMS\/)[^"']*)["']/ig;
    let m;
    while ((m = hrefRe.exec(html)) !== null) {
      smsLink = absoluteUrl(url, m[1]);
      if (smsLink) break;
    }

    if (smsLink) {
      await sleep(300);
      try {
        const smsHtml = await fetchRetry(smsLink, MAX_RETRIES, FETCH_TIMEOUT_MS, 'sms');
        let regLink = '';
        const regRe = /href=["']([^"']*CarrierRegistration\.aspx[^"']*)["']/ig;
        while ((m = regRe.exec(smsHtml)) !== null) {
          regLink = absoluteUrl(smsLink, m[1]);
          if (regLink) break;
        }
        if (regLink) {
          await sleep(300);
          const regHtml = await fetchRetry(regLink, MAX_RETRIES, FETCH_TIMEOUT_MS, 'registration');
          const spanRe = /<span[^>]*class=["']dat["'][^>]*>([\s\S]*?)<\/span>/ig;
          let foundEmail = '', foundPhone = '';
          let s;
          while ((s = spanRe.exec(regHtml)) !== null) {
            const txt = htmlToText(s[1] || '');
            if (!foundEmail && /@/.test(txt)) foundEmail = txt;
            if (!foundPhone) {
              const ph = txt.match(/\(?\d{3}\)?[\s\-]*\d{3}[\s\-]*\d{4}/);
              if (ph) foundPhone = ph[0];
            }
          }
          if (!foundEmail) {
            const em = regHtml.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
            if (em) foundEmail = em[1];
          }
          if (!foundPhone) {
            const ph2 = regHtml.match(/\(?\d{3}\)?[\s\-]*\d{3}[\s\-]*\d{4}/);
            if (ph2) foundPhone = ph2[0];
          }
          email = foundEmail || '';
          if (foundPhone) phone = foundPhone;
        }
      } catch (e) {
        console.log(`[${now()}] Deep fetch error: ${e?.message}`);
      }
    }

    return { email, mcNumber, phone, url };
  } finally { clearTimeout(timer); }
}

async function handleMC(mc) {
  const url = mcToSnapshotUrl(mc);
  try {
    const html = await fetchRetry(url, MAX_RETRIES, FETCH_TIMEOUT_MS, 'snapshot');
    const low = html.toLowerCase();
    if (low.includes('record not found') || low.includes('record inactive')) {
      console.log(`[${now()}] INVALID (not found/inactive) MC ${mc}`);
      return { valid: false };
    }
    const puMatch = html.match(/Power\s*Units[^0-9]*([0-9,]+)/i);
    if (puMatch) {
      const n = Number((puMatch[1] || '').replace(/,/g, ''));
      if (!isNaN(n) && n === 0) {
        console.log(`[${now()}] INVALID (PU=0) MC ${mc}`);
        return { valid: false };
      }
    }

    if (MODE === 'urls') return { valid: true, url };

    const row = await extractOne(url);
    console.log(`[${now()}] Saved → ${row.mcNumber || ''} | ${row.email || '(no email)'} | ${row.phone || ''}`);
    return { valid: true, url, row };
  } catch (err) {
    console.log(`[${now()}] Fetch error MC ${mc} → ${err?.message}`);
    return { valid: false };
  }
}

async function run() {
  if (!fs.existsSync(INPUT_FILE)) {
    console.error('No input file found (batch.txt or mc_list.txt).');
    process.exit(1);
  }

  const raw = fs.readFileSync(INPUT_FILE, 'utf-8');
  const allMCs = raw.split(/\r?\n/).map(s => s.trim()).filter(Boolean);

  const start = BATCH_INDEX * BATCH_SIZE;
  const end = start + BATCH_SIZE;
  const mcList = allMCs.slice(start, end);

  console.log(`[${now()}] Total MCs: ${allMCs.length}, running batch ${BATCH_INDEX} → ${start}–${end - 1}, count=${mcList.length}`);

  if (mcList.length === 0) {
    console.log(`[${now()}] No MCs in this batch. Exiting.`);
    return;
  }

  const rows = [];
  const validUrls = [];

  for (let i = 0; i < mcList.length; i += CONCURRENCY) {
    const slice = mcList.slice(i, i + CONCURRENCY);
    const results = await Promise.all(slice.map(handleMC));
    for (const r of results) {
      if (r?.valid) {
        if (r.url) validUrls.push(r.url);
        if (r.row) rows.push(r.row);
      }
    }
    await sleep(Math.max(50, DELAY));
  }

  // Write CSV for this run
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const outCsv = path.join(OUTPUT_DIR, `fmcsa_batch_${BATCH_INDEX}_${ts}.csv`);
  const headers = ['email', 'mcNumber', 'phone', 'url'];
  const csv = [headers.join(',')]
    .concat(rows.map(r => headers.map(h => `"${String(r[h] || '').replace(/"/g, '""')}"`).join(',')))
    .join('\n');
  fs.writeFileSync(outCsv, csv);
  console.log(`[${now()}] CSV written: ${outCsv} (rows=${rows.length})`);

  if (MODE === 'urls' && validUrls.length) {
    const listPath = path.join(OUTPUT_DIR, `fmcsa_remaining_urls_${BATCH_INDEX}_${Date.now()}.txt`);
    fs.writeFileSync(listPath, validUrls.join('\n'));
    console.log(`[${now()}] Remaining URLs saved: ${listPath}`);
  }
}

run().catch(e => {
  console.error('Fatal:', e);
  process.exit(1);
});
