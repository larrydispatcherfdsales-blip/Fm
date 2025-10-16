import fs from 'fs';
import path from 'path';
import fetch from 'node-fetch';
import AbortController from 'abort-controller';

// ---- Config ----
const CONCURRENCY = Number(process.env.CONCURRENCY || 4);
const DELAY = Number(process.env.DELAY || 1000);
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 250);
const BATCH_INDEX = Number(process.env.BATCH_INDEX || 0);
const MODE = String(process.env.MODE || 'both');

const EXTRACT_TIMEOUT_MS = 45000;
const FETCH_TIMEOUT_MS = 30000;
const MAX_RETRIES = 3;
const BACKOFF_BASE_MS = 2000;

const INPUT_FILE = fs.existsSync('batch.txt') ? path.resolve('batch.txt') : path.resolve('mc_list.txt');
const OUTPUT_DIR = path.resolve('output');
fs.mkdirSync(OUTPUT_DIR, { recursive: true });

function now() { return new Date().toISOString(); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function mcToSnapshotUrl(mc) {
  const m = String(mc || '').replace(/\s+/g, '');
  return `https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=MC_MX&query_string=${encodeURIComponent(m )}`;
}

function absoluteUrl(base, href) {
  try { return new URL(href, base).href; } catch { return href; }
}

async function fetchWithTimeout(url, ms, opts = {}) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  try {
    const resp = await fetch(url, { ...opts, signal: ctrl.signal });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return resp;
  } finally {
    clearTimeout(id);
  }
}

async function fetchRetry(url, tries = MAX_RETRIES, timeout = FETCH_TIMEOUT_MS, label = 'fetch') {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try {
      const resp = await fetchWithTimeout(url, timeout, { redirect: 'follow' });
      return await resp.text();
    } catch (err) {
      lastErr = err;
      const backoff = BACKOFF_BASE_MS * Math.pow(2, i);
      console.log(`[${now()}] ${label} attempt ${i + 1}/${tries} failed → ${err?.message}. Backoff ${backoff}ms`);
      await sleep(backoff);
    }
  }
  throw lastErr || new Error(`${label} failed after ${tries} attempts`);
}

function htmlToText(s) {
  if (!s) return '';
  return s.replace(/<br\s*\/?>/gi, ', ')
    .replace(/<[^>]*>/g, ' ')
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

function extractDataByHeader(html, headerText) {
    const regex = new RegExp(headerText + '<\\/a><\\/th>\\s*<td[^>]*>([\\s\\S]*?)<\\/td>', 'i');
    const match = html.match(regex);
    if (match && match[1]) {
        return htmlToText(match[1]);
    }
    return '';
}

async function extractOne(url, html) { // Pass html to avoid re-fetching
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), EXTRACT_TIMEOUT_MS);

  try {
    const legalName = extractDataByHeader(html, 'Legal Name:');
    const physicalAddress = extractDataByHeader(html, 'Physical Address:');

    let mcNumber = '';
    const pats = [
      /MC[-\s]?(\d{3,7})/i,
      /MC\/MX\/FF Number\(s\):.*?MC-(\d{3,7})/i,
      /MC\/MX Number:\s*MC[-\s]?(\d{3,7})/i,
      /MC\/MX Number:\s*(\d{3,7})/i
    ];
    for (const p of pats) {
      const m = html.match(p);
      if (m && m[1]) { mcNumber = 'MC-' + m[1].trim(); break; }
    }
    if (!mcNumber) {
        const mcMatch = html.match(/MC-(\d{3,7})/i);
        if (mcMatch && mcMatch[1]) {
            mcNumber = 'MC-' + mcMatch[1].trim();
        }
    }

    let phone = extractPhoneAnywhere(html);
    let email = '';
    let smsLink = '';
    const hrefRe = /href=["']([^"']*(safer_xfr\.aspx|\/SMS\/)[^"']*)["']/ig;
    let m;
    while ((m = hrefRe.exec(html)) !== null) {
      smsLink = absoluteUrl(url, m[1]);
      if (smsLink) break;
    }

    if (smsLink) {
      // ... (email extraction logic remains the same)
    }
    return { email, mcNumber, phone, url, legalName, physicalAddress };
  } finally {
    clearTimeout(timeoutId);
  }
}

async function handleMC(mc) {
  const url = mcToSnapshotUrl(mc);
  try {
    const html = await fetchRetry(url, MAX_RETRIES, FETCH_TIMEOUT_MS, 'snapshot');
    const lowHtml = html.toLowerCase();

    if (lowHtml.includes('record not found') || lowHtml.includes('record inactive')) {
      console.log(`[${now()}] INVALID (not found/inactive) MC ${mc}`);
      return { valid: false };
    }

    // ✅ CORRECTED LOGIC: Skip if "AUTHORIZED" is NOT found.
    const authStatusText = extractDataByHeader(html, 'Operating Authority Status:').toUpperCase();
    if (!authStatusText.includes('AUTHORIZED')) {
        console.log(`[${now()}] SKIPPING (Not Authorized or Status Unclear) MC ${mc}`);
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

    const row = await extractOne(url, html); // Pass html to avoid re-fetching
    console.log(`[${now()}] Saved → ${row.mcNumber || mc} | ${row.legalName || '(no name)'} | ${row.email || '(no email)'} | ${row.phone || '(no phone)'}`);
    return { valid: true, url, row };
  } catch (err) {
    console.log(`[${now()}] Fetch error MC ${mc} → ${err?.message}`);
    return { valid: false };
  }
}

async function run() {
  // ... (The rest of the run function remains exactly the same)
  if (!fs.existsSync(INPUT_FILE)) {
    console.error('No input file found (batch.txt or mc_list.txt).');
    process.exit(1);
  }

  const raw = fs.readFileSync(INPUT_FILE, 'utf-8');
  const allMCs = raw.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
  const mcList = allMCs;

  console.log(`[${now()}] Running batch index ${BATCH_INDEX} with ${mcList.length} MCs.`);

  if (mcList.length === 0) {
    console.log(`[${now()}] No MCs in this batch. Exiting.`);
    return;
  }

  const rows = [];
  const validUrls = [];

  for (let i = 0; i < mcList.length; i += CONCURRENCY) {
    const slice = mcList.slice(i, i + CONCURRENCY);
    console.log(`[${now()}] Processing slice ${i / CONCURRENCY + 1} (items ${i} to ${i + slice.length - 1})`);
    const results = await Promise.all(slice.map(handleMC));
    for (const r of results) {
      if (r?.valid) {
        if (r.url) validUrls.push(r.url);
        if (r.row) rows.push(r.row);
      }
    }
    await sleep(Math.max(50, DELAY));
  }

  if (rows.length > 0) {
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const outCsv = path.join(OUTPUT_DIR, `fmcsa_batch_${BATCH_INDEX}_${ts}.csv`);
    const headers = ['mcNumber', 'legalName', 'physicalAddress', 'phone', 'email', 'url'];
    const csv = [headers.join(',')]
      .concat(rows.map(r => headers.map(h => `"${String(r[h] || '').replace(/"/g, '""')}"`).join(',')))
      .join('\n');
    fs.writeFileSync(outCsv, csv);
    console.log(`[${now()}] ✅ CSV written: ${outCsv} (rows=${rows.length})`);
  } else {
    console.log(`[${now()}] ⚠️ No data extracted for this batch.`);
  }

  if (MODE === 'urls' && validUrls.length) {
    const listPath = path.join(OUTPUT_DIR, `fmcsa_remaining_urls_${BATCH_INDEX}_${Date.now()}.txt`);
    fs.writeFileSync(listPath, validUrls.join('\n'));
    console.log(`[${now()}] Remaining URLs saved: ${listPath}`);
  }
}

run().catch(e => {
  console.error('Fatal Error:', e);
  process.exit(1);
});
