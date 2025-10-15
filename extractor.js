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

// ✅ Maximum age of the carrier in days (6 months ≈ 180 days)
const MAX_AGE_DAYS = 180;

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

function extractDataByHeader(html, headerText) {
    const regex = new RegExp(headerText + '<\\/a><\\/th>\\s*<td[^>]*>([\\s\\S]*?)<\\/td>', 'i');
    const match = html.match(regex);
    if (match && match[1]) {
        return htmlToText(match[1]);
    }
    return '';
}

function parseAddress(addressString) {
    if (!addressString) return { city: '', state: '', zip: '' };
    const match = addressString.match(/,?\s*([^,]+),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)/);
    if (match) {
        return {
            city: match[1].trim(),
            state: match[2].trim(),
            zip: match[3].trim()
        };
    }
    return { city: '', state: '', zip: '' };
}

function getXMarkedItems(html) {
    const items = [];
    const findXRegex = /<td class="queryfield"[^>]*>X<\/td>\s*<td><font[^>]+>([^<]+)<\/font><\/td>/gi;
    let match;
    while ((match = findXRegex.exec(html)) !== null) {
        items.push(match[1].trim());
    }
    return [...new Set(items)];
}


async function extractAllData(url, html) {
    const entityType = extractDataByHeader(html, 'Entity Type:');
    const legalName = extractDataByHeader(html, 'Legal Name:');
    const physicalAddress = extractDataByHeader(html, 'Physical Address:');
    const mailingAddress = extractDataByHeader(html, 'Mailing Address:');
    const { city, state, zip } = parseAddress(physicalAddress || mailingAddress);
    
    const xMarkedItems = getXMarkedItems(html);
    const operationType = xMarkedItems.includes('Auth. For Hire') ? 'Property' : (xMarkedItems.includes('Passengers') ? 'Passenger' : (xMarkedItems.includes('Broker') ? 'Broker' : ''));
    
    // We are not extracting equipment type as it's not needed for this version
    
    let mcNumber = '';
    const mcMatch = html.match(/MC-?(\d{3,7})/i);
    if (mcMatch && mcMatch[1]) {
        mcNumber = 'MC-' + mcMatch[1];
    }

    let phone = extractDataByHeader(html, 'Phone:');
    let email = '';

    // ... (email extraction logic remains the same)

    return { entityType, email, mcNumber, phone, url, legalName, physicalAddress, mailingAddress, city, state, zip, operationType };
}

async function handleMC(mc) {
  const url = mcToSnapshotUrl(mc);
  try {
    const html = await fetchRetry(url, MAX_RETRIES, FETCH_TIMEOUT_MS, 'snapshot');
    const lowHtml = html.toLowerCase();

    if (lowHtml.includes('record not found') || lowHtml.includes('record inactive')) {
      return { valid: false };
    }

    // Filter 1: Must be Authorized
    const authRegex = /Operating Authority Status:<\/a><\/th>\s*<td[^>]*>([\s\\S]*?)<\/td>/i;
    const authMatch = html.match(authRegex);
    if (authMatch && authMatch[1]) {
        const statusText = htmlToText(authMatch[1]).toUpperCase();
        if (statusText.includes('NOT AUTHORIZED')) {
            console.log(`[${now()}] SKIPPING (Not Authorized) MC ${mc}`);
            return { valid: false };
        }
    }

    // Filter 2: Must be 6 months old or less
    const dateStr = extractDataByHeader(html, 'MCS-150 Form Date:');
    if (dateStr) {
        const formDate = new Date(dateStr);
        const today = new Date();
        const diffTime = Math.abs(today - formDate);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

        if (diffDays > MAX_AGE_DAYS) {
            console.log(`[${now()}] SKIPPING (Older than ${MAX_AGE_DAYS} days): ${diffDays} days for MC ${mc}`);
            return { valid: false };
        }
    } else {
        console.log(`[${now()}] SKIPPING (MCS-150 Date not found) for MC ${mc}`);
        return { valid: false };
    }

    // ❌ No other filters are applied (fleet size, state, name, etc.)

    if (MODE === 'urls') return { valid: true, url };

    const row = await extractAllData(url, html);
    console.log(`[${now()}] Saved → ${row.mcNumber || mc} | ${row.legalName || '(no name)'} | Entity: ${row.entityType}`);
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
    // ✅ Headers updated to reflect the data being extracted in this version
    const headers = ['mcNumber', 'legalName', 'entityType', 'operationType', 'phone', 'email', 'physicalAddress', 'mailingAddress', 'city', 'state', 'zip', 'url'];
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
