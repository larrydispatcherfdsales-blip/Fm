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

// ✅ Minimum age of the carrier in days (6 months ≈ 180 days)
const MIN_AGE_DAYS = 180;

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
    const regex = new RegExp(`>${headerText}<\\/a><\\/th>\\s*<td[^>]*>([\\s\\S]*?)<\\/td>`, 'i');
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
    const dbaName = extractDataByHeader(html, 'DBA Name:');
    const physicalAddress = extractDataByHeader(html, 'Physical Address:');
    const mailingAddress = extractDataByHeader(html, 'Mailing Address:');
    const { city, state, zip } = parseAddress(physicalAddress || mailingAddress);
    
    const xMarkedItems = getXMarkedItems(html);
    const operationType = xMarkedItems.includes('Auth. For Hire') ? 'Property' : (xMarkedItems.includes('Passengers') ? 'Passenger' : (xMarkedItems.includes('Broker') ? 'Broker' : ''));
    
    let mcNumber = '';
    const mcMatch = html.match(/MC-(\d{3,7})/i);
    if (mcMatch && mcMatch[1]) {
        mcNumber = 'MC-' + mcMatch[1];
    }

    let phone = extractDataByHeader(html, 'Phone:');
    let email = '';

    // ✅✅✅ CORRECTED: Email deep-fetch logic is now included ✅✅✅
    const smsLinkMatch = html.match(/href=["']([^"']*(safer_xfr\.aspx|\/SMS\/)[^"']*)["']/i);
    if (smsLinkMatch && smsLinkMatch[1]) {
        const smsLink = absoluteUrl(url, smsLinkMatch[1]);
        await sleep(300);
        try {
            const smsHtml = await fetchRetry(smsLink, MAX_RETRIES, FETCH_TIMEOUT_MS, 'sms');
            const regLinkMatch = smsHtml.match(/href=["']([^"']*CarrierRegistration\.aspx[^"']*)["']/i);
            if (regLinkMatch && regLinkMatch[1]) {
                const regLink = absoluteUrl(smsLink, regLinkMatch[1]);
                await sleep(300);
                const regHtml = await fetchRetry(regLink, MAX_RETRIES, FETCH_TIMEOUT_MS, 'registration');
                const emailMatch = regHtml.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
                if (emailMatch) email = emailMatch[1];
            }
        } catch (e) {
            console.log(`[${now()}] Deep fetch error for ${url}: ${e?.message}`);
        }
    }

    return { entityType, legalName, dbaName, mcNumber, phone, email, physicalAddress, mailingAddress, city, state, zip, operationType, url };
}

async function handleMC(mc) {
  const url = mcToSnapshotUrl(mc);
  try {
    const html = await fetchRetry(url, MAX_RETRIES, FETCH_TIMEOUT_MS, 'snapshot');
    const upperCaseHtml = html.toUpperCase();

    if (upperCaseHtml.includes('RECORD NOT FOUND') || upperCaseHtml.includes('RECORD INACTIVE')) {
      return { valid: false };
    }

    // Filter 1: Authorization Status
    const authStatusText = extractDataByHeader(html, 'Operating Authority Status:').toUpperCase();
    if (authStatusText.includes('NOT AUTHORIZED') || !authStatusText.includes('AUTHORIZED')) {
        console.log(`[${now()}] SKIPPING (Not Authorized) MC ${mc}`);
        return { valid: false };
    }

    // Filter 2: Carrier Age (6+ months)
    const dateStr = extractDataByHeader(html, 'MCS-150 Form Date:');
    if (dateStr) {
        const formDate = new Date(dateStr);
        const today = new Date();
        const diffTime = Math.abs(today - formDate);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        if (diffDays < MIN_AGE_DAYS) {
            console.log(`[${now()}] SKIPPING (Newer than ${MIN_AGE_DAYS} days): ${diffDays} days for MC ${mc}`);
            return { valid: false };
        }
    } else {
        console.log(`[${now()}] SKIPPING (MCS-150 Date not found) for MC ${mc}`);
        return { valid: false };
    }

    // Filter 3: Power Units (min 1)
    const puText = extractDataByHeader(html, 'Power Units:');
    const powerUnits = Number(puText.replace(/,/g, ''));
    if (isNaN(powerUnits) || powerUnits < 1) {
        console.log(`[${now()}] SKIPPING (PU < 1): ${puText || 'N/A'} units for MC ${mc}`);
        return { valid: false };
    }

    // Filter 4: Drivers (min 1)
    const driverText = extractDataByHeader(html, 'Drivers:');
    const drivers = Number(driverText.replace(/,/g, ''));
    if (isNaN(drivers) || drivers < 1) {
        console.log(`[${now()}] SKIPPING (Drivers < 1): ${driverText || 'N/A'} drivers for MC ${mc}`);
        return { valid: false };
    }

    if (MODE === 'urls') return { valid: true, url };

    const row = await extractAllData(url, html);
    console.log(`[${now()}] SAVED → ${row.mcNumber || mc} | ${row.legalName || '(no name)'} | Email: ${row.email || 'N/A'}`);
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
    const headers = ['mcNumber', 'legalName', 'dbaName', 'entityType', 'operationType', 'phone', 'email', 'physicalAddress', 'mailingAddress', 'city', 'state', 'zip', 'url'];
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
