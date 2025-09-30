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

const EXTRACT_TIMEOUT_MS = 45000; // Timeout barha diya gaya hai
const FETCH_TIMEOUT_MS = 30000;   // Timeout barha diya gaya hai
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

// 🆕 Extract company name
function extractCompanyName(html) {
  const m = html.match(/<td[^>]*class=["']queryfield["'][^>]*>(.*?)<\/td>/i);
  if (m) return htmlToText(m[1]);
  return '';
}

async function extractOne(url) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), EXTRACT_TIMEOUT_MS);

  try {
    const html = await fetchRetry(url, MAX_RETRIES, FETCH_TIMEOUT_MS, 'snapshot');

    // 🆕 Company name extract
    const companyName = extractCompanyName(html);

    let mcNumber = '';
    const pats = [
      /MC[-\s]?(\d{3,7})/i,
      /MC\/MX\/FF Number\(s\):\s*MC[-\s]?(\d{3,7})/i,
      /MC\/MX Number:\s*MC[-\s]?(\d{3,7})/i,
      /MC\/MX Number:\s*(\d{3,7})/i
    ];
    for (const p of pats) {
      const m = html.match(p);
      if (m && m[1]) { mcNumber = 'MC-' + m[1]; break; }
    }
    if (!mcNumber) {
      const any = html.match(/MC[-\s]?(\d{3,7})/i);
      if (any && any[1]) mcNumber = 'MC-' + any[1];
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
              const ph = txt.match(/\(?\d{3}\)?[\s\-]*\d{3}[-\s]*\d{4}/);
              if (ph) foundPhone = ph[0];
            }
          }
          if (!foundEmail) {
            const em = regHtml.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/);
            if (em) foundEmail = em[1];
          }
          if (!foundPhone) {
            const ph2 = regHtml.match(/\(?\d{3}\)?[\s\-]*\d{3}[-\s]*\d{4}/);
            if (ph2) foundPhone = ph2[0];
          }
          email = foundEmail || '';
          if (foundPhone) phone = foundPhone;
        }
      } catch (e) {
        console.log(`[${now()}] Deep fetch error for ${url}: ${e?.message}`);
      }
    }
    return { email, mcNumber, phone, url, companyName }; // 🆕 companyName add
  } finally {
    clearTimeout(timeoutId);
  }
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
    console.log(`[${now()}] Saved → ${row.mcNumber || mc} | ${row.companyName || '(no name)'} | ${row.email || '(no email)'} | ${row.phone || '(no phone)'}`);
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
    const headers = ['email', 'mcNumber', 'phone', 'url', 'companyName']; // 🆕 header updated
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
