
async function finalizeBatchAndDownload(){
  const rows = (await load('extractedData'))['extractedData'] || [];
  if (!rows.length) return;
  const headers = ['email','mcNumber','phone','url'];
  const csv = [headers.join(',')]
    .concat(rows.map(r => headers.map(h=>`"${String(r[h]||'').replace(/"/g,'""')}"`).join(',')))
    .join('\n');
  const ts = new Date().toISOString().replace(/[:.]/g,'-');
  const filename = `fmcsa_batch_${ts}.csv`;
  const dataUrl = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv);
  try {
    await new Promise((resolve)=>{
      chrome.downloads.download({ url: dataUrl, filename, saveAs: false }, ()=> resolve(true));
    });
    pushLog('Batch CSV downloaded: ' + filename);
  } catch(e){ pushLog('Batch CSV download failed: ' + (e.message||e)); }
  // Reset extracted + logs for next batch
  await save({ 'extractedData': [], 'extractedDataMap': {}, 'logBuffer': [], 'remainingUrls': [] });
}

// FMCSA All-in-One — v2.4.0
// What's new in v2.4.0:
// - Robust retry with exponential backoff on *every* network step (snapshot, SMS, registration).
// - Per-URL extraction timeout + auto-skip (never freezes the pipeline).
// - Heartbeat keepalive (1 min) + existing keepalive (4 min) for MV3 Service Worker.
// - Watchdog: detects stalled extractor (no progress) and force-unblocks queue.
// - Batch+Pause loop continues automatically across the entire MC list.
// - Immediate tab close for invalid/not-found/PU=0 pages; valid tabs close after extraction.
// - Defensive inFlightExtract decrement in finally; strong logging.
//
// NOTE: Tabs are optional (visualization). Extraction uses fetch() with retries & timeouts.

const SKEY = {
  PIPE_STATE: 'pipeState',
  EXTRACT_DATA: 'extractedData',
  EXTRACT_MAP: 'extractedDataMap',
  REMAINING_URLS: 'remainingUrls',
  LOG: 'logBuffer',
  HEARTBEAT: 'heartbeat'
};

const EXTRACT_TIMEOUT_MS = 20000;   // per URL extraction timeout
const FETCH_TIMEOUT_MS = 20000;     // per fetch timeout
const MAX_RETRIES = 3;
const BACKOFF_BASE_MS = 2000;       // 2s, 4s, 8s

function now() { return new Date().toLocaleTimeString(); }
function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }

async function save(obj){ return new Promise(res=>chrome.storage.local.set(obj,res)); }
async function load(keys){ return new Promise(res=>chrome.storage.local.get(keys,res)); }

function pushLog(line){
  chrome.storage.local.get(['logBuffer'], (res)=>{
    const buf = res['logBuffer'] || [];
    buf.unshift(`[${now()}] ${line}`);
    if (buf.length > 2000) buf.length = 2000;
    chrome.storage.local.set({ 'logBuffer': buf, 'heartbeat': Date.now() });
    chrome.runtime.sendMessage({ action: 'status', text: line });
  });
}

// Keepalive (MV3) — dual strategy
chrome.alarms.create('keepalive_4m', { periodInMinutes: 4 });
chrome.alarms.create('heartbeat_1m', { periodInMinutes: 1 });
chrome.alarms.create('extract_watchdog', { periodInMinutes: 1 });

chrome.alarms.onAlarm.addListener(async a => {
  if (a.name === 'keepalive_4m' || a.name === 'heartbeat_1m') {
    await save({ 'heartbeat': Date.now() });
    await load('pipeState');
    return;
  }
  if (a.name === 'extract_watchdog') {
    const st = (await load('pipeState'))['pipeState'] || defaultState;
    if (!st.running) return;
    const last = st.lastProgressTs || 0;
    const idleFor = Date.now() - last;
    if (idleFor > 70000) {
      const before = st.inFlightExtract;
      st.inFlightExtract = 0;
      await save({ 'pipeState': st });
      pushLog(`Watchdog: extractor idle ${Math.round(idleFor/1000)}s → inFlight reset (${before}→0), restarting loops.`);
      driver();
      if (st.mode === 'fetch_and_extract') runExtractorLoop();
    }
    return;
  }
  if (a.name === 'batchResume') {
    const st = (await load('pipeState'))['pipeState'] || defaultState;
    if (!st.running || st.stopped) return;
    if (st.fetchPaused) {
      st.fetchPaused = false;
      st.batchStartIndex = st.index;
      await save({ 'pipeState': st });
      pushLog('Auto-resume after timer.');
      chrome.runtime.sendMessage({ action:'status', text:'Auto-resumed after timer.', event:'progress', processed:st.processed, remaining:st.validCount, extracted:st.extractedCount });
      driver();
      if (st.mode === 'fetch_and_extract') runExtractorLoop();
    }
    return;
  }
});

/******************** State ********************/
const defaultState = {
  running: false,
  paused: false,
  fetchPaused: false,
  stopped: false,
  mode: 'fetch_only',
  mcList: [],
  index: 0,
  batchSize: 500,
  autoPause: true,
  waitSeconds: 0,
  batchStartIndex: 0,
  openTabs: false,
  tabsWindowId: null,
  urlTabMap: {},
  concurrency: 6,
  delay: 300,

  processed: 0,
  validCount: 0,
  extractedCount: 0,

  validUrls: [],
  toExtract: [],
  inFlightExtract: 0,

  fetchingComplete: false,
  lastProgressTs: 0
};

/******************** Helpers ********************/
function mcToSnapshotUrl(mc){
  const m = (mc||'').replace(/\s+/g,'');
  return `https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=MC_MX&query_string=${encodeURIComponent(m)}`;
}
function htmlToText(s){
  if (!s) return '';
  let t = s.replace(/<[^>]*>/g,' ');
  t = t.replace(/&nbsp;/g,' ').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>');
  return t.replace(/\s+/g,' ').trim();
}
function cleanPhone(raw){
  const txt = htmlToText(raw);
  const m = txt.match(/\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}/);
  return m ? m[0] : '';
}
function absoluteUrl(base, href){
  try { return new URL(href, base).href; } catch(e){ return href; }
}

/******************** Tab management ********************/
async function ensureTabsWindow(st){
  if (st.tabsWindowId) return st.tabsWindowId;
  const w = await chrome.windows.create({ url: 'about:blank' });
  st.tabsWindowId = w.id;
  await save({ 'pipeState': st });
  return st.tabsWindowId;
}
async function openVisualTabForUrl(st, url){
  try{
    const winId = await ensureTabsWindow(st);
    const tab = await chrome.tabs.create({ windowId: winId, url, active: false });
    const st2 = (await load('pipeState'))['pipeState'] || defaultState;
    st2.urlTabMap = st2.urlTabMap || {};
    st2.urlTabMap[url] = tab.id;
    await save({ 'pipeState': st2 });
    return tab.id;
  } catch(e){
    pushLog('Tab open error: ' + (e.message||e));
    return null;
  }
}
async function closeTabForUrl(url){
  try{
    const st = (await load('pipeState'))['pipeState'] || defaultState;
    const map = st.urlTabMap || {};
    const tabId = map[url];
    if (tabId){
      try{ await chrome.tabs.remove(tabId); }catch(e){}
      delete map[url];
      await save({ 'pipeState': { ...st, urlTabMap: map } });
    }
  } catch(e){}
}
async function closeAllTabsWindow(){
  try{
    const st = (await load('pipeState'))['pipeState'] || defaultState;
    if (st.tabsWindowId){
      try{ await chrome.windows.remove(st.tabsWindowId); }catch(e){}
      st.tabsWindowId = null;
      st.urlTabMap = {};
      await save({ 'pipeState': st });
    }
  } catch(e){}
}

/******************** Retry + Timeout ********************/
function withTimeout(promise, ms, label){
  let to;
  const timeout = new Promise((_res, rej)=>{
    to = setTimeout(()=> rej(new Error(`${label||'Task'} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise.finally(()=>clearTimeout(to)), timeout]);
}
async function fetchWithTimeout(url, ms, opts={}){
  const ctrl = new AbortController();
  const id = setTimeout(()=>ctrl.abort(), ms);
  try{
    const resp = await fetch(url, { credentials: 'omit', signal: ctrl.signal, ...opts });
    return resp;
  } finally {
    clearTimeout(id);
  }
}
async function fetchRetry(url, tries=MAX_RETRIES, timeout=FETCH_TIMEOUT_MS, label='fetch'){
  let lastErr;
  for (let i=0; i<tries; i++){
    try{
      const resp = await fetchWithTimeout(url, timeout);
      if (!resp.ok) throw new Error(`${label} HTTP ${resp.status}`);
      return await resp.text();
    } catch(err){
      lastErr = err;
      const backoff = BACKOFF_BASE_MS * Math.pow(2, i);
      pushLog(`${label} attempt ${i+1}/${tries} failed → ${err && err.message}. Backoff ${backoff}ms`);
      await new Promise(r=>setTimeout(r, backoff));
    }
  }
  throw lastErr || new Error(`${label} failed`);
}

/******************** Extractor ********************/
async function addExtractedRow(row){
  try {
    if (!row) return false;
    let mcDigits = null;
    if (row.mcNumber){
      const m = String(row.mcNumber).match(/(\d+)/);
      if (m) mcDigits = m[1].replace(/^0+/, '') || m[1];
    }
    let key;
    if (mcDigits) key = 'MC-' + mcDigits;
    else if (row.url) key = String(row.url).trim();
    else key = JSON.stringify(row);

    const storedMap = (await load('extractedDataMap'))['extractedDataMap'] || {};
    if (storedMap[key]) { pushLog('addExtractedRow: duplicate skipped → ' + key); return false; }
    storedMap[key] = row;
    const arr = Object.values(storedMap);
    await save({ 'extractedDataMap': storedMap, 'extractedData': arr });
    const ps = (await load('pipeState'))['pipeState'] || {};
    ps.extractedCount = arr.length;
    ps.lastProgressTs = Date.now();
    await save({ 'pipeState': ps });
    pushLog('addExtractedRow: saved → ' + key);
    return true;
  } catch (e){
    pushLog('addExtractedRow error: ' + (e && e.message));
    return false;
  }
}

async function extractOne(url){
  try{
    pushLog(`Extracting: ${url}`);
    const html = await fetchRetry(url, MAX_RETRIES, FETCH_TIMEOUT_MS, 'snapshot');
    // MC
    let mcNumber = '';
    const patterns = [
      /MC[-\s]?(\d{3,7})/i,
      /MC\/MX\/FF Number\(s\):\s*MC[-\s]?(\d{3,7})/i,
      /MC\/MX\/FF Number\(s\):\s*(\d{3,7})/i,
      /MC\/MX Number:\s*(\d{3,7})/i,
      /MC\/MX Number:\s*MC[-\s]?(\d{3,7})/i
    ];
    for (const p of patterns){
      const m = html.match(p);
      if (m && m[1]) { mcNumber = 'MC-' + m[1]; break; }
    }
    if (!mcNumber){
      const any = html.match(/MC[-\s]?(\d{3,7})/i);
      if (any && any[1]) mcNumber = 'MC-' + any[1];
    }

    // Phone
    let phone = '';
    const phoneMatch = html.match(/Phone:\s*([^)()\d\-+\s]{0,3}?\(?\d{3}\)?[\s\-]*\d{3}[-\s]*\d{4})/i) 
                    || html.match(/\(?\d{3}\)?[\s\-]*\d{3}[-\s]*\d{4}/);
    if (phoneMatch) phone = (function(raw){const txt = raw.replace(/<[^>]*>/g,' '); const m = txt.match(/\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}/); return m?m[0]:'';})(phoneMatch[1] ? phoneMatch[1] : phoneMatch[0]);

    // SMS link
    let smsLink = '';
    const hrefRe = /href=["']([^"']*(safer_xfr\.aspx|\/SMS\/safer_xfr\.aspx|\/SMS\/Carrier\/)[^"']*)["']/ig;
    let m;
    while ((m = hrefRe.exec(html)) !== null) {
      const href = m[1];
      if (href && href.toLowerCase().includes('safer_xfr.aspx')) { smsLink = absoluteUrl(url, href); break; }
      if (href && href.toLowerCase().includes('/sms/')) { smsLink = absoluteUrl(url, href); break; }
    }
    if (!smsLink) {
      const anchorTextRe = /<a[^>]*>([\s\S]{0,200}?)<\/a>/ig;
      while ((m = anchorTextRe.exec(html)) !== null) {
        const aHtml = m[0];
        if (/SMS Results/i.test(aHtml)) {
          const h = aHtml.match(/href=["']([^"']+)["']/i);
          if (h && h[1]) { smsLink = absoluteUrl(url, h[1]); break; }
        }
      }
    }

    let email = '';
    if (smsLink){
      await new Promise(r=>setTimeout(r,300));
      try{
        const smsHtml = await fetchRetry(smsLink, MAX_RETRIES, FETCH_TIMEOUT_MS, 'sms');
        let regLink = '';
        const regRe = /href=["']([^"']*(CarrierRegistration\.aspx|\/Carrier\/\d+\/CarrierRegistration\.aspx)[^"']*)["']/ig;
        while ((m = regRe.exec(smsHtml)) !== null) {
          regLink = m[1];
          if (regLink) { regLink = absoluteUrl(smsLink, regLink); break; }
        }
        if (!regLink) {
          const anchorTextRe2 = /<a[^>]*>([\s\S]{0,400}?)<\/a>/ig;
          while ((m = anchorTextRe2.exec(smsHtml)) !== null) {
            const aHtml = m[0];
            if (/Carrier Registration Details/i.test(aHtml)) {
              const h = aHtml.match(/href=["']([^"']+)["']/i);
              if (h && h[1]) { regLink = absoluteUrl(smsLink, h[1]); break; }
            }
          }
        }
        if (regLink){
          await new Promise(r=>setTimeout(r,300));
          const regHtml = await fetchRetry(regLink, MAX_RETRIES, FETCH_TIMEOUT_MS, 'registration');
          const spanRe = /<span[^>]*class=["']dat["'][^>]*>([\s\S]*?)<\/span>/ig;
          let foundEmail='', foundPhone='';
          while ((m = spanRe.exec(regHtml)) !== null) {
            const txt = (m[1]||'').replace(/<[^>]*>/g,' ').replace(/\s+/g,' ').trim();
            if (!foundEmail && /@/.test(txt)) foundEmail = txt;
            if (!foundPhone){
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
      } catch(e){
        pushLog('Deep fetch error (sms/registration): ' + (e.message||e));
      }
    }

    const row = { email: email||'', mcNumber: mcNumber||'', phone: phone||'', url };
    await addExtractedRow(row);
    pushLog(`Saved → ${row.mcNumber || ''} | ${row.email || '(no email)'} | ${row.phone || ''}`);
  } catch(err){
    pushLog('Extract error: ' + (err.message||err));
  } finally {
    await closeTabForUrl(url);
    const st = (await load('pipeState'))['pipeState'] || defaultState;
    st.lastProgressTs = Date.now();
    await save({ 'pipeState': st });
  }
}

async function extractOneWithTimeout(url){
  try{
    await withTimeout(extractOne(url), EXTRACT_TIMEOUT_MS, 'Extraction');
  } catch(e){
    pushLog(`Extraction timeout/skip: ${url} — ${e && e.message}`);
  } finally {
    await closeTabForUrl(url);
  }
}

/******************** Fetcher (with optional tab opening) ********************/
async function handleMC(mc, st){
  const url = mcToSnapshotUrl(mc);
  if (st.openTabs) await openVisualTabForUrl(st, url);
  try{
    pushLog(`Fetching MC ${mc} …`);
    const html = await fetchRetry(url, MAX_RETRIES, FETCH_TIMEOUT_MS, 'snapshot');
    const low = html.toLowerCase();
    if (low.includes('record not found') || low.includes('record inactive')) {
      await closeTabForUrl(url);
      pushLog('INVALID (not found/inactive) → closed tab.');
      st.lastProgressTs = Date.now();
      await save({ 'pipeState': st });
      return { valid: false };
    }
    // Power Units = 0 filter
    let puZero = false;
    const puMatch = html.match(/Power\s*Units[^0-9]*([0-9,]+)/i);
    if (puMatch){
      const n = Number((puMatch[1]||'').replace(/,/g,''));
      if (!isNaN(n) && n === 0) puZero = true;
    }
    if (puZero){
      await closeTabForUrl(url);
      pushLog('INVALID (PU=0) → closed tab.');
      st.lastProgressTs = Date.now();
      await save({ 'pipeState': st });
      return { valid: false };
    }

    const abs = url;
    const remRes = await load('remainingUrls');
    const rem = remRes['remainingUrls'] || [];
    rem.push(abs);
    await save({ 'remainingUrls': rem });

    st.validCount += 1;
    st.validUrls.push(abs);
    st.toExtract.push(abs);
    st.lastProgressTs = Date.now();
    await save({ 'pipeState': st });

    chrome.runtime.sendMessage({ action: 'status', event: 'progress', processed: st.processed, remaining: st.validCount, extracted: st.extractedCount });
    pushLog(`VALID → ${abs}`);

    if (st.mode === 'fetch_and_extract') runExtractorLoop();
    return { valid: true, url: abs };
  } catch(err){
    pushLog('Fetch error: ' + (err.message||err));
    await closeTabForUrl(url);
    const st2 = (await load('pipeState'))['pipeState'] || defaultState;
    st2.lastProgressTs = Date.now();
    await save({ 'pipeState': st2 });
    return { valid: false, error: err.message||String(err) };
  }
}

/******************** Pipeline Driver ********************/
async function driver(){
  let st = (await load('pipeState'))['pipeState'] || defaultState;
  if (!st.running || st.stopped || st.paused || st.fetchPaused) return;

  if (st.autoPause && st.batchSize > 0){
    const processedInBatch = st.index - st.batchStartIndex;
    if (processedInBatch >= st.batchSize){
      st.fetchPaused = true;
      await save({ 'pipeState': st });
      let msg = 'Batch completed. Extraction will continue. ';
      if (st.waitSeconds > 0){
        const when = Date.now() + st.waitSeconds * 1000;
        chrome.alarms.create('batchResume', { when });
        msg += `Timer started (${st.waitSeconds}s). Will auto-resume.`;
      } else {
        msg += 'Please change VPN and click Resume.';
      }
      chrome.runtime.sendMessage({ action: 'status', text: msg, event: 'awaiting_vpn_change' });
      pushLog(msg);
      return;
    }
  }

  const inflight = [];
  for (let i=0; i<st.concurrency; i++){
    if (st.index >= st.mcList.length) break;
    const mc = st.mcList[st.index++];
    st.processed = st.index;
    inflight.push(handleMC(mc, st));
  }
  await save({ 'pipeState': st });

  if (inflight.length){
    await Promise.all(inflight);
    await new Promise(r=>setTimeout(r, Math.max(50, st.delay)));
    driver();
  } else {
    if (st.mode === 'fetch_and_extract') {
      st.fetchingComplete = true;
      await save({ 'pipeState': st });
      chrome.runtime.sendMessage({ action: 'status', text: `Fetching complete. Waiting for extractor to finish...`, event: 'fetching_complete', remaining: st.validCount, extracted: st.extractedCount });
      pushLog('Fetching complete; extractor will finish remaining items.');
      runExtractorLoop();
      return;
    }
    st.running = false;
    await save({ 'pipeState': st });
    chrome.runtime.sendMessage({ action: 'status', text: `Done. MCs processed: ${st.processed}. Valid URLs: ${st.validCount}`, event: 'ready_to_download_urls', autoDownload: false, remaining: st.validCount, extracted: st.extractedCount });
    pushLog('Pipeline complete.');
    await closeAllTabsWindow();
  }
}

/******************** Extractor loop ********************/
async function runExtractorLoop(){
  const st = (await load('pipeState'))['pipeState'] || defaultState;
  if (!st.running && !st.fetchingComplete) return;
  if (st.paused || st.stopped) return;

  while (true){
    const st2 = (await load('pipeState'))['pipeState'] || defaultState;
    if (!st2.running && !st2.fetchingComplete) return;
    if (st2.stopped || st2.paused) return;
    const limit = Math.max(1, Math.floor(st2.concurrency/2));
    if (st2.inFlightExtract >= limit) break;
    if (st2.toExtract.length === 0) break;

    const url = st2.toExtract.shift();
    st2.inFlightExtract += 1;
    await save({ 'pipeState': st2 });
    extractOneWithTimeout(url).finally(async ()=>{
      const st3 = (await load('pipeState'))['pipeState'] || defaultState;
      st3.inFlightExtract = Math.max(0, st3.inFlightExtract - 1);
      st3.lastProgressTs = Date.now();
      await save({ 'pipeState': st3 });
      chrome.runtime.sendMessage({ action: 'status', event: 'progress', processed: st3.processed, remaining: st3.validCount, extracted: st3.extractedCount });
      runExtractorLoop();

      const finalState = (await load('pipeState'))['pipeState'] || defaultState;
      if (finalState.fetchingComplete && (finalState.toExtract.length === 0) && finalState.inFlightExtract === 0){
        finalState.running = false;
        await save({ 'pipeState': finalState });
        chrome.runtime.sendMessage({ action: 'status', text: `Done. MCs processed: ${finalState.processed}. Valid URLs: ${finalState.validCount}`, event: 'ready_to_download_urls', autoDownload: false, remaining: finalState.validCount, extracted: finalState.extractedCount });
        pushLog('Pipeline complete.');
        await closeAllTabsWindow();
      }
    });
  }
}

/******************** Commands from popup ********************/
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  (async () => {
    if (msg.action === 'startFetchPipeline' || msg.action === 'startFetchAndExtract'){
      const mode = (msg.action === 'startFetchAndExtract') ? 'fetch_and_extract' : 'fetch_only';
      const prev = (await load('pipeState'))['pipeState'] || { ...defaultState };
      if (prev.running) { sendResponse({ ok:false, reason:'Already running' }); return; }

      const mcList = Array.isArray(msg.mcList) ? msg.mcList : [];
      const st = { ...defaultState };
      Object.assign(st, {
        running: true,
        paused: false,
        fetchPaused: false,
        stopped: false,
        mode,
        mcList,
        index: 0,
        processed: 0,
        extractedCount: (await load('extractedData'))['extractedData']?.length || 0,
        validUrls: [],
        toExtract: [],
        inFlightExtract: 0,
        batchSize: Number(msg.options?.batchSize)||500,
        autoPause: !!msg.options?.autoPause,
        waitSeconds: Number(msg.options?.waitSeconds)||0,
        batchStartIndex: 0,
        openTabs: !!msg.options?.openTabs,
        tabsWindowId: null,
        urlTabMap: {},
        concurrency: Number(msg.options?.concurrency)||6,
        delay: Number(msg.options?.delay)||300,
        fetchingComplete: false,
        lastProgressTs: Date.now()
      });
      await save({ 'pipeState': st, 'remainingUrls': [] });
      pushLog(`Pipeline started (${mode}). MCs: ${mcList.length}, batchSize=${st.batchSize}, autoPause=${st.autoPause}, wait=${st.waitSeconds}s, openTabs=${st.openTabs}`);
      chrome.runtime.sendMessage({ action:'status', text:'Pipeline started', event:'progress', processed:0, remaining:0, extracted:st.extractedCount });
      driver();
      if (mode === 'fetch_and_extract') runExtractorLoop();
      sendResponse({ ok:true });
      return;
    }

    if (msg.action === 'resumePipeline'){
      const st = (await load('pipeState'))['pipeState'] || defaultState;
      st.fetchPaused = false;
      st.stopped = false;
      st.batchStartIndex = st.index;
      st.lastProgressTs = Date.now();
      await save({ 'pipeState': st });
      pushLog('Resumed.');
      chrome.runtime.sendMessage({ action:'status', text:'Resumed.', event:'progress', processed:st.processed, remaining:st.validCount, extracted:st.extractedCount });
      driver();
      if (st.mode === 'fetch_and_extract') runExtractorLoop();
      sendResponse({ ok:true }); return;
    }

    if (msg.action === 'stopPipeline'){
      const st = (await load('pipeState'))['pipeState'] || defaultState;
      st.running = false;
      st.fetchPaused = false;
      st.stopped = true;
      await save({ 'pipeState': st });
      pushLog('Pipeline stopped by user.');
      chrome.runtime.sendMessage({ action:'status', text:'Stopped.', event:'stopped' });
      await closeAllTabsWindow();
      sendResponse({ ok:true }); return;
    }

    if (msg.action === 'downloadCSV'){
      const rows = (await load('extractedData'))['extractedData'] || [];
      const all = rows || [];
      const headers = ['email','mcNumber','phone','url'];
      const csv = [headers.join(',')]
        .concat(all.map(r => headers.map(h=>`"${String(r[h]||'').replace(/"/g,'""')}"`).join(',')))
        .join('\n');
      try {
        const dataUrl = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv);
        chrome.downloads.download({ url: dataUrl, filename: 'fmcsa_data.csv', saveAs: true }, (dlId)=>{
          if (chrome.runtime.lastError){ pushLog('downloadCSV failed: ' + chrome.runtime.lastError.message); sendResponse({ ok:false, reason: chrome.runtime.lastError.message }); }
          else sendResponse({ ok:true, count: all.length });
        });
      } catch(e){ pushLog('downloadCSV error: ' + (e && e.message)); sendResponse({ ok:false, reason: e && e.message }); }
      return;
    }

    if (msg.action === 'getState'){
      const st = (await load('pipeState'))['pipeState'] || defaultState;
      const log = (await load('logBuffer'))['logBuffer'] || [];
      const rem = (await load('remainingUrls'))['remainingUrls'] || [];
      sendResponse({ ok:true, state: st, log, remainingUrls: rem });
      return;
    }

    if (msg.action === 'resetAll'){
      try {
        const st = (await load('pipeState'))['pipeState'] || {};
        st.running = false;
        st.stopped = true;
        await save({ 'pipeState': st });
      } catch(e){}
      try {
        await save({
          'extractedData': [],
          'extractedDataMap': {},
          'remainingUrls': [],
          'logBuffer': []
        });
        await new Promise(res => chrome.storage.local.remove(['pipeState'], res));
      } catch (e){}
      await closeAllTabsWindow();
      chrome.runtime.sendMessage({ action: 'status', text: 'Reset performed.' });
      sendResponse({ ok:true });
      return;
    }
  })();
  return true;
});
