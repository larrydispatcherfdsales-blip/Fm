// Popup controller (persistent UI + live counters)
const $ = id => document.getElementById(id);
const startFetch = $('startFetch');
const startBoth = $('startBoth');
const downloadUrls = $('downloadUrls');
const copyUrls = $('copyUrls');
const downloadCSV = $('downloadCSV');
const resetBtn = $('resetBtn');

const statusEl = $('status');
const logEl = $('log');
const mcInput = $('mcInput');
const sProcessed = $('sProcessed');
const sValid = $('sValid');
const sExtracted = $('sExtracted');

const concurr = $('concurrency');
const delay = $('delay');
const batchSize = $('batchSize');
const autoPause = $('autoPause');
const waitSeconds = $('waitSeconds');
const openTabs = $('openTabs');

const resumeBtn = $('resumeBtn');
const stopBtn = $('stopBtn');
const vpnNote = $('vpnNote');

function log(msg){
  const time = new Date().toLocaleTimeString();
  logEl.textContent = `[${time}] ${msg}\n` + logEl.textContent;
  statusEl.textContent = msg;
}

function pullState(){
  chrome.runtime.sendMessage({ action: 'getState' }, (resp)=>{
    if (!resp || !resp.ok) return;
    const st = resp.state;
    const lb = resp.log || [];
    const rem = resp.remainingUrls || [];
    sProcessed.textContent = st.processed||0;
    sValid.textContent = st.validCount||0;
    sExtracted.textContent = st.extractedCount||0;
    statusEl.textContent = st.running ? (st.paused ? 'paused' : 'running') : 'idle';
    resumeBtn.style.display = st.paused ? 'inline-block' : 'none';
    stopBtn.style.display = st.running ? 'inline-block' : 'none';
    vpnNote.style.display = st.paused ? 'block' : 'none';
    logEl.textContent = (lb||[]).join('\n');

    chrome.storage.local.get(['lastMcInput','options'], (res)=>{
      if (res.lastMcInput) mcInput.value = res.lastMcInput;
      if (res.options){
        if (res.options.concurrency) concurr.value = res.options.concurrency;
        if (res.options.delay) delay.value = res.options.delay;
        if (res.options.batchSize) batchSize.value = res.options.batchSize;
        autoPause.checked = !!res.options.autoPause;
        if (typeof res.options.waitSeconds === 'number') waitSeconds.value = res.options.waitSeconds;
        openTabs.checked = !!res.options.openTabs;
      }
    });
  });
}

document.addEventListener('DOMContentLoaded', pullState);

if (resetBtn){
  resetBtn.addEventListener('click', ()=>{
    try{ sProcessed.textContent='0'; sValid.textContent='0'; sExtracted.textContent='0'; logEl.textContent=''; }catch(e){}
    if (!confirm('Reset will clear all stored data. Proceed?')) return;
    chrome.runtime.sendMessage({ action: 'resetAll' }, (resp)=>{
      if (resp && resp.ok){ log('Reset complete.'); pullState(); }
      else { log('Reset failed: ' + (resp && resp.reason)); }
    });
  });
}

function gatherOptions(){
  const options = {
    concurrency: Number(concurr.value)||6,
    delay: Number(delay.value)||300,
    batchSize: Number(batchSize.value)||500,
    autoPause: !!autoPause.checked,
    waitSeconds: Number(waitSeconds.value)||0,
    openTabs: !!openTabs.checked
  };
  chrome.storage.local.set({ options });
  return options;
}

startFetch.addEventListener('click', ()=>{
  const raw = mcInput.value.trim();
  if (!raw) return alert('Paste MC numbers');
  const mcList = raw.split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
  const options = gatherOptions();
  chrome.storage.local.set({ lastMcInput: raw }, ()=>{});
  chrome.runtime.sendMessage({ action: 'startFetchPipeline', mcList, options }, (resp)=>{
    if (!resp || !resp.ok) log('Already running or failed to start');
    else log(`Started (URLs only) for ${mcList.length} MCs`);
  });
});

startBoth.addEventListener('click', ()=>{
  const raw = mcInput.value.trim();
  if (!raw) return alert('Paste MC numbers');
  const mcList = raw.split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
  const options = gatherOptions();
  chrome.storage.local.set({ lastMcInput: raw }, ()=>{});
  chrome.runtime.sendMessage({ action: 'startFetchAndExtract', mcList, options }, (resp)=>{
    if (!resp || !resp.ok) log('Already running or failed to start');
    else log(`Started (Auto Extract) for ${mcList.length} MCs`);
  });
});

resumeBtn.addEventListener('click', ()=>{
  chrome.runtime.sendMessage({ action: 'resumePipeline' }, (resp)=>{
    if (resp && resp.ok) log('Resumed'); else log('Resume failed');
  });
});

stopBtn.addEventListener('click', ()=>{
  chrome.runtime.sendMessage({ action: 'stopPipeline' }, (resp)=>{
    if (resp && resp.ok) log('Stopped'); else log('Stop failed');
  });
});

downloadUrls.addEventListener('click', ()=>{
  chrome.runtime.sendMessage({ action: 'getState' }, (resp)=>{
    if (!resp || !resp.ok) return;
    const urls = resp.remainingUrls || [];
    if (!urls.length) return alert('No URLs');
    const blob = new Blob([urls.join('\n')], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    chrome.downloads.download({ url, filename: 'fmcsa_remaining_urls.txt', saveAs: true });
  });
});

copyUrls.addEventListener('click', ()=>{
  chrome.runtime.sendMessage({ action: 'getState' }, (resp)=>{
    if (!resp || !resp.ok) return;
    const urls = resp.remainingUrls || [];
    if (!urls.length) return alert('No URLs');
    navigator.clipboard.writeText(urls.join('\n')).then(()=> log(`Copied ${urls.length} URLs`));
  });
});

downloadCSV.addEventListener('click', ()=>{
  chrome.runtime.sendMessage({ action: 'downloadCSV' }, (resp)=>{});
});

chrome.runtime.onMessage.addListener((msg)=>{
  if (msg.action !== 'status') return;
  if (msg.text) log(msg.text);
  if (msg.event === 'progress'){
    if (typeof msg.processed === 'number') sProcessed.textContent = msg.processed;
    if (typeof msg.remaining === 'number') sValid.textContent = msg.remaining;
    if (typeof msg.extracted === 'number') sExtracted.textContent = msg.extracted;
  }
  if (msg.event === 'awaiting_vpn_change'){
    resumeBtn.style.display = 'inline-block';
    stopBtn.style.display = 'inline-block';
    vpnNote.style.display = 'block';
  }
});