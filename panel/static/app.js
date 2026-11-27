const state = {
  templates: [], configs: [], serverRunning: false,
  search: '', sort: 'name'
};

const fleet = {
  active: 'dashboard',
  nodes: [], routes: [], users: [], settings: null, caddy: null,
  domainDraft: null, livePolling: false, livePaused: false,
  version: '', update: null, updateChecking: false, updating: false,
  loading: { dashboard: false, nodes: false, routes: false, users: false, configs: false }
};

/* Background refresh must never disturb the user: it is skipped while a modal
   is open or while focus is inside a form control. */
let modalOpen = false;

const LIVE_POLL_INTERVAL = 3000;
const RELATIVE_TIME_INTERVAL = 1000;

function isEditableFocus() {
  const el = document.activeElement;
  if (!el) return false;
  const tag = (el.tagName || '').toUpperCase();
  return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || el.isContentEditable;
}

function canAutoRefresh() {
  const root = document.getElementById('modal-root');
  // modalOpen can be reset asynchronously by the close animation; also check
  // that the modal root is really visible so the flag can never get stuck.
  const modalVisible = modalOpen && !(root && root.classList.contains('hidden'));
  return !modalVisible && !document.hidden && !isEditableFocus() && !fleet.livePaused;
}

function restoreLiveState() {
  fleet.livePaused = localStorage.getItem('shampoore_live') === 'off';
  updateLiveUI();
}

function updateLiveUI() {
  const btn = document.getElementById('btn-live');
  const label = document.getElementById('live-label');
  const icon = document.getElementById('live-icon');
  if (!btn) return;
  btn.classList.toggle('is-paused', fleet.livePaused);
  btn.setAttribute('aria-pressed', fleet.livePaused ? 'false' : 'true');
  btn.title = t(fleet.livePaused ? 'live.resume' : 'live.pause');
  btn.dataset.langTitle = fleet.livePaused ? 'live.resume' : 'live.pause';
  if (label) {
    label.dataset.lang = fleet.livePaused ? 'live.paused' : 'live.on';
    label.textContent = t(fleet.livePaused ? 'live.paused' : 'live.on');
  }
  if (icon) icon.textContent = fleet.livePaused ? 'play_arrow' : 'pause';
}

function toggleLive() {
  fleet.livePaused = !fleet.livePaused;
  localStorage.setItem('shampoore_live', fleet.livePaused ? 'off' : 'on');
  updateLiveUI();
  if (!fleet.livePaused) pollFleetLive();
}

function toggleSidebar() {
  const sidebar = document.getElementById('sidebar');
  const backdrop = document.getElementById('sidebar-backdrop');
  if (!sidebar) return;
  const open = sidebar.classList.toggle('open');
  if (backdrop) backdrop.classList.toggle('hidden', !open);
}

function closeSidebar() {
  document.getElementById('sidebar')?.classList.remove('open');
  document.getElementById('sidebar-backdrop')?.classList.add('hidden');
}

(async function boot() {
  const langLabel = document.getElementById('current-lang-label');
  const savedLang = localStorage.getItem('shampoore_lang') || 'en';
  
  await loadLang(savedLang);
  LANGS.current = savedLang;
  langLabel.textContent = { en: 'English', ru: 'Русский', eo: 'Esperanto' }[savedLang];
  applyLang();

  document.addEventListener('click', (e) => {
    document.querySelectorAll('.dropdown').forEach(d => {
      if (!d.contains(e.target)) d.classList.remove('show');
    });
  });

  document.querySelectorAll('.dropdown > button').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const parent = btn.closest('.dropdown');
      document.querySelectorAll('.dropdown').forEach(d => { if (d !== parent) d.classList.remove('show'); });
      parent.classList.toggle('show');
    });
  });

  document.getElementById('btn-add').addEventListener('click', openAddDialog);
  document.getElementById('btn-refresh').addEventListener('click', () => load());
  document.getElementById('btn-logout').addEventListener('click', logout);
  document.getElementById('btn-live').addEventListener('click', toggleLive);
  const backdrop = document.getElementById('sidebar-backdrop');
  if (backdrop) backdrop.addEventListener('click', closeSidebar);
  restoreLiveState();
  document.getElementById('search').addEventListener('input', e => { state.search = e.target.value.toLowerCase(); render(); });
  const menu = document.getElementById('btn-menu');
  if (menu) menu.addEventListener('click', toggleSidebar);
  setupFleetTabs();

  await load();
  await loadPanelVersion();
  await loadPanel('dashboard');
  checkForUpdates({ silent: true }).catch(() => {});
  // Live mode is on by default: the first poll starts right after the initial
  // render and then repeats every few seconds. A separate one-second timer
  // keeps the relative "Updated" labels fresh even between polls.
  pollFleetLive();
  setInterval(pollFleetLive, LIVE_POLL_INTERVAL);
  setInterval(refreshRelativeTimes, RELATIVE_TIME_INTERVAL);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) pollFleetLive(); });
})();

function formatBytes(bytes) {
  if (bytes === 0) return '0B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  const val = bytes / Math.pow(k, i);
  return (val >= 10 ? Math.round(val) : parseFloat(val.toFixed(1))) + sizes[i];
}

/* Human readable throughput for the live "Online • 1.2 MB/s" badge and the
   users summary. Zero is rendered as "0 KB/s" on purpose: a connected user
   with no traffic should still show a unit instead of an empty label. */
function formatSpeed(bps) {
  const n = Math.max(0, Number(bps) || 0);
  if (n === 0) return t('users.speedKBps').replace('{value}', '0');
  if (n < 1024) return t('users.speedBps').replace('{value}', String(Math.round(n)));
  const k = 1024;
  const units = [
    { div: k, key: 'users.speedKBps' },
    { div: k * k, key: 'users.speedMBps' },
    { div: k * k * k, key: 'users.speedGBps' }
  ];
  let unit = units[0];
  for (let i = units.length - 1; i >= 0; i -= 1) {
    if (n >= units[i].div) { unit = units[i]; break; }
  }
  const value = n / unit.div;
  const formatted = value >= 10 ? String(Math.round(value)) : String(parseFloat(value.toFixed(1)));
  return t(unit.key).replace('{value}', formatted);
}

async function loadPanelVersion() {
  try {
    const info = await api('/api/system/version');
    fleet.version = info.version || '';
    const el = document.getElementById('panel-version');
    if (el && fleet.version) el.textContent = fleet.version;
  } catch (e) {
    // Keep the build-time placeholder when the version endpoint is unavailable.
  }
}

function updateCandidates(info) {
  return {
    stable: info && info.latest_stable ? info.latest_stable : null,
    beta: info && info.latest_beta ? info.latest_beta : null
  };
}

function primaryUpdateTag(info) {
  const { stable, beta } = updateCandidates(info);
  return (stable && stable.tag_name) || (beta && beta.tag_name) || (info && info.latest_version) || '';
}

function hasUpdateCandidates(info) {
  const { stable, beta } = updateCandidates(info);
  return !!(stable || beta);
}

function setUpdateBadge(info) {
  const badge = document.getElementById('update-badge');
  const button = document.getElementById('btn-check-updates');
  if (!badge) return;
  const tag = primaryUpdateTag(info);
  if (tag && hasUpdateCandidates(info)) {
    badge.textContent = tag;
    badge.classList.remove('hidden');
    if (button) button.title = t('update.available');
  } else {
    badge.textContent = '';
    badge.classList.add('hidden');
    if (button) button.title = t('update.check');
  }
}

async function checkForUpdates({ silent = false, notifyError = false } = {}) {
  if (fleet.updateChecking) return fleet.update;
  const button = document.getElementById('btn-check-updates');
  fleet.updateChecking = true;
  if (button) button.disabled = true;
  try {
    const info = await api('/api/system/update');
    fleet.update = info || null;
    setUpdateBadge(fleet.update);
    if (!silent) {
      const { stable, beta } = updateCandidates(info);
      if (stable) toast(`${t('update.available')}: ${stable.tag_name}`, 'success');
      else if (beta) toast(t('update.betaAvailable').replace('{version}', beta.tag_name), 'warning');
      else toast(t('update.upToDate'));
    }
    return fleet.update;
  } catch (e) {
    if (notifyError) toast(e.message, 'error');
    return null;
  } finally {
    fleet.updateChecking = false;
    if (button) button.disabled = false;
  }
}

async function onUpdateButton() {
  if (fleet.updating) return;
  const info = await checkForUpdates({ silent: true, notifyError: true });
  if (!info) return;
  if (hasUpdateCandidates(info)) openUpdateDialog(info);
  else toast(t('update.upToDate'));
}

/* Full SemVer comparison (including prerelease identifiers), so the panel does
   not reload before a beta restart actually finished: beta.1 < beta.2 < final. */
function compareSemver(a, b) {
  const parse = (value) => {
    let rest = String(value || '').trim().replace(/^v+/i, '');
    if (!rest) return null;
    const plus = rest.indexOf('+');
    if (plus >= 0) rest = rest.slice(0, plus);
    let pre = [];
    const dash = rest.indexOf('-');
    if (dash >= 0) {
      pre = rest.slice(dash + 1).split('.').filter(Boolean);
      rest = rest.slice(0, dash);
    }
    const core = rest.split('.').map(part => parseInt(part, 10));
    if (!core.length || core.some(n => !Number.isFinite(n))) return null;
    return { core, pre };
  };
  const left = parse(a);
  const right = parse(b);
  if (!left && !right) return 0;
  if (!left) return -1;
  if (!right) return 1;
  const length = Math.max(left.core.length, right.core.length);
  for (let i = 0; i < length; i++) {
    const l = left.core[i] || 0;
    const r = right.core[i] || 0;
    if (l !== r) return l < r ? -1 : 1;
  }
  if (!left.pre.length && !right.pre.length) return 0;
  if (!left.pre.length) return 1;
  if (!right.pre.length) return -1;
  const preLength = Math.max(left.pre.length, right.pre.length);
  for (let i = 0; i < preLength; i++) {
    const l = left.pre[i];
    const r = right.pre[i];
    if (l === undefined) return -1;
    if (r === undefined) return 1;
    const lNum = /^\d+$/.test(l) ? parseInt(l, 10) : null;
    const rNum = /^\d+$/.test(r) ? parseInt(r, 10) : null;
    if (lNum !== null && rNum !== null) {
      if (lNum !== rNum) return lNum < rNum ? -1 : 1;
    } else if (lNum !== null) {
      return -1;
    } else if (rNum !== null) {
      return 1;
    } else if (l !== r) {
      return l < r ? -1 : 1;
    }
  }
  return 0;
}

function updateCandidateSection(candidate, { beta = false } = {}) {
  const tag = candidate.tag_name || '';
  const notes = (candidate.release_notes || '').trim();
  const published = candidate.published_at
    ? `<div class="update-meta">${esc(t('update.published'))}: ${esc(new Date(candidate.published_at).toLocaleString())}</div>`
    : '';
  const title = beta ? t('update.betaTitle') : t('update.stableTitle');
  const warning = beta
    ? `<div class="update-warning"><span class="material-symbols-rounded">warning</span><span>${esc(t('update.betaWarning'))}</span></div>`
    : '';
  const label = beta
    ? t('update.acceptBetaRisk').replace('{version}', tag)
    : t('update.updateTo').replace('{version}', tag);
  const buttonClass = beta ? 'btn--danger' : 'btn--filled';
  return `
    <section class="update-option${beta ? ' update-option--beta' : ''}">
      <div class="update-option__head">
        <div>
          <h3 class="update-notes-title">${esc(title)}</h3>
          <span class="update-version update-version--new">${esc(tag)}</span>
        </div>
        <button class="btn ${buttonClass} btn--compact" type="button" data-update-tag="${escAttr(tag)}">
          <span class="material-symbols-rounded">system_update</span>${esc(label)}
        </button>
      </div>
      ${warning}
      ${published}
      <details class="update-notes-wrap"${beta ? '' : ' open'}>
        <summary>${esc(t('update.notes'))}</summary>
        <div class="update-notes">${notes ? esc(notes) : esc(t('update.noNotes'))}</div>
      </details>
    </section>`;
}

function openUpdateDialog(info) {
  const { stable, beta } = updateCandidates(info);
  const current = info.current_version || fleet.version || '';
  let body = `
    <div class="update-summary-line">
      <span>${esc(t('update.current'))}:</span>
      <span class="update-version">${esc(current)}</span>
    </div>`;
  if (stable) body += updateCandidateSection(stable);
  if (beta && !stable) body += `<div class="hint">${esc(t('update.stableUpToDate'))}</div>`;
  if (beta) body += updateCandidateSection(beta, { beta: true });
  if (!stable && !beta) {
    body += `<div class="update-up-to-date"><span class="material-symbols-rounded">verified</span><span>${esc(t('update.upToDate'))}</span></div>`;
  }
  body += `<div class="hint">${esc(t('update.preserveHint'))}</div>`;

  const dialog = openModal({
    title: t('update.title'),
    body,
    actions: [{ kind: 'tonal', label: t('actions.close'), onClick: closeModal }]
  });
  document.getElementById('modal-root').querySelectorAll('[data-update-tag]').forEach(el => {
    el.addEventListener('click', () => {
      const tag = el.dataset.updateTag;
      const candidate = [stable, beta].find(item => item && item.tag_name === tag);
      if (candidate) runUpdate(dialog, candidate);
    });
  });
  return dialog;
}

async function runUpdate(dialog, candidate) {
  if (fleet.updating) return;
  fleet.updating = true;
  const modal = dialog || openModal({ title: t('update.title'), body: '' });
  const target = (candidate && candidate.tag_name) || '';
  modal.setBody(`<div class="update-progress"><span class="update-spinner"></span><span>${esc(t('update.installing'))}</span></div>`);
  try {
    const opts = { method: 'POST' };
    if (target) {
      opts.headers = { 'Content-Type': 'application/json' };
      opts.body = JSON.stringify({ tag: target });
    }
    const result = await api('/api/system/update', opts);
    const latest = (result && result.latest_version) || target;
    modal.setBody(`
      <div class="deploy-summary is-ok"><span class="material-symbols-rounded">check_circle</span>${esc(t('update.restarting'))}</div>
      <div class="update-progress"><span class="update-spinner"></span><span>${esc(t('update.waiting'))}</span></div>`);
    toast(t('update.done'), 'success');
    await waitForPanelRestart(latest);
  } catch (e) {
    modal.setBody(`<div class="deploy-summary is-error"><span class="material-symbols-rounded">error</span>${esc(e.message)}</div>`);
    toast(e.message, 'error');
  } finally {
    fleet.updating = false;
  }
}

async function waitForPanelRestart(expectedVersion) {
  const deadline = Date.now() + 90000;
  while (Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, 2000));
    try {
      const res = await fetch('/api/system/version', {
        credentials: 'same-origin',
        cache: 'no-store',
        headers: { 'Accept': 'application/json' }
      });
      if (!res.ok) continue;
      const info = await res.json();
      if (!expectedVersion || compareSemver(info.version, expectedVersion) >= 0) {
        window.location.reload();
        return;
      }
    } catch (e) {
      // The panel is still restarting; keep polling.
    }
  }
  toast(t('update.failed'), 'error');
  window.location.reload();
}



window.changeLanguage = async (lang) => {
  LANGS.current = lang;
  localStorage.setItem('shampoore_lang', lang);
  const label = document.getElementById('current-lang-label');
  if (label) label.textContent = { en: 'English', ru: 'Русский', eo: 'Esperanto' }[lang] || lang;
  await loadLang(lang);
  applyLang();
  setUpdateBadge(fleet.update);
  updateLiveUI();
  // Re-render the active view right away from cached data, so switching the
  // language does not wait for any background refresh.
  rerenderActivePanel();
};

window.changeSort = (sort) => {
  state.sort = sort;
  document.getElementById('current-sort-label').textContent = t('config.sort.' + sort);
  render();
};

async function api(path, opts = {}) {
  opts.credentials = 'same-origin';
  // Always revalidate: the panel counters and node telemetry change every
  // heartbeat, and a cached /api/nodes answer freezes the Live widgets.
  if (opts.cache === undefined) opts.cache = 'no-store';
  opts.headers = Object.assign({ 'Accept': 'application/json' }, opts.headers || {});
  const res = await fetch(path, opts);
  if (res.status === 401) {
    window.location.replace('/login');
    throw new Error('unauthorized');
  }
  const text = await res.text();
  let body = {}; try { body = text ? JSON.parse(text) : {}; } catch { body = { error: text }; }
  if (!res.ok) {
    const err = new Error(body.error || res.statusText);
    err.payload = body;
    err.status = res.status;
    throw err;
  }
  return body;
}

async function logout() {
  try { await api('/api/logout', { method: 'POST' }); } catch (e) {}
  window.location.replace('/login');
}

async function load() {
  try {
    if (!state.templates.length) state.templates = (await api('/api/templates')) || [];
    syncTransportCatalog();
    const sst = await api('/api/server/status');
    state.serverRunning = sst.running;
    state.configs = (await api('/api/configs')) || [];
    render();
  } catch (e) {
    state.serverRunning = false;
    render();
  }
}

function render() {
  const root = document.getElementById('configs');
  const empty = document.getElementById('empty');
  
  const filtered = state.configs.filter(c => !state.search || c.name.toLowerCase().includes(state.search) || (c.kind||'').toLowerCase().includes(state.search));
  filtered.sort((a, b) => {
    if (state.sort === 'status') {
      const order = { running: 0, starting: 1, error: 2, stopped: 3 };
      return (order[a.status] ?? 9) - (order[b.status] ?? 9);
    }
    return a.name.localeCompare(b.name);
  });

  if (!filtered.length) {
    root.innerHTML = '';
    empty.classList.remove('hidden');
    empty.innerHTML = `<span class="material-symbols-rounded">dns</span><p>${t('config.empty')}</p>`;
    return;
  }
  empty.classList.add('hidden');

  root.innerHTML = filtered.map(c => {
    const n = escAttr(c.name);
    const isEnabled = c.enabled;
    const warn = !state.serverRunning && isEnabled 
      ? `<div class="card__warn"><span class="material-symbols-rounded">warning</span>${t('config.warningOffline')}</div>` : '';

    return `
      <div class="card">
        <div class="card__header">
          <div>
            <h3 class="card__title">${esc(c.name)}</h3>
            <span class="card__subtitle">${esc(c.kind || '—')}</span>
          </div>
          <span class="chip chip--${c.status}">${esc(t('status.' + c.status) || c.status)}</span>
        </div>
        ${c.error ? `<div class="card__error">${esc(c.error)}</div>` : ''}
        ${warn}
        <div class="card__footer">
          <div class="switch-wrapper">
            <label class="switch">
              <input type="checkbox" ${isEnabled ? 'checked' : ''} onchange="toggleCfg('${n}', this.checked)">
              <span class="slider"></span>
            </label>
            <span>${isEnabled ? t('status.enabled') : t('status.disabled')}</span>
          </div>
          <div class="spacer"></div>
          <button class="btn btn--tonal btn--compact" onclick="exportCfg('${n}')" title="${t('actions.export')}" style="height:32px; font-size:12px; padding:0 12px;">
            <span class="material-symbols-rounded" style="font-size:16px;">ios_share</span> ${t('actions.export')}
          </button>
          <button class="icon-btn" onclick="openLogs('${n}')" title="${t('actions.logs')}"><span class="material-symbols-rounded">terminal</span></button>
          <button class="icon-btn" onclick="openEditDialog('${n}')" title="${t('actions.edit')}"><span class="material-symbols-rounded">edit</span></button>
          <button class="icon-btn icon-btn--danger" onclick="delCfg('${n}')" title="${t('actions.delete')}"><span class="material-symbols-rounded">delete</span></button>
        </div>
      </div>`;
  }).join('');
}

async function toggleCfg(name, enabled) {
  try {
    await api(`/api/configs/${encodeURIComponent(name)}/${enabled ? 'enable' : 'disable'}`, { method: 'POST' });
    toast(t(enabled ? 'toast.started' : 'toast.stopped'));
    await loadActiveView(); render();
  } catch (e) { toast(e.message, 'error'); await loadActiveView(); render(); }
}

async function delCfg(name) {
  if (!confirm(t('confirm.deleteConfig'))) return;
  try { await api(`/api/configs/${encodeURIComponent(name)}`, { method: 'DELETE' }); toast(t('toast.deleted')); await loadActiveView(); render(); }
  catch (e) { toast(e.message, 'error'); }
}

async function exportCfg(name) {
  const d = await api(`/api/configs/${encodeURIComponent(name)}/export`);
  await copyToClipboard(d.url);
  toast(t('toast.copied'));

  let qrHTML = '';
  if (typeof qrcode !== 'undefined') {
    try {
      const qr = qrcode(0, 'L');
      qr.addData(d.url);
      qr.make();
      qrHTML = `
        <div class="qr-container">
          <div class="qr-box">${qr.createImgTag(6, 8)}</div>
        </div>`;
    } catch (e) { console.error(e); }
  }

  openModal({
    title: t('dialog.exportTitle'),
    body: `
      ${qrHTML}
      <p style="color:var(--md-on-surface-variant);margin-bottom:16px;text-align:center;">
        ${t('dialog.exportHint')}
      </p>
      <div class="field">
        <textarea readonly>${esc(d.url)}</textarea>
      </div>`
  });
}

async function openLogs(name) {
  const d = await api(`/api/configs/${encodeURIComponent(name)}/logs?limit=500`);
  openModal({ title: `${t('actions.logs')}: ${name}`, body: `<div class="logs">${esc((d.lines || []).join('\n'))}</div>` });
}

function openAddDialog() { openConfigDialog(); }
async function openEditDialog(name) {
  try {
    const c = await api(`/api/configs/${encodeURIComponent(name)}`);
    openConfigDialog(name, c.settings);
  } catch (e) { toast(e.message, 'error'); }
}

function openConfigDialog(editName = null, initialData = null) {
  const configDefaultKind = (state.templates[0] || {}).kind || 'raw_ws';
  let data = initialData ? JSON.parse(JSON.stringify(initialData)) : {};
  if (!editName && !initialData) {
    data = { type: configDefaultKind, transport: {}, advanced: {} };
  }
  delete data.name;

  let form = {
    name: editName || '',
    type: data.type || configDefaultKind,
    encryption_password: data.encryption_password || '',
    transport: data.transport || {},
    advanced: data.advanced || {}
  };
  
  let activeTab = 'form';
  let jsonCache = '';
  let selectedKind = form.type;
  let fieldValues = { ...form.transport };
  let multiStrategy = form.transport.strategy || 'flowmux';
  let multiSubflows = form.transport.transports || [];

  const dialog = openModal({
    title: t(editName ? 'dialog.editConfig' : 'dialog.newConfig'),
    tabs: [{ id: 'form', label: t('dialog.tabForm') }, { id: 'json', label: t('dialog.tabJson') }],
    actions: [
      { label: t('actions.cancel'), kind: 'text', onClick: closeModal },
      { label: t(editName ? 'actions.save' : 'actions.create'), kind: 'filled', onClick: () => submit() }
    ]
  });

  function getSchema(kind) {
    return state.templates.find(t => t.kind === kind);
  }

  function buildObj() {
    const obj = { type: selectedKind };
    if (form.name) obj.name = form.name;
    if (form.encryption_password) obj.encryption_password = form.encryption_password;
    
    if (isMultiKind(selectedKind)) {
      obj.transport = {
        strategy: multiStrategy,
        transports: multiSubflows
      };
    } else {
      obj.transport = {};
      const schema = getSchema(selectedKind);
      if (schema) {
        schema.fields.forEach(f => {
          if (fieldValues[f.key] !== undefined && fieldValues[f.key] !== '') {
            obj.transport[f.key] = fieldValues[f.key];
          }
        });
      } else {
         Object.assign(obj.transport, fieldValues);
      }
    }
    
    if (form.advanced && Object.keys(form.advanced).length) obj.advanced = form.advanced;
    return obj;
  }

  function renderBody() {
    if (activeTab === 'json') {
      dialog.setBody(`
        <div class="form-group">
          <div class="field">
            <label>${t('dialog.json')}</label>
            <textarea id="json-editor" spellcheck="false">${esc(jsonCache || JSON.stringify(buildObj(), null, 2))}</textarea>
          </div>
        </div>
      `);
      document.getElementById('json-editor').addEventListener('input', e => { jsonCache = e.target.value; });
      return;
    }

    const schema = getSchema(selectedKind);

    let html = `
      <div class="form-group">
        <div class="field">
          <label>${t('dialog.name')}</label>
          <input type="text" id="f-name" value="${escAttr(form.name)}" placeholder="${t('dialog.namePlaceholder')}">
        </div>
        <div class="field">
          <label>${t('dialog.type')}</label>
          <select id="f-type" onchange="window.selectTransport(this.value)">
            ${state.templates.map(item => `<option value="${escAttr(item.kind)}" ${item.kind === selectedKind ? 'selected' : ''}>${esc(item.display_name)}</option>`).join('')}
          </select>
        </div>
        <div class="field">
          <label>${t('dialog.encryption')}</label>
          <input type="password" id="f-pw" value="${escAttr(form.encryption_password)}">
        </div>
        <hr style="border:none;border-top:1px solid var(--md-outline-variant);margin:8px 0;">
    `;

    if (isMultiKind(selectedKind)) {
      const strategyOptions = multiStrategies(selectedKind).map(s => `
            <option value="${escAttr(s.id)}" ${multiStrategy === s.id ? 'selected' : ''}>${esc(s.label || s.id)}</option>`).join('');
      html += `
        <div class="field">
          <label>${t('dialog.strategy')}</label>
          <select id="f-strategy" class="input" style="height:56px;">
            ${strategyOptions}
          </select>
        </div>
        <div class="field">
          <label>${t('dialog.subflows')}</label>
          <div id="multi-transports">
            ${multiSubflows.map((t, i) => `
              <div style="background:var(--md-surface-container-highest); padding:16px; border-radius:16px; margin-bottom:16px;">
                <div style="display:flex; gap:8px; align-items:center; margin-bottom:12px;">
                  <select class="input" style="flex:1; height:40px;" onchange="window.updateSubflowType(${i}, this.value)">
                    ${state.templates.filter(k => !isMultiKind(k.kind)).map(k => `<option value="${escAttr(k.kind)}" ${k.kind === t.type ? 'selected' : ''}>${esc(k.display_name)}</option>`).join('')}
                  </select>
                  <button type="button" class="icon-btn icon-btn--danger" onclick="window.rmSubflow(${i})"><span class="material-symbols-rounded">delete</span></button>
                </div>
                <div class="field" style="margin-bottom:12px;">
                  <label>${t('dialog.rateLimit')}</label>
                  <input type="number" class="input" style="height:40px;" value="${t.rate_limit_kb || 0}" oninput="window.updateSubflowRate(${i}, this.value)">
                </div>
                <div class="field">
                  <label>${t('dialog.transportParams')}</label>
                  <div id="kv-transports-${i}" class="kv-container">
                    ${renderSubflowFields(i, t)}
                  </div>
                </div>
              </div>
            `).join('')}
          </div>
          <button type="button" class="btn btn--tonal" style="align-self:flex-start; margin-top:8px;" onclick="window.addSubflow()">
            <span class="material-symbols-rounded">add</span> ${t('dialog.addTransport')}
          </button>
        </div>
      `;
    } else if (schema && schema.fields) {
      const basicFields = schema.fields.filter(f => !f.advanced);
      const advancedFields = schema.fields.filter(f => f.advanced);
      html += `
        <div class="field">
          <label>${t('dialog.transportParams')}</label>
          ${basicFields.map(renderField).join('')}
          ${advancedFields.length ? `
            <details style="margin-top:8px;">
              <summary style="cursor:pointer; font-weight:500;">${t('dialog.advancedOptions')}</summary>
              <div style="margin-top:8px;">
                ${advancedFields.map(renderField).join('')}
              </div>
            </details>` : ''}
        </div>
      `;
    }

    html += `
        <hr style="border:none;border-top:1px solid var(--md-outline-variant);margin:8px 0;">
        <div class="field">
          <label>${t('dialog.advanced')}</label>
          <div id="kv-advanced" class="kv-container">
             ${Object.entries(form.advanced || {}).map(([k, v], i) => renderKV(k, v, i)).join('')}
          </div>
          <button type="button" class="btn btn--tonal" style="align-self:flex-start; margin-top:8px;" onclick="window.addKV()">
            <span class="material-symbols-rounded">add</span> ${t('dialog.addParameter')}
          </button>
        </div>
      </div>
    `;

    dialog.setBody(html);

    document.getElementById('f-name').addEventListener('input', e => form.name = e.target.value);
    document.getElementById('f-pw').addEventListener('input', e => form.encryption_password = e.target.value);
    
    if (isMultiKind(selectedKind)) {
      document.getElementById('f-strategy').addEventListener('change', e => { multiStrategy = e.target.value; });
    }
  }

  function renderField(f) {
    const value = String(fieldValues[f.key] ?? '');
    const hasOptions = Array.isArray(f.options) && f.options.length > 0;
    const control = hasOptions
      ? `<select class="input" style="height:40px;" onchange="window.updateField('${escAttr(f.key)}', this.value)">
           <option value="">${escAttr(f.placeholder || 'default')}</option>
           ${f.options.map(opt => `<option value="${escAttr(opt)}" ${value === opt ? 'selected' : ''}>${esc(opt)}</option>`).join('')}
         </select>`
      : `<input type="${f.type === 'password' ? 'password' : f.type === 'number' ? 'number' : 'text'}"
                class="input"
                style="height:40px;"
                value="${escAttr(value)}"
                placeholder="${escAttr(f.placeholder || '')}"
                oninput="window.updateField('${escAttr(f.key)}', this.value)">`;
    return `
      <div class="field" style="margin-bottom:8px;">
        <label>${esc(f.label)} ${f.required ? '*' : ''}</label>
        ${control}
        ${f.hint ? `<span class="hint" style="font-size:11px; margin-top:4px;">${esc(f.hint)}</span>` : ''}
      </div>
    `;
  }

  function renderSubflowFields(idx, subflow) {
    const subSchema = getSchema(subflow.type);
    if (!subSchema) return '';
    const fields = subSchema.fields || [];
    const transport = subflow.transport || {};
    const render = (f) => {
      const value = String(transport[f.key] ?? '');
      const hasOptions = Array.isArray(f.options) && f.options.length > 0;
      const control = hasOptions
        ? `<select class="input" style="height:40px;" onchange="window.updateSubflowField(${idx}, '${escAttr(f.key)}', this.value)">
             <option value="">${escAttr(f.placeholder || 'default')}</option>
             ${f.options.map(opt => `<option value="${escAttr(opt)}" ${value === opt ? 'selected' : ''}>${esc(opt)}</option>`).join('')}
           </select>`
        : `<input type="text" class="input" style="height:40px;"
                  value="${escAttr(value)}"
                  placeholder="${escAttr(f.placeholder || '')}"
                  oninput="window.updateSubflowField(${idx}, '${escAttr(f.key)}', this.value)">`;
      return `
        <div class="field" style="margin-bottom:8px;">
          <label>${esc(f.label)} ${f.required ? '*' : ''}</label>
          ${control}
          ${f.hint ? `<span class="hint" style="font-size:11px; margin-top:4px;">${esc(f.hint)}</span>` : ''}
        </div>
      `;
    };
    const basicFields = fields.filter(f => !f.advanced);
    const advancedFields = fields.filter(f => f.advanced);
    return basicFields.map(render).join('') + (advancedFields.length ? `
      <details style="margin-top:8px;">
        <summary style="cursor:pointer; font-weight:500;">${t('dialog.advancedOptions')}</summary>
        <div style="margin-top:8px;">${advancedFields.map(render).join('')}</div>
      </details>` : '');
  }

  function renderKV(k, v, i) {
    return `
      <div class="kv-row">
        <input type="text" value="${escAttr(k)}" onchange="window.updateKVKey(${i}, this.value)" placeholder="${escAttr(t('dialog.keyPlaceholder'))}">
        <input type="text" value="${escAttr(String(v))}" onchange="window.updateKVVal(${i}, this.value)" placeholder="${escAttr(t('dialog.valuePlaceholder'))}">
        <button type="button" class="icon-btn icon-btn--danger" onclick="window.rmKV(${i})"><span class="material-symbols-rounded">delete</span></button>
      </div>
    `;
  }

  window.selectTransport = (kind) => {
    selectedKind = kind;
    fieldValues = {}; // Reset fields
    renderBody();
  };

  window.updateField = (key, val) => {
    fieldValues[key] = val;
  };

  window.addSubflow = () => {
    const defaultType = (TRANSPORT_KINDS[0] || {}).value || 'raw_ws';
    multiSubflows.push({ type: defaultType, transport: {}, rate_limit_kb: 0 });
    renderBody();
  };

  window.rmSubflow = (i) => {
    multiSubflows.splice(i, 1);
    renderBody();
  };

  window.updateSubflowType = (i, val) => {
    multiSubflows[i].type = val;
    multiSubflows[i].transport = {};
    renderBody();
  };

  window.updateSubflowRate = (i, val) => {
    multiSubflows[i].rate_limit_kb = Number(val) || 0;
  };

  window.updateSubflowField = (i, key, val) => {
    if (!multiSubflows[i].transport) multiSubflows[i].transport = {};
    multiSubflows[i].transport[key] = val;
  };

  window.updateKVKey = (idx, newKey) => {
    const keys = Object.keys(form.advanced);
    const oldKey = keys[idx];
    if (oldKey === newKey) return;
    const val = form.advanced[oldKey];
    delete form.advanced[oldKey];
    form.advanced[newKey] = val;
    renderBody();
  };

  window.updateKVVal = (idx, newVal) => {
    const k = Object.keys(form.advanced)[idx];
    form.advanced[k] = newVal;
  };

  window.rmKV = (idx) => {
    delete form.advanced[Object.keys(form.advanced)[idx]];
    renderBody();
  };

  window.addKV = () => {
    form.advanced['key'] = '';
    renderBody();
  };

  async function submit() {
    let payloadObj = form;
    if (activeTab === 'json') {
      try { payloadObj = JSON.parse(document.getElementById('json-editor').value); }
      catch (e) { return toast(e.message, 'error'); }
    } else {
      payloadObj = buildObj();
    }

    const path = editName ? `/api/configs/${encodeURIComponent(editName)}` : '/api/configs';
    const method = editName ? 'PUT' : 'POST';
    const body = editName ? JSON.stringify({ settings: payloadObj }) : JSON.stringify({ name: payloadObj.name || '', content: JSON.stringify(payloadObj) });

    try {
      await api(path, { method, headers: { 'Content-Type': 'application/json' }, body });
      toast(t('toast.saved'));
      closeModal();
      await loadActiveView();
      render();
    } catch (e) {
      toast(e.message, 'error');
    }
  }

  dialog.onTabChange = id => {
    if (activeTab === 'form' && id === 'json') {
      jsonCache = JSON.stringify(buildObj(), null, 2);
    } else if (activeTab === 'json' && id === 'form') {
      try {
        const parsed = JSON.parse(jsonCache);
        form = { ...form, ...parsed };
        if (parsed.transport) {
            if (isMultiKind(parsed.type)) {
                multiStrategy = parsed.transport.strategy || defaultMultiStrategy(parsed.type);
                multiSubflows = parsed.transport.transports || [];
            } else {
                fieldValues = parsed.transport;
            }
        }
        selectedKind = parsed.type || (TRANSPORT_KINDS[0] || {}).value || 'raw_ws';
      } catch (e) {
        toast(t('dialog.invalidJson'), 'error');
        return false;
      }
    }
    activeTab = id;
    renderBody();
    return true;
  };

  renderBody();
}

let activeModalKeyHandler = null;
let modalCloseTimer = null;

function openModal({ title, body = '', actions = [], tabs = [] }) {
  const root = document.getElementById('modal-root');
  if (modalCloseTimer) {
    clearTimeout(modalCloseTimer);
    modalCloseTimer = null;
  }
  modalOpen = true;
  root.classList.remove('hidden');
  document.body.classList.add('modal-open');
  root.setAttribute('role', 'dialog');
  root.setAttribute('aria-modal', 'true');

  const tabsHTML = tabs.length ? `<div class="modal__tabs">${tabs.map((t,i) => `<button class="tab ${i===0?'active':''}" data-tab="${t.id}">${t.label}</button>`).join('')}</div>` : '';
  const actionsHTML = actions.map((a,i) => `<button class="btn btn--${a.kind}" data-act="${i}">${a.label}</button>`).join('');

  root.innerHTML = `
    <div class="modal">
      <div class="modal__header">
        <h2>${esc(title)}</h2>
        <button class="icon-btn" type="button" data-modal-close aria-label="${esc(t('actions.close'))}"><span class="material-symbols-rounded">close</span></button>
      </div>
      ${tabsHTML}
      <div class="modal__body">${body}</div>
      ${actions.length ? `<div class="modal__footer">${actionsHTML}</div>` : ''}
    </div>`;

  root.querySelectorAll('.tab').forEach(el => el.addEventListener('click', () => {
    if (dialogObj.onTabChange && dialogObj.onTabChange(el.dataset.tab) === false) return;
    root.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    el.classList.add('active');
  }));

  root.querySelectorAll('[data-act]').forEach(el => el.addEventListener('click', actions[el.dataset.act].onClick));
  root.querySelectorAll('[data-modal-close]').forEach(el => el.addEventListener('click', closeModal));
  root.onclick = event => { if (event.target === root) closeModal(); };
  if (activeModalKeyHandler) document.removeEventListener('keydown', activeModalKeyHandler);
  activeModalKeyHandler = event => {
    if (event.key === 'Escape' && modalOpen) {
      event.preventDefault();
      closeModal();
    }
  };
  document.addEventListener('keydown', activeModalKeyHandler);
  requestAnimationFrame(() => {
    root.classList.add('show');
    const focusable = root.querySelector('.modal__body input, .modal__body select, .modal__body textarea, .modal__footer .btn--filled');
    if (focusable) focusable.focus({ preventScroll: true });
  });

  const dialogObj = { setBody: h => root.querySelector('.modal__body').innerHTML = h, onTabChange: null, setFocus: id => document.getElementById(id)?.focus() };
  return dialogObj;
}

function closeModal() {
  const root = document.getElementById('modal-root');
  modalOpen = false;
  document.body.classList.remove('modal-open');
  root.classList.remove('show');
  root.removeAttribute('role');
  root.removeAttribute('aria-modal');
  root.onclick = null;
  if (activeModalKeyHandler) {
    document.removeEventListener('keydown', activeModalKeyHandler);
    activeModalKeyHandler = null;
  }
  if (modalCloseTimer) clearTimeout(modalCloseTimer);
  modalCloseTimer = setTimeout(() => { root.classList.add('hidden'); root.innerHTML = ''; modalCloseTimer = null; }, 200);
}

function toast(msg, type = 'info') {
  const el = document.createElement('div');
  el.className = 'toast' + (type && type !== 'info' ? ' ' + type : '');
  el.textContent = msg;
  document.getElementById('toasts').appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; setTimeout(() => el.remove(), 300); }, 3200);
}

function esc(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])); }
function escAttr(s) { return esc(s).replace(/\n/g, '&#10;'); }
async function copyToClipboard(text) {
  try { await navigator.clipboard.writeText(text); } catch {
    const ta = document.createElement('textarea'); ta.value = text; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); ta.remove();
  }
}
const geoState = { rules: [], sources: [], exits: [], loaded: false };

function jsonArg(value) { return escAttr(JSON.stringify(value)); }

async function loadGeoRules(silent = false) {
  try {
    const [rules, sources, nodes] = await Promise.all([
      api('/api/rules'), api('/api/georules'), api('/api/nodes')
    ]);
    geoState.rules = rules || [];
    geoState.sources = sources || [];
    geoState.exits = (nodes || []).filter(node => node.is_exit);
    geoState.loaded = true;
    renderGeoRules();
  } catch (e) {
    if (!silent) toast(e.message, 'error');
  }
}

function geoSourceFor(name) {
  return geoState.sources.find(source => source.name === name);
}

function geoExitGrid(selectedIDs, ruleName) {
  if (!geoState.exits.length) {
    return `<div class="hint">${esc(t('rules.noExitNodes'))}</div>`;
  }
  const selected = new Set((selectedIDs || []).map(Number));
  const all = selected.size === 0;
  const nameLiteral = JSON.stringify(ruleName || '');
  return `<div class="exit-node-grid">${geoState.exits.map(node => `
    <label class="check-row">
      <input type="checkbox" data-rule-node="${escAttr(node.id)}" data-rule-name="${escAttr(ruleName || '')}"
        ${all || selected.has(Number(node.id)) ? 'checked' : ''}
        onchange="saveGeoRuleNodes(${escAttr(nameLiteral)}, this)">
      <span>${esc(node.name)}</span>
    </label>`).join('')}</div>`;
}

function geoSourceMeta(source) {
  const meta = [
    ['link', t('rules.source'), geoSourceHost(source.url) || source.url],
    ['schedule', t('rules.lastUpdated'), geoUpdatedLabel(source)],
    ['autorenew', t('rules.interval'), geoIntervalLabel(source.update_interval_minutes)]
  ].map(([icon, label, value]) => `<span><span class="material-symbols-rounded">${icon}</span>${esc(label)}: ${esc(value)}</span>`).join('');
  const error = source.last_error
    ? `<div class="card__warn"><span class="material-symbols-rounded">warning</span>${esc(source.last_error)}</div>` : '';
  return `<div class="geo-rule__meta">${meta}</div>${error}`;
}


function renderGeoRules() {
  const root = document.getElementById('georules-list');
  if (!root) return;
  if (!geoState.loaded) {
    root.innerHTML = `<div class="empty"><span class="material-symbols-rounded">hourglass_top</span><p>${esc(t('rules.loading'))}</p></div>`;
    return;
  }
  if (!geoState.rules.length) {
    root.innerHTML = emptyState('public', t('rules.empty'), t('rules.addManual'), 'openAddGeoRuleModal()');
    return;
  }
  root.innerHTML = geoState.rules.map(rule => {
    const source = geoSourceFor(rule.name);
    const nameArg = jsonArg(rule.name);
    return `<article class="geo-rule">
      <div class="geo-rule__main">
        <div>
          <div class="geo-rule__name">${esc(rule.name)}</div>
          <div class="hint">${esc(t('rules.cidrCount').replace('{count}', (rule.cidrs || []).length))}</div>
        </div>
        <div class="geo-rule__actions">
          <label class="switch">
            <input type="checkbox" ${rule.enabled ? 'checked' : ''} onchange="toggleGeoRule(${nameArg}, this.checked)">
            <span class="slider"></span>
          </label>
          ${source ? `<button class="btn btn--tonal btn--compact" type="button" onclick="refreshGeoRuleSource(${nameArg})">
            <span class="material-symbols-rounded">refresh</span>${esc(t('rules.refreshNow'))}</button>` : ''}
          <button class="icon-btn icon-btn--danger" type="button" title="${esc(t('actions.delete'))}" onclick="deleteGeoRule(${nameArg})">
            <span class="material-symbols-rounded">delete</span>
          </button>
        </div>
      </div>
      <div class="geo-rule__nodes">
        <div class="geo-rule__nodes-label">${esc(t('rules.exitNodes'))}</div>
        ${geoExitGrid(rule.exit_node_ids || [], rule.name)}
        <span class="hint">${esc(t('rules.exitNodesHint'))}</span>
      </div>
      ${source ? geoSourceMeta(source) : `<div class="hint">${esc(t('rules.manualHint'))}</div>`}
    </article>`;
  }).join('');
}

async function saveGeoRuleNodes(name, checkbox) {
  if (!name) return;
  const card = checkbox.closest('.geo-rule');
  const checks = card ? Array.from(card.querySelectorAll('input[data-rule-node]')) : [checkbox];
  const selected = checks.filter(item => item.checked).map(item => Number(item.dataset.ruleNode));
  if (!selected.length) {
    checkbox.checked = true;
    toast(t('rules.exitNodesRequired'), 'error');
    return;
  }
  const payload = selected.length === geoState.exits.length ? [] : selected;
  try {
    await api(`/api/georules/${encodeURIComponent(name)}/nodes`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ exit_node_ids: payload })
    });
    const rule = geoState.rules.find(item => item.name === name);
    if (rule) rule.exit_node_ids = payload;
    toast(t('rules.exitNodesSaved'), 'success');
  } catch (e) {
    checkbox.checked = !checkbox.checked;
    toast(e.message, 'error');
  }
}

async function toggleGeoRule(name, enabled) {
  try {
    await api(`/api/rules/${encodeURIComponent(name)}/${enabled ? 'enable' : 'disable'}`, { method: 'POST' });
    toast(enabled ? t('rules.enabledToast') : t('rules.disabledToast'));
    await loadGeoRules(true);
  } catch (e) { toast(e.message, 'error'); }
}

async function deleteGeoRule(name) {
  if (!confirm(t('rules.confirmDelete').replace('{name}', name))) return;
  try {
    await api(`/api/rules/${encodeURIComponent(name)}`, { method: 'DELETE' });
    toast(t('rules.deleted'));
    await loadGeoRules(true);
  } catch (e) { toast(e.message, 'error'); }
}

async function refreshGeoRuleSource(name) {
  try {
    await api(`/api/georules/${encodeURIComponent(name)}/refresh`, { method: 'POST' });
    toast(t('rules.refreshed'), 'success');
    await loadGeoRules(true);
  } catch (e) { toast(e.message, 'error'); }
}

async function applyGeoRules() {
  try {
    const res = await api('/api/rules/apply', { method: 'POST' });
    toast(res.restarted ? t('rules.applied') : t('rules.appliedOffline'));
  } catch (e) { toast(e.message, 'error'); }
}

function bindDraftExitChecks(onChange) {
  const root = document.querySelector('.modal__body');
  if (!root) return;
  const checks = Array.from(root.querySelectorAll('input[data-draft-node]'));
  checks.forEach(cb => cb.addEventListener('change', () => {
    const selected = checks.filter(item => item.checked).map(item => Number(item.dataset.draftNode));
    if (!selected.length) {
      cb.checked = true;
      toast(t('rules.exitNodesRequired'), 'error');
      return;
    }
    onChange(selected.length === geoState.exits.length ? [] : selected);
  }));
}

function draftExitGrid(exitIDs) {
  if (!geoState.exits.length) return `<div class="hint">${esc(t('rules.noExitNodes'))}</div>`;
  const selected = new Set((exitIDs || []).map(Number));
  const all = selected.size === 0;
  return `<div class="exit-node-grid">${geoState.exits.map(node => `
    <label class="check-row">
      <input type="checkbox" data-draft-node="${escAttr(node.id)}" ${all || selected.has(Number(node.id)) ? 'checked' : ''}>
      <span>${esc(node.name)}</span>
    </label>`).join('')}</div>`;
}

function openAddGeoRuleModal() {
  let name = '';
  let cidrs = '';
  let exitIDs = [];
  const dialog = openModal({
    title: t('rules.newTitle'),
    actions: [
      { label: t('actions.cancel'), kind: 'text', onClick: closeModal },
      { label: t('actions.create'), kind: 'filled', onClick: async () => {
          if (!name.trim()) return toast(t('rules.nameRequired'), 'error');
          try {
            await api('/api/rules', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                name: name.trim(),
                cidrs: cidrs.split(/\r?\n/).map(s => s.trim()).filter(Boolean),
                exit_node_ids: exitIDs
              })
            });
            toast(t('rules.created'), 'success');
            closeModal();
            await loadGeoRules(true);
          } catch (e) { toast(e.message, 'error'); }
        }
      }
    ]
  });
  dialog.setBody(`
    <div class="form-group">
      <div class="field">
        <label>${esc(t('rules.name'))}</label>
        <input type="text" id="r-name" placeholder="${escAttr(t('rules.namePlaceholder'))}" value="${escAttr(name)}">
      </div>
      <div class="field">
        <label>${esc(t('rules.cidrs'))}</label>
        <textarea id="r-cidrs" placeholder="${escAttr(t('rules.cidrsPlaceholder'))}">${esc(cidrs)}</textarea>
      </div>
      <div class="field">
        <label>${esc(t('rules.exitNodes'))}</label>
        ${draftExitGrid(exitIDs)}
        <span class="hint">${esc(t('rules.exitNodesHint'))}</span>
      </div>
    </div>`);
  document.getElementById('r-name').addEventListener('input', e => { name = e.target.value; });
  document.getElementById('r-cidrs').addEventListener('input', e => { cidrs = e.target.value; });
  bindDraftExitChecks(value => { exitIDs = value; });
}

function openImportGeoRuleModal() {
  let name = '';
  let url = '';
  let interval = 0;
  let exitIDs = [];
  const dialog = openModal({
    title: t('rules.importTitle'),
    actions: [
      { label: t('actions.cancel'), kind: 'text', onClick: closeModal },
      { label: t('rules.import'), kind: 'filled', onClick: async () => {
          if (!name.trim()) return toast(t('rules.nameRequired'), 'error');
          if (!url.trim()) return toast(t('rules.urlRequired'), 'error');
          try {
            const res = await api('/api/georules/import-url', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                name: name.trim(), url: url.trim(),
                update_interval_minutes: interval, exit_node_ids: exitIDs
              })
            });
            toast(t('rules.imported').replace('{count}', res.rules || 0), 'success');
            closeModal();
            await loadGeoRules(true);
          } catch (e) { toast(e.message, 'error'); }
        }
      }
    ]
  });
  dialog.setBody(`
    <div class="form-group">
      <div class="field">
        <label>${esc(t('rules.name'))}</label>
        <input type="text" id="gr-name" placeholder="${escAttr(t('rules.namePlaceholder'))}" value="${escAttr(name)}">
      </div>
      <div class="field">
        <label>${esc(t('rules.url'))}</label>
        <input type="url" id="gr-url" placeholder="${escAttr(t('rules.urlPlaceholder'))}" value="${escAttr(url)}" spellcheck="false" autocomplete="off">
        <span class="hint">${esc(t('rules.urlHint'))}</span>
      </div>
      <div class="field">
        <label>${esc(t('rules.updateInterval'))}</label>
        <select id="gr-interval">
          <option value="0" selected>${esc(t('rules.intervalManual'))}</option>
          <option value="60">${esc(t('rules.intervalHourly'))}</option>
          <option value="360">${esc(t('rules.interval6h'))}</option>
          <option value="1440">${esc(t('rules.interval24h'))}</option>
        </select>
      </div>
      <div class="field">
        <label>${esc(t('rules.exitNodes'))}</label>
        ${draftExitGrid(exitIDs)}
        <span class="hint">${esc(t('rules.exitNodesHint'))}</span>
      </div>
    </div>`);
  document.getElementById('gr-name').addEventListener('input', e => { name = e.target.value; });
  document.getElementById('gr-url').addEventListener('input', e => { url = e.target.value; });
  document.getElementById('gr-interval').addEventListener('change', e => { interval = Number(e.target.value) || 0; });
  bindDraftExitChecks(value => { exitIDs = value; });
}



function geoIntervalLabel(minutes) {
  const value = Number(minutes) || 0;
  if (value === 0) return t('rules.intervalManual');
  if (value === 60) return t('rules.intervalHourly');
  if (value === 360) return t('rules.interval6h');
  if (value === 1440) return t('rules.interval24h');
  return t('rules.intervalMinutes').replace('{minutes}', value);
}

function geoSourceHost(url) {
  try { return new URL(url).hostname; } catch { return ''; }
}

function geoUpdatedLabel(source) {
  if (!source || !source.last_updated_at) return t('rules.neverUpdated');
  const ts = new Date(source.last_updated_at).getTime();
  if (!Number.isFinite(ts) || new Date(ts).getUTCFullYear() < 1970) return t('rules.neverUpdated');
  return relativeTime(source.last_updated_at) || t('rules.neverUpdated');
}

/* ------------------------------------------------------------------ */
/* Fleet control plane: dashboard, nodes, routes, users                */
/* ------------------------------------------------------------------ */

let TRANSPORT_KINDS = [];

/* Editable transport secrets come from /api/templates capabilities. */
let NODE_SECRET_FIELDS = {};

const SECRET_LABEL_KEYS = {
  raw_ws_password: 'nodes.secretRawWSPassword',
  livekit_api_key: 'nodes.secretLiveKitKey',
  livekit_api_secret: 'nodes.secretLiveKitSecret',
  minio_root_user: 'nodes.secretMinioUser',
  minio_root_password: 'nodes.secretMinioPassword',
  s3_bucket: 'nodes.secretS3Bucket'
};

function templateCaps(template) {
  return (template && template.capabilities) || {};
}

function syncTransportCatalog() {
  TRANSPORT_KINDS = state.templates
    .filter(t => templateCaps(t).deployable)
    .map(t => ({
      value: t.kind,
      label: t.display_name,
      default: !!templateCaps(t).default_relay,
      decoy: !!templateCaps(t).decoy,
      secrets: templateCaps(t).secrets || []
    }));
  NODE_SECRET_FIELDS = {};
  for (const t of state.templates) {
    const specs = templateCaps(t).secrets || [];
    NODE_SECRET_FIELDS[t.kind] = specs.map(spec => ({
      key: spec.key,
      label: SECRET_LABEL_KEYS[spec.key] || spec.label || spec.key,
      generator: spec.generator || { kind: 'hex', bytes: 24 }
    }));
  }
}

function transportKindInfo(kind) {
  return TRANSPORT_KINDS.find(k => k.value === kind);
}

function isMultiKind(kind) {
  const template = state.templates.find(t => t.kind === kind);
  return !!(template && templateCaps(template).multi);
}

function primaryMultiKind() {
  const template = state.templates.find(t => templateCaps(t).multi);
  return template ? template.kind : 'multi';
}

function multiStrategies(kind) {
  const template = state.templates.find(t => t.kind === (kind || primaryMultiKind()));
  return templateCaps(template).strategies || [];
}

function defaultMultiStrategy(kind) {
  const strategies = multiStrategies(kind);
  return strategies.length ? strategies[0].id : 'bond';
}


function randomBytes(n) {
  const buf = new Uint8Array(n);
  if (window.crypto && window.crypto.getRandomValues) window.crypto.getRandomValues(buf);
  else for (let i = 0; i < n; i++) buf[i] = Math.floor(Math.random() * 256);
  return buf;
}

function randomHex(n) {
  return Array.from(randomBytes(n), b => b.toString(16).padStart(2, '0')).join('');
}

function randomBase64(n) {
  return btoa(String.fromCharCode.apply(null, Array.from(randomBytes(n))));
}

function generateSecretValue(spec) {
  const generator = (spec && spec.generator) || { kind: 'hex', bytes: 24 };
  const bytes = Number(generator.bytes) || 24;
  const prefix = generator.prefix || '';
  if (generator.kind === 'literal') return prefix;
  if (generator.kind === 'base64') return prefix + randomBase64(bytes);
  return prefix + randomHex(bytes);
}

function transportSecretSpecs(type) {
  return NODE_SECRET_FIELDS[type] || [];
}

function secretSpecByKey(key) {
  if (key === 'encryption_password') {
    return { key, generator: { kind: 'hex', bytes: 32 } };
  }
  for (const specs of Object.values(NODE_SECRET_FIELDS)) {
    const found = specs.find(spec => spec.key === key);
    if (found) return found;
  }
  return { key, generator: { kind: 'hex', bytes: 24 } };
}

function fillSecretKeys(draft, specs) {
  if (!draft.secrets) draft.secrets = {};
  specs.forEach(spec => {
    const key = spec.key || spec;
    if (!draft.secrets[key]) draft.secrets[key] = generateSecretValue(spec);
  });
}

function fillDraftSecrets(draft, type) {
  fillSecretKeys(draft, transportSecretSpecs(type));
}

function takenRouteTransports(entryID, exitID, excludeID) {
  const taken = new Set();
  for (const route of fleet.routes || []) {
    if (excludeID && route.id === excludeID) continue;
    if (isMultiKind(route.transport_type)) continue;
    if (route.entry_node_id === entryID && route.exit_node_id === exitID) {
      taken.add(route.transport_type);
    }
  }
  return taken;
}

const PANEL_META = {
  dashboard: { title: 'nav.dashboard', subtitle: 'app.tagline' },
  nodes: { title: 'nodes.title', subtitle: 'nodes.subtitle' },
  routes: { title: 'routes.title', subtitle: 'routes.subtitle' },
  users: { title: 'users.title', subtitle: 'users.subtitle' },
  georules: { title: 'rules.title', subtitle: 'rules.subtitle' },
  configs: { title: 'config.title', subtitle: 'config.subtitle' }
};

function setupFleetTabs() {
  document.querySelectorAll('.nav-item[data-panel]').forEach(btn => {
    btn.addEventListener('click', () => switchPanel(btn.dataset.panel));
  });
  const bind = (id, fn) => { const el = document.getElementById(id); if (el) el.addEventListener('click', fn); };
  bind('btn-dashboard-refresh', () => loadPanel('dashboard'));
  bind('btn-refresh-routes', () => loadPanel('routes'));
  bind('btn-refresh-users', () => loadPanel('users'));
  bind('btn-refresh-georules', () => loadPanel('georules', false, true));
  bind('btn-redeploy-all', fleetRedeployAll);
  bind('btn-apply-georules', applyGeoRules);
  bind('btn-add-georule', openAddGeoRuleModal);
  bind('btn-add-georule-url', openImportGeoRuleModal);
  bind('btn-add-node', () => openNodeModal(null, 'relay'));
  bind('btn-add-route', () => fleetAddRoute());
  bind('btn-add-user', () => openUserModal(null));
  bind('btn-check-updates', onUpdateButton);
}

function switchPanel(name) {
  if (!name) return;
  fleet.active = name;
  document.querySelectorAll('.nav-item[data-panel]').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.panel === name);
  });
  document.querySelectorAll('.panel-view').forEach(view => {
    view.classList.toggle('hidden', view.id !== 'view-' + name);
  });
  const meta = PANEL_META[name] || PANEL_META.dashboard;
  const title = document.getElementById('page-title');
  const subtitle = document.getElementById('page-subtitle');
  if (title) { title.dataset.lang = meta.title; title.textContent = t(meta.title); }
  if (subtitle) { subtitle.dataset.lang = meta.subtitle; subtitle.textContent = t(meta.subtitle); }
  closeSidebar();
  if (name === 'configs') load();
  else loadPanel(name);
}

async function loadPanel(name, silent = false, force = false) {
  if (!name) return;
  if (fleet.loading[name]) {
    if (!force) return;
    // An older request is still in flight: wait for it, then fetch fresh data
    // so the UI never stays stale after a create/update/delete action.
    const deadline = Date.now() + 10000;
    while (fleet.loading[name] && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 50));
    }
  }
  fleet.loading[name] = true;
  try {
    if (name === 'dashboard') {
      const [settings, nodes, routes, users] = await Promise.all([
        api('/api/settings'), api('/api/nodes'), api('/api/routes'), api('/api/users')
      ]);
      fleet.settings = settings;
      fleet.nodes = nodes || [];
      fleet.routes = routes || [];
      fleet.users = users || [];
      renderDashboard();
    } else if (name === 'nodes') {
      const [nodes, settings] = await Promise.all([api('/api/nodes'), api('/api/settings')]);
      fleet.nodes = nodes || [];
      fleet.settings = settings;
      renderNodes();
    } else if (name === 'routes') {
      const [routes, nodes] = await Promise.all([api('/api/routes'), api('/api/nodes')]);
      fleet.routes = routes || [];
      fleet.nodes = nodes || [];
      renderRoutes();
    } else if (name === 'users') {
      const [users, routes, nodes, settings] = await Promise.all([
        api('/api/users'), api('/api/routes'), api('/api/nodes'), api('/api/settings')
      ]);
      fleet.users = users || [];
      fleet.routes = routes || [];
      fleet.nodes = nodes || [];
      fleet.settings = settings;
      renderUsers();
    } else if (name === 'georules') {
      await loadGeoRules(silent);
    }
    applyLang();
  } catch (e) {
    if (!silent) toast(e.message, 'error');
  } finally {
    fleet.loading[name] = false;
  }
}

/* Re-render the current view from already loaded data (used for instant
   language switching). Never triggers network requests. */
function rerenderActivePanel() {
  if (fleet.active === 'configs') { render(); return; }
  if (fleet.active === 'dashboard') renderDashboard();
  else if (fleet.active === 'nodes') renderNodes();
  else if (fleet.active === 'routes') renderRoutes();
  else if (fleet.active === 'users') renderUsers();
  else if (fleet.active === 'georules') renderGeoRules();
}

/* Fetch fresh data for the visible page and redraw it. Every create, update
   and delete action goes through this helper so the active view updates
   immediately without an extra click. */
async function loadActiveView() {
  const name = fleet.active || 'dashboard';
  if (name === 'configs') {
    await load();
    render();
    return;
  }
  await loadPanel(name, false, true);
}

/* Smart background poll: statuses, CPU/RAM/Disk meters and user traffic are
   refreshed every few seconds while the tab is visible and no form is being
   edited. Both /api/nodes and /api/users are requested with cache: 'no-store'
   so the browser can never serve a stale snapshot. */
async function pollFleetLive() {
  if (!canAutoRefresh()) return;
  const active = fleet.active;
  if (active !== 'nodes' && active !== 'users' && active !== 'dashboard') return;
  if (fleet.loading[active] || fleet.livePolling) return;
  fleet.livePolling = true;
  try {
    const [nodes, users] = await Promise.all([
      api('/api/nodes', { cache: 'no-store' }),
      api('/api/users', { cache: 'no-store' })
    ]);
    if (fleet.active !== active || !canAutoRefresh()) return;
    const previousIds = (fleet.nodes || []).map(n => String(n.id)).sort().join(',');
    const previousUserIds = (fleet.users || []).map(u => String(u.id)).sort().join(',');
    applyNodeLiveStatus(nodes || []);
    const nextIds = (fleet.nodes || []).map(n => String(n.id)).sort().join(',');
    // A node added or removed elsewhere must appear without a manual refresh.
    if (previousIds !== nextIds) {
      if (active === 'nodes') renderNodes();
      else if (active === 'dashboard') renderDashboard();
    }
    applyUserLiveStatus(users || []);
    const nextUserIds = (fleet.users || []).map(u => String(u.id)).sort().join(',');
    if (previousUserIds !== nextUserIds && active === 'users') renderUsers();
    if (active === 'dashboard') applyDashboardStats();
  } catch (e) {
    // Background refresh stays silent, the manual refresh button reports errors.
  } finally {
    fleet.livePolling = false;
  }
}

/* Recompute the relative "Updated" labels once a second, so "just now" turns
   into "1 min ago" even while no poll is in flight or Live is paused. */
function refreshRelativeTimes() {
  const nodes = fleet.nodes || [];
  if (nodes.length) {
    const byId = new Map(nodes.map(n => [String(n.id), n]));
    document.querySelectorAll('[data-node-last-seen]').forEach(el => {
      const node = byId.get(el.dataset.nodeLastSeen);
      if (node) el.textContent = nodeLastSeenText(node);
    });
  }
  refreshNodeTimes();
}

function clampPercent(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.min(100, Math.round(n));
}

function percentOf(used, total) {
  if (!total) return 0;
  return clampPercent(used / total * 100);
}

function meterClass(percent) {
  if (percent < 60) return 'good';
  if (percent < 85) return 'warn';
  return 'danger';
}

function metricPair(used, total) {
  if (!total) return '—';
  return `${formatBytes(used)} / ${formatBytes(total)}`;
}

function nodeMetricValues(node) {
  const cpu = clampPercent(node.cpu_percent);
  const ram = percentOf(node.mem_used_bytes, node.mem_total_bytes);
  const disk = percentOf(node.disk_used_bytes, node.disk_total_bytes);
  return {
    cpu: { percent: cpu, value: cpu + '%' },
    ram: { percent: ram, value: metricPair(node.mem_used_bytes, node.mem_total_bytes) },
    disk: { percent: disk, value: metricPair(node.disk_used_bytes, node.disk_total_bytes) }
  };
}

// Host telemetry is considered present once the agent has reported memory or
// disk totals; CPU alone may legitimately be 0%.
function nodeHasMetrics(node) {
  return !!(node && (Number(node.mem_total_bytes) > 0 || Number(node.disk_total_bytes) > 0));
}

function nodeMetricsRows(node) {
  const values = nodeMetricValues(node);
  const row = (key, label) => `
    <div class="metric" data-node-metric="${key}">
      <span class="metric__label">${esc(label)}</span>
      <span class="meter meter--${meterClass(values[key].percent)}"><span class="meter__fill" style="width:${values[key].percent}%"></span></span>
      <span class="metric__value">${esc(values[key].value)}</span>
    </div>`;
  return `${row('cpu', t('nodes.metricsCpu'))}
    ${row('ram', t('nodes.metricsRam'))}
    ${row('disk', t('nodes.metricsDisk'))}`;
}

function nodeMetricsHTML(node) {
  if (!nodeHasMetrics(node)) {
    return `<div class="node-metrics node-metrics--empty" data-node-metrics="${escAttr(node.id)}"><span class="metric__placeholder">${esc(t('nodes.metricsPending'))}</span></div>`;
  }
  return `<div class="node-metrics" data-node-metrics="${escAttr(node.id)}">${nodeMetricsRows(node)}</div>`;
}

function nodeLastSeenText(node) {
  if (!node || !node.last_seen) return t('nodes.neverSeen');
  const rel = relativeTime(node.last_seen);
  return rel ? t('nodes.lastSeen').replace('{time}', rel) : t('nodes.neverSeen');
}

/* Deployment/config-update time for the dashboard "Updated" column. The
   heartbeat's last_seen is deliberately not used: it changes every 5s and
   would keep the column stuck at "just now". */
function nodeDeployedText(node) {
  if (!node || !node.updated_at) return t('nodes.neverSeen');
  return formatRelativeTime(node.updated_at) || t('nodes.neverSeen');
}

/* Keep every [data-time-iso] label fresh. The ISO value is synced from the
   latest node snapshot so a deploy/config update is picked up by the poll. */
function refreshNodeTimes() {
  const byId = new Map((fleet.nodes || []).map(n => [String(n.id), n]));
  document.querySelectorAll('[data-node-updated]').forEach(el => {
    const node = byId.get(el.dataset.nodeUpdated);
    if (node && node.updated_at) el.dataset.timeIso = node.updated_at;
  });
  document.querySelectorAll('[data-time-iso]').forEach(el => {
    el.textContent = formatRelativeTime(el.dataset.timeIso) || t('nodes.neverSeen');
  });
}

function applyNodeLiveStatus(nodes) {
  const byId = new Map((nodes || []).map(n => [String(n.id), n]));
  const existing = new Map((fleet.nodes || []).map(n => [String(n.id), n]));
  fleet.nodes = (nodes || []).map(live => {
    const base = existing.get(String(live.id)) || {};
    // The API returns the full node record (name, roles, transports, SSH host
    // and telemetry), so a node created elsewhere is complete after a poll.
    return Object.assign({}, base, live);
  });
  document.querySelectorAll('[data-node-status]').forEach(el => {
    const live = byId.get(el.dataset.nodeStatus);
    if (!live) return;
    el.className = `status-chip status-chip--${statusClass(live.status)}`;
    el.textContent = nodeStatusText(live);
  });
  document.querySelectorAll('[data-node-last-seen]').forEach(el => {
    const live = byId.get(el.dataset.nodeLastSeen);
    if (!live) return;
    el.textContent = nodeLastSeenText(live);
  });
  document.querySelectorAll('[data-node-metrics]').forEach(root => {
    const live = byId.get(root.dataset.nodeMetrics);
    if (!live || !nodeHasMetrics(live)) return;
    // The first heartbeat with telemetry replaces the "no metrics yet"
    // placeholder with real gauges; later heartbeats only move the bars.
    if (!root.querySelector('.meter')) {
      root.classList.remove('node-metrics--empty');
      root.innerHTML = nodeMetricsRows(live);
    }
    const values = nodeMetricValues(live);
    root.querySelectorAll('[data-node-metric]').forEach(el => {
      const value = values[el.dataset.nodeMetric];
      if (!value) return;
      const meter = el.querySelector('.meter');
      if (meter) {
        meter.classList.remove('meter--good', 'meter--warn', 'meter--danger');
        meter.classList.add('meter--' + meterClass(value.percent));
      }
      const fill = el.querySelector('.meter__fill');
      if (fill) fill.style.width = value.percent + '%';
      const valueEl = el.querySelector('.metric__value');
      if (valueEl) valueEl.textContent = value.value;
    });
  });
  refreshNodeTimes();
}

function userTrafficHTML(user) {
  const used = userUsedBytes(user);
  const limit = user.traffic_limit_bytes || 0;
  const percent = limit > 0 ? percentOf(used, limit) : 0;
  const label = limit > 0 ? `${formatBytes(used)} / ${formatBytes(limit)}` : formatBytes(used);
  return `<div class="user-quota">
    <div class="user-quota__line">
      <span data-user-traffic-label>${esc(label)}</span>
      ${limit > 0 ? `<span data-user-traffic-percent>${percent}%</span>` : ''}
    </div>
    <span class="meter meter--${limit > 0 ? meterClass(percent) : 'muted'}"><span class="meter__fill" data-user-traffic-bar style="width:${percent}%"></span></span>
  </div>`;
}

function applyUserLiveStatus(users) {
  const existing = new Map((fleet.users || []).map(u => [String(u.id), u]));
  fleet.users = (users || []).map(live => Object.assign({}, existing.get(String(live.id)) || {}, live));

  const statusEls = new Map();
  document.querySelectorAll('[data-user-status]').forEach(el => statusEls.set(el.dataset.userStatus, el));
  const trafficEls = new Map();
  document.querySelectorAll('[data-user-traffic]').forEach(el => trafficEls.set(el.dataset.userTraffic, el));
  const expiryEls = new Map();
  document.querySelectorAll('[data-user-expiry]').forEach(el => expiryEls.set(el.dataset.userExpiry, el));
  const onlineEls = new Map();
  document.querySelectorAll('[data-user-online]').forEach(el => onlineEls.set(el.dataset.userOnline, el));

  fleet.users.forEach(user => {
    const id = String(user.id);
    const active = userActive(user);

    const statusEl = statusEls.get(id);
    if (statusEl) {
      statusEl.className = `status-chip ${active ? 'status-chip--online' : 'status-chip--error'}`;
      statusEl.textContent = active ? t('users.active') : t('users.inactive');
    }

    const onlineEl = onlineEls.get(id);
    if (onlineEl) onlineEl.innerHTML = userOnlineBadge(user);

    const expiryEl = expiryEls.get(id);
    if (expiryEl) expiryEl.textContent = formatExpiry(user.expires_at);

    const trafficEl = trafficEls.get(id);
    if (trafficEl) trafficEl.innerHTML = userTrafficHTML(user);
  });

  renderUsersSummary();
  if (fleet.active === 'dashboard') applyDashboardStats();
}

function applyDashboardStats() {
  const nodes = fleet.nodes || [];
  const users = fleet.users || [];
  const routes = fleet.routes || [];
  const relays = nodes.filter(n => n.is_relay).length;
  const exits = nodes.filter(n => n.is_exit).length;
  const activeUsers = users.filter(userActive).length;
  const enabledRoutes = routes.filter(r => r.enabled).length;
  const traffic = users.reduce((sum, user) => sum + userUsedBytes(user), 0);
  const set = (key, value) => document.querySelectorAll(`[data-stat="${key}"]`).forEach(el => { el.textContent = value; });
  set('servers', String(nodes.length));
  set('servers-hint', t('dashboard.serversHint').replace('{relays}', relays).replace('{exits}', exits));
  set('routes', String(routes.length));
  set('routes-hint', t('dashboard.routesHint').replace('{active}', enabledRoutes).replace('{total}', routes.length));
  set('users', String(users.length));
  set('users-hint', t('dashboard.usersHint').replace('{active}', activeUsers).replace('{total}', users.length));
  set('traffic', formatBytes(traffic));
}

/* Go marshals time.Time as RFC3339Nano (e.g. 2026-01-02T03:04:05.123456789Z).
   Trim nanoseconds to milliseconds and keep the timezone suffix (Z or ±hh:mm),
   so the browser parses the exact instant without losing the UTC offset. */
function parseISOTime(value) {
  if (!value) return NaN;
  const text = String(value).trim()
    .replace(/\.(\d{3})\d+(?=Z|[+-]\d{2}:?\d{2}$)/, '.$1');
  const ts = Date.parse(text);
  return Number.isFinite(ts) ? ts : NaN;
}

/* Precise relative time used by the "Updated" columns:
   < 60s -> "just now"; 1..59 min -> "N min ago"; >= 1h -> "1 h 20 min ago". */
function formatRelativeTime(isoString) {
  if (!isoString) return '';
  const then = parseISOTime(isoString);
  if (!Number.isFinite(then) || new Date(then).getUTCFullYear() < 1970) return '';
  const sec = Math.max(0, Math.round((Date.now() - then) / 1000));
  if (sec < 60) return t('time.justNow');
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min} ${t('time.minAgo')}`;
  const hours = Math.floor(min / 60);
  if (hours < 24) {
    const restMin = min % 60;
    if (restMin === 0) return t('time.hoursAgo').replace('{hours}', hours);
    return t('time.hoursMinutesAgo').replace('{hours}', hours).replace('{minutes}', restMin);
  }
  return t('time.daysAgo').replace('{days}', Math.floor(hours / 24));
}

function relativeTime(value) {
  return formatRelativeTime(value);
}

function nodeStatusText(node) {
  const status = node.status || 'new';
  if (status === 'deploying') return t('nodes.statusDeploying');
  if (status === 'stopped') return t('nodes.statusStopped');
  if (status === 'error') return t('nodes.statusError');
  if (status === 'deployed' || status === 'online') return t('nodes.statusOnline');
  return t('nodes.statusNew');
}

function nodeRoles(node) {
  if (node.is_relay && node.is_exit) return t('roles.both');
  if (node.is_relay) return t('roles.relay');
  if (node.is_exit) return t('roles.exit');
  return '—';
}

function transportLabel(kind) {
  if (isMultiKind(kind)) return t('routes.multiBadge');
  const found = TRANSPORT_KINDS.find(k => k.value === kind);
  return found ? found.label : kind;
}

function multiStrategyLabel(strategy) {
  if (strategy === 'bond') return t('routes.strategyBondShort');
  if (strategy === 'flowmux') return t('routes.strategyFlowmuxShort');
  return strategy || '';
}

function subRouteNames(ids) {
  if (!ids || !ids.length) return '';
  return ids.map(id => {
    const route = fleet.routes.find(r => r.id === id);
    return route ? route.name : '#' + id;
  }).join(', ');
}

function emptyState(icon, text, actionLabel, action) {
  return `<div class="empty"><span class="material-symbols-rounded">${icon}</span><p>${esc(text)}</p>${actionLabel ? `<button class="btn btn--filled" type="button" onclick="${action}">${esc(actionLabel)}</button>` : ''}</div>`;
}

function renderDashboard() {
  const settings = fleet.settings || {};
  const activeEl = document.activeElement;
  const keepDomainFocus = !!(activeEl && activeEl.id === 'subscription-domain');
  const nodes = fleet.nodes || [];
  const relays = nodes.filter(n => n.is_relay);
  const exits = nodes.filter(n => n.is_exit);
  const users = fleet.users || [];
  const routes = fleet.routes || [];
  const activeUsers = users.filter(userActive).length;
  const enabledRoutes = routes.filter(r => r.enabled).length;
  const traffic = users.reduce((sum, user) => sum + userUsedBytes(user), 0);
  const publicURL = (() => {
    const override = (settings.public_url || '').trim();
    if (override) return override;
    const domain = (settings.public_domain || '').trim();
    return domain ? 'https://' + domain : '';
  })();

  const domainInput = document.getElementById('subscription-domain');
  const domainValue = fleet.domainDraft != null ? fleet.domainDraft : (settings.public_domain || '');
  if (domainInput) {
    if (!keepDomainFocus) domainInput.value = domainValue;
    domainInput.oninput = e => { fleet.domainDraft = e.target.value; };
  }

  const domainStatus = document.getElementById('dashboard-domain-status');
  if (domainStatus) {
    domainStatus.className = 'status-chip ' + (publicURL ? 'status-chip--online' : '');
    domainStatus.textContent = t(publicURL ? 'dashboard.domainReady' : 'dashboard.domainMissing');
  }

  const hasDomain = !!(settings.public_domain || '').trim();
  const subscriptionsToggle = document.getElementById('subscriptions-enabled');
  if (subscriptionsToggle) {
    subscriptionsToggle.checked = !!settings.subscriptions_enabled;
    subscriptionsToggle.disabled = !hasDomain;
  }
  const deleteDomainBtn = document.getElementById('btn-delete-domain');
  if (deleteDomainBtn) deleteDomainBtn.classList.toggle('hidden', !hasDomain);

  const stats = document.getElementById('dashboard-stats');
  if (stats) {
    stats.innerHTML = `
      <div class="stat-card">
        <span class="stat-card__label">${esc(t('dashboard.servers'))}</span>
        <strong data-stat="servers">${nodes.length}</strong>
        <span class="stat-card__hint" data-stat="servers-hint">${esc(t('dashboard.serversHint').replace('{relays}', relays.length).replace('{exits}', exits.length))}</span>
      </div>
      <div class="stat-card">
        <span class="stat-card__label">${esc(t('dashboard.routes'))}</span>
        <strong data-stat="routes">${routes.length}</strong>
        <span class="stat-card__hint" data-stat="routes-hint">${esc(t('dashboard.routesHint').replace('{active}', enabledRoutes).replace('{total}', routes.length))}</span>
      </div>
      <div class="stat-card">
        <span class="stat-card__label">${esc(t('dashboard.users'))}</span>
        <strong data-stat="users">${users.length}</strong>
        <span class="stat-card__hint" data-stat="users-hint">${esc(t('dashboard.usersHint').replace('{active}', activeUsers).replace('{total}', users.length))}</span>
      </div>
      <div class="stat-card">
        <span class="stat-card__label">${esc(t('dashboard.traffic'))}</span>
        <strong data-stat="traffic">${formatBytes(traffic)}</strong>
        <span class="stat-card__hint">${esc(t('dashboard.trafficHint'))}</span>
      </div>`;
  }

  const list = document.getElementById('dashboard-nodes');
  if (list) {
    if (!nodes.length) {
      list.innerHTML = emptyState('dns', t('nodes.empty'), t('nodes.add'), "openNodeModal(null,'relay')");
    } else {
      const rows = nodes.map(node => {
        const host = (node.ssh_host || '—') + (node.ssh_port && node.ssh_port !== 22 ? `:${node.ssh_port}` : '');
        const transports = (node.transports || [])
          .filter(tr => tr.enabled && tr.domain)
          .map(tr => transportLabel(tr.type))
          .join(', ');
        return `<tr data-node-row="${escAttr(node.id)}">
          <td>
            <strong>${esc(node.name)}</strong>
            <div class="table-sub">${esc(host)}</div>
            ${transports ? `<div class="table-sub">${esc(transports)}</div>` : ''}
          </td>
          <td><span class="role-chip">${esc(nodeRoles(node))}</span></td>
          <td>
            <span class="status-chip status-chip--${esc(statusClass(node.status))}" data-node-status="${escAttr(node.id)}">${esc(nodeStatusText(node))}</span>
            ${node.last_error ? `<div class="table-sub">${esc(node.last_error)}</div>` : ''}
          </td>
          <td>${nodeMetricsHTML(node)}</td>
          <td><span class="table-sub" data-node-updated="${escAttr(node.id)}" data-time-iso="${escAttr(node.updated_at || '')}">${esc(nodeDeployedText(node))}</span></td>
          <td>
            <div class="table-actions">
              <button class="icon-btn" type="button" title="${esc(t('nodes.deploy'))}" onclick="fleetDeployNode(${node.id})"><span class="material-symbols-rounded">rocket_launch</span></button>
              <button class="icon-btn" type="button" title="${esc(t('actions.edit'))}" onclick="fleetEditNode(${node.id})"><span class="material-symbols-rounded">edit</span></button>
              <button class="icon-btn icon-btn--danger" type="button" title="${esc(t('actions.delete'))}" onclick="fleetDeleteNode(${node.id})"><span class="material-symbols-rounded">delete</span></button>
            </div>
          </td>
        </tr>`;
      }).join('');
      list.innerHTML = `<div class="table-wrap"><table class="data-table">
        <thead><tr>
          <th>${esc(t('nodes.server'))}</th>
          <th>${esc(t('nodes.roles'))}</th>
          <th>${esc(t('users.status'))}</th>
          <th>${esc(t('nodes.resources'))}</th>
          <th>${esc(t('nodes.updatedAt'))}</th>
          <th>${esc(t('nodes.actions'))}</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table></div>`;
    }
  }
}

async function saveSubscriptionDomain() {
  const input = document.getElementById('subscription-domain');
  if (!input) return;
  const domain = input.value.trim()
    .replace(/^https?:\/\//i, '')
    .replace(/\/.*$/, '')
    .replace(/\.$/, '');
  if (!domain) return toast(t('dashboard.domainRequired'), 'error');
  try {
    const res = await api('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ public_domain: domain })
    });
    fleet.settings = res.settings || fleet.settings;
    fleet.caddy = res.caddy || null;
    fleet.domainDraft = null;
    const message = res.caddy && res.caddy.message ? res.caddy.message : t('toast.saved');
    toast(message, res.caddy && res.caddy.applied ? 'success' : 'info');
    await loadActiveView();
  } catch (e) { toast(e.message, 'error'); }
}

async function toggleSubscriptions(enabled) {
  try {
    const res = await api('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ subscriptions_enabled: enabled })
    });
    fleet.settings = res.settings || fleet.settings;
    fleet.caddy = res.caddy || null;
    if (enabled && fleet.settings && !fleet.settings.subscriptions_enabled) {
      toast(t('dashboard.domainRequired'), 'error');
    } else {
      toast(res.caddy && res.caddy.message ? res.caddy.message : t('toast.saved'), res.caddy && res.caddy.applied ? 'success' : 'info');
    }
    await loadActiveView();
  } catch (e) {
    toast(e.message, 'error');
    await loadActiveView();
  }
}

async function deleteSubscriptionDomain() {
  if (!confirm(t('dashboard.confirmDeleteDomain'))) return;
  try {
    const res = await api('/api/settings/domain', { method: 'DELETE' });
    fleet.settings = res.settings || fleet.settings;
    fleet.caddy = res.caddy || null;
    fleet.domainDraft = null;
    toast(t('dashboard.domainDeleted'), 'success');
    await loadActiveView();
  } catch (e) { toast(e.message, 'error'); }
}

function networkSubscriptionsEnabled() {
  const settings = fleet.settings || {};
  return !!(settings.subscriptions_enabled && (settings.public_domain || '').trim());
}

function statusClass(status) {
  return ['new', 'deploying', 'deployed', 'online', 'stopped', 'error'].includes(status) ? status : 'new';
}

function nodeServicesRunning(node) {
  return node.status === 'deployed' || node.status === 'online';
}

function nodeServicesLabel(node) {
  if (nodeServicesRunning(node)) return t('nodes.servicesOn');
  if (node.status === 'stopped') return t('nodes.servicesOff');
  if (node.status === 'new') return t('nodes.servicesNotDeployed');
  if (node.status === 'deploying') return t('nodes.servicesBusy');
  return t('nodes.servicesUnknown');
}

function renderNodes() {
  const root = document.getElementById('nodes-list');
  if (!root) return;
  if (!fleet.nodes.length) {
    root.innerHTML = emptyState('dns', t('nodes.empty'), t('nodes.add'), "openNodeModal(null,'relay')");
    return;
  }
  root.innerHTML = fleet.nodes.map(node => {
    const exitOnly = node.is_exit && !node.is_relay;
    const transportChips = (node.transports || []).map(tr =>
      `<span class="transport-chip ${tr.enabled ? '' : 'muted'}">${esc(transportLabel(tr.type))}${tr.domain ? ' · ' + esc(tr.domain) : ''}</span>`
    ).join('');
    const transports = exitOnly ? '' : (transportChips || `<span class="transport-chip muted">${esc(t('nodes.noTransports'))}</span>`);
    const deployed = node.status === 'deployed' || node.status === 'online';
    const servicesRunning = nodeServicesRunning(node);
    const canToggleServices = node.status !== 'new' && node.status !== 'deploying';
    return `
      <article class="node-card" data-node-id="${escAttr(node.id)}">
        <div class="node-card__header">
          <div>
            <h3 class="card__title">${esc(node.name)}</h3>
            <span class="card__subtitle">${esc(node.ssh_user || 'root')}@${esc(node.ssh_host || '—')}:${esc(node.ssh_port || 22)}</span>
          </div>
          <span class="status-chip status-chip--${esc(statusClass(node.status))}" data-node-status="${escAttr(node.id)}">${esc(nodeStatusText(node))}</span>
        </div>
        <div class="node-card__meta">
          <span class="role-chip">${esc(nodeRoles(node))}</span>
          <span class="meta-chip"><span class="material-symbols-rounded">sensors</span><span data-node-last-seen="${escAttr(node.id)}">${esc(nodeLastSeenText(node))}</span></span>
          <span class="meta-chip" title="${escAttr(t('nodes.updatedAt'))}"><span class="material-symbols-rounded">rocket_launch</span><span data-node-updated="${escAttr(node.id)}" data-time-iso="${escAttr(node.updated_at || '')}">${esc(nodeDeployedText(node))}</span></span>
        </div>
        ${exitOnly ? '' : `<div class="transport-list">${transports}</div>`}
        ${nodeMetricsHTML(node)}
        ${node.last_error ? `<div class="card__error">${esc(node.last_error)}</div>` : ''}
        <div class="node-card__footer">
          <label class="switch-wrapper" title="${esc(t('nodes.toggleServices'))}">
            <span class="switch">
              <input type="checkbox" ${servicesRunning ? 'checked' : ''} ${canToggleServices ? '' : 'disabled'}
                     onchange="fleetToggleNodeServices(${node.id}, this.checked)">
              <span class="slider"></span>
            </span>
            <span>${esc(nodeServicesLabel(node))}</span>
          </label>
          <button class="btn btn--filled btn--compact" type="button" onclick="fleetDeployNode(${node.id})">
            <span class="material-symbols-rounded">rocket_launch</span>${esc(deployed ? t('nodes.redeploy') : t('nodes.deploy'))}
          </button>
          <button class="btn btn--tonal btn--compact" type="button" onclick="fleetEditNode(${node.id})">
            <span class="material-symbols-rounded">edit</span>${esc(t('actions.edit'))}
          </button>
          <button class="icon-btn icon-btn--danger" type="button" title="${esc(t('actions.delete'))}" onclick="fleetDeleteNode(${node.id})">
            <span class="material-symbols-rounded">delete</span>
          </button>
        </div>
      </article>`;
  }).join('');
}

async function fleetToggleNodeServices(id, running) {
  const node = fleet.nodes.find(n => n.id === id);
  const name = node ? node.name : String(id);
  if (!running && !confirm(t('nodes.confirmStopServices').replace('{name}', name))) {
    await loadActiveView();
    return;
  }
  try {
    const result = await api(`/api/nodes/${encodeURIComponent(id)}/${running ? 'start' : 'stop'}`, { method: 'POST' });
    toast(t(running ? 'toast.nodeStarted' : 'toast.nodeStopped'), 'success');
    if (result && !result.success) {
      const dialog = openModal({ title: name, body: '' });
      renderDeployResult(dialog, result);
    }
  } catch (e) {
    toast(e.message, 'error');
    if (e.payload && e.payload.service) {
      const dialog = openModal({ title: name, body: '' });
      renderDeployResult(dialog, e.payload.service);
    }
  }
  await loadActiveView();
}

async function fleetDeleteNode(id) {
  const node = fleet.nodes.find(n => n.id === id);
  const name = node ? node.name : String(id);
  let force = false;
  const dialog = openModal({
    title: t('nodes.deleteTitle'),
    body: `
      <p>${esc(t('nodes.deleteConfirmBody').replace('{name}', name))}</p>
      <ul class="cleanup-list">
        <li><span class="material-symbols-rounded">inventory_2</span>${esc(t('nodes.cleanupContainers'))}</li>
        <li><span class="material-symbols-rounded">folder_delete</span>${esc(t('nodes.cleanupFiles'))}</li>
        <li><span class="material-symbols-rounded">settings_suggest</span>${esc(t('nodes.cleanupSystemd'))}</li>
      </ul>
      <label class="check-row"><input type="checkbox" id="n-force"> ${esc(t('nodes.deleteForceOption'))}</label>
      <div class="hint">${esc(t('nodes.deleteForceHint'))}</div>`,
    actions: [
      { label: t('actions.cancel'), kind: 'text', onClick: closeModal },
      { label: t('nodes.deleteConfirmAction'), kind: 'danger', onClick: () => removeNode(dialog, id, force) }
    ]
  });
  const forceEl = document.getElementById('n-force');
  if (forceEl) forceEl.addEventListener('change', e => { force = e.target.checked; });
}

async function removeNode(dialog, id, force) {
  dialog.setBody(`<div class="deploy-log"><div class="deploy-step">${esc(t('nodes.cleaning'))}</div></div>`);
  try {
    const res = await api(`/api/nodes/${encodeURIComponent(id)}${force ? '?force=true' : ''}`, { method: 'DELETE' });
    if (res.cleanup) renderDeployResult(dialog, res.cleanup);
    else dialog.setBody(`<div class="deploy-summary is-ok"><span class="material-symbols-rounded">check_circle</span>${esc(t('toast.deleted'))}</div>`);
    toast(t('toast.deleted'), 'success');
    await loadActiveView();
  } catch (e) {
    if (e.payload && e.payload.cleanup) renderDeployResult(dialog, e.payload.cleanup);
    else dialog.setBody(`<div class="deploy-log"><div class="deploy-step is-error">${esc(e.message)}</div></div>`);
    toast(e.message, 'error');
  }
}

async function fleetRedeployAll() {
  try {
    const res = await api('/api/nodes/redeploy-all', { method: 'POST' });
    const queued = res.queued || [];
    if (!queued.length) {
      toast(t('nodes.redeployAllEmpty'));
      return;
    }
    toast(t('nodes.redeployAllQueued').replace('{count}', queued.length), 'success');
    const ids = new Set(queued.map(item => String(item.id)));
    const names = new Map(queued.map(item => [String(item.id), item.name]));
    const previous = new Map();
    const dialog = openModal({
      title: t('nodes.redeployAll'),
      body: `<div class="deploy-log" id="redeploy-all-log">${queued.map(item =>
        `<div class="deploy-step" data-redeploy-step="${escAttr(item.id)}"><div class="deploy-step__name">${esc(item.name)}</div><div class="deploy-step__cmd">${esc(t('nodes.queued'))}</div></div>`
      ).join('')}</div>`
    });
    const started = Date.now();
    const poll = async () => {
      if (!document.getElementById('redeploy-all-log')) return;
      try {
        const nodes = (await api('/api/nodes')) || [];
        const targets = nodes.filter(node => ids.has(String(node.id)));
        targets.forEach(node => {
          const id = String(node.id);
          const was = previous.get(id);
          if (was && was !== node.status && (node.status === 'deployed' || node.status === 'error')) {
            toast(`${names.get(id) || id}: ${nodeStatusText(node)}`, node.status === 'error' ? 'error' : 'success');
          }
          previous.set(id, node.status);
          const step = document.querySelector(`[data-redeploy-step="${escAttr(id)}"]`);
          if (!step) return;
          step.className = `deploy-step ${node.status === 'error' ? 'is-error' : node.status === 'deployed' ? 'is-ok' : ''}`;
          step.innerHTML = `<div class="deploy-step__name">${esc(names.get(id) || node.name)}</div>
            <div class="deploy-step__cmd">${esc(nodeStatusText(node))}</div>`;
        });
        const busy = targets.some(node => node.status === 'deploying');
        if (!busy && Date.now() - started > 1500) {
          await loadActiveView();
          toast(t('nodes.redeployAllDone'), 'success');
          return;
        }
      } catch (e) {
        // keep polling; the panel may be briefly unavailable
      }
      setTimeout(poll, 2000);
    };
    setTimeout(poll, 800);
    return dialog;
  } catch (e) { toast(e.message, 'error'); }
}

async function fleetDeployNode(id) {
  const node = fleet.nodes.find(n => n.id === id);
  const dialog = openModal({
    title: `${t('nodes.deploy')}: ${node ? node.name : id}`,
    body: `<div class="deploy-log"><div class="deploy-step">${esc(t('nodes.connecting'))}</div></div>`
  });
  try {
    const result = await api(`/api/nodes/${encodeURIComponent(id)}/deploy`, { method: 'POST' });
    renderDeployResult(dialog, result);
    toast(t('nodes.deployDone'), 'success');
  } catch (e) {
    if (e.payload && e.payload.deployment) renderDeployResult(dialog, e.payload.deployment);
    else dialog.setBody(`<div class="deploy-log"><div class="deploy-step is-error">${esc(e.message)}</div></div>`);
    toast(e.message, 'error');
  }
  await loadActiveView();
}

function renderDeployResult(dialog, result) {
  const steps = (result && result.steps) || [];
  const success = !!(result && result.success);
  let html = `<div class="deploy-summary ${success ? 'is-ok' : 'is-error'}">
    <span class="material-symbols-rounded">${success ? 'check_circle' : 'error'}</span>
    ${esc(success ? t('nodes.deploySucceeded') : t('nodes.deployFailed'))}
  </div><div class="deploy-log">`;
  if (!steps.length) html += `<div class="deploy-step">${esc(t('nodes.noSteps'))}</div>`;
  steps.forEach(step => {
    html += `<div class="deploy-step ${step.error ? 'is-error' : 'is-ok'}">
      <div class="deploy-step__name">${step.error ? '✗' : '✓'} ${esc(step.name)}</div>
      ${step.command ? `<div class="deploy-step__cmd">${esc(step.command)}</div>` : ''}
      ${step.output ? `<pre class="deploy-step__out">${esc(step.output)}</pre>` : ''}
      ${step.error ? `<pre class="deploy-step__err">${esc(step.error)}</pre>` : ''}
    </div>`;
  });
  html += '</div>';
  dialog.setBody(html);
  const body = document.querySelector('.modal__body');
  if (body) body.scrollTop = body.scrollHeight;
}

function emptyNodeDraft(role) {
  const relay = role === 'relay' || role === 'both';
  const exit = role === 'exit' || role === 'both';
  const defaultKind = (TRANSPORT_KINDS.find(k => k.default) || TRANSPORT_KINDS[0] || {}).value || '';
  const draft = {
    name: '', ssh_host: '', ssh_port: 22, ssh_user: 'root',
    ssh_private_key: '', ssh_password: '', authMode: 'key',
    is_relay: relay, is_exit: exit,
    secrets: {},
    transports: relay && defaultKind ? [{ type: defaultKind, domain: '', decoy_repo_url: '', enabled: true }] : []
  };
  if (relay) fillSecretKeys(draft, [{ key: 'encryption_password', generator: { kind: 'hex', bytes: 32 } }]);
  draft.transports.forEach(tr => fillDraftSecrets(draft, tr.type));
  return draft;
}

function nodeDraftFrom(node) {
  const draft = emptyNodeDraft('relay');
  if (!node) return draft;
  Object.assign(draft, {
    name: node.name || '',
    ssh_host: node.ssh_host || '',
    ssh_port: node.ssh_port || 22,
    ssh_user: node.ssh_user || 'root',
    ssh_private_key: node.ssh_private_key || '',
    ssh_password: node.ssh_password || '',
    is_relay: !!node.is_relay,
    is_exit: !!node.is_exit,
    secrets: Object.assign({}, node.secrets || node.generated_secrets || {}),
    transports: (node.transports || []).map(tr => ({ type: tr.type, domain: tr.domain || '', decoy_repo_url: tr.decoy_repo_url || '', enabled: !!tr.enabled }))
  });
  draft.authMode = draft.ssh_private_key ? 'key' : (draft.ssh_password ? 'password' : 'key');
  if (draft.is_relay) fillSecretKeys(draft, [{ key: 'encryption_password', generator: { kind: 'hex', bytes: 32 } }]);
  draft.transports.forEach(tr => fillDraftSecrets(draft, tr.type));
  return draft;
}

async function fleetEditNode(id) {
  if (!fleet.nodes.length) await loadPanel('nodes', true);
  const node = fleet.nodes.find(n => n.id === id);
  openNodeModal(node || null);
}

function openNodeModal(node, role) {
  const draft = node ? nodeDraftFrom(node) : emptyNodeDraft(role || 'relay');
  const editID = node ? node.id : null;
  fleet.nodeDraft = draft;

  const dialog = openModal({
    title: editID ? `${t('actions.edit')}: ${node.name}` : t('nodes.add'),
    actions: [
      { label: t('actions.cancel'), kind: 'text', onClick: closeModal },
      { label: editID ? t('actions.save') : t('actions.create'), kind: 'filled', onClick: submitNode }
    ]
  });

  function secretInput(field, value) {
    return `<label class="secret-field">
      <span>${esc(t(field.label))}</span>
      <span class="secret-field__control">
        <input type="text" class="input" autocomplete="off" spellcheck="false" value="${escAttr(value || '')}"
          oninput="fleetNodeSetSecret('${field.key}', this.value)">
        <button type="button" class="icon-btn secret-gen" title="${escAttr(t('nodes.regenerate'))}"
          onclick="fleetNodeRandomSecret('${field.key}')">🎲</button>
      </span>
    </label>`;
  }

  function transportRow(tr, i) {
    const usedByOthers = new Set(draft.transports.filter((_, idx) => idx !== i).map(item => item.type));
    const options = TRANSPORT_KINDS.map(k => {
      const used = usedByOthers.has(k.value);
      const label = used ? `${k.label} (${t('nodes.transportAlreadyAdded')})` : k.label;
      return `<option value="${escAttr(k.value)}" ${k.value === tr.type ? 'selected' : ''} ${used ? 'disabled' : ''}>${esc(label)}</option>`;
    }).join('');
    const fields = NODE_SECRET_FIELDS[tr.type] || [];
    const secrets = fields.length
      ? `<div class="transport-secrets">${fields.map(field => secretInput(field, (draft.secrets || {})[field.key])).join('')}</div>`
      : '';
    const decoy = (transportKindInfo(tr.type) || {}).decoy
      ? `<div class="transport-decoy">
          <label class="secret-field">
            <span>${esc(t('nodes.decoyRepoLabel'))}</span>
            <input type="text" class="input" autocomplete="off" spellcheck="false"
              value="${escAttr(tr.decoy_repo_url || '')}"
              placeholder="${escAttr(t('nodes.decoyRepoPlaceholder'))}"
              oninput="fleetNodeSetTransport(${i}, 'decoy_repo_url', this.value)">
          </label>
          <div class="hint">${esc(t('nodes.decoyRepoHint'))}</div>
        </div>`
      : '';
    return `<div class="transport-card">
      <div class="transport-row">
        <select class="input" onchange="fleetNodeChangeTransportType(${i}, this.value)">${options}</select>
        <input type="text" class="input" value="${escAttr(tr.domain)}" placeholder="domain.example.com"
               oninput="fleetNodeSetTransport(${i}, 'domain', this.value)">
        <label class="check-row"><input type="checkbox" ${tr.enabled ? 'checked' : ''}
          onchange="fleetNodeSetTransport(${i}, 'enabled', this.checked)"> ${esc(t('nodes.enabled'))}</label>
        <button type="button" class="icon-btn icon-btn--danger" title="${esc(t('actions.remove'))}"
          onclick="fleetNodeRemoveTransport(${i})"><span class="material-symbols-rounded">delete</span></button>
      </div>
      ${secrets}
      ${decoy}
    </div>`;
  }

  function renderNodeBody() {
    const transports = draft.transports.map(transportRow).join('') ||
      `<div class="hint">${esc(t('nodes.noTransports'))}</div>`;
    const authField = draft.authMode === 'password'
      ? `<input type="password" id="n-pass" value="${escAttr(draft.ssh_password)}" placeholder="${escAttr(t('nodes.passwordPlaceholder'))}" autocomplete="new-password">`
      : `<textarea id="n-key" placeholder="-----BEGIN OPENSSH PRIVATE KEY-----">${esc(draft.ssh_private_key)}</textarea>`;
    const usedTransportTypes = new Set(draft.transports.map(tr => tr.type));
    const canAddTransport = usedTransportTypes.size < TRANSPORT_KINDS.length;
    const addTransportButton = canAddTransport
      ? `<button type="button" class="btn btn--tonal btn--compact" style="align-self:flex-start;margin-top:8px"
            onclick="fleetNodeAddTransport()"><span class="material-symbols-rounded">add</span>${esc(t('nodes.addTransport'))}</button>`
      : '';
    const encryptionSection = draft.is_relay
      ? `<div class="field">
          <label>${esc(t('nodes.encryptionTitle'))}</label>
          <div class="encryption-badge">
            <span class="material-symbols-rounded">lock</span>
            <div><strong>${esc(t('nodes.encryptionCipher'))}</strong><span class="cipher">XChaCha20-Poly1305</span></div>
          </div>
          <div class="transport-secrets" style="margin-top:8px">
            ${secretInput({ key: 'encryption_password', label: 'nodes.encryptionPassword' }, draft.secrets.encryption_password)}
          </div>
          <div class="hint">${esc(t('nodes.encryptionHint'))}</div>
        </div>`
      : '';
    const transportSection = draft.is_relay
      ? `${encryptionSection}
        <div class="field">
          <label>${esc(t('nodes.transports'))}</label>
          <div class="transport-rows">${transports}</div>
          ${addTransportButton}
          <div class="hint">${esc(t('nodes.secretsHint'))}</div>
        </div>`
      : '';
    dialog.setBody(`
      <div class="form-group">
        <div class="field">
          <label>${esc(t('roles.title'))}</label>
          <div class="role-row">
            <label class="role-option">
              <input type="checkbox" class="role-option__input" id="n-relay" ${draft.is_relay ? 'checked' : ''}>
              <span class="role-option__box"><span class="material-symbols-rounded">check</span></span>
              <span class="role-option__text"><strong>${esc(t('roles.relay'))}</strong><span>${esc(t('roles.relayHint'))}</span></span>
            </label>
            <label class="role-option">
              <input type="checkbox" class="role-option__input" id="n-exit" ${draft.is_exit ? 'checked' : ''}>
              <span class="role-option__box"><span class="material-symbols-rounded">check</span></span>
              <span class="role-option__text"><strong>${esc(t('roles.exit'))}</strong><span>${esc(t('roles.exitHint'))}</span></span>
            </label>
          </div>
        </div>
        <div class="form-row">
          <div class="field"><label>${esc(t('nodes.name'))}</label><input type="text" id="n-name" value="${escAttr(draft.name)}" placeholder="helsinki-1"></div>
          <div class="field"><label>${esc(t('nodes.host'))}</label><input type="text" id="n-host" value="${escAttr(draft.ssh_host)}" placeholder="203.0.113.10" autocomplete="off" spellcheck="false"></div>
        </div>
        <div class="form-row">
          <div class="field"><label>${esc(t('nodes.user'))}</label><input type="text" id="n-user" value="${escAttr(draft.ssh_user)}" placeholder="root" autocomplete="off"></div>
          <div class="field field--small"><label>${esc(t('nodes.sshPort'))}</label><input type="number" id="n-port" value="${escAttr(draft.ssh_port)}" min="1"></div>
        </div>
        <div class="field">
          <label>${esc(t('nodes.sshAuth'))}</label>
          <div class="segmented" role="group" aria-label="${escAttr(t('nodes.sshAuth'))}">
            <button type="button" class="segmented__btn ${draft.authMode === 'password' ? '' : 'active'}" onclick="fleetNodeSetAuth('key')">${esc(t('nodes.authKey'))}</button>
            <button type="button" class="segmented__btn ${draft.authMode === 'password' ? 'active' : ''}" onclick="fleetNodeSetAuth('password')">${esc(t('nodes.authPassword'))}</button>
          </div>
          <div class="hint">${esc(t('nodes.authHint'))}</div>
          ${authField}
        </div>
        ${transportSection}
      </div>`);
    bindNodeForm();
  }

  function bindNodeForm() {
    const bindInput = (id, key, numeric) => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('input', e => {
        draft[key] = numeric ? Number(e.target.value) || 0 : e.target.value;
      });
    };
    bindInput('n-name', 'name');
    bindInput('n-host', 'ssh_host');
    bindInput('n-port', 'ssh_port', true);
    bindInput('n-user', 'ssh_user');
    bindInput('n-key', 'ssh_private_key');
    bindInput('n-pass', 'ssh_password');
    const relay = document.getElementById('n-relay');
    if (relay) relay.addEventListener('change', e => {
      draft.is_relay = e.target.checked;
      if (draft.is_relay && !draft.transports.length) {
        const defaultKind = (TRANSPORT_KINDS.find(k => k.default) || TRANSPORT_KINDS[0] || {}).value;
        if (defaultKind) draft.transports.push({ type: defaultKind, domain: '', decoy_repo_url: '', enabled: true });
      }
      if (draft.is_relay) {
        fillSecretKeys(draft, [{ key: 'encryption_password', generator: { kind: 'hex', bytes: 32 } }]);
        draft.transports.forEach(tr => fillDraftSecrets(draft, tr.type));
      }
      renderNodeBody();
    });
    const exit = document.getElementById('n-exit');
    if (exit) exit.addEventListener('change', e => { draft.is_exit = e.target.checked; });
  }

  async function submitNode() {
    const name = draft.name.trim();
    const host = draft.ssh_host.trim();
    if (!name) return toast(t('nodes.nameRequired'), 'error');
    if (!host) return toast(t('nodes.hostRequired'), 'error');
    if (!draft.is_relay && !draft.is_exit) return toast(t('nodes.roleRequired'), 'error');
    if (draft.is_relay) {
      const types = draft.transports.map(tr => tr.type);
      if (new Set(types).size !== types.length) return toast(t('nodes.duplicateTransport'), 'error');
      const enabled = draft.transports.filter(tr => tr.enabled);
      if (!enabled.length) return toast(t('nodes.transportRequired'), 'error');
      if (enabled.some(tr => !tr.domain.trim())) return toast(t('nodes.domainRequired'), 'error');
    }
    const payload = {
      name,
      ssh_host: host,
      ssh_port: draft.ssh_port,
      ssh_user: draft.ssh_user.trim() || 'root',
      ssh_private_key: draft.ssh_private_key,
      ssh_password: draft.ssh_password,
      is_relay: draft.is_relay,
      is_exit: draft.is_exit,
      transports: draft.is_relay ? draft.transports.map(tr => {
        const item = { type: tr.type, domain: tr.domain, enabled: tr.enabled };
        const decoyURL = String(tr.decoy_repo_url || '').trim();
        if ((transportKindInfo(tr.type) || {}).decoy && decoyURL) item.decoy_repo_url = decoyURL;
        return item;
      }) : []
    };
    if (draft.is_relay) {
      const secrets = {};
      Object.entries(draft.secrets || {}).forEach(([key, value]) => {
        if (String(value || '').trim()) secrets[key] = value;
      });
      payload.secrets = secrets;
    }
    const path = editID ? `/api/nodes/${encodeURIComponent(editID)}` : '/api/nodes';
    try {
      await api(path, {
        method: editID ? 'PUT' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      toast(editID ? t('toast.saved') : t('toast.created'), 'success');
      closeModal();
      await loadActiveView();
    } catch (e) {
      toast(e.message, 'error');
    }
  }

  fleet.renderNodeBody = renderNodeBody;
  renderNodeBody();
}

function fleetNodeSetTransport(index, key, value) {
  const draft = fleet.nodeDraft;
  if (!draft || !draft.transports[index]) return;
  draft.transports[index][key] = value;
}

function fleetNodeSetSecret(key, value) {
  const draft = fleet.nodeDraft;
  if (!draft) return;
  if (!draft.secrets) draft.secrets = {};
  draft.secrets[key] = value;
}

function fleetNodeRandomSecret(key) {
  const draft = fleet.nodeDraft;
  if (!draft) return;
  if (!draft.secrets) draft.secrets = {};
  draft.secrets[key] = generateSecretValue(secretSpecByKey(key));
  if (fleet.renderNodeBody) fleet.renderNodeBody();
}

function fleetNodeChangeTransportType(index, value) {
  const draft = fleet.nodeDraft;
  if (!draft || !draft.transports[index]) return;
  if (draft.transports.some((tr, idx) => idx !== index && tr.type === value)) {
    toast(t('nodes.duplicateTransport'), 'error');
    if (fleet.renderNodeBody) fleet.renderNodeBody();
    return;
  }
  draft.transports[index].type = value;
  fillDraftSecrets(draft, value);
  if (fleet.renderNodeBody) fleet.renderNodeBody();
}

function fleetNodeSetAuth(mode) {
  const draft = fleet.nodeDraft;
  if (!draft) return;
  draft.authMode = mode === 'password' ? 'password' : 'key';
  if (fleet.renderNodeBody) fleet.renderNodeBody();
}

function fleetNodeAddTransport() {
  const draft = fleet.nodeDraft;
  if (!draft) return;
  const next = TRANSPORT_KINDS.find(kind => !draft.transports.some(tr => tr.type === kind.value));
  if (!next) {
    toast(t('nodes.allTransportsAdded'), 'info');
    return;
  }
  draft.transports.push({ type: next.value, domain: '', decoy_repo_url: '', enabled: true });
  fillDraftSecrets(draft, next.value);
  if (fleet.renderNodeBody) fleet.renderNodeBody();
}

function fleetNodeRemoveTransport(index) {
  const draft = fleet.nodeDraft;
  if (!draft) return;
  draft.transports.splice(index, 1);
  if (fleet.renderNodeBody) fleet.renderNodeBody();
}

function nodeName(id) {
  const node = fleet.nodes.find(n => n.id === id);
  return node ? node.name : '#' + id;
}

function renderRoutes() {
  const root = document.getElementById('routes-list');
  if (!root) return;
  if (!fleet.routes.length) {
    root.innerHTML = emptyState('alt_route', t('routes.empty'), t('routes.add'), 'fleetAddRoute()');
    return;
  }
  const rows = fleet.routes.map(route => {
    const isMulti = isMultiKind(route.transport_type);
    const transportCell = isMulti
      ? `<span class="transport-chip transport-chip--multi">${esc(t('routes.multiBadge'))} · ${esc(multiStrategyLabel(route.multi_strategy))}</span>
         <div class="table-sub">${esc(subRouteNames(route.sub_route_ids))}</div>`
      : `<span class="transport-chip">${esc(transportLabel(route.transport_type))}</span>`;
    return `
    <tr>
      <td><strong>${esc(route.name)}</strong><div class="table-sub">#${esc(route.id)}</div></td>
      <td>${esc(nodeName(route.entry_node_id))} <span class="arrow">→</span> ${esc(nodeName(route.exit_node_id))}</td>
      <td>${transportCell}</td>
      <td>
        <label class="switch">
          <input type="checkbox" ${route.enabled ? 'checked' : ''} onchange="fleetToggleRoute(${route.id}, this.checked)">
          <span class="slider"></span>
        </label>
      </td>
      <td>
        <div class="table-actions">
          <button class="icon-btn" type="button" title="${esc(t('actions.edit'))}" onclick="fleetEditRoute(${route.id})"><span class="material-symbols-rounded">edit</span></button>
          <button class="icon-btn icon-btn--danger" type="button" title="${esc(t('actions.delete'))}" onclick="fleetDeleteRoute(${route.id})"><span class="material-symbols-rounded">delete</span></button>
        </div>
      </td>
    </tr>`;
  }).join('');
  root.innerHTML = `<div class="table-wrap"><table class="data-table">
    <thead><tr><th>${esc(t('routes.name'))}</th><th>${esc(t('routes.path'))}</th><th>${esc(t('routes.transport'))}</th><th>${esc(t('routes.enabled'))}</th><th></th></tr></thead>
    <tbody>${rows}</tbody>
  </table></div>`;
}

async function fleetToggleRoute(id, enabled) {
  const route = fleet.routes.find(r => r.id === id);
  if (!route) return;
  try {
    await api(`/api/routes/${encodeURIComponent(id)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(isMultiKind(route.transport_type)
        ? {
            name: route.name,
            transport_type: route.transport_type,
            multi_strategy: route.multi_strategy || defaultMultiStrategy(route.transport_type),
            sub_route_ids: route.sub_route_ids || [],
            enabled: enabled
          }
        : {
            name: route.name,
            entry_node_id: route.entry_node_id,
            exit_node_id: route.exit_node_id,
            transport_type: route.transport_type,
            enabled: enabled
          })
    });
    route.enabled = enabled;
    toast(enabled ? t('routes.enabledToast') : t('routes.disabledToast'), 'success');
    await loadActiveView();
  } catch (e) {
    toast(e.message, 'error');
    await loadActiveView();
  }
}

async function fleetDeleteRoute(id) {
  const route = fleet.routes.find(r => r.id === id);
  if (!confirm(t('routes.confirmDelete').replace('{name}', route ? route.name : String(id)))) return;
  try {
    await api(`/api/routes/${encodeURIComponent(id)}`, { method: 'DELETE' });
    toast(t('toast.deleted'), 'success');
    await loadActiveView();
  } catch (e) { toast(e.message, 'error'); }
}

async function fleetEditRoute(id) {
  const route = fleet.routes.find(r => r.id === id) || null;
  if (!route) return;
  if (!fleet.nodes.length) await loadPanel('routes', true);
  openRouteModal(route);
}

async function fleetAddRoute() {
  if (!fleet.nodes.length) await loadPanel('routes', true);
  openRouteModal();
}

function openRouteModal(route) {
  const editID = route ? route.id : null;
  const relays = fleet.nodes.filter(n => n.is_relay);
  const exits = fleet.nodes.filter(n => n.is_exit);
  const isMulti = !!route && isMultiKind(route.transport_type);
  const draft = {
    mode: isMulti ? 'multi' : 'single',
    name: route ? route.name : '',
    entry_node_id: route ? route.entry_node_id : (relays[0] ? relays[0].id : 0),
    exit_node_id: route ? route.exit_node_id : (exits[0] ? exits[0].id : 0),
    transport_type: route && !isMultiKind(route.transport_type) ? route.transport_type : (TRANSPORT_KINDS[0] || {}).value,
    multi_strategy: route && route.multi_strategy ? route.multi_strategy : defaultMultiStrategy(primaryMultiKind()),
    sub_route_ids: route && Array.isArray(route.sub_route_ids) ? route.sub_route_ids.slice() : [],
    enabled: route ? !!route.enabled : true
  };
  fleet.routeDraft = draft;
  fleet.routeEditID = editID;

  function selectOptions(nodes, selected, emptyLabel) {
    if (!nodes.length) return `<option value="0">${esc(emptyLabel)}</option>`;
    return nodes.map(n => `<option value="${n.id}" ${n.id === selected ? 'selected' : ''}>${esc(n.name)}</option>`).join('');
  }

  function transportOptions() {
    const taken = takenRouteTransports(draft.entry_node_id, draft.exit_node_id, editID);
    return TRANSPORT_KINDS.map(k => {
      const disabled = taken.has(k.value);
      const label = disabled ? `${k.label} (${t('routes.transportTaken')})` : k.label;
      return `<option value="${escAttr(k.value)}" ${k.value === draft.transport_type ? 'selected' : ''} ${disabled ? 'disabled' : ''}>${esc(label)}</option>`;
    }).join('');
  }

  function strategyOptions() {
    return multiStrategies(primaryMultiKind()).map(option => {
      const label = option.id === 'bond' ? t('routes.strategyBond')
        : option.id === 'flowmux' ? t('routes.strategyFlowmux')
        : (option.label || option.id);
      return `<option value="${escAttr(option.id)}" ${option.id === draft.multi_strategy ? 'selected' : ''}>${esc(label)}</option>`;
    }).join('');
  }

  function childRoutes() {
    return (fleet.routes || []).filter(r => !isMultiKind(r.transport_type) && (!editID || r.id !== editID));
  }

  function childCheckboxes() {
    const options = childRoutes();
    if (!options.length) return `<div class="hint">${esc(t('routes.noSubRoutes'))}</div>`;
    return options.map(r => {
      const checked = (draft.sub_route_ids || []).includes(r.id);
      const disabled = r.enabled ? '' : ` · ${t('routes.childDisabled')}`;
      return `<label class="check-row"><input type="checkbox" ${checked ? 'checked' : ''}
        onchange="fleetRouteToggleChild(${r.id}, this.checked)">
        <span>${esc(r.name)} · ${esc(transportLabel(r.transport_type))}${esc(disabled)}</span></label>`;
    }).join('');
  }

  const dialog = openModal({
    title: editID ? `${t('actions.edit')}: ${route.name}` : t('routes.add'),
    actions: [
      { label: t('actions.cancel'), kind: 'text', onClick: closeModal },
      { label: editID ? t('actions.save') : t('actions.create'), kind: 'filled', onClick: submitRoute }
    ]
  });

  function renderRouteBody() {
    const multi = draft.mode === 'multi';
    dialog.setBody(`
      <div class="form-group">
        <div class="field"><label>${esc(t('routes.name'))}</label><input type="text" id="r-name" value="${escAttr(draft.name)}" placeholder="helsinki-frankfurt"></div>
        <div class="field">
          <label>${esc(t('routes.mode'))}</label>
          <div class="segmented" role="group">
            <button type="button" class="segmented__btn ${multi ? '' : 'active'}" onclick="fleetRouteSetMode('single')">${esc(t('routes.modeSingle'))}</button>
            <button type="button" class="segmented__btn ${multi ? 'active' : ''}" onclick="fleetRouteSetMode('multi')">${esc(t('routes.modeMulti'))}</button>
          </div>
        </div>
        ${multi ? `
        <div class="field"><label>${esc(t('routes.multiStrategy'))}</label>
          <select id="r-strategy" onchange="fleetRouteSetStrategy(this.value)">${strategyOptions()}</select>
        </div>
        <div class="field"><label>${esc(t('routes.subRoutes'))}</label>
          <div class="check-list">${childCheckboxes()}</div>
          <div class="hint">${esc(t('routes.multiHint'))}</div>
        </div>` : `
        <div class="form-row">
          <div class="field"><label>${esc(t('routes.from'))}</label><select id="r-entry" onchange="fleetRouteSetNode('entry_node_id', this.value)">${selectOptions(relays, draft.entry_node_id, t('routes.noRelays'))}</select></div>
          <div class="field"><label>${esc(t('routes.to'))}</label><select id="r-exit" onchange="fleetRouteSetNode('exit_node_id', this.value)">${selectOptions(exits, draft.exit_node_id, t('routes.noExits'))}</select></div>
        </div>
        <div class="field"><label>${esc(t('routes.transport'))}</label><select id="r-transport" onchange="fleetRouteSetTransport(this.value)">
          ${transportOptions()}
        </select></div>`}
        <label class="check-row"><input type="checkbox" id="r-enabled" ${draft.enabled ? 'checked' : ''}> ${esc(t('routes.enabled'))}</label>
        <div class="hint">${esc(t('routes.hint'))}</div>
      </div>`);
    const nameEl = document.getElementById('r-name');
    if (nameEl) nameEl.addEventListener('input', e => { draft.name = e.target.value; });
    const enabledEl = document.getElementById('r-enabled');
    if (enabledEl) enabledEl.addEventListener('change', e => { draft.enabled = e.target.checked; });
  }

  fleet.renderRouteBody = renderRouteBody;
  renderRouteBody();

  async function submitRoute() {
    if (draft.mode === 'multi') {
      if (!draft.sub_route_ids || draft.sub_route_ids.length < 2) {
        return toast(t('routes.subRoutesRequired'), 'error');
      }
    } else {
      if (!draft.entry_node_id || !draft.exit_node_id) return toast(t('routes.nodesRequired'), 'error');
      if (takenRouteTransports(draft.entry_node_id, draft.exit_node_id, editID).has(draft.transport_type)) {
        return toast(t('routes.transportTaken'), 'error');
      }
    }
    if (!draft.name.trim()) {
      if (draft.mode === 'multi') {
        const names = (draft.sub_route_ids || []).map(id => {
          const child = fleet.routes.find(r => r.id === id);
          return child ? child.name : String(id);
        });
        draft.name = (names.slice(0, 2).join('-') || 'multi') + '-multi';
      } else {
        draft.name = `${nodeName(draft.entry_node_id)}-${nodeName(draft.exit_node_id)}`;
      }
      draft.name = draft.name.toLowerCase().replace(/[^a-z0-9_-]+/g, '-');
    }
    const payload = draft.mode === 'multi'
      ? {
          name: draft.name,
          transport_type: primaryMultiKind(),
          multi_strategy: draft.multi_strategy || defaultMultiStrategy(primaryMultiKind()),
          sub_route_ids: (draft.sub_route_ids || []).slice(),
          enabled: draft.enabled
        }
      : {
          name: draft.name,
          entry_node_id: draft.entry_node_id,
          exit_node_id: draft.exit_node_id,
          transport_type: draft.transport_type,
          enabled: draft.enabled
        };
    const path = editID ? `/api/routes/${encodeURIComponent(editID)}` : '/api/routes';
    try {
      await api(path, {
        method: editID ? 'PUT' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      toast(editID ? t('toast.saved') : t('toast.created'), 'success');
      closeModal();
      await loadActiveView();
    } catch (e) {
      toast(e.status === 409 && draft.mode !== 'multi' ? t('routes.transportTaken') : e.message, 'error');
    }
  }
  dialog.setFocus?.('r-name');
}

function fleetRouteSetMode(mode) {
  const draft = fleet.routeDraft;
  if (!draft) return;
  draft.mode = mode === 'multi' ? 'multi' : 'single';
  if (draft.mode === 'single' && isMultiKind(draft.transport_type)) {
    const taken = takenRouteTransports(draft.entry_node_id, draft.exit_node_id, fleet.routeEditID);
    const next = TRANSPORT_KINDS.find(k => !taken.has(k.value));
    draft.transport_type = next ? next.value : TRANSPORT_KINDS[0].value;
  }
  if (fleet.renderRouteBody) fleet.renderRouteBody();
}

function fleetRouteSetStrategy(strategy) {
  const draft = fleet.routeDraft;
  if (!draft) return;
  draft.multi_strategy = strategy === 'flowmux' ? 'flowmux' : 'bond';
}

function fleetRouteToggleChild(id, checked) {
  const draft = fleet.routeDraft;
  if (!draft) return;
  const selected = new Set(draft.sub_route_ids || []);
  if (checked) selected.add(Number(id)); else selected.delete(Number(id));
  draft.sub_route_ids = Array.from(selected);
}

function fleetRouteSetNode(key, value) {
  const draft = fleet.routeDraft;
  if (!draft) return;
  draft[key] = Number(value) || 0;
  const taken = takenRouteTransports(draft.entry_node_id, draft.exit_node_id, fleet.routeEditID);
  if (taken.has(draft.transport_type)) {
    const next = TRANSPORT_KINDS.find(k => !taken.has(k.value));
    if (next) draft.transport_type = next.value;
  }
  if (fleet.renderRouteBody) fleet.renderRouteBody();
}

function fleetRouteSetTransport(value) {
  const draft = fleet.routeDraft;
  if (!draft) return;
  draft.transport_type = value;
}

function userUsedBytes(user) {
  return (user.used_upload_bytes || 0) + (user.used_download_bytes || 0);
}

function expiryTime(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  if (!Number.isFinite(ts) || ts <= 0) return null;
  // The Go API serialises a zero time.Time as 0001-01-01T00:00:00Z, which is
  // how "no expiry" reaches the browser. Treat anything before 1970 as unset.
  if (new Date(ts).getUTCFullYear() < 1970) return null;
  return ts;
}

function userActive(user) {
  if (!user.enabled) return false;
  const expires = expiryTime(user.expires_at);
  if (expires && expires <= Date.now()) return false;
  if (user.traffic_limit_bytes > 0 && userUsedBytes(user) >= user.traffic_limit_bytes) return false;
  return true;
}

function formatExpiry(value) {
  const ts = expiryTime(value);
  return ts ? new Date(ts).toLocaleDateString() : t('users.never');
}

function routeNames(ids) {
  if (!ids || !ids.length) return t('users.allRoutes');
  return ids.map(id => {
    const route = fleet.routes.find(r => r.id === id);
    return route ? route.name : '#' + id;
  }).join(', ');
}

function userOnlineBadge(user) {
  if (!user || !user.online) return '';
  const speed = formatSpeed(user.current_speed_bps);
  return `<span class="status-chip status-chip--online user-online"><span class="material-symbols-rounded">check_circle</span>${esc(t('users.online'))}<span class="user-online__speed">• ${esc(speed)}</span></span>`;
}

/* Compact material summary above the users table: online users, the summed
   live throughput and the summed traffic counter. Rebuilt on every first
   render and on every live poll so the numbers stay in sync with the rows. */
function renderUsersSummary() {
  const root = document.getElementById('users-summary');
  if (!root) return;
  const users = fleet.users || [];
  if (!users.length) {
    root.innerHTML = '';
    return;
  }
  const online = users.filter(user => !!user.online).length;
  const speed = users.reduce((sum, user) => sum + (Number(user.current_speed_bps) || 0), 0);
  const traffic = users.reduce((sum, user) => sum + userUsedBytes(user), 0);
  const item = (icon, label, value) => `<div class="users-summary__item">
      <span class="material-symbols-rounded">${icon}</span>
      <span class="users-summary__label">${esc(label)}</span>
      <strong class="users-summary__value">${esc(value)}</strong>
    </div>`;
  root.innerHTML = item('group', t('users.summaryOnline'), `${online} / ${users.length}`) +
    item('speed', t('users.summarySpeed'), formatSpeed(speed)) +
    item('data_usage', t('users.summaryTraffic'), formatBytes(traffic));
}

function userSpeedLimitText(kbps) {
  return t('users.speedLimitValue').replace('{value}', kbps);
}

function renderUsers() {
  const root = document.getElementById('users-list');
  if (!root) return;
  renderUsersSummary();
  if (!fleet.users.length) {
    root.innerHTML = emptyState('group', t('users.empty'), t('users.add'), 'openUserModal(null)');
    return;
  }
  const canNetwork = networkSubscriptionsEnabled();
  const rows = fleet.users.map(user => {
    const active = userActive(user);
    const subActions = canNetwork
      ? `<button class="icon-btn" type="button" title="${esc(t('users.qr'))}" onclick="openSubscriptionDialog('${escAttr(user.id)}')"><span class="material-symbols-rounded">qr_code_2</span></button>
        <button class="icon-btn" type="button" title="${esc(t('users.copy'))}" onclick="fleetCopySubscription('${escAttr(user.id)}')"><span class="material-symbols-rounded">content_copy</span></button>`
      : `<button class="icon-btn" type="button" title="${esc(t('users.offline'))}" onclick="openSubscriptionDialog('${escAttr(user.id)}')"><span class="material-symbols-rounded">description</span></button>`;
    const routeChips = (user.allowed_route_ids && user.allowed_route_ids.length)
      ? user.allowed_route_ids.map(id => {
          const route = (fleet.routes || []).find(r => r.id === id);
          return `<span class="chip">${esc(route ? route.name : '#' + id)}</span>`;
        }).join('')
      : `<span class="chip chip--stopped">${esc(t('users.allRoutes'))}</span>`;
    return `<tr data-user-id="${escAttr(user.id)}">
      <td>
        <div class="user-name-row"><strong>${esc(user.name || user.id)}</strong><span data-user-online="${escAttr(user.id)}">${userOnlineBadge(user)}</span></div>
        <div class="table-sub">${esc(user.id)}</div>
        ${user.max_speed_kbps > 0 ? `<div class="table-sub" data-user-speed="${escAttr(user.id)}">${esc(userSpeedLimitText(user.max_speed_kbps))}</div>` : ''}
      </td>
      <td>
        <span class="status-chip ${active ? 'status-chip--online' : 'status-chip--error'}" data-user-status="${escAttr(user.id)}">${esc(active ? t('users.active') : t('users.inactive'))}</span>
        <div class="table-sub" data-user-expiry="${escAttr(user.id)}">${esc(formatExpiry(user.expires_at))}${expiryLeft(user.expires_at)}</div>
      </td>
      <td data-user-traffic="${escAttr(user.id)}">${userTrafficHTML(user)}</td>
      <td><div class="chip-row">${routeChips}</div></td>
      <td>
        <div class="table-actions">
          ${subActions}
          <button class="icon-btn" type="button" title="${esc(t('users.edit'))}" onclick="openUserModal('${escAttr(user.id)}')"><span class="material-symbols-rounded">edit</span></button>
          <button class="icon-btn" type="button" title="${esc(t('users.resetTraffic'))}" onclick="fleetResetUserTraffic('${escAttr(user.id)}')"><span class="material-symbols-rounded">restart_alt</span></button>
          <button class="icon-btn" type="button" title="${esc(t('actions.enable'))}" onclick="fleetToggleUser('${escAttr(user.id)}', ${active ? 'false' : 'true'})"><span class="material-symbols-rounded">${active ? 'toggle_on' : 'toggle_off'}</span></button>
          <button class="icon-btn icon-btn--danger" type="button" title="${esc(t('actions.delete'))}" onclick="fleetDeleteUser('${escAttr(user.id)}')"><span class="material-symbols-rounded">delete</span></button>
        </div>
      </td>
    </tr>`;
  }).join('');
  root.innerHTML = `<div class="table-wrap"><table class="data-table">
    <thead><tr><th>${esc(t('users.name'))}</th><th>${esc(t('users.status'))}</th><th>${esc(t('users.traffic'))}</th><th>${esc(t('users.routes'))}</th><th>${esc(t('nodes.actions'))}</th></tr></thead>
    <tbody>${rows}</tbody>
  </table></div>`;
}

function expiryLeft(value) {
  const ts = expiryTime(value);
  if (!ts) return '';
  const days = Math.ceil((ts - Date.now()) / 86400000);
  if (days < 0) return '';
  return ` · ${t('users.daysLeft').replace('{days}', days)}`;
}

function networkSubscriptionURL(user) {
  if (!networkSubscriptionsEnabled()) return '';
  return user.subscription_url || '';
}

async function fleetCopySubscription(id) {
  const user = fleet.users.find(u => u.id === id);
  if (!user) return;
  const url = networkSubscriptionURL(user);
  if (!url) return toast(t('users.noPublicURL'), 'error');
  try { await copyToClipboard(url); toast(t('toast.copied'), 'success'); }
  catch { toast(t('toast.copyFailed'), 'error'); }
}

async function fetchUserBundle(user) {
  const res = await api(`/api/users/${encodeURIComponent(user.id)}/bundle`);
  return JSON.stringify(res, null, 2);
}

async function copyOfflineLink(id) {
  const user = fleet.users.find(u => u.id === id);
  if (!user) return;
  try {
    const bundle = await api(`/api/users/${encodeURIComponent(user.id)}/bundle`);
    const link = Array.isArray(bundle.links) ? bundle.links[0] : '';
    if (!link) return toast(t('users.bundleEmpty'), 'error');
    await copyToClipboard(link);
    toast(t('toast.copied'), 'success');
  } catch (e) { toast(e.message, 'error'); }
}

function bundleFileName(user) {
  const base = String(user.name || user.id || 'shampoore')
    .replace(/[^a-zA-Z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return `shampoore-${base || 'user'}.json`;
}

async function copyUserBundle(id) {
  const user = fleet.users.find(u => u.id === id);
  if (!user) return;
  try {
    const json = await fetchUserBundle(user);
    await copyToClipboard(json);
    toast(t('users.bundleCopied'), 'success');
  } catch (e) { toast(e.message, 'error'); }
}

async function downloadUserBundle(id) {
  const user = fleet.users.find(u => u.id === id);
  if (!user) return;
  try {
    const json = await fetchUserBundle(user);
    const blob = new Blob([json], { type: 'application/json' });
    const objectURL = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = objectURL;
    link.download = bundleFileName(user);
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(objectURL), 1000);
  } catch (e) { toast(e.message, 'error'); }
}

function renderQR(container, text) {
  if (!container) return;
  if (!text) {
    container.innerHTML = `<div class="hint">${esc(t('users.noPublicURL'))}</div>`;
    return;
  }
  if (typeof qrcode !== 'function') {
    container.innerHTML = `<div class="hint">QR: ${esc(text)}</div>`;
    return;
  }
  try {
    const qr = qrcode(0, 'M');
    qr.addData(text);
    qr.make();
    container.innerHTML = qr.createImgTag(5, 8);
  } catch (e) {
    container.innerHTML = `<div class="hint">${esc(text)}</div>`;
  }
}

function openSubscriptionDialog(id) {
  const user = fleet.users.find(u => u.id === id);
  if (!user) return;
  const url = networkSubscriptionURL(user);
  const showNetwork = !!url;
  const dialog = openModal({
    title: `${t('users.subscription')}: ${user.name || user.id}`,
    actions: [{ label: t('actions.close'), kind: 'text', onClick: closeModal }]
  });
  const networkBlock = showNetwork ? `
    <section class="sub-section">
      <div class="sub-section__title"><span class="material-symbols-rounded">qr_code_2</span>${esc(t('users.networkSubscription'))}</div>
      <div class="qr-card">
        <div class="qr-box" id="user-qr"></div>
        <div class="qr-card__info">
          <div class="sub-link" id="user-sub-link">${esc(url)}</div>
          <p class="hint" style="margin-top:10px">${esc(t('users.qrHint'))}</p>
        </div>
      </div>
      <div class="sub-section__actions">
        <button class="btn btn--tonal btn--compact" type="button" onclick="fleetCopySubscription('${escAttr(user.id)}')">
          <span class="material-symbols-rounded">content_copy</span>${esc(t('actions.copy'))}
        </button>
      </div>
    </section>` : '';
  const offlineBlock = `
    <section class="sub-section">
      <div class="sub-section__title"><span class="material-symbols-rounded">description</span>${esc(t('users.offlineTitle'))}</div>
      <p class="hint">${esc(showNetwork ? t('users.offlineHint') : t('users.offlineOnly'))}</p>
      <textarea class="sub-json" id="user-bundle" readonly spellcheck="false"></textarea>
      <p class="hint hidden" id="user-bundle-empty">${esc(t('users.bundleEmpty'))}</p>
      <div class="sub-section__actions">
        <button class="btn btn--tonal btn--compact" type="button" onclick="copyOfflineLink('${escAttr(user.id)}')">
          <span class="material-symbols-rounded">link</span>${esc(t('users.copyLink'))}
        </button>
        <button class="btn btn--tonal btn--compact" type="button" onclick="copyUserBundle('${escAttr(user.id)}')">
          <span class="material-symbols-rounded">content_copy</span>${esc(t('users.copyJson'))}
        </button>
        <button class="btn btn--filled btn--compact" type="button" onclick="downloadUserBundle('${escAttr(user.id)}')">
          <span class="material-symbols-rounded">download</span>${esc(t('users.downloadJson'))}
        </button>
      </div>
    </section>`;
  dialog.setBody(`${networkBlock}${offlineBlock}`);
  if (showNetwork) renderQR(document.getElementById('user-qr'), url);
  const bundleEl = document.getElementById('user-bundle');
  if (!bundleEl) return dialog;
  fetchUserBundle(user).then(json => {
    bundleEl.value = json;
    const emptyEl = document.getElementById('user-bundle-empty');
    try {
      const parsed = JSON.parse(json);
      if (emptyEl && (!parsed.links || !parsed.links.length)) emptyEl.classList.remove('hidden');
    } catch { /* keep the raw text */ }
  }).catch(e => {
    bundleEl.value = e.message;
    toast(e.message, 'error');
  });
  return dialog;
}

async function fleetResetUserTraffic(id) {
  try {
    await api(`/api/users/${encodeURIComponent(id)}/reset-traffic`, { method: 'POST' });
    toast(t('users.trafficReset'), 'success');
    await loadActiveView();
  } catch (e) { toast(e.message, 'error'); }
}

async function fleetToggleUser(id, enabled) {
  const user = fleet.users.find(u => u.id === id);
  if (!user) return;
  try {
    await api(`/api/users/${encodeURIComponent(id)}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled })
    });
    toast(enabled ? t('users.enabledToast') : t('users.disabledToast'), 'success');
    await loadActiveView();
  } catch (e) { toast(e.message, 'error'); }
}

async function fleetDeleteUser(id) {
  const user = fleet.users.find(u => u.id === id);
  if (!confirm(t('users.confirmDelete').replace('{name}', user ? (user.name || user.id) : id))) return;
  try {
    await api(`/api/users/${encodeURIComponent(id)}`, { method: 'DELETE' });
    toast(t('toast.deleted'), 'success');
    await loadActiveView();
  } catch (e) { toast(e.message, 'error'); }
}

function openUserModal(userId = null) {
  const editing = userId ? (fleet.users || []).find(u => String(u.id) === String(userId)) : null;
  const expires = editing ? expiryTime(editing.expires_at) : null;
  const limitGB = editing && editing.traffic_limit_bytes > 0
    ? Math.round(editing.traffic_limit_bytes / 1024 / 1024 / 1024 * 10) / 10 : 0;
  // The create path must never read from a (missing) user record: start from a
  // clean draft so the full form renders and POST /api/users gets sane values.
  const draft = editing ? {
    id: editing.id,
    name: editing.name || '',
    enabled: !!editing.enabled,
    expiryMode: expires ? 'date' : 'never',
    expiryDate: expires ? new Date(expires).toISOString().slice(0, 10) : '',
    trafficMode: limitGB > 0 ? 'limited' : 'unlimited',
    gb: limitGB || 100,
    speedKbps: editing.max_speed_kbps || 0,
    routeIDs: Array.isArray(editing.allowed_route_ids) ? editing.allowed_route_ids.slice() : []
  } : {
    id: '',
    name: '',
    enabled: true,
    expiryMode: 'never',
    expiryDate: '',
    trafficMode: 'unlimited',
    gb: 100,
    speedKbps: 0,
    routeIDs: []
  };
  fleet.userDraft = draft;

  const dialog = openModal({
    title: editing ? `${t('users.edit')}: ${editing.name || editing.id}` : t('users.add'),
    actions: [
      { label: t('actions.cancel'), kind: 'text', onClick: closeModal },
      { label: editing ? t('actions.save') : t('actions.create'), kind: 'filled', onClick: submitUser }
    ]
  });

  function renderBody() {
    dialog.setBody(`
      <div class="form-group">
        <div class="field"><label>${esc(t('users.name'))}</label>
          <input type="text" id="u-name" placeholder="Alice" value="${escAttr(draft.name)}"></div>
        <label class="switch-wrapper">
          <span class="switch"><input type="checkbox" id="u-enabled" ${draft.enabled ? 'checked' : ''}><span class="slider"></span></span>
          <span>${esc(t('users.enabled'))}</span>
        </label>
        <div class="form-row">
          <div class="field"><label>${esc(t('users.expiry'))}</label>
            <select id="u-expiry-mode">
              <option value="never" ${draft.expiryMode === 'never' ? 'selected' : ''}>${esc(t('users.never'))}</option>
              <option value="date" ${draft.expiryMode === 'date' ? 'selected' : ''}>${esc(t('users.expiryDate'))}</option>
            </select>
          </div>
          <div class="field"><label>${esc(t('users.date'))}</label>
            <input type="date" id="u-expiry-date" value="${escAttr(draft.expiryDate)}" ${draft.expiryMode === 'date' ? '' : 'disabled'}></div>
        </div>
        <div class="form-row">
          <div class="field"><label>${esc(t('users.trafficLimit'))}</label>
            <select id="u-traffic-mode">
              <option value="unlimited" ${draft.trafficMode === 'unlimited' ? 'selected' : ''}>${esc(t('users.unlimited'))}</option>
              <option value="limited" ${draft.trafficMode === 'limited' ? 'selected' : ''}>${esc(t('users.limited'))}</option>
            </select>
          </div>
          <div class="field"><label>GB</label>
            <input type="number" id="u-gb" value="${escAttr(draft.gb)}" min="1" ${draft.trafficMode === 'limited' ? '' : 'disabled'}></div>
        </div>
        <div class="field">
          <label>${esc(t('users.speedLimit'))}</label>
          <input type="number" id="u-speed" value="${escAttr(draft.speedKbps)}" min="0" step="1" placeholder="0">
          <span class="hint">${esc(t('users.speedHint'))}</span>
        </div>
        <div class="field">
          <label>${esc(t('users.routesOrder'))}</label>
          <div id="u-routes"></div>
          <span class="hint">${esc(t('users.orderHint'))}</span>
        </div>
        ${editing && networkSubscriptionsEnabled() ? `<div class="sub-section">
          <div class="sub-section__title"><span class="material-symbols-rounded">qr_code_2</span>${esc(t('users.subscription'))}</div>
          <div class="qr-card"><div class="qr-box" id="user-edit-qr"></div>
            <div class="qr-card__info"><div class="sub-link">${esc(editing.subscription_url || '')}</div></div>
          </div>
          <div class="sub-section__actions">
            <button class="btn btn--tonal btn--compact" type="button" onclick="fleetCopySubscription('${escAttr(editing.id)}')">
              <span class="material-symbols-rounded">content_copy</span>${esc(t('users.copy'))}
            </button>
          </div>
        </div>` : ''}
      </div>`);
    bindFields();
    renderUserRoutes();
    if (editing && networkSubscriptionsEnabled()) {
      renderQR(document.getElementById('user-edit-qr'), editing.subscription_url || '');
    }
  }

  function bindFields() {
    const nameEl = document.getElementById('u-name');
    const enabledEl = document.getElementById('u-enabled');
    const expiryEl = document.getElementById('u-expiry-mode');
    const dateEl = document.getElementById('u-expiry-date');
    const trafficEl = document.getElementById('u-traffic-mode');
    const gbEl = document.getElementById('u-gb');
    const speedEl = document.getElementById('u-speed');
    if (nameEl) nameEl.addEventListener('input', e => { draft.name = e.target.value; });
    if (enabledEl) enabledEl.addEventListener('change', e => { draft.enabled = e.target.checked; });
    if (expiryEl) expiryEl.addEventListener('change', e => {
      draft.expiryMode = e.target.value;
      if (dateEl) dateEl.disabled = draft.expiryMode !== 'date';
    });
    if (dateEl) dateEl.addEventListener('change', e => { draft.expiryDate = e.target.value; });
    if (trafficEl) trafficEl.addEventListener('change', e => {
      draft.trafficMode = e.target.value;
      if (gbEl) gbEl.disabled = draft.trafficMode !== 'limited';
    });
    if (gbEl) gbEl.addEventListener('input', e => { draft.gb = Number(e.target.value) || 0; });
    if (speedEl) speedEl.addEventListener('input', e => { draft.speedKbps = Number(e.target.value) || 0; });
  }

  async function submitUser() {
    if (!draft.name.trim()) return toast(t('users.nameRequired'), 'error');
    if (draft.expiryMode === 'date' && !draft.expiryDate) return toast(t('users.dateRequired'), 'error');
    if (draft.trafficMode === 'limited' && !(draft.gb > 0)) return toast(t('users.limitRequired'), 'error');
    if (!(draft.speedKbps >= 0)) return toast(t('users.speedInvalid'), 'error');
    const payload = {
      name: draft.name.trim(),
      enabled: draft.enabled,
      allowed_route_ids: draft.routeIDs
    };
    payload.expires_at = draft.expiryMode === 'date'
      ? new Date(draft.expiryDate + 'T23:59:59Z').toISOString()
      : null;
    payload.traffic_limit_bytes = draft.trafficMode === 'limited'
      ? Math.round(draft.gb * 1024 * 1024 * 1024) : 0;
    payload.max_speed_kbps = Math.max(0, Math.round(draft.speedKbps || 0));
    try {
      if (editing) {
        await api(`/api/users/${encodeURIComponent(editing.id)}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        toast(t('toast.saved'), 'success');
        closeModal();
        await loadActiveView();
      } else {
        const created = await api('/api/users', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        toast(t('toast.created'), 'success');
        closeModal();
        await loadActiveView();
        openSubscriptionDialog(created.id);
      }
    } catch (e) { toast(e.message, 'error'); }
  }

  renderBody();
}

let userDragRouteID = null;

function renderUserRoutes() {
  const root = document.getElementById('u-routes');
  const draft = fleet.userDraft;
  if (!root || !draft) return;
  const routes = fleet.routes || [];
  const selected = draft.routeIDs
    .map(id => routes.find(route => route.id === id))
    .filter(Boolean);
  const available = routes.filter(route => !draft.routeIDs.includes(route.id));
  const selectedHTML = selected.length ? selected.map((route, index) => `
    <div class="route-order__item" draggable="true" data-route-id="${escAttr(route.id)}"
         ondragstart="fleetUserDragStart(event, ${route.id})"
         ondragover="fleetUserDragOver(event)"
         ondrop="fleetUserDrop(event, ${route.id})">
      <span class="drag-handle material-symbols-rounded" title="${esc(t('users.dragHint'))}">drag_indicator</span>
      <span class="route-order__name">${esc(route.name)}</span>
      <span class="route-order__actions">
        <button class="icon-btn" type="button" title="${esc(t('users.moveUp'))}" ${index === 0 ? 'disabled' : ''}
          onclick="fleetUserMoveRoute(${route.id}, -1)"><span class="material-symbols-rounded">arrow_upward</span></button>
        <button class="icon-btn" type="button" title="${esc(t('users.moveDown'))}" ${index === selected.length - 1 ? 'disabled' : ''}
          onclick="fleetUserMoveRoute(${route.id}, 1)"><span class="material-symbols-rounded">arrow_downward</span></button>
        <button class="icon-btn icon-btn--danger" type="button" title="${esc(t('actions.remove'))}"
          onclick="fleetUserToggleRoute(${route.id}, false)"><span class="material-symbols-rounded">close</span></button>
      </span>
    </div>`).join('')
    : `<div class="hint">${esc(t('users.noRoutesSelected'))}</div>`;
  const availableHTML = available.length ? available.map(route => `
    <label class="check-row route-picker__item">
      <input type="checkbox" onchange="fleetUserToggleRoute(${route.id}, this.checked)">
      <span>${esc(route.name)} <span class="hint">(${esc(transportLabel(route.transport_type))})</span></span>
    </label>`).join('')
    : `<div class="hint">${esc(t('users.allRoutesSelected'))}</div>`;
  root.innerHTML = `<div class="route-editor">
    <div class="route-editor__col">
      <div class="route-editor__title">${esc(t('users.routesAvailable'))}</div>
      <div class="check-list route-picker">${availableHTML}</div>
    </div>
    <div class="route-editor__col">
      <div class="route-editor__title">${esc(t('users.routesSelected'))}</div>
      <div class="route-order">${selectedHTML}</div>
    </div>
  </div>`;
}

function fleetUserToggleRoute(id, checked) {
  const draft = fleet.userDraft;
  if (!draft) return;
  const ids = draft.routeIDs.filter(routeID => routeID !== id);
  if (checked) ids.push(id);
  draft.routeIDs = ids;
  renderUserRoutes();
}

function fleetUserMoveRoute(id, direction) {
  const draft = fleet.userDraft;
  if (!draft) return;
  const ids = draft.routeIDs;
  const index = ids.indexOf(id);
  const next = index + direction;
  if (index < 0 || next < 0 || next >= ids.length) return;
  [ids[index], ids[next]] = [ids[next], ids[index]];
  renderUserRoutes();
}

function fleetUserDragStart(event, id) {
  userDragRouteID = id;
  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = 'move';
    event.dataTransfer.setData('text/plain', String(id));
  }
  event.currentTarget?.classList.add('dragging');
}

function fleetUserDragOver(event) {
  event.preventDefault();
}

function fleetUserDrop(event, targetID) {
  event.preventDefault();
  const draft = fleet.userDraft;
  const source = Number(userDragRouteID);
  userDragRouteID = null;
  if (!draft || !source || source === targetID) return;
  const ids = draft.routeIDs.filter(id => id !== source);
  const index = ids.indexOf(targetID);
  if (index < 0) return;
  ids.splice(index, 0, source);
  draft.routeIDs = ids;
  renderUserRoutes();
}
