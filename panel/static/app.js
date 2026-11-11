const state = {
  templates: [], configs: [], serverRunning: false,
  search: '', sort: 'name'
};

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
  document.getElementById('btn-backup').addEventListener('click', () => {
    window.location.href = '/api/backup';
  });
  document.getElementById('search').addEventListener('input', e => { state.search = e.target.value.toLowerCase(); render(); });

  await load();
  await loadSystemStats();
  setInterval(load, 5000);
  setInterval(loadSystemStats, 3000);
})();

function formatBytes(bytes) {
  if (bytes === 0) return '0B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  const val = bytes / Math.pow(k, i);
  return (val >= 10 ? Math.round(val) : parseFloat(val.toFixed(1))) + sizes[i];
}

function getColorClass(percent) {
  if (percent < 60) return 'good';
  if (percent < 85) return 'warn';
  return 'danger';
}

async function loadSystemStats() {
  try {
    const s = await api('/api/system/stats');
    const el = document.getElementById('sys-stats');
    
    const cpuPercent = Math.round(s.cpu_usage);
    const cpuClass = getColorClass(cpuPercent);

    const ramPercent = Math.round((s.mem_used / s.mem_total) * 100);
    const ramClass = getColorClass(ramPercent);
    const ramUsedStr = formatBytes(s.mem_used);
    const ramTotalStr = formatBytes(s.mem_total);

    const d = Math.floor(s.uptime / 86400);
    const h = Math.floor((s.uptime % 86400) / 3600);
    const m = Math.floor((s.uptime % 3600) / 60);
    let upStr = '';
    if (d > 0) upStr += `${d}d `;
    if (h > 0 || d > 0) upStr += `${h}h `;
    upStr += `${m}m`;

    el.innerHTML = `
      <span class="stat-item cpu ${cpuClass}" title="CPU Usage: ${cpuPercent}%">
        <span class="material-symbols-rounded">speed</span>
        <span class="stat-value">${cpuPercent}%</span>
      </span>
      <span class="stat-item ram ${ramClass}" title="RAM Usage: ${ramPercent}% (${ramUsedStr} / ${ramTotalStr})">
        <span class="material-symbols-rounded">memory</span>
        <span class="stat-value">${ramUsedStr}</span> <span class="stat-total">/ ${ramTotalStr}</span>
      </span>
      <span class="stat-item uptime" title="System Uptime">
        <span class="material-symbols-rounded">schedule</span>
        <span class="stat-value">${upStr}</span>
      </span>
    `;
    el.style.display = 'flex';
  } catch (e) {
    document.getElementById('sys-stats').style.display = 'none';
  }
}

window.changeLanguage = async (lang) => {
  LANGS.current = lang;
  localStorage.setItem('shampoore_lang', lang);
  document.getElementById('current-lang-label').textContent = { en: 'English', ru: 'Русский', eo: 'Esperanto' }[lang];
  await loadLang(lang);
  applyLang();
  renderServerBar();
  render();
};

window.changeSort = (sort) => {
  state.sort = sort;
  document.getElementById('current-sort-label').textContent = t('config.sort.' + sort);
  render();
};

const TOKEN = () => localStorage.getItem('shampoore_token') || '';
async function api(path, opts = {}) {
  opts.headers = Object.assign({ 'Accept': 'application/json' }, opts.headers || {});
  if (TOKEN()) opts.headers['X-Panel-Token'] = TOKEN();
  const res = await fetch(path, opts);
  const text = await res.text();
  let body = {}; try { body = text ? JSON.parse(text) : {}; } catch { body = { error: text }; }
  if (!res.ok) throw new Error(body.error || res.statusText);
  return body;
}

async function load() {
  try {
    if (!state.templates.length) state.templates = (await api('/api/templates')) || [];
    const sst = await api('/api/server/status');
    state.serverRunning = sst.running;
    document.getElementById('global-status').className = `status-dot ${state.serverRunning ? 'online' : ''}`;
    
    state.configs = (await api('/api/configs')) || [];
    renderServerBar();
    render();
  } catch (e) {
    document.getElementById('global-status').className = 'status-dot';
  }
}

function renderServerBar() {
  const bar = document.getElementById('server-bar');
  const running = state.serverRunning;
  const label = running ? t('server.running') : t('server.stopped');
  
  const btnLogs = `<button class="btn btn--tonal" onclick="openServerLogs()"><span class="material-symbols-rounded">terminal</span>${t('actions.logs')}</button>`;
  const btnAction = running 
    ? `<button class="btn btn--filled btn--danger" onclick="stopServer()"><span class="material-symbols-rounded">stop</span>${t('server.stop')}</button>`
    : `<button class="btn btn--filled btn--success" onclick="startServer()"><span class="material-symbols-rounded">play_arrow</span>${t('server.start')}</button>`;

  bar.innerHTML = `
    <div class="server-control__status">
      <span class="status-dot ${running ? 'online' : ''}"></span>
      ${label}
    </div>
    <div class="server-control__actions">${btnLogs}${btnAction}</div>
  `;
}

async function startServer() {
  try { await api('/api/server/start', { method: 'POST' }); toast(t('toast.serverStarted')); await load(); }
  catch (e) { toast(e.message, 'error'); }
}
async function stopServer() {
  if (!confirm(t('server.confirmStop'))) return;
  try { await api('/api/server/stop', { method: 'POST' }); toast(t('toast.serverStopped')); await load(); }
  catch (e) { toast(e.message, 'error'); }
}
async function openServerLogs() {
  const d = await api('/api/server/logs?limit=1000');
  openModal({ title: t('actions.logs'), body: `<div class="logs">${esc((d.lines || []).join('\n'))}</div>` });
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
          <label class="switch" title="${isEnabled ? 'Enabled' : 'Disabled'}">
            <input type="checkbox" ${isEnabled ? 'checked' : ''} onchange="toggleCfg('${n}', this.checked)">
            <span class="slider"></span>
          </label>
          <div class="card__actions">
            <button class="btn btn--tonal btn--compact" onclick="exportCfg('${n}')" style="height:32px; font-size:12px; padding:0 12px;">
              <span class="material-symbols-rounded" style="font-size:16px;">ios_share</span> ${t('actions.export')}
            </button>
            <button class="icon-btn" onclick="openLogs('${n}')" title="${t('actions.logs')}"><span class="material-symbols-rounded">terminal</span></button>
            <button class="icon-btn" onclick="openEditDialog('${n}')" title="${t('actions.edit')}"><span class="material-symbols-rounded">edit</span></button>
            <button class="icon-btn icon-btn--danger" onclick="delCfg('${n}')" title="${t('actions.delete')}"><span class="material-symbols-rounded">delete</span></button>
          </div>
        </div>
      </div>`;
  }).join('');
}

async function toggleCfg(name, enabled) {
  try {
    await api(`/api/configs/${encodeURIComponent(name)}/${enabled ? 'enable' : 'disable'}`, { method: 'POST' });
    toast(t(enabled ? 'toast.started' : 'toast.stopped'));
    await load();
  } catch (e) { toast(e.message, 'error'); await load(); }
}

async function delCfg(name) {
  if (!confirm(t('confirm.deleteConfig'))) return;
  try { await api(`/api/configs/${encodeURIComponent(name)}`, { method: 'DELETE' }); toast(t('toast.deleted')); await load(); }
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
          <div>${qr.createImgTag(6, 8)}</div>
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
  let data = initialData ? JSON.parse(JSON.stringify(initialData)) : {};
  if (!editName && !initialData) {
    data = { type: 'yandex', transport: {}, advanced: {} };
  }
  delete data.name;

  let form = {
    name: editName || '',
    type: data.type || 'yandex',
    encryption_password: data.encryption_password || '',
    transport: data.transport || {},
    advanced: data.advanced || {}
  };
  let activeTab = 'form';
  let jsonCache = '';

  const dialog = openModal({
    title: t(editName ? 'dialog.editConfig' : 'dialog.newConfig'),
    tabs: [{ id: 'form', label: t('dialog.tabForm') }, { id: 'json', label: t('dialog.tabJson') }],
    actions: [
      { label: t('actions.cancel'), kind: 'text', onClick: closeModal },
      { label: t(editName ? 'actions.save' : 'actions.create'), kind: 'filled', onClick: () => submit() }
    ]
  });

  function buildObj() {
    const obj = { type: form.type, transport: form.transport };
    if (form.encryption_password) obj.encryption_password = form.encryption_password;
    if (form.advanced && Object.keys(form.advanced).length) obj.advanced = form.advanced;
    if (form.name) obj.name = form.name;
    return obj;
  }

  function getObjByPath(path) {
    const parts = path.split('.');
    let cur = form;
    for (const p of parts) {
      if (!cur[p]) cur[p] = {};
      cur = cur[p];
    }
    return cur;
  }

  function drawKV(obj, path, containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;
    
    container.innerHTML = Object.entries(obj).map(([k, v], i) => `
      <div class="kv-row">
        <input type="text" value="${escAttr(k)}" onchange="updateKVKey('${path}', ${i}, this.value, '${containerId}')" placeholder="key">
        <input type="text" value="${escAttr(String(v))}" onchange="updateKVVal('${path}', ${i}, this.value)" placeholder="value">
        <button type="button" class="icon-btn icon-btn--danger" onclick="removeKV('${path}', ${i}, '${containerId}')"><span class="material-symbols-rounded">delete</span></button>
      </div>
    `).join('');
  }

  window.updateKVKey = (path, idx, newKey, containerId) => {
    const obj = getObjByPath(path);
    const keys = Object.keys(obj);
    const oldKey = keys[idx];
    if (oldKey === newKey) return;
    const val = obj[oldKey];
    delete obj[oldKey];
    obj[newKey] = val;
    drawKV(obj, path, containerId);
  };

  window.updateKVVal = (path, idx, newVal) => {
    const obj = getObjByPath(path);
    const k = Object.keys(obj)[idx];
    obj[k] = parseVal(newVal);
  };

  window.removeKV = (path, idx, containerId) => {
    const obj = getObjByPath(path);
    delete obj[Object.keys(obj)[idx]];
    drawKV(obj, path, containerId);
  };

  window.addKV = (path, containerId) => {
    const obj = getObjByPath(path);
    let newKey = 'key';
    let i = 1;
    while (obj[newKey] !== undefined) {
      newKey = 'key' + i++;
    }
    obj[newKey] = '';
    drawKV(obj, path, containerId);
  };

  function getKnownTypes() {
    const types = new Set(state.templates.map(t => t.kind));
    types.add('multi');
    if (form.type) types.add(form.type);
    return Array.from(types).sort();
  }

  function getTransportDefaults(type) {
    const tpl = state.templates.find(t => t.kind === type);
    if (tpl && tpl.example) {
      try {
        const ex = JSON.parse(tpl.example);
        return ex.transport || {};
      } catch (e) {}
    }
    return {};
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

    const knownTypes = getKnownTypes();
    
    let html = `
      <div class="form-group">
        <div class="field">
          <label>${t('dialog.name')}</label>
          <input type="text" id="f-name" value="${escAttr(form.name)}" placeholder="${t('dialog.namePlaceholder')}">
        </div>
        <div class="field">
          <label>${t('dialog.type')}</label>
          <select id="f-type" class="input">
            ${knownTypes.map(kt => `<option value="${escAttr(kt)}" ${kt === form.type ? 'selected' : ''}>${esc(kt)}</option>`).join('')}
          </select>
        </div>
        <div class="field">
          <label>${t('dialog.encryption')}</label>
          <input type="password" id="f-pw" value="${escAttr(form.encryption_password)}">
        </div>
    `;

    if (form.type === 'multi') {
      html += `
        <hr>
        <div class="field">
          <label>Strategy</label>
          <select id="f-strategy" class="input">
            <option value="flowmux" ${form.transport.strategy === 'flowmux' ? 'selected' : ''}>flowmux</option>
            <option value="bond" ${form.transport.strategy === 'bond' ? 'selected' : ''}>bond</option>
          </select>
        </div>
        
        <div class="field">
          <label>Transports (Subflows)</label>
          <div id="multi-transports">
            ${(form.transport.transports || []).map((t, i) => `
              <div style="background:var(--md-surface-container-highest); padding:16px; border-radius:16px; margin-bottom:16px;">
                <div style="display:flex; gap:8px; align-items:center; margin-bottom:12px;">
                  <select class="input" style="flex:1; height:40px;" data-mt-type="${i}">
                    ${knownTypes.filter(kt => kt !== 'multi').map(kt => `<option value="${escAttr(kt)}" ${kt === t.type ? 'selected' : ''}>${esc(kt)}</option>`).join('')}
                  </select>
                  <button type="button" class="icon-btn icon-btn--danger" onclick="rmTransport(${i})"><span class="material-symbols-rounded">delete</span></button>
                </div>
                <div class="field" style="margin-bottom:12px;">
                  <label>Rate limit (KB/s, 0 = unlimited)</label>
                  <input type="number" class="input" style="height:40px;" data-mt-ratelimit="${i}" value="${t.rate_limit_kb || 0}">
                </div>
                <div class="field">
                  <label>Transport Params</label>
                  <div id="kv-transports-${i}" class="kv-container"></div>
                  <button type="button" class="btn btn--tonal" style="align-self:flex-start; margin-top:8px;" onclick="addKV('transports.${i}.transport', 'kv-transports-${i}')">
                    <span class="material-symbols-rounded">add</span> Add parameter
                  </button>
                </div>
              </div>
            `).join('')}
          </div>
          <button type="button" class="btn btn--tonal" style="align-self:flex-start; margin-top:8px;" onclick="addTransport()">
            <span class="material-symbols-rounded">add</span> Add Transport
          </button>
        </div>
      `;
    } else {
      html += `
        <hr>
        <div class="field">
          <label>Transport Params</label>
          <div id="kv-transport" class="kv-container"></div>
          <button type="button" class="btn btn--tonal" style="align-self:flex-start; margin-top:8px;" onclick="addKV('transport', 'kv-transport')">
            <span class="material-symbols-rounded">add</span> Add parameter
          </button>
        </div>
      `;
    }

    html += `
      <hr>
      <div class="field">
        <label>${t('dialog.advanced')}</label>
        <div id="kv-advanced" class="kv-container"></div>
        <button type="button" class="btn btn--tonal" style="align-self:flex-start; margin-top:8px;" onclick="addKV('advanced', 'kv-advanced')">
          <span class="material-symbols-rounded">add</span> Add parameter
        </button>
      </div>
    </div>`;

    dialog.setBody(html);

    document.getElementById('f-name').addEventListener('input', e => form.name = e.target.value);
    
    document.getElementById('f-type').addEventListener('change', e => {
      const newType = e.target.value;
      form.type = newType;
      if (newType === 'multi') {
        if (!form.transport.transports) {
          form.transport = { strategy: 'flowmux', transports: [] };
        }
      } else {
        form.transport = getTransportDefaults(newType);
      }
      renderBody();
    });
    
    document.getElementById('f-pw').addEventListener('input', e => form.encryption_password = e.target.value);

    if (form.type === 'multi') {
      document.getElementById('f-strategy').addEventListener('change', e => {
        form.transport.strategy = e.target.value;
      });

      document.querySelectorAll('[data-mt-type]').forEach(el => el.addEventListener('change', e => {
        const idx = Number(e.target.dataset.mtType);
        const newType = e.target.value;
        form.transport.transports[idx].type = newType;
        form.transport.transports[idx].transport = getTransportDefaults(newType);
        renderBody();
      }));

      document.querySelectorAll('[data-mt-ratelimit]').forEach(el => el.addEventListener('input', e => {
        const idx = Number(e.target.dataset.mtRatelimit);
        form.transport.transports[idx].rate_limit_kb = Number(e.target.value) || 0;
      }));

      (form.transport.transports || []).forEach((t, i) => {
        drawKV(t.transport || {}, `transports.${i}.transport`, `kv-transports-${i}`);
      });

      window.addTransport = () => {
        form.transport.transports = form.transport.transports || [];
        const defaultType = knownTypes.find(t => t !== 'multi') || 'yandex';
        form.transport.transports.push({ type: defaultType, rate_limit_kb: 0, transport: getTransportDefaults(defaultType) });
        renderBody();
      };

      window.rmTransport = (i) => {
        form.transport.transports.splice(i, 1);
        renderBody();
      };
    } else {
      drawKV(form.transport || {}, 'transport', 'kv-transport');
    }

    drawKV(form.advanced || {}, 'advanced', 'kv-advanced');
  }

  function submit() {
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

    api(path, { method, headers: { 'Content-Type': 'application/json' }, body })
      .then(() => { toast(t('toast.saved')); closeModal(); load(); })
      .catch(e => toast(e.message, 'error'));
  }

  dialog.onTabChange = id => {
    if (activeTab === 'form' && id === 'json') {
      jsonCache = JSON.stringify(buildObj(), null, 2);
    } else if (activeTab === 'json' && id === 'form') {
      try {
        const parsed = JSON.parse(jsonCache);
        form = { ...form, ...parsed };
        form.transport = form.transport || {};
        form.advanced = form.advanced || {};
      } catch (e) {
        toast('Invalid JSON, cannot switch to form', 'error');
        return false;
      }
    }
    activeTab = id;
    renderBody();
    return true;
  };

  renderBody();
}

function parseVal(v) {
  if (v === 'true') return true;
  if (v === 'false') return false;
  if (v !== '' && !isNaN(v)) return Number(v);
  return v;
}

function openModal({ title, body = '', actions = [], tabs = [] }) {
  const root = document.getElementById('modal-root');
  root.classList.remove('hidden');
  
  const tabsHTML = tabs.length ? `<div class="modal__tabs">${tabs.map((t,i) => `<button class="tab ${i===0?'active':''}" data-tab="${t.id}">${t.label}</button>`).join('')}</div>` : '';
  const actionsHTML = actions.map((a,i) => `<button class="btn btn--${a.kind}" data-act="${i}">${a.label}</button>`).join('');

  root.innerHTML = `
    <div class="modal">
      <div class="modal__header">
        <h2>${esc(title)}</h2>
        <button class="icon-btn" onclick="closeModal()"><span class="material-symbols-rounded">close</span></button>
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
  requestAnimationFrame(() => root.classList.add('show'));

  const dialogObj = { setBody: h => root.querySelector('.modal__body').innerHTML = h, onTabChange: null };
  return dialogObj;
}

function closeModal() {
  const root = document.getElementById('modal-root');
  root.classList.remove('show');
  setTimeout(() => { root.classList.add('hidden'); root.innerHTML = ''; }, 200);
}

function toast(msg, type = 'info') {
  const el = document.createElement('div');
  el.className = 'toast';
  el.textContent = msg;
  document.getElementById('toasts').appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; setTimeout(() => el.remove(), 300); }, 3000);
}

function esc(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])); }
function escAttr(s) { return esc(s).replace(/\n/g, '&#10;'); }
async function copyToClipboard(text) {
  try { await navigator.clipboard.writeText(text); } catch {
    const ta = document.createElement('textarea'); ta.value = text; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); ta.remove();
  }
}