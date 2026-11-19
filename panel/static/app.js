const state = {
  templates: [], configs: [], serverRunning: false,
  search: '', sort: 'name'
};

const fleet = {
  active: 'configs',
  nodes: [], routes: [], users: [],
  loading: { nodes: false, routes: false, users: false }
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
  document.getElementById('btn-rules').addEventListener('click', openRulesDialog);
  document.getElementById('btn-refresh').addEventListener('click', () => load());
  document.getElementById('btn-backup').addEventListener('click', () => {
    window.location.href = '/api/backup';
  });
  document.getElementById('btn-logout').addEventListener('click', logout);
  document.getElementById('search').addEventListener('input', e => { state.search = e.target.value.toLowerCase(); render(); });
  setupFleetTabs();

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

async function api(path, opts = {}) {
  opts.credentials = 'same-origin';
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
          <div class="switch-wrapper">
            <label class="switch">
              <input type="checkbox" ${isEnabled ? 'checked' : ''} onchange="toggleCfg('${n}', this.checked)">
              <span class="slider"></span>
            </label>
            <span>${isEnabled ? 'Enabled' : 'Disabled'}</span>
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
    
    if (selectedKind === 'multi') {
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
    
    // Custom Select HTML
    let optionsHtml = state.templates.map(t => `
      <div class="custom-select-option ${t.kind === selectedKind ? 'selected' : ''}" onclick="window.selectTransport('${t.kind}')">
        ${esc(t.display_name)}
      </div>
    `).join('');

    let html = `
      <div class="form-group">
        <div class="field">
          <label>${t('dialog.name')}</label>
          <input type="text" id="f-name" value="${escAttr(form.name)}" placeholder="${t('dialog.namePlaceholder')}">
        </div>
        <div class="field">
          <label>${t('dialog.type')}</label>
          <div class="custom-select-wrapper" id="transport-select-wrapper">
            <div class="custom-select-trigger" onclick="document.getElementById('transport-options').classList.toggle('hidden')">
              <span id="current-transport-label">${esc(schema ? schema.display_name : selectedKind)}</span>
              <span class="material-symbols-rounded">arrow_drop_down</span>
            </div>
            <div class="custom-select-options hidden" id="transport-options">
              ${optionsHtml}
            </div>
          </div>
        </div>
        <div class="field">
          <label>${t('dialog.encryption')}</label>
          <input type="password" id="f-pw" value="${escAttr(form.encryption_password)}">
        </div>
        <hr style="border:none;border-top:1px solid var(--md-outline-variant);margin:8px 0;">
    `;

    if (selectedKind === 'multi') {
      html += `
        <div class="field">
          <label>Strategy</label>
          <select id="f-strategy" class="input" style="height:56px;">
            <option value="flowmux" ${multiStrategy === 'flowmux' ? 'selected' : ''}>flowmux</option>
            <option value="bond" ${multiStrategy === 'bond' ? 'selected' : ''}>bond</option>
          </select>
        </div>
        <div class="field">
          <label>Transports (Subflows)</label>
          <div id="multi-transports">
            ${multiSubflows.map((t, i) => `
              <div style="background:var(--md-surface-container-highest); padding:16px; border-radius:16px; margin-bottom:16px;">
                <div style="display:flex; gap:8px; align-items:center; margin-bottom:12px;">
                  <select class="input" style="flex:1; height:40px;" onchange="window.updateSubflowType(${i}, this.value)">
                    ${state.templates.filter(k => k.kind !== 'multi').map(k => `<option value="${escAttr(k.kind)}" ${k.kind === t.type ? 'selected' : ''}>${esc(k.display_name)}</option>`).join('')}
                  </select>
                  <button type="button" class="icon-btn icon-btn--danger" onclick="window.rmSubflow(${i})"><span class="material-symbols-rounded">delete</span></button>
                </div>
                <div class="field" style="margin-bottom:12px;">
                  <label>Rate limit (KB/s, 0 = unlimited)</label>
                  <input type="number" class="input" style="height:40px;" value="${t.rate_limit_kb || 0}" oninput="window.updateSubflowRate(${i}, this.value)">
                </div>
                <div class="field">
                  <label>Transport Params</label>
                  <div id="kv-transports-${i}" class="kv-container">
                    ${renderSubflowFields(i, t)}
                  </div>
                </div>
              </div>
            `).join('')}
          </div>
          <button type="button" class="btn btn--tonal" style="align-self:flex-start; margin-top:8px;" onclick="window.addSubflow()">
            <span class="material-symbols-rounded">add</span> Add Transport
          </button>
        </div>
      `;
    } else if (schema && schema.fields) {
      html += `
        <div class="field">
          <label>Transport Params</label>
          ${schema.fields.map(f => `
            <div class="field" style="margin-bottom:8px;">
              <label>${esc(f.label)} ${f.required ? '*' : ''}</label>
              <input type="${f.type === 'password' ? 'password' : f.type === 'number' ? 'number' : 'text'}" 
                     class="input" 
                     style="height:40px;" 
                     value="${escAttr(String(fieldValues[f.key] || ''))}"
                     placeholder="${escAttr(f.placeholder || '')}"
                     oninput="window.updateField('${escAttr(f.key)}', this.value)">
              ${f.hint ? `<span class="hint" style="font-size:11px; margin-top:4px;">${esc(f.hint)}</span>` : ''}
            </div>
          `).join('')}
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
            <span class="material-symbols-rounded">add</span> Add parameter
          </button>
        </div>
      </div>
    `;

    dialog.setBody(html);

    document.getElementById('f-name').addEventListener('input', e => form.name = e.target.value);
    document.getElementById('f-pw').addEventListener('input', e => form.encryption_password = e.target.value);
    
    if (selectedKind === 'multi') {
      document.getElementById('f-strategy').addEventListener('change', e => { multiStrategy = e.target.value; });
    }
  }

  function renderSubflowFields(idx, subflow) {
    const subSchema = getSchema(subflow.type);
    if (!subSchema) return '';
    return subSchema.fields.map(f => `
      <div class="field" style="margin-bottom:8px;">
        <label>${esc(f.label)} ${f.required ? '*' : ''}</label>
        <input type="text" class="input" style="height:40px;" 
               value="${escAttr(String((subflow.transport && subflow.transport[f.key]) || ''))}"
               placeholder="${escAttr(f.placeholder || '')}"
               oninput="window.updateSubflowField(${idx}, '${escAttr(f.key)}', this.value)">
      </div>
    `).join('');
  }

  function renderKV(k, v, i) {
    return `
      <div class="kv-row">
        <input type="text" value="${escAttr(k)}" onchange="window.updateKVKey(${i}, this.value)" placeholder="key">
        <input type="text" value="${escAttr(String(v))}" onchange="window.updateKVVal(${i}, this.value)" placeholder="value">
        <button type="button" class="icon-btn icon-btn--danger" onclick="window.rmKV(${i})"><span class="material-symbols-rounded">delete</span></button>
      </div>
    `;
  }

  window.selectTransport = (kind) => {
    selectedKind = kind;
    fieldValues = {}; // Reset fields
    document.getElementById('transport-options').classList.add('hidden');
    renderBody();
  };

  window.updateField = (key, val) => {
    fieldValues[key] = val;
  };

  window.addSubflow = () => {
    const defaultType = state.templates.find(t => t.kind !== 'multi')?.kind || 'yandex';
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
        if (parsed.transport) {
            if (parsed.type === 'multi') {
                multiStrategy = parsed.transport.strategy || 'flowmux';
                multiSubflows = parsed.transport.transports || [];
            } else {
                fieldValues = parsed.transport;
            }
        }
        selectedKind = parsed.type || 'yandex';
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
async function openRulesDialog() {
  let rules = [];
  try {
    rules = (await api('/api/rules')) || [];
  } catch (e) {
    toast(e.message, 'error');
  }

  function renderRulesBody() {
    let html = `
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px;">
        <span style="color:var(--md-on-surface-variant);">Manage geoip rules and CIDRs</span>
        <button class="btn btn--filled" type="button" onclick="window.openAddRuleDialog()">
          <span class="material-symbols-rounded">add</span> Add Rule
        </button>
      </div>
      <div style="display:flex; flex-direction:column; gap:12px;">
    `;

    if (!rules.length) {
      html += `<div style="text-align:center; padding:32px; color:var(--md-on-surface-variant);">No rules found.</div>`;
    } else {
      rules.forEach(r => {
        const nameEsc = escAttr(r.name);
        const isEnabled = r.enabled;
        const cidrsCount = (r.cidrs || []).length;
        html += `
          <div style="background:var(--md-surface-container-highest); padding:16px; border-radius:16px; display:flex; align-items:center; justify-content:space-between; gap:12px;">
            <div>
              <div style="font-weight:500; font-size:16px; margin-bottom:4px;">${esc(r.name)}</div>
              <div style="font-size:13px; color:var(--md-on-surface-variant);">${cidrsCount} CIDRs</div>
            </div>
            <div style="display:flex; align-items:center; gap:12px;">
              <label class="switch">
                <input type="checkbox" ${isEnabled ? 'checked' : ''} onchange="window.toggleRule('${nameEsc}', this.checked)">
                <span class="slider"></span>
              </label>
              <button class="icon-btn icon-btn--danger" onclick="window.delRule('${nameEsc}')" title="Delete">
                <span class="material-symbols-rounded">delete</span>
              </button>
            </div>
          </div>
        `;
      });
    }
    html += `</div>`;
    dialog.setBody(html);
  }

  const dialog = openModal({
    title: 'Geoip Rules',
    actions: [
      { label: 'Apply (Restart Server)', kind: 'filled', onClick: async () => {
          try {
            const res = await api('/api/rules/apply', { method: 'POST' });
            if (res.restarted) toast('Server restarted with new rules');
            else toast('Server was offline, rules applied');
            closeModal();
          } catch (e) { toast(e.message, 'error'); }
        }
      },
      { label: 'Close', kind: 'text', onClick: closeModal }
    ]
  });

  window.toggleRule = async (name, enabled) => {
    try {
      await api(`/api/rules/${encodeURIComponent(name)}/${enabled ? 'enable' : 'disable'}`, { method: 'POST' });
      toast(enabled ? 'Rule enabled' : 'Rule disabled');
      rules = await api('/api/rules');
      renderRulesBody();
    } catch (e) { toast(e.message, 'error'); }
  };

  window.delRule = async (name) => {
    if (!confirm(`Delete rule "${name}"?`)) return;
    try {
      await api(`/api/rules/${encodeURIComponent(name)}`, { method: 'DELETE' });
      toast('Rule deleted');
      rules = await api('/api/rules');
      renderRulesBody();
    } catch (e) { toast(e.message, 'error'); }
  };

  window.openAddRuleDialog = () => {
    let rName = '';
    let rCidrs = '';
    const addDlg = openModal({
      title: 'New Geoip Rule',
      actions: [
        { label: 'Cancel', kind: 'text', onClick: openRulesDialog },
        { label: 'Create', kind: 'filled', onClick: async () => {
            if (!rName.trim()) return toast('Name is required', 'error');
            const cidrs = rCidrs.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
            try {
              await api('/api/rules', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: rName, cidrs })
              });
              toast('Rule created successfully');
              openRulesDialog();
            } catch (e) { toast(e.message, 'error'); }
          }
        }
      ]
    });
    addDlg.setBody(`
      <div class="form-group">
        <div class="field">
          <label>Rule Name</label>
          <input type="text" id="r-name" placeholder="e.g. custom_bypass" value="${escAttr(rName)}">
        </div>
        <div class="field">
          <label>CIDRs (one per line)</label>
          <textarea id="r-cidrs" placeholder="192.168.1.0/24&#10;10.0.0.0/8">${esc(rCidrs)}</textarea>
        </div>
      </div>
    `);
    document.getElementById('r-name').addEventListener('input', e => { rName = e.target.value; });
    document.getElementById('r-cidrs').addEventListener('input', e => { rCidrs = e.target.value; });
  };

  renderRulesBody();
}

/* ------------------------------------------------------------------ */
/* Fleet control plane: nodes, routes, users                           */
/* ------------------------------------------------------------------ */

const TRANSPORT_KINDS = [
  { value: 'raw_ws', label: 'Raw WebSocket' },
  { value: 'silo', label: 'Silo S3' },
  { value: 'livekit', label: 'LiveKit' }
];

function setupFleetTabs() {
  document.querySelectorAll('.panel-tab').forEach(btn => {
    btn.addEventListener('click', () => switchPanel(btn.dataset.panel));
  });
  const bind = (id, fn) => { const el = document.getElementById(id); if (el) el.addEventListener('click', fn); };
  bind('btn-refresh-nodes', () => loadPanel('nodes'));
  bind('btn-refresh-routes', () => loadPanel('routes'));
  bind('btn-refresh-users', () => loadPanel('users'));
  bind('btn-add-node', () => openNodeModal(null));
  bind('btn-add-route', () => fleetAddRoute());
  bind('btn-add-user', () => openUserModal());

  setInterval(() => {
    if (fleet.active === 'nodes' || fleet.active === 'routes' || fleet.active === 'users') {
      loadPanel(fleet.active, true);
    }
  }, 6000);
}

function switchPanel(name) {
  if (!name) return;
  fleet.active = name;
  document.querySelectorAll('.panel-tab').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.panel === name);
  });
  document.querySelectorAll('.panel-view').forEach(view => {
    view.classList.toggle('hidden', view.id !== 'panel-' + name);
  });
  if (name === 'configs') render();
  else loadPanel(name);
}

async function loadPanel(name, silent = false) {
  if (!name || name === 'configs' || fleet.loading[name]) return;
  fleet.loading[name] = true;
  try {
    if (name === 'nodes') {
      fleet.nodes = (await api('/api/nodes')) || [];
      renderNodes();
    } else if (name === 'routes') {
      const [routes, nodes] = await Promise.all([api('/api/routes'), api('/api/nodes')]);
      fleet.routes = routes || [];
      fleet.nodes = nodes || [];
      renderRoutes();
    } else if (name === 'users') {
      const [users, routes] = await Promise.all([api('/api/users'), api('/api/routes')]);
      fleet.users = users || [];
      fleet.routes = routes || [];
      renderUsers();
    }
  } catch (e) {
    if (!silent) toast(e.message, 'error');
  } finally {
    fleet.loading[name] = false;
  }
}

function nodeName(id) {
  const node = fleet.nodes.find(n => n.id === id);
  return node ? node.name : '#' + id;
}

function nodeRoles(node) {
  if (node.is_relay && node.is_exit) return 'Relay + Exit';
  if (node.is_relay) return 'Relay';
  if (node.is_exit) return 'Exit';
  return '—';
}

function statusClass(status) {
  return ['new', 'deploying', 'deployed', 'online', 'error'].includes(status) ? status : 'new';
}

function renderNodes() {
  const root = document.getElementById('nodes-list');
  if (!root) return;
  if (!fleet.nodes.length) {
    root.innerHTML = `<div class="empty"><span class="material-symbols-rounded">dns</span><p>No servers yet. Add your first node.</p></div>`;
    return;
  }
  root.innerHTML = fleet.nodes.map(node => {
    const transports = (node.transports || []).map(tr => {
      const flags = [tr.enabled ? 'enabled' : 'disabled'];
      if (tr.on_demand) flags.push('on-demand');
      return `<span class="transport-chip ${tr.enabled ? '' : 'muted'}">${esc(tr.type)}${tr.domain ? ' · ' + esc(tr.domain) : ''} <em>${esc(flags.join(', '))}</em></span>`;
    }).join('') || `<span class="transport-chip muted">no transports</span>`;
    return `
      <div class="node-card">
        <div class="node-card__header">
          <div>
            <h3 class="card__title">${esc(node.name)}</h3>
            <span class="card__subtitle">${esc(node.ssh_user || 'root')}@${esc(node.ssh_host || '—')}:${esc(node.ssh_port || 22)}</span>
          </div>
          <span class="status-chip status-chip--${statusClass(node.status)}">${esc(node.status || 'new')}</span>
        </div>
        <div class="node-card__meta">
          <span class="role-chip">${esc(nodeRoles(node))}</span>
          ${node.socks_port ? `<span class="meta-chip">SOCKS :${esc(node.socks_port)}</span>` : ''}
        </div>
        <div class="transport-list">${transports}</div>
        ${node.last_error ? `<div class="card__error">${esc(node.last_error)}</div>` : ''}
        <div class="node-card__footer">
          <button class="btn btn--filled btn--compact" type="button" onclick="fleetDeployNode(${node.id})">
            <span class="material-symbols-rounded">rocket_launch</span> Deploy
          </button>
          <button class="btn btn--tonal btn--compact" type="button" onclick="fleetEditNode(${node.id})">
            <span class="material-symbols-rounded">edit</span> Edit
          </button>
          <button class="icon-btn icon-btn--danger" type="button" title="Delete" onclick="fleetDeleteNode(${node.id})">
            <span class="material-symbols-rounded">delete</span>
          </button>
        </div>
      </div>`;
  }).join('');
}

async function fleetDeleteNode(id) {
  const node = fleet.nodes.find(n => n.id === id);
  if (!confirm(`Delete node "${node ? node.name : id}"?`)) return;
  try {
    await api(`/api/nodes/${encodeURIComponent(id)}`, { method: 'DELETE' });
    toast('Node deleted');
    await loadPanel('nodes');
  } catch (e) { toast(e.message, 'error'); }
}

async function fleetDeployNode(id) {
  const node = fleet.nodes.find(n => n.id === id);
  const dialog = openModal({
    title: `Deploy: ${node ? node.name : id}`,
    body: `<div class="deploy-log"><div class="deploy-step">Connecting to the node…</div></div>`
  });
  try {
    const result = await api(`/api/nodes/${encodeURIComponent(id)}/deploy`, { method: 'POST' });
    renderDeployResult(dialog, result);
    toast('Deploy finished');
  } catch (e) {
    if (e.payload && e.payload.deployment) renderDeployResult(dialog, e.payload.deployment);
    else dialog.setBody(`<div class="deploy-log"><div class="deploy-step is-error">${esc(e.message)}</div></div>`);
    toast(e.message, 'error');
  }
  loadPanel('nodes', true);
}

function renderDeployResult(dialog, result) {
  const steps = (result && result.steps) || [];
  const success = !!(result && result.success);
  let html = `<div class="deploy-summary ${success ? 'is-ok' : 'is-error'}">
    <span class="material-symbols-rounded">${success ? 'check_circle' : 'error'}</span>
    ${success ? 'Deployment succeeded' : 'Deployment failed'}
  </div><div class="deploy-log">`;
  if (!steps.length) html += `<div class="deploy-step">No steps returned.</div>`;
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


function emptyNodeDraft() {
  return {
    name: '', ssh_host: '', ssh_port: 22, ssh_user: 'root',
    ssh_private_key: '', ssh_password: '',
    is_relay: true, is_exit: false, socks_port: 1080,
    transports: []
  };
}

function nodeDraftFrom(node) {
  const draft = emptyNodeDraft();
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
    socks_port: node.socks_port || 1080,
    transports: (node.transports || []).map(tr => ({
      type: tr.type, domain: tr.domain || '',
      enabled: !!tr.enabled, on_demand: !!tr.on_demand
    }))
  });
  return draft;
}

async function fleetEditNode(id) {
  if (!fleet.nodes.length) await loadPanel('nodes', true);
  const node = fleet.nodes.find(n => n.id === id);
  openNodeModal(node || null);
}

function openNodeModal(node) {
  const draft = nodeDraftFrom(node);
  const editID = node ? node.id : null;
  fleet.nodeDraft = draft;

  const dialog = openModal({
    title: editID ? `Edit: ${node.name}` : 'Add server',
    actions: [
      { label: t('actions.cancel'), kind: 'text', onClick: closeModal },
      { label: editID ? t('actions.save') : t('actions.create'), kind: 'filled', onClick: submitNode }
    ]
  });

  function transportRow(tr, i) {
    const options = TRANSPORT_KINDS.map(k =>
      `<option value="${escAttr(k.value)}" ${k.value === tr.type ? 'selected' : ''}>${esc(k.label)}</option>`
    ).join('');
    return `<div class="transport-row">
      <select class="input" onchange="fleetNodeChangeTransportType(${i}, this.value)">${options}</select>
      <input type="text" class="input" value="${escAttr(tr.domain)}" placeholder="domain.example.com"
             oninput="fleetNodeSetTransport(${i}, 'domain', this.value)">
      <label class="check-row"><input type="checkbox" ${tr.enabled ? 'checked' : ''}
        onchange="fleetNodeSetTransport(${i}, 'enabled', this.checked)"> enabled</label>
      ${tr.type === 'livekit' ? `<label class="check-row" title="Clients get short-lived JWT via /session"><input type="checkbox" ${tr.on_demand ? 'checked' : ''}
        onchange="fleetNodeSetTransport(${i}, 'on_demand', this.checked)"> on-demand</label>` : ''}
      <button type="button" class="icon-btn icon-btn--danger" title="Remove"
        onclick="fleetNodeRemoveTransport(${i})"><span class="material-symbols-rounded">delete</span></button>
    </div>`;
  }

  function renderNodeBody() {
    const transports = draft.transports.map(transportRow).join('') ||
      `<div class="hint">No transports yet. A relay node needs at least one enabled transport.</div>`;
    dialog.setBody(`
      <div class="form-group">
        <div class="field">
          <label>Name</label>
          <input type="text" id="n-name" value="${escAttr(draft.name)}" placeholder="relay-de">
        </div>
        <div class="form-row">
          <div class="field"><label>SSH host</label><input type="text" id="n-host" value="${escAttr(draft.ssh_host)}" placeholder="203.0.113.10"></div>
          <div class="field field--small"><label>Port</label><input type="number" id="n-port" value="${escAttr(draft.ssh_port)}" min="1" max="65535"></div>
        </div>
        <div class="form-row">
          <div class="field"><label>SSH user</label><input type="text" id="n-user" value="${escAttr(draft.ssh_user)}" placeholder="root"></div>
          <div class="field field--small"><label>SOCKS port (exit)</label><input type="number" id="n-socks" value="${escAttr(draft.socks_port)}" min="0" max="65535"></div>
        </div>
        <div class="field">
          <label>SSH private key</label>
          <textarea id="n-key" class="compact-textarea" spellcheck="false" placeholder="-----BEGIN OPENSSH PRIVATE KEY-----">${esc(draft.ssh_private_key)}</textarea>
        </div>
        <div class="field">
          <label>SSH password (optional)</label>
          <input type="password" id="n-pass" value="${escAttr(draft.ssh_password)}">
        </div>
        <div class="field">
          <label>Roles</label>
          <div class="role-row">
            <label class="check-row"><input type="checkbox" id="n-relay" ${draft.is_relay ? 'checked' : ''}> Relay</label>
            <label class="check-row"><input type="checkbox" id="n-exit" ${draft.is_exit ? 'checked' : ''}> Exit</label>
          </div>
        </div>
        <div class="field">
          <label>Transports &amp; domains</label>
          <div class="transport-rows">${transports}</div>
          <button type="button" class="btn btn--tonal" style="align-self:flex-start;margin-top:8px"
            onclick="fleetNodeAddTransport()"><span class="material-symbols-rounded">add</span> Add transport</button>
        </div>
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
    bindInput('n-socks', 'socks_port', true);
    bindInput('n-key', 'ssh_private_key');
    bindInput('n-pass', 'ssh_password');
    const relay = document.getElementById('n-relay');
    if (relay) relay.addEventListener('change', e => { draft.is_relay = e.target.checked; });
    const exit = document.getElementById('n-exit');
    if (exit) exit.addEventListener('change', e => { draft.is_exit = e.target.checked; });
  }

  function submitNode() {
    if (!draft.name.trim()) return toast('Name is required', 'error');
    if (!draft.ssh_host.trim()) return toast('SSH host is required', 'error');
    if (!draft.is_relay && !draft.is_exit) return toast('Choose Relay and/or Exit role', 'error');
    const path = editID ? `/api/nodes/${encodeURIComponent(editID)}` : '/api/nodes';
    api(path, {
      method: editID ? 'PUT' : 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(draft)
    }).then(() => {
      toast(editID ? 'Node saved' : 'Node created');
      closeModal();
      loadPanel('nodes');
    }).catch(e => toast(e.message, 'error'));
  }

  fleet.renderNodeBody = renderNodeBody;
  renderNodeBody();
}

function fleetNodeSetTransport(index, key, value) {
  const draft = fleet.nodeDraft;
  if (!draft || !draft.transports[index]) return;
  draft.transports[index][key] = value;
}

function fleetNodeChangeTransportType(index, value) {
  const draft = fleet.nodeDraft;
  if (!draft || !draft.transports[index]) return;
  draft.transports[index].type = value;
  if (value !== 'livekit') draft.transports[index].on_demand = false;
  if (fleet.renderNodeBody) fleet.renderNodeBody();
}

function fleetNodeAddTransport() {
  const draft = fleet.nodeDraft;
  if (!draft) return;
  draft.transports.push({ type: 'raw_ws', domain: '', enabled: true, on_demand: false });
  if (fleet.renderNodeBody) fleet.renderNodeBody();
}

function fleetNodeRemoveTransport(index) {
  const draft = fleet.nodeDraft;
  if (!draft) return;
  draft.transports.splice(index, 1);
  if (fleet.renderNodeBody) fleet.renderNodeBody();
}


function transportLabel(kind) {
  const found = TRANSPORT_KINDS.find(k => k.value === kind);
  return found ? found.label : kind;
}

function renderRoutes() {
  const root = document.getElementById('routes-list');
  if (!root) return;
  if (!fleet.routes.length) {
    root.innerHTML = `<div class="empty"><span class="material-symbols-rounded">alt_route</span><p>No routes yet. Create an Entry → Exit chain.</p></div>`;
    return;
  }
  const rows = fleet.routes.map(route => `
    <tr>
      <td><strong>${esc(route.name)}</strong><div class="table-sub">#${esc(route.id)}</div></td>
      <td>${esc(nodeName(route.entry_node_id))} <span class="arrow">→</span> ${esc(nodeName(route.exit_node_id))}</td>
      <td><span class="transport-chip">${esc(transportLabel(route.transport_type))}</span></td>
      <td>
        <label class="switch">
          <input type="checkbox" ${route.enabled ? 'checked' : ''} onchange="fleetToggleRoute(${route.id}, this.checked)">
          <span class="slider"></span>
        </label>
      </td>
      <td class="table-actions">
        <button class="icon-btn" type="button" title="Edit" onclick="fleetEditRoute(${route.id})"><span class="material-symbols-rounded">edit</span></button>
        <button class="icon-btn icon-btn--danger" type="button" title="Delete" onclick="fleetDeleteRoute(${route.id})"><span class="material-symbols-rounded">delete</span></button>
      </td>
    </tr>`).join('');
  root.innerHTML = `<div class="table-wrap"><table class="data-table">
    <thead><tr><th>Name</th><th>Entry → Exit</th><th>Transport</th><th>Enabled</th><th></th></tr></thead>
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
      body: JSON.stringify({
        name: route.name,
        entry_node_id: route.entry_node_id,
        exit_node_id: route.exit_node_id,
        transport_type: route.transport_type,
        enabled: enabled
      })
    });
    route.enabled = enabled;
    toast(enabled ? 'Route enabled' : 'Route disabled');
  } catch (e) {
    toast(e.message, 'error');
    loadPanel('routes', true);
  }
}

async function fleetDeleteRoute(id) {
  const route = fleet.routes.find(r => r.id === id);
  if (!confirm(`Delete route "${route ? route.name : id}"?`)) return;
  try {
    await api(`/api/routes/${encodeURIComponent(id)}`, { method: 'DELETE' });
    toast('Route deleted');
    await loadPanel('routes');
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
  const entryID = route ? route.entry_node_id : (relays[0] ? relays[0].id : 0);
  const exitID = route ? route.exit_node_id : (exits[0] ? exits[0].id : 0);
  const transport = route ? route.transport_type : (TRANSPORT_KINDS[0].value);
  const draft = {
    name: route ? route.name : '',
    entry_node_id: entryID,
    exit_node_id: exitID,
    transport_type: transport,
    enabled: route ? !!route.enabled : true
  };

  function selectOptions(nodes, selected) {
    if (!nodes.length) return `<option value="0">No nodes available</option>`;
    return nodes.map(n => `<option value="${n.id}" ${n.id === selected ? 'selected' : ''}>${esc(n.name)}</option>`).join('');
  }

  openModal({
    title: editID ? `Edit route: ${route.name}` : 'Add route',
    actions: [
      { label: t('actions.cancel'), kind: 'text', onClick: closeModal },
      { label: editID ? t('actions.save') : t('actions.create'), kind: 'filled', onClick: submitRoute }
    ],
    body: `
      <div class="form-group">
        <div class="field"><label>Name</label><input type="text" id="r-name" value="${escAttr(draft.name)}" placeholder="de-fr"></div>
        <div class="field"><label>Entry node (relay)</label><select id="r-entry" class="input">${selectOptions(relays, entryID)}</select></div>
        <div class="field"><label>Exit node</label><select id="r-exit" class="input">${selectOptions(exits, exitID)}</select></div>
        <div class="field"><label>Transport</label><select id="r-transport" class="input">
          ${TRANSPORT_KINDS.map(k => `<option value="${escAttr(k.value)}" ${k.value === transport ? 'selected' : ''}>${esc(k.label)}</option>`).join('')}
        </select></div>
        <label class="check-row"><input type="checkbox" id="r-enabled" ${draft.enabled ? 'checked' : ''}> Enabled</label>
      </div>`
  });

  document.getElementById('r-name').addEventListener('input', e => { draft.name = e.target.value; });
  document.getElementById('r-entry').addEventListener('change', e => { draft.entry_node_id = Number(e.target.value) || 0; });
  document.getElementById('r-exit').addEventListener('change', e => { draft.exit_node_id = Number(e.target.value) || 0; });
  document.getElementById('r-transport').addEventListener('change', e => { draft.transport_type = e.target.value; });
  document.getElementById('r-enabled').addEventListener('change', e => { draft.enabled = e.target.checked; });

  function submitRoute() {
    if (!draft.name.trim()) return toast('Name is required', 'error');
    if (!draft.entry_node_id || !draft.exit_node_id) return toast('Choose entry and exit nodes', 'error');
    const path = editID ? `/api/routes/${encodeURIComponent(editID)}` : '/api/routes';
    api(path, {
      method: editID ? 'PUT' : 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(draft)
    }).then(() => {
      toast(editID ? 'Route saved' : 'Route created');
      closeModal();
      loadPanel('routes');
    }).catch(e => toast(e.message, 'error'));
  }
}


function userUsedBytes(user) {
  return (Number(user.used_upload_bytes) || 0) + (Number(user.used_download_bytes) || 0);
}

function formatExpiry(value) {
  if (!value) return 'Unlimited';
  const date = new Date(value);
  if (isNaN(date.getTime()) || date.getUTCFullYear() <= 1) return 'Unlimited';
  return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function userExpired(value) {
  if (!value) return false;
  const date = new Date(value);
  if (isNaN(date.getTime()) || date.getUTCFullYear() <= 1) return false;
  return date.getTime() <= Date.now();
}

function routeNames(ids) {
  if (!ids || !ids.length) return 'All routes';
  return ids.map(id => nodeRouteName(id)).join(', ');
}

function nodeRouteName(id) {
  const route = fleet.routes.find(r => r.id === id);
  return route ? route.name : '#' + id;
}

function renderUsers() {
  const root = document.getElementById('users-list');
  if (!root) return;
  if (!fleet.users.length) {
    root.innerHTML = `<div class="empty"><span class="material-symbols-rounded">group</span><p>No users yet.</p></div>`;
    return;
  }
  root.innerHTML = fleet.users.map((user, index) => {
    const used = userUsedBytes(user);
    const limit = Number(user.traffic_limit_bytes) || 0;
    const percent = limit > 0 ? Math.min(100, Math.round(used / limit * 100)) : 0;
    const progressClass = limit <= 0 ? 'unlimited' : (percent >= 90 ? 'danger' : percent >= 70 ? 'warn' : 'good');
    const active = user.enabled && !userExpired(user.expires_at) &&
      (limit <= 0 || used < limit);
    return `
      <div class="user-card">
        <div class="user-card__header">
          <div>
            <h3 class="card__title">${esc(user.name || user.id)}</h3>
            <span class="card__subtitle">${esc(user.id)}</span>
          </div>
          <span class="status-chip status-chip--${active ? 'online' : 'error'}">${active ? 'active' : 'inactive'}</span>
        </div>
        <div class="user-card__meta">
          <span class="meta-chip">Expires: ${esc(formatExpiry(user.expires_at))}</span>
          <span class="meta-chip">${esc(routeNames(user.allowed_route_ids))}</span>
        </div>
        <div class="traffic">
          <div class="traffic__labels">
            <span>↓ ${formatBytes(Number(user.used_download_bytes) || 0)}</span>
            <span>↑ ${formatBytes(Number(user.used_upload_bytes) || 0)}</span>
            <span>${limit > 0 ? formatBytes(used) + ' / ' + formatBytes(limit) : formatBytes(used) + ' / unlimited'}</span>
          </div>
          <div class="progress progress--${progressClass}">
            <div class="progress__bar" style="width:${limit > 0 ? percent : Math.min(100, Math.round(used / (1024 * 1024 * 1024) * 100))}%"></div>
          </div>
        </div>
        <div class="user-card__footer">
          <button class="btn btn--tonal btn--compact" type="button" onclick="fleetCopySubscription(${index})">
            <span class="material-symbols-rounded">link</span> Copy subscription
          </button>
          <button class="btn btn--tonal btn--compact" type="button" onclick="fleetResetUserTraffic(${index})">
            <span class="material-symbols-rounded">restart_alt</span> Reset traffic
          </button>
          <button class="btn btn--tonal btn--compact" type="button" onclick="fleetToggleUser(${index})">
            <span class="material-symbols-rounded">${user.enabled ? 'block' : 'lock_open'}</span> ${user.enabled ? 'Block' : 'Unblock'}
          </button>
          <button class="icon-btn icon-btn--danger" type="button" title="Delete" onclick="fleetDeleteUser(${index})">
            <span class="material-symbols-rounded">delete</span>
          </button>
        </div>
      </div>`;
  }).join('');
}

async function fleetCopySubscription(index) {
  const user = fleet.users[index];
  if (!user) return;
  const url = user.subscription_url || (location.origin + '/sub/' + encodeURIComponent(user.sub_token));
  await copyToClipboard(url);
  toast('Subscription link copied');
}

async function fleetResetUserTraffic(index) {
  const user = fleet.users[index];
  if (!user || !confirm(`Reset traffic for "${user.name || user.id}"?`)) return;
  try {
    await api(`/api/users/${encodeURIComponent(user.id)}/reset-traffic`, { method: 'POST' });
    toast('Traffic reset');
    await loadPanel('users');
  } catch (e) { toast(e.message, 'error'); }
}

async function fleetToggleUser(index) {
  const user = fleet.users[index];
  if (!user) return;
  try {
    await api(`/api/users/${encodeURIComponent(user.id)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: user.name,
        sub_token: user.sub_token,
        enabled: !user.enabled,
        expires_at: user.expires_at,
        traffic_limit_bytes: user.traffic_limit_bytes,
        allowed_route_ids: user.allowed_route_ids || []
      })
    });
    toast(user.enabled ? 'User blocked' : 'User unblocked');
    await loadPanel('users');
  } catch (e) { toast(e.message, 'error'); }
}

async function fleetDeleteUser(index) {
  const user = fleet.users[index];
  if (!user || !confirm(`Delete user "${user.name || user.id}"?`)) return;
  try {
    await api(`/api/users/${encodeURIComponent(user.id)}`, { method: 'DELETE' });
    toast('User deleted');
    await loadPanel('users');
  } catch (e) { toast(e.message, 'error'); }
}


function openUserModal() {
  if (!fleet.routes.length) loadPanel('users', true).then(() => openUserModalNow());
  else openUserModalNow();
}

function openUserModalNow() {
  if (!fleet.routes.length) loadPanel('users', true);
  const draft = {
    name: '',
    enabled: true,
    expiryMode: 'days',
    days: 30,
    trafficMode: 'limited',
    gb: 100,
    routeIDs: []
  };

  const routeOptions = fleet.routes.length
    ? fleet.routes.map(route => `<label class="check-row">
        <input type="checkbox" id="u-route-${route.id}" data-route-id="${route.id}"
          onchange="fleetUserToggleRoute(${route.id}, this.checked)">
        ${esc(route.name)} <span class="hint">(${esc(transportLabel(route.transport_type))})</span>
      </label>`).join('')
    : `<div class="hint">No routes yet. The user will get access to all routes created later.</div>`;

  openModal({
    title: 'Add user',
    actions: [
      { label: t('actions.cancel'), kind: 'text', onClick: closeModal },
      { label: t('actions.create'), kind: 'filled', onClick: submitUser }
    ],
    body: `
      <div class="form-group">
        <div class="field"><label>Name</label><input type="text" id="u-name" placeholder="Alice"></div>
        <label class="check-row"><input type="checkbox" id="u-enabled" checked> Enabled</label>
        <div class="form-row">
          <div class="field"><label>Expires</label><select id="u-expiry-mode" class="input">
            <option value="days">In N days</option>
            <option value="never">Never</option>
          </select></div>
          <div class="field field--small"><label>Days</label><input type="number" id="u-days" value="30" min="1"></div>
        </div>
        <div class="form-row">
          <div class="field"><label>Traffic limit</label><select id="u-traffic-mode" class="input">
            <option value="limited">Limited</option>
            <option value="unlimited">Unlimited</option>
          </select></div>
          <div class="field field--small"><label>GB</label><input type="number" id="u-gb" value="100" min="1"></div>
        </div>
        <div class="field"><label>Allowed routes (empty = all)</label><div class="check-list">${routeOptions}</div></div>
      </div>`
  });

  fleet.userDraft = draft;

  const nameEl = document.getElementById('u-name');
  const enabledEl = document.getElementById('u-enabled');
  const expiryEl = document.getElementById('u-expiry-mode');
  const daysEl = document.getElementById('u-days');
  const trafficEl = document.getElementById('u-traffic-mode');
  const gbEl = document.getElementById('u-gb');

  nameEl.addEventListener('input', e => { draft.name = e.target.value; });
  enabledEl.addEventListener('change', e => { draft.enabled = e.target.checked; });
  expiryEl.addEventListener('change', e => {
    draft.expiryMode = e.target.value;
    daysEl.disabled = draft.expiryMode === 'never';
  });
  daysEl.addEventListener('input', e => { draft.days = Number(e.target.value) || 0; });
  trafficEl.addEventListener('change', e => {
    draft.trafficMode = e.target.value;
    gbEl.disabled = draft.trafficMode === 'unlimited';
  });
  gbEl.addEventListener('input', e => { draft.gb = Number(e.target.value) || 0; });

  function submitUser() {
    const payload = {
      name: draft.name.trim(),
      enabled: draft.enabled,
      allowed_route_ids: draft.routeIDs
    };
    if (draft.expiryMode === 'days' && draft.days > 0) {
      payload.expires_at = new Date(Date.now() + draft.days * 86400000).toISOString();
    }
    if (draft.trafficMode === 'limited' && draft.gb > 0) {
      payload.traffic_limit_bytes = Math.round(draft.gb * 1024 * 1024 * 1024);
    }
    api('/api/users', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }).then(() => {
      toast('User created');
      closeModal();
      loadPanel('users');
    }).catch(e => toast(e.message, 'error'));
  }
}

function fleetUserToggleRoute(id, checked) {
  const draft = fleet.userDraft;
  if (!draft) return;
  const set = new Set(draft.routeIDs);
  if (checked) set.add(id);
  else set.delete(id);
  draft.routeIDs = Array.from(set).sort((a, b) => a - b);
}

