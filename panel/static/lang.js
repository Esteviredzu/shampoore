const LANGS = {
  available: ['en', 'ru', 'eo'],
  data: { en: {} },
  current: 'en'
};

async function loadLang(lang) {
  if (LANGS.data[lang] && Object.keys(LANGS.data[lang]).length) return;
  try {
    const res = await fetch(`locales/${lang}.json?v=4`, { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    LANGS.data[lang] = await res.json();
  } catch (e) {
    if (lang !== 'en') return loadLang('en');
    LANGS.data[lang] = {};
  }
}

function t(key) {
  const dict = LANGS.data[LANGS.current] || {};
  const en = LANGS.data.en || {};
  return lookup(dict, key) ?? lookup(en, key) ?? key;
}

function lookup(obj, path) {
  return path.split('.').reduce((o, p) => (o && o[p] !== undefined) ? o[p] : null, obj);
}

function applyLang(root = document) {
  root.querySelectorAll('[data-lang]').forEach(el => el.textContent = t(el.dataset.lang));
  root.querySelectorAll('[data-lang-placeholder]').forEach(el => el.placeholder = t(el.dataset.langPlaceholder));
  root.querySelectorAll('[data-lang-title]').forEach(el => el.title = t(el.dataset.langTitle));
}