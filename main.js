import Papa from 'papaparse';
import './style.css';

// ============================================================
// STATE
// ============================================================
const state = {
  rawData: [],           // all parsed rows
  studies: [],           // unique study_ids
  studyMap: {},          // study_id -> rows
  decisions: {},         // "study_id::rowIndex" -> { decision, correctedValue }
  pubmedCache: {},       // study_id -> pubmed result
  activeStudy: null,
  activeSheet: 'all',
  searchQuery: '',
  filterSource: 'all',   // all | consensus | consistency | audit
  filterStatus: 'all',   // all | pending | resolved
  filterType: 'all',     // all | field_mismatch | only_in_run1 | only_in_run2
  pubmedPanelOpen: false,
  pubmedLoading: false,
  pubmedShowAll: false,  // show all 6 results vs top 3
  pubmedSelected: null,  // selected article PMID
  pubmedExtracted: null, // extracted fields from selected article
  pubmedFieldAccepted: {}, // field -> true/false/null for acceptance
  pdfOpen: false,
  pdfDarkMode: true,    // default: dark mode to match UI
};

// ---- Persistence ----
const STORAGE_KEY = 'review_platform_decisions_v2';
const PUBMED_CACHE_KEY = 'review_platform_pubmed_cache';

function loadDecisions() {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) state.decisions = JSON.parse(saved);
  } catch (e) { console.warn('Failed to load decisions', e); }
  try {
    const cached = localStorage.getItem(PUBMED_CACHE_KEY);
    if (cached) state.pubmedCache = JSON.parse(cached);
  } catch (e) { /* ignore */ }
}

function saveDecisions() {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state.decisions));
  } catch (e) { console.warn('Failed to save decisions', e); }
}

function savePubmedCache() {
  try {
    localStorage.setItem(PUBMED_CACHE_KEY, JSON.stringify(state.pubmedCache));
  } catch (e) { /* ignore */ }
}

function getDecisionKey(studyId, rowIdx) {
  return `${studyId}::${rowIdx}`;
}

// ============================================================
// PUBMED API
// ============================================================
async function fetchPubMed(studyId) {
  if (state.pubmedCache[studyId]) return state.pubmedCache[studyId];

  const parts = studyId.replace(/[_-]/g, ' ').split(' ');
  const year = parts.find(p => /^\d{4}$/.test(p));
  const authorParts = parts.filter(p => !/^\d{4}$/.test(p) && p !== 'b' && p !== 'c');
  const author = authorParts.join(' ');

  const studyRows = (state.studyMap[studyId] || []).filter(r => r.sheet === 'study');
  let journal = '';
  for (const row of studyRows) {
    if (row.field === 'Journal' && (row.run1 || row.run2)) {
      journal = (row.run1 || row.run2).replace(/null/gi, '').trim();
    }
  }

  let query = `${author}[Author]`;
  if (year) query += ` AND ${year}[Date - Publication]`;
  if (journal) query += ` AND ${journal}[Journal]`;
  query += ' AND (sarcopenia OR vertebral fracture OR osteoporosis OR muscle)';

  try {
    // Search for up to 6 PMIDs
    const searchUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=${encodeURIComponent(query)}&retmax=6&retmode=json`;
    const searchRes = await fetch(searchUrl);
    const searchData = await searchRes.json();
    let ids = searchData?.esearchresult?.idlist || [];

    if (ids.length === 0) {
      const simpleQuery = `${author}[Author] AND ${year || ''}[Date - Publication]`;
      const retryUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=${encodeURIComponent(simpleQuery)}&retmax=6&retmode=json`;
      const retryRes = await fetch(retryUrl);
      const retryData = await retryRes.json();
      ids = retryData?.esearchresult?.idlist || [];
      if (ids.length === 0) {
        const result = { error: 'No results found', query };
        state.pubmedCache[studyId] = result;
        savePubmedCache();
        return result;
      }
    }

    // Fetch paper summaries
    const fetchUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=${ids.join(',')}&retmode=json`;
    const fetchRes = await fetch(fetchUrl);
    const fetchData = await fetchRes.json();

    const articles = ids.map(id => {
      const a = fetchData?.result?.[id];
      if (!a) return null;
      return {
        pmid: id,
        title: a.title || '',
        firstAuthor: (a.authors && a.authors[0]) ? a.authors[0].name : '',
        authors: (a.authors || []).map(x => x.name).join(', '),
        authorList: (a.authors || []).map(x => x.name),
        journal: a.fulljournalname || a.source || '',
        journalAbbrev: a.source || '',
        pubdate: a.pubdate || '',
        doi: (a.articleids || []).find(x => x.idtype === 'doi')?.value || '',
        volume: a.volume || '',
        issue: a.issue || '',
        pages: a.pages || '',
        lang: a.lang || [],
        pubtype: a.pubtype || [],
      };
    }).filter(Boolean);

    const result = { articles, query, searchedAt: new Date().toISOString() };
    state.pubmedCache[studyId] = result;
    savePubmedCache();
    return result;
  } catch (err) {
    const result = { error: err.message, query };
    state.pubmedCache[studyId] = result;
    savePubmedCache();
    return result;
  }
}

// Fetch abstract and detailed metadata for a specific PMID
async function fetchPubMedDetail(pmid) {
  try {
    const url = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=${pmid}&rettype=xml&retmode=text`;
    const res = await fetch(url);
    const xmlText = await res.text();
    const parser = new DOMParser();
    const doc = parser.parseFromString(xmlText, 'text/xml');

    const abstractParts = doc.querySelectorAll('AbstractText');
    let abstract = '';
    abstractParts.forEach(p => {
      const label = p.getAttribute('Label');
      if (label) abstract += `${label}: `;
      abstract += (p.textContent || '') + '\n';
    });

    // Extract affiliation for country detection
    const affiliations = [];
    doc.querySelectorAll('AffiliationInfo Affiliation').forEach(a => {
      affiliations.push(a.textContent || '');
    });

    // MeSH terms
    const meshTerms = [];
    doc.querySelectorAll('MeshHeading DescriptorName').forEach(m => {
      meshTerms.push(m.textContent || '');
    });

    // Keywords
    const keywords = [];
    doc.querySelectorAll('Keyword').forEach(k => {
      keywords.push(k.textContent || '');
    });

    return { abstract: abstract.trim(), affiliations, meshTerms, keywords };
  } catch (err) {
    return { abstract: '', affiliations: [], meshTerms: [], keywords: [], error: err.message };
  }
}

// Extract country from affiliation strings
function detectCountry(affiliations) {
  const countries = [
    'China', 'Japan', 'South Korea', 'Korea', 'Taiwan', 'Turkey', 'Iran',
    'United States', 'USA', 'Canada', 'Australia', 'Germany', 'France',
    'Italy', 'Spain', 'Netherlands', 'Belgium', 'Switzerland', 'Austria',
    'United Kingdom', 'UK', 'Brazil', 'Israel', 'India', 'Thailand',
    'Singapore', 'Malaysia', 'Egypt', 'Saudi Arabia', 'Ethiopia',
    'Sweden', 'Norway', 'Denmark', 'Finland', 'Poland', 'Czech Republic',
    'Greece', 'Portugal', 'New Zealand', 'Mexico', 'Colombia', 'Chile',
    'Argentina', 'South Africa', 'Nigeria', 'Vietnam', 'Philippines',
    'Indonesia', 'Pakistan', 'Bangladesh', 'Iraq', 'Lebanon', 'Morocco',
  ];
  const joinedAff = affiliations.join(' ');
  for (const c of countries) {
    if (joinedAff.includes(c)) return c;
  }
  return null;
}

// Extract schema-mapped fields from a PubMed article + detail
function extractFieldsFromPubMed(article, detail) {
  const fields = {};

  // First_Author
  if (article.firstAuthor) {
    const lastName = article.firstAuthor.split(' ')[0];
    fields.First_Author = lastName;
  }

  // Year
  const yearMatch = (article.pubdate || '').match(/(\d{4})/);
  if (yearMatch) fields.Year = yearMatch[1];

  // Journal
  if (article.journal) fields.Journal = article.journal;

  // Country from affiliations
  if (detail.affiliations && detail.affiliations.length > 0) {
    const country = detectCountry(detail.affiliations);
    if (country) fields.Country = country;
  }

  // DOI
  if (article.doi) fields.DOI = article.doi;

  // PMID
  fields.PMID = article.pmid;

  // Title (useful for cross-referencing)
  if (article.title) fields.Title = article.title;

  // Abstract info — try to extract design, population, sample size
  if (detail.abstract) {
    // Try to find sample size from abstract
    const nMatch = detail.abstract.match(/(?:n\s*=\s*|sample(?:\s+size)?\s+(?:of\s+|was\s+)?)(\d[\d,]+)/i);
    if (nMatch) {
      fields.Total_N = nMatch[1].replace(/,/g, '');
    }

    // Try to detect study design keywords
    const designKeywords = {
      'cross[- ]sectional': 'Cross-Sectional',
      'prospective cohort': 'Prospective Cohort',
      'retrospective cohort': 'Retrospective Cohort',
      'case[- ]control': 'Case-Control',
      'longitudinal': 'Prospective Cohort',
      'retrospective': 'Retrospective Cohort',
    };
    for (const [pattern, design] of Object.entries(designKeywords)) {
      if (new RegExp(pattern, 'i').test(detail.abstract)) {
        fields.Design = design;
        break;
      }
    }
  }

  // MeSH terms for fracture/sarcopenia info
  if (detail.meshTerms && detail.meshTerms.length > 0) {
    fields._meshTerms = detail.meshTerms.join(', ');
  }

  // Keywords
  if (detail.keywords && detail.keywords.length > 0) {
    fields._keywords = detail.keywords.join(', ');
  }

  // Abstract itself for reference
  if (detail.abstract) {
    fields._abstract = detail.abstract;
  }

  return fields;
}

// ============================================================
// PARSING
// ============================================================
function parseCSV(csvText) {
  const result = Papa.parse(csvText, {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: false,
  });

  state.rawData = result.data;

  // Build study map
  const studySet = new Set();
  state.studyMap = {};

  result.data.forEach((row, idx) => {
    row._originalIndex = idx;
    const sid = row.study_id;
    if (!sid) return;
    studySet.add(sid);
    if (!state.studyMap[sid]) state.studyMap[sid] = [];
    state.studyMap[sid].push(row);
  });

  state.studies = Array.from(studySet).sort();
}

// ============================================================
// RENDER HELPERS
// ============================================================
function escapeHtml(str) {
  if (str == null) return '<span style="color:var(--text-muted);font-style:italic;">null</span>';
  const s = String(str);
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function formatValue(val) {
  if (val == null || val === '' || val === 'undefined') return '<span style="color:var(--text-muted);font-style:italic;">—</span>';
  const s = String(val).trim();
  // Try to pretty-print JSON-like strings
  if ((s.startsWith('{') || s.startsWith('[')) && s.length > 2) {
    try {
      const parsed = JSON.parse(s.replace(/'/g, '"').replace(/None/g, 'null').replace(/True/g, 'true').replace(/False/g, 'false'));
      return escapeHtml(JSON.stringify(parsed, null, 2));
    } catch { /* fall through */ }
  }
  return escapeHtml(s);
}

function getSheets(rows) {
  const sheets = new Set();
  rows.forEach(r => { if (r.sheet) sheets.add(r.sheet); });
  return Array.from(sheets).sort();
}

function getSources(rows) {
  const sources = new Set();
  rows.forEach(r => { if (r.source) sources.add(r.source); });
  return Array.from(sources).sort();
}

function getTypes(rows) {
  const types = new Set();
  rows.forEach(r => { if (r.type) types.add(r.type); });
  return Array.from(types).sort();
}

function getRowsForView() {
  if (!state.activeStudy) return [];
  let rows = state.studyMap[state.activeStudy] || [];

  if (state.activeSheet !== 'all') {
    rows = rows.filter(r => r.sheet === state.activeSheet);
  }
  if (state.filterSource !== 'all') {
    rows = rows.filter(r => r.source === state.filterSource);
  }
  if (state.filterType !== 'all') {
    rows = rows.filter(r => r.type === state.filterType);
  }
  if (state.filterStatus === 'pending') {
    rows = rows.filter(r => !state.decisions[getDecisionKey(state.activeStudy, r._originalIndex)]);
  } else if (state.filterStatus === 'resolved') {
    rows = rows.filter(r => !!state.decisions[getDecisionKey(state.activeStudy, r._originalIndex)]);
  }

  return rows;
}

function getStudyStats(studyId) {
  const rows = state.studyMap[studyId] || [];
  const total = rows.length;
  let resolved = 0;
  rows.forEach(r => {
    if (state.decisions[getDecisionKey(studyId, r._originalIndex)]) resolved++;
  });
  return { total, resolved, pending: total - resolved };
}

function getGlobalStats() {
  let total = 0, resolved = 0;
  state.studies.forEach(sid => {
    const s = getStudyStats(sid);
    total += s.total;
    resolved += s.resolved;
  });
  return { total, resolved, pending: total - resolved };
}

// ============================================================
// TOAST
// ============================================================
function showToast(message) {
  const existing = document.querySelector('.toast');
  if (existing) existing.remove();

  const toast = document.createElement('div');
  toast.className = 'toast';
  toast.innerHTML = `✓ ${message}`;
  document.body.appendChild(toast);
  setTimeout(() => { if (toast.parentNode) toast.remove(); }, 3000);
}

// ============================================================
// RENDER: UPLOAD SCREEN
// ============================================================
function renderUploadScreen() {
  const app = document.getElementById('app');
  app.innerHTML = `
    <div class="upload-screen">
      <div class="upload-hero">
        <div class="subtitle">Meta-Analysis Extraction Pipeline</div>
        <h1>Human Review</h1>
        <p>Upload your review-queue.csv to begin adjudicating discrepancies across extraction runs, consistency checks, and audits.</p>
      </div>
      <div class="drop-zone" id="drop-zone">
        <span class="icon">📋</span>
        <div class="label">
          Drop <strong>review-queue.csv</strong> here<br/>
          or click to browse
        </div>
        <input type="file" id="file-input" accept=".csv,.tsv" />
      </div>
    </div>
  `;

  const dropZone = document.getElementById('drop-zone');
  const fileInput = document.getElementById('file-input');

  dropZone.addEventListener('click', () => fileInput.click());

  dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropZone.classList.add('drag-over');
  });

  dropZone.addEventListener('dragleave', () => {
    dropZone.classList.remove('drag-over');
  });

  dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    const file = e.dataTransfer.files[0];
    if (file) readFile(file);
  });

  fileInput.addEventListener('change', (e) => {
    const file = e.target.files[0];
    if (file) readFile(file);
  });
}

function readFile(file) {
  const reader = new FileReader();
  reader.onload = (e) => {
    parseCSV(e.target.result);
    if (state.studies.length > 0) {
      state.activeStudy = state.studies[0];
    }
    renderApp();
  };
  reader.readAsText(file);
}

// ============================================================
// RENDER: MAIN APP
// ============================================================
function renderApp() {
  const app = document.getElementById('app');
  const globalStats = getGlobalStats();
  const progressPct = globalStats.total > 0 ? Math.round(globalStats.resolved / globalStats.total * 100) : 0;

  app.innerHTML = `
    <div class="app-layout ${state.pdfOpen ? 'pdf-open' : ''}">
      <div class="top-bar">
        <div class="brand">
          <button class="btn btn-sm mobile-toggle" id="toggle-sidebar">☰</button>
          <h2>Human Review</h2>
          <span class="tag">Sarcopenia × VF</span>
        </div>
        <div class="stats">
          <div class="stat-pill"><span class="dot total"></span>${globalStats.total} items</div>
          <div class="stat-pill"><span class="dot done"></span>${globalStats.resolved} reviewed</div>
          <div class="stat-pill"><span class="dot pending"></span>${globalStats.pending} pending</div>
        </div>
        <div class="actions">
          <button class="btn btn-sm" id="btn-pull" title="Pull latest data from review-queue.csv">⬇ Pull</button>
          <button class="btn btn-sm btn-primary" id="btn-push" title="Push decisions back to review-queue.csv">⬆ Push</button>
        </div>
      </div>

      <div class="sidebar" id="sidebar">
        <div class="search-box">
          <span class="search-icon">🔍</span>
          <input type="text" id="search-input" placeholder="Search studies..." value="${escapeHtml(state.searchQuery) || ''}" />
        </div>

        <div class="progress-container">
          <div class="progress-bar"><div class="progress-fill" style="width:${progressPct}%"></div></div>
          <div class="progress-text">${progressPct}% complete · ${globalStats.resolved}/${globalStats.total}</div>
        </div>

        <div class="sidebar-section">
          <h3>Review Status</h3>
          <div class="filter-chips">
            <button class="chip ${state.filterStatus === 'all' ? 'active' : ''}" data-filter="status" data-value="all">All</button>
            <button class="chip ${state.filterStatus === 'pending' ? 'active' : ''}" data-filter="status" data-value="pending">⏳ Pending</button>
            <button class="chip ${state.filterStatus === 'resolved' ? 'active' : ''}" data-filter="status" data-value="resolved">✓ Done</button>
          </div>
        </div>

        <div class="sidebar-section">
          <h3>Source</h3>
          <div class="filter-chips">
            <button class="chip ${state.filterSource === 'all' ? 'active' : ''}" data-filter="source" data-value="all">All</button>
            <button class="chip ${state.filterSource === 'consensus' ? 'active' : ''}" data-filter="source" data-value="consensus">Consensus</button>
            <button class="chip ${state.filterSource === 'consistency' ? 'active' : ''}" data-filter="source" data-value="consistency">Consistency</button>
            <button class="chip ${state.filterSource === 'audit' ? 'active' : ''}" data-filter="source" data-value="audit">Audit</button>
          </div>
        </div>

        <div class="sidebar-section">
          <h3>Discrepancy Type</h3>
          <div class="filter-chips">
            <button class="chip ${state.filterType === 'all' ? 'active' : ''}" data-filter="type" data-value="all">All</button>
            <button class="chip ${state.filterType === 'field_mismatch' ? 'active' : ''}" data-filter="type" data-value="field_mismatch">Field Mismatch</button>
            <button class="chip ${state.filterType === 'only_in_run1' ? 'active' : ''}" data-filter="type" data-value="only_in_run1">Run 1 Only</button>
            <button class="chip ${state.filterType === 'only_in_run2' ? 'active' : ''}" data-filter="type" data-value="only_in_run2">Run 2 Only</button>
          </div>
        </div>

        <div class="sidebar-section">
          <h3>Studies (${state.studies.length})</h3>
          <div class="study-list" id="study-list">
            ${renderStudyList()}
          </div>
        </div>
      </div>

      <div class="main-content" id="main-content">
        ${renderMainContent()}
      </div>

      ${state.pdfOpen ? renderPdfPane() : ''}
    </div>
  `;

  bindAppEvents();
}

function renderStudyList() {
  const query = state.searchQuery.toLowerCase();
  return state.studies
    .filter(sid => !query || sid.toLowerCase().includes(query))
    .map(sid => {
      const stats = getStudyStats(sid);
      const isActive = sid === state.activeStudy;
      const countClass = stats.pending === 0 ? 'all-done' : (stats.resolved > 0 ? 'has-pending' : '');
      return `
        <div class="study-item ${isActive ? 'active' : ''}" data-study="${sid}">
          <span class="name">${sid}</span>
          <span class="count ${countClass}">${stats.resolved}/${stats.total}</span>
        </div>
      `;
    }).join('');
}

function renderMainContent() {
  if (!state.activeStudy) {
    return `
      <div class="empty-state">
        <div class="icon">📑</div>
        <h3>Select a study to begin review</h3>
        <p>Choose a study from the sidebar to see its discrepancies.</p>
      </div>
    `;
  }

  const allRows = state.studyMap[state.activeStudy] || [];
  const sheets = getSheets(allRows);
  const viewRows = getRowsForView();
  const studyStats = getStudyStats(state.activeStudy);
  const sources = getSources(allRows);

  const sheetTabsHtml = `
    <button class="sheet-tab ${state.activeSheet === 'all' ? 'active' : ''}" data-sheet="all">
      All <span class="badge">${allRows.length}</span>
    </button>
    ${sheets.map(s => {
      const count = allRows.filter(r => r.sheet === s).length;
      return `<button class="sheet-tab ${state.activeSheet === s ? 'active' : ''}" data-sheet="${s}">
        ${s} <span class="badge">${count}</span>
      </button>`;
    }).join('')}
  `;

  // PubMed panel
  const pubmedData = state.pubmedCache[state.activeStudy];
  const pubmedPanelHtml = renderPubmedPanel(pubmedData);

  return `
    <div class="study-header">
      <div class="study-header-top">
        <div>
          <h2>${state.activeStudy}</h2>
          <div class="meta">
            ${studyStats.resolved} of ${studyStats.total} items reviewed ·
            ${sheets.length} sheets ·
            sources: ${sources.join(', ')}
          </div>
        </div>
        <div class="study-header-actions">
          <button class="btn btn-sm btn-pdf ${state.pdfOpen ? 'btn-primary' : ''}" id="btn-toggle-pdf" title="Toggle PDF viewer">
            📄 ${state.pdfOpen ? 'Hide PDF' : 'View PDF'}
          </button>
          <button class="btn btn-sm" id="btn-pubmed" title="Fetch paper info from PubMed">
            ${state.pubmedLoading ? '⏳' : '🔬'} PubMed Lookup
          </button>
        </div>
      </div>
    </div>

    ${pubmedPanelHtml}

    <div class="sheet-tabs">${sheetTabsHtml}</div>

    <div class="view-info">
      <span class="view-count">${viewRows.length} item${viewRows.length !== 1 ? 's' : ''} shown</span>
      ${state.filterSource !== 'all' || state.filterType !== 'all' || state.filterStatus !== 'all'
        ? '<button class="btn btn-sm" id="btn-clear-filters">✕ Clear Filters</button>' : ''}
    </div>

    <div class="bulk-ops-bar">
      <span class="bulk-label">Bulk:</span>
      <button class="btn btn-sm" data-bulk="accept-matching" title="Accept run1 where run1 = run2">✓ Accept Matching</button>
      <button class="btn btn-sm" data-bulk="accept-run1-all" title="Accept Run 1 for all pending mismatches">Run 1 → All</button>
      <button class="btn btn-sm" data-bulk="accept-run2-all" title="Accept Run 2 for all pending mismatches">Run 2 → All</button>
      <span class="bulk-sep">|</span>
      <button class="btn btn-sm" data-bulk="include-run1-only" title="Include all pending run1-only items">✓ Include Run1-Only</button>
      <button class="btn btn-sm" data-bulk="exclude-run1-only" title="Exclude all pending run1-only items">✕ Exclude Run1-Only</button>
      <button class="btn btn-sm" data-bulk="include-run2-only" title="Include all pending run2-only items">✓ Include Run2-Only</button>
      <button class="btn btn-sm" data-bulk="exclude-run2-only" title="Exclude all pending run2-only items">✕ Exclude Run2-Only</button>
      <span class="bulk-sep">|</span>
      <button class="btn btn-sm" data-bulk="skip-all" title="Skip all remaining pending items">Skip All</button>
    </div>

    ${viewRows.length === 0 ? `
      <div class="empty-state">
        <div class="icon">✅</div>
        <h3>No items to show</h3>
        <p>All items have been reviewed, or no items match your current filters.</p>
      </div>
    ` : viewRows.map((row, i) => renderCard(row, i)).join('')}
  `;
}

function renderPubmedPanel(pubmedData) {
  if (!state.pubmedPanelOpen) return '';

  if (!pubmedData) {
    return `
      <div class="pubmed-panel">
        <div class="pubmed-panel-header">
          <h3>🔬 PubMed Lookup</h3>
          <button class="btn btn-sm" id="btn-close-pubmed">✕</button>
        </div>
        <div class="pubmed-empty">
          ${state.pubmedLoading ? '⏳ Searching PubMed...' : 'Click "PubMed Lookup" to search for this paper.'}
        </div>
      </div>
    `;
  }

  if (pubmedData.error) {
    return `
      <div class="pubmed-panel">
        <div class="pubmed-panel-header">
          <h3>🔬 PubMed Lookup</h3>
          <button class="btn btn-sm" id="btn-close-pubmed">✕</button>
        </div>
        <div class="pubmed-error">
          <strong>No results found</strong>
          <div class="pubmed-query">Query: ${escapeHtml(pubmedData.query)}</div>
          <button class="btn btn-sm" id="btn-pubmed-retry">Retry with broader search</button>
        </div>
      </div>
    `;
  }

  const allArticles = pubmedData.articles || [];
  const showCount = state.pubmedShowAll ? allArticles.length : Math.min(3, allArticles.length);
  const visibleArticles = allArticles.slice(0, showCount);
  const hasMore = allArticles.length > showCount;

  // If an article is selected and we have extracted fields, show that
  if (state.pubmedSelected && state.pubmedExtracted) {
    const selArticle = allArticles.find(a => a.pmid === state.pubmedSelected);
    return renderPubmedExtractedPanel(selArticle, state.pubmedExtracted);
  }

  return `
    <div class="pubmed-panel">
      <div class="pubmed-panel-header">
        <h3>🔬 PubMed Candidates (${allArticles.length})</h3>
        <button class="btn btn-sm" id="btn-close-pubmed">✕</button>
      </div>
      <div class="pubmed-query">Query: ${escapeHtml(pubmedData.query)}</div>
      ${visibleArticles.map((a, i) => `
        <div class="pubmed-article ${i === 0 ? 'pubmed-article-primary' : ''}" data-pmid="${a.pmid}">
          <div class="pubmed-article-row">
            <div class="pubmed-article-info">
              <div class="pubmed-title">
                <a href="https://pubmed.ncbi.nlm.nih.gov/${a.pmid}/" target="_blank" rel="noopener">${escapeHtml(a.title)}</a>
              </div>
              <div class="pubmed-authors">${escapeHtml(a.authors)}</div>
              <div class="pubmed-meta">
                <span>${escapeHtml(a.journal)}</span>
                <span>${escapeHtml(a.pubdate)}</span>
                ${a.volume ? `<span>Vol. ${escapeHtml(a.volume)}${a.issue ? `(${escapeHtml(a.issue)})` : ''}</span>` : ''}
                ${a.pages ? `<span>pp. ${escapeHtml(a.pages)}</span>` : ''}
              </div>
              <div class="pubmed-ids">
                <span class="pubmed-id">PMID: ${a.pmid}</span>
                ${a.doi ? `<span class="pubmed-id">DOI: <a href="https://doi.org/${a.doi}" target="_blank" rel="noopener">${escapeHtml(a.doi)}</a></span>` : ''}
              </div>
            </div>
            <button class="btn btn-sm btn-primary pubmed-select-btn" data-pmid="${a.pmid}">🎯 THIS ONE</button>
          </div>
        </div>
      `).join('')}
      ${hasMore ? `
        <div style="padding: 0.75rem 1.25rem; text-align: center;">
          <button class="btn btn-sm" id="btn-pubmed-show-more">Show +${allArticles.length - showCount} more</button>
        </div>
      ` : ''}
    </div>
  `;
}

function renderPubmedExtractedPanel(article, extracted) {
  const schemaFields = ['First_Author', 'Year', 'Journal', 'Country', 'DOI', 'PMID', 'Total_N', 'Design', 'Title'];

  // Check which fields have matching cards
  const studyRows = state.studyMap[state.activeStudy] || [];
  const matchableFields = new Set();
  studyRows.forEach(row => {
    if (row.sheet === 'study' && row.field) matchableFields.add(row.field);
  });

  return `
    <div class="pubmed-panel pubmed-extracted-panel">
      <div class="pubmed-panel-header">
        <h3>🎯 Extracted from PubMed</h3>
        <div style="display:flex;gap:0.35rem;">
          <button class="btn btn-sm" id="btn-pubmed-back">← Back to results</button>
          <button class="btn btn-sm" id="btn-close-pubmed">✕</button>
        </div>
      </div>
      ${article ? `
        <div class="pubmed-selected-summary">
          <div class="pubmed-title"><a href="https://pubmed.ncbi.nlm.nih.gov/${article.pmid}/" target="_blank">${escapeHtml(article.title)}</a></div>
          <div class="pubmed-authors" style="font-size:0.75rem;">${escapeHtml(article.authors)}</div>
        </div>
      ` : ''}
      <div class="pubmed-fields-list">
        <div class="pubmed-fields-header">
          <span>Field</span>
          <span>PubMed Value</span>
          <span>Action</span>
        </div>
        ${schemaFields.filter(f => extracted[f] != null && extracted[f] !== '').map(f => {
          const accepted = state.pubmedFieldAccepted[f];
          const statusClass = accepted === true ? 'accepted' : (accepted === false ? 'denied' : '');
          const hasMatchingCard = matchableFields.has(f);
          return `
            <div class="pubmed-field-row ${statusClass}" data-field="${f}">
              <span class="pubmed-field-name">${f}${hasMatchingCard ? ' <span class="card-match-dot" title="Has matching discrepancy card">●</span>' : ''}</span>
              <span class="pubmed-field-value">${escapeHtml(String(extracted[f]))}</span>
              <span class="pubmed-field-actions">
                ${accepted === true
                  ? `<span class="resolved-badge" style="font-size:0.65rem;padding:0.15rem 0.4rem;">✓ Accepted</span>${hasMatchingCard ? `<button class="btn btn-sm btn-primary" data-pubmed-apply="${f}" title="Apply this value to matching cards">⇒ Apply</button>` : ''}<button class="btn btn-sm" data-pubmed-field-action="undo" data-field="${f}">Undo</button>`
                  : accepted === false
                    ? `<span style="color:var(--accent-rose);font-size:0.7rem;">Denied</span><button class="btn btn-sm" data-pubmed-field-action="undo" data-field="${f}">Undo</button>`
                    : `<button class="btn btn-sm btn-success" data-pubmed-field-action="accept" data-field="${f}">✓ Accept</button><button class="btn btn-sm btn-danger" data-pubmed-field-action="deny" data-field="${f}">✕</button>`}
              </span>
            </div>
          `;
        }).join('')}
      </div>
      ${extracted._abstract ? `
        <details class="pubmed-abstract-details">
          <summary>View Abstract</summary>
          <div class="pubmed-abstract-text">${escapeHtml(extracted._abstract)}</div>
        </details>
      ` : ''}
      ${extracted._meshTerms ? `
        <details class="pubmed-abstract-details">
          <summary>MeSH Terms</summary>
          <div class="pubmed-abstract-text">${escapeHtml(extracted._meshTerms)}</div>
        </details>
      ` : ''}
      ${extracted._keywords ? `
        <details class="pubmed-abstract-details">
          <summary>Keywords</summary>
          <div class="pubmed-abstract-text">${escapeHtml(extracted._keywords)}</div>
        </details>
      ` : ''}
      <div style="padding: 0.75rem 1.25rem; display:flex; gap:0.5rem; flex-wrap:wrap;">
        <button class="btn btn-sm btn-primary" id="btn-pubmed-accept-all">✓ Accept All Remaining</button>
        <button class="btn btn-sm btn-danger" id="btn-pubmed-deny-all">✕ Deny All Remaining</button>
        <button class="btn btn-sm btn-success" id="btn-pubmed-apply-all" title="Apply all accepted fields to matching cards">⇒ Apply All Accepted</button>
      </div>
    </div>
  `;
}

function renderCard(row, viewIndex) {
  const key = getDecisionKey(state.activeStudy, row._originalIndex);
  const decision = state.decisions[key];
  const isResolved = !!decision;
  const type = row.type || '';
  const source = row.source || 'consensus';

  let headerInfo = '';
  if (row.field) {
    headerInfo = row.field;
  } else if (row.key) {
    headerInfo = row.key;
  } else {
    headerInfo = `Row ${row._originalIndex}`;
  }

  const sourceClass = source === 'consistency' ? 'consistency' : (source === 'audit' ? 'audit' : 'consensus');

  let bodyHtml = '';

  if (type === 'field_mismatch') {
    bodyHtml = renderFieldMismatch(row);
  } else if (type === 'only_in_run1' || type === 'only_in_run2') {
    bodyHtml = renderOnlyInRun(row, type);
  } else {
    bodyHtml = renderGenericRow(row);
  }

  // Show extra context fields if available
  let contextHtml = '';
  const contextFields = ['check', 'detail', 'issue', 'field_or_row', 'explanation', 'suggested_fix'];
  const hasContext = contextFields.some(f => row[f] && row[f].trim());
  if (hasContext) {
    contextHtml = `
      <div class="card-context">
        ${contextFields.filter(f => row[f] && row[f].trim()).map(f => `
          <div class="context-item">
            <span class="context-label">${f.replace(/_/g, ' ')}</span>
            <span class="context-value">${escapeHtml(row[f])}</span>
          </div>
        `).join('')}
      </div>
    `;
  }

  let actionsHtml = '';
  if (isResolved) {
    actionsHtml = `
      <div class="card-actions">
        <div class="resolved-badge">✓ ${escapeDecision(decision.decision)}${decision.correctedValue ? ': ' + escapeHtml(decision.correctedValue) : ''}</div>
        <button class="btn btn-sm btn-danger" data-action="undo" data-key="${key}">Undo</button>
      </div>
    `;
  } else {
    actionsHtml = `
      <div class="card-actions">
        <span class="decision-label">Decision:</span>
        ${row.run1 != null && row.run1 !== '' ? `<button class="btn btn-sm btn-success" data-action="accept" data-key="${key}" data-which="run1">✓ Run 1</button>` : ''}
        ${row.run2 != null && row.run2 !== '' ? `<button class="btn btn-sm btn-success" data-action="accept" data-key="${key}" data-which="run2">✓ Run 2</button>` : ''}
        ${type === 'only_in_run1' ? `<button class="btn btn-sm btn-success" data-action="accept" data-key="${key}" data-which="include">✓ Include</button><button class="btn btn-sm btn-danger" data-action="accept" data-key="${key}" data-which="exclude">✕ Exclude</button>` : ''}
        ${type === 'only_in_run2' ? `<button class="btn btn-sm btn-success" data-action="accept" data-key="${key}" data-which="include">✓ Include</button><button class="btn btn-sm btn-danger" data-action="accept" data-key="${key}" data-which="exclude">✕ Exclude</button>` : ''}
        <button class="btn btn-sm" data-action="accept" data-key="${key}" data-which="skip">Skip</button>
        <input class="correction-input" data-key="${key}" placeholder="Or type corrected value..." />
        <button class="btn btn-sm btn-primary" data-action="correct" data-key="${key}">Apply</button>
      </div>
    `;
  }

  return `
    <div class="review-card ${isResolved ? 'resolved' : ''}" id="card-${row._originalIndex}">
      <div class="card-header">
        <span class="type-badge ${sourceClass}">${source}</span>
        <span class="sheet-label">${escapeHtml(row.sheet || '')}</span>
        <span class="field-name">${escapeHtml(headerInfo)}</span>
        <span class="issue-type">${escapeHtml(type || 'discrepancy')}</span>
      </div>
      <div class="card-body">
        ${bodyHtml}
        ${contextHtml}
        ${actionsHtml}
      </div>
    </div>
  `;
}

function escapeDecision(d) {
  if (d === 'run1') return 'Accepted Run 1';
  if (d === 'run2') return 'Accepted Run 2';
  if (d === 'skip') return 'Skipped';
  if (d === 'corrected') return 'Corrected';
  if (d === 'include') return 'Include';
  if (d === 'exclude') return 'Exclude';
  return d;
}

function renderFieldMismatch(row) {
  // Field-level mismatch with run1/run2 values
  if (row.run1 || row.run2) {
    return `
      <div class="value-comparison">
        <div class="value-box" data-select="run1">
          <div class="label">Run 1</div>
          <div class="value">${formatValue(row.run1)}</div>
        </div>
        <div class="value-box" data-select="run2">
          <div class="label">Run 2</div>
          <div class="value">${formatValue(row.run2)}</div>
        </div>
      </div>
    `;
  }

  // Row-level mismatch with 'fields' containing the diff details
  let fieldsHtml = '';
  if (row.fields) {
    try {
      const parsed = JSON.parse(row.fields.replace(/'/g, '"').replace(/None/g, 'null'));
      if (Array.isArray(parsed)) {
        fieldsHtml = `
          <table class="mismatch-table">
            <tr><th>Field</th><th>Run 1</th><th>Run 2</th></tr>
            ${parsed.map(f => `
              <tr>
                <td>${escapeHtml(f.field)}</td>
                <td class="highlight-diff">${formatValue(f.run1)}</td>
                <td class="highlight-diff">${formatValue(f.run2)}</td>
              </tr>
            `).join('')}
          </table>
        `;
      }
    } catch { /* fall through */ }
  }

  if (row.key) {
    return `
      <div class="row-data">
        <div class="label">Key</div>
        <pre>${formatValue(row.key)}</pre>
      </div>
      ${fieldsHtml}
    `;
  }

  return fieldsHtml || `<div class="row-data"><pre>${formatValue(JSON.stringify(row))}</pre></div>`;
}

function renderOnlyInRun(row, type) {
  const runLabel = type === 'only_in_run1' ? 'Run 1 Only' : 'Run 2 Only';

  let rowDataHtml = '';
  if (row.row) {
    rowDataHtml = `
      <div class="row-data">
        <div class="label">Data</div>
        <pre>${formatValue(row.row)}</pre>
      </div>
    `;
  }

  let keyHtml = '';
  if (row.key) {
    keyHtml = `
      <div class="row-data">
        <div class="label">Key</div>
        <pre>${formatValue(row.key)}</pre>
      </div>
    `;
  }

  return `
    <div style="margin-bottom:0.75rem;">
      <span class="type-badge" style="background:var(--accent-amber-dim);color:var(--accent-amber);">${runLabel}</span>
    </div>
    ${keyHtml}
    ${rowDataHtml}
  `;
}

function renderGenericRow(row) {
  const hasRuns = (row.run1 != null && row.run1 !== '') || (row.run2 != null && row.run2 !== '');

  if (hasRuns) {
    return `
      <div class="value-comparison">
        <div class="value-box" data-select="run1">
          <div class="label">Run 1</div>
          <div class="value">${formatValue(row.run1)}</div>
        </div>
        <div class="value-box" data-select="run2">
          <div class="label">Run 2</div>
          <div class="value">${formatValue(row.run2)}</div>
        </div>
      </div>
    `;
  }

  // Show all non-empty fields
  const ignoreKeys = ['_originalIndex', 'study_id', 'source', 'sheet', 'type', 'status', 'reviewer_decision', 'corrected_value'];
  const entries = Object.entries(row).filter(([k, v]) => !ignoreKeys.includes(k) && v != null && v !== '');

  if (entries.length === 0) return '<div class="row-data"><pre>No data</pre></div>';

  return entries.map(([k, v]) => `
    <div class="row-data">
      <div class="label">${escapeHtml(k)}</div>
      <pre>${formatValue(v)}</pre>
    </div>
  `).join('');
}

// ============================================================
// PDF VIEWER PANE
// ============================================================
function renderPdfPane() {
  const studyId = state.activeStudy;
  if (!studyId) return '';

  const pdfUrl = `/papers/${encodeURIComponent(studyId)}/original.pdf`;

  return `
    <div class="pdf-pane" id="pdf-pane">
      <div class="pdf-resize-handle" id="pdf-resize-handle"></div>
      <div class="pdf-pane-header">
        <span class="pdf-title">📄 ${studyId}/original.pdf</span>
        <div class="pdf-controls">
          <button class="pdf-mode-toggle ${!state.pdfDarkMode ? 'active' : ''}" id="btn-pdf-light" title="Light mode">☀️ Light</button>
          <button class="pdf-mode-toggle ${state.pdfDarkMode ? 'active' : ''}" id="btn-pdf-dark" title="Dark mode">🌙 Dark</button>
          <button class="btn btn-sm" id="btn-pdf-newtab" title="Open in new tab">↗</button>
          <button class="btn btn-sm btn-danger" id="btn-pdf-close" title="Close PDF">✕</button>
        </div>
      </div>
      <div class="pdf-body ${state.pdfDarkMode ? 'pdf-dark' : ''}" id="pdf-body">
        <iframe src="${pdfUrl}" title="PDF Viewer"></iframe>
      </div>
    </div>
  `;
}

function bindPdfEvents() {
  // Toggle PDF
  document.getElementById('btn-toggle-pdf')?.addEventListener('click', () => {
    state.pdfOpen = !state.pdfOpen;
    togglePdfPane();
  });

  // Close PDF
  document.getElementById('btn-pdf-close')?.addEventListener('click', () => {
    state.pdfOpen = false;
    togglePdfPane();
  });

  // Light mode
  document.getElementById('btn-pdf-light')?.addEventListener('click', () => {
    state.pdfDarkMode = false;
    const body = document.getElementById('pdf-body');
    if (body) {
      body.classList.remove('pdf-dark');
    }
    document.getElementById('btn-pdf-light')?.classList.add('active');
    document.getElementById('btn-pdf-dark')?.classList.remove('active');
  });

  // Dark mode
  document.getElementById('btn-pdf-dark')?.addEventListener('click', () => {
    state.pdfDarkMode = true;
    const body = document.getElementById('pdf-body');
    if (body) {
      body.classList.add('pdf-dark');
    }
    document.getElementById('btn-pdf-dark')?.classList.add('active');
    document.getElementById('btn-pdf-light')?.classList.remove('active');
  });

  // Open in new tab
  document.getElementById('btn-pdf-newtab')?.addEventListener('click', () => {
    if (state.activeStudy) {
      window.open(`/papers/${encodeURIComponent(state.activeStudy)}/original.pdf`, '_blank');
    }
  });

  // Resize handle
  const handle = document.getElementById('pdf-resize-handle');
  if (handle) {
    let startX, startWidth;
    handle.addEventListener('mousedown', (e) => {
      e.preventDefault();
      const pdfPane = document.getElementById('pdf-pane');
      startX = e.clientX;
      startWidth = pdfPane.offsetWidth;
      handle.classList.add('dragging');

      const onMouseMove = (e) => {
        const diff = startX - e.clientX;
        const newWidth = Math.max(300, Math.min(startWidth + diff, window.innerWidth * 0.6));
        pdfPane.style.width = newWidth + 'px';
        // Update grid
        const layout = document.querySelector('.app-layout');
        if (layout) {
          layout.style.gridTemplateColumns = `300px 1fr ${newWidth}px`;
        }
      };

      const onMouseUp = () => {
        handle.classList.remove('dragging');
        document.removeEventListener('mousemove', onMouseMove);
        document.removeEventListener('mouseup', onMouseUp);
      };

      document.addEventListener('mousemove', onMouseMove);
      document.addEventListener('mouseup', onMouseUp);
    });
  }
}

// ============================================================
// BIND EVENTS
// ============================================================
function bindAppEvents() {
  // Search
  const searchInput = document.getElementById('search-input');
  if (searchInput) {
    searchInput.addEventListener('input', (e) => {
      state.searchQuery = e.target.value;
      const list = document.getElementById('study-list');
      if (list) list.innerHTML = renderStudyList();
      bindStudyListEvents();
    });
  }

  // Toggle sidebar (mobile)
  const toggleBtn = document.getElementById('toggle-sidebar');
  if (toggleBtn) {
    toggleBtn.addEventListener('click', () => {
      document.getElementById('sidebar').classList.toggle('open');
    });
  }

  // Study list
  bindStudyListEvents();

  // Filter chips (source, status, type)
  document.querySelectorAll('.chip[data-filter]').forEach(chip => {
    chip.addEventListener('click', () => {
      const filterName = chip.dataset.filter;
      const filterValue = chip.dataset.value;
      if (filterName === 'status') state.filterStatus = filterValue;
      if (filterName === 'source') state.filterSource = filterValue;
      if (filterName === 'type') state.filterType = filterValue;
      // Update chip active states in-place instead of full re-render
      document.querySelectorAll(`.chip[data-filter="${filterName}"]`).forEach(c => {
        c.classList.toggle('active', c.dataset.value === filterValue);
      });
      renderMainContentOnly();
    });
  });

  // Sheet tabs
  document.querySelectorAll('.sheet-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      state.activeSheet = tab.dataset.sheet;
      renderMainContentOnly();
    });
  });

  // Card actions (delegated)
  const mainContent = document.getElementById('main-content');
  if (mainContent) {
    mainContent.addEventListener('click', handleCardAction);
    mainContent.addEventListener('click', handleValueBoxClick);
  }

  // Push / Pull
  document.getElementById('btn-push')?.addEventListener('click', handlePush);
  document.getElementById('btn-pull')?.addEventListener('click', handlePull);

  // PubMed
  document.getElementById('btn-pubmed')?.addEventListener('click', handlePubmedLookup);
  document.getElementById('btn-close-pubmed')?.addEventListener('click', () => {
    state.pubmedPanelOpen = false;
    renderMainContentOnly();
  });

  // Clear filters
  document.getElementById('btn-clear-filters')?.addEventListener('click', () => {
    state.filterSource = 'all';
    state.filterType = 'all';
    state.filterStatus = 'all';
    document.querySelectorAll('.chip[data-filter]').forEach(c => {
      c.classList.toggle('active', c.dataset.value === 'all');
    });
    renderMainContentOnly();
  });

  // Bulk actions
  document.querySelectorAll('[data-bulk]').forEach(btn => {
    btn.addEventListener('click', () => handleBulkAction(btn.dataset.bulk));
  });

  // PDF viewer
  bindPdfEvents();

  // Keyboard shortcuts
  document.removeEventListener('keydown', handleKeyboard);
  document.addEventListener('keydown', handleKeyboard);
}

function bindStudyListEvents() {
  document.querySelectorAll('.study-item').forEach(item => {
    item.addEventListener('click', () => {
      // Update active state in sidebar without re-rendering
      document.querySelectorAll('.study-item').forEach(s => s.classList.remove('active'));
      item.classList.add('active');

      state.activeStudy = item.dataset.study;
      state.activeSheet = 'all';
      state.pubmedPanelOpen = false;
      state.pubmedSelected = null;
      state.pubmedExtracted = null;

      // Only re-render main content + update PDF pane if open
      renderMainContentOnly();
      updateStatsInPlace();

      // Update PDF if open
      if (state.pdfOpen) {
        const pdfPane = document.getElementById('pdf-pane');
        if (pdfPane) {
          const temp = document.createElement('div');
          temp.innerHTML = renderPdfPane();
          const newPane = temp.firstElementChild;
          if (newPane) pdfPane.replaceWith(newPane);
          bindPdfEvents();
        }
      }
    });
  });
}

function handleValueBoxClick(e) {
  const box = e.target.closest('.value-box[data-select]');
  if (box) {
    const parent = box.closest('.value-comparison');
    parent.querySelectorAll('.value-box').forEach(b => b.classList.remove('selected'));
    box.classList.add('selected');
  }
}

function handleCardAction(e) {
  const btn = e.target.closest('[data-action]');
  if (!btn) return;

  const action = btn.dataset.action;
  const key = btn.dataset.key;

  if (action === 'accept') {
    const which = btn.dataset.which;
    state.decisions[key] = { decision: which, correctedValue: null, timestamp: new Date().toISOString() };
    saveDecisions();
    showToast(`Decision saved: ${escapeDecision(which)}`);
    updateCardInPlace(key);
    updateStatsInPlace();
  }

  if (action === 'correct') {
    const input = e.target.closest('.card-actions')?.querySelector('.correction-input');
    const val = input?.value?.trim();
    if (val) {
      state.decisions[key] = { decision: 'corrected', correctedValue: val, timestamp: new Date().toISOString() };
      saveDecisions();
      showToast('Corrected value saved');
      updateCardInPlace(key);
      updateStatsInPlace();
    }
  }

  if (action === 'undo') {
    delete state.decisions[key];
    saveDecisions();
    showToast('Decision undone');
    updateCardInPlace(key);
    updateStatsInPlace();
  }
}

// ---- Targeted DOM updates (no full re-render) ----
function updateCardInPlace(key) {
  // Parse the original index from the key
  const [studyId, origIdxStr] = key.split('::');
  const origIdx = parseInt(origIdxStr);
  const cardEl = document.getElementById(`card-${origIdx}`);
  if (!cardEl) return;

  // Find the row data
  const row = state.rawData[origIdx];
  if (!row) return;

  // Re-render just this card
  const tempDiv = document.createElement('div');
  tempDiv.innerHTML = renderCard(row, 0);
  const newCard = tempDiv.firstElementChild;

  // Replace the old card with the new one — no scroll reset
  cardEl.replaceWith(newCard);
}

function updateStatsInPlace() {
  const globalStats = getGlobalStats();
  const progressPct = globalStats.total > 0 ? Math.round(globalStats.resolved / globalStats.total * 100) : 0;

  // Update top-bar stat pills
  const statPills = document.querySelectorAll('.stat-pill');
  if (statPills.length >= 3) {
    statPills[0].innerHTML = `<span class="dot total"></span>${globalStats.total} items`;
    statPills[1].innerHTML = `<span class="dot done"></span>${globalStats.resolved} reviewed`;
    statPills[2].innerHTML = `<span class="dot pending"></span>${globalStats.pending} pending`;
  }

  // Update progress bar
  const progressFill = document.querySelector('.progress-fill');
  if (progressFill) progressFill.style.width = `${progressPct}%`;
  const progressText = document.querySelector('.progress-text');
  if (progressText) progressText.textContent = `${progressPct}% complete · ${globalStats.resolved}/${globalStats.total}`;

  // Update study header meta
  if (state.activeStudy) {
    const studyStats = getStudyStats(state.activeStudy);
    const metaEl = document.querySelector('.study-header .meta');
    if (metaEl) {
      const allRows = state.studyMap[state.activeStudy] || [];
      const sheets = getSheets(allRows);
      const sources = getSources(allRows);
      metaEl.textContent = `${studyStats.resolved} of ${studyStats.total} items reviewed · ${sheets.length} sheets · sources: ${sources.join(', ')}`;
    }
  }

  // Update sidebar study count for active study
  const activeItem = document.querySelector(`.study-item[data-study="${state.activeStudy}"]`);
  if (activeItem) {
    const stats = getStudyStats(state.activeStudy);
    const countEl = activeItem.querySelector('.count');
    if (countEl) {
      countEl.textContent = `${stats.resolved}/${stats.total}`;
      countEl.className = 'count ' + (stats.pending === 0 ? 'all-done' : (stats.resolved > 0 ? 'has-pending' : ''));
    }
  }
}

async function handlePubmedLookup() {
  state.pubmedPanelOpen = true;
  state.pubmedLoading = true;
  state.pubmedShowAll = false;
  state.pubmedSelected = null;
  state.pubmedExtracted = null;
  state.pubmedFieldAccepted = {};
  // First time opening: need full content render to show the panel
  renderMainContentOnly();

  await fetchPubMed(state.activeStudy);

  state.pubmedLoading = false;
  // After fetch: only swap the pubmed panel, not the cards
  renderPubmedPanelOnly();
}

function bindPubmedPanelEvents() {
  // Close
  document.getElementById('btn-close-pubmed')?.addEventListener('click', () => {
    state.pubmedPanelOpen = false;
    state.pubmedSelected = null;
    state.pubmedExtracted = null;
    state.pubmedFieldAccepted = {};
    // Remove the panel DOM directly
    const panel = document.querySelector('.pubmed-panel');
    if (panel) panel.remove();
  });

  // Show more
  document.getElementById('btn-pubmed-show-more')?.addEventListener('click', () => {
    state.pubmedShowAll = true;
    renderPubmedPanelOnly();
  });

  // "THIS ONE" buttons
  document.querySelectorAll('.pubmed-select-btn').forEach(btn => {
    btn.addEventListener('click', async (e) => {
      e.stopPropagation();
      const pmid = btn.dataset.pmid;
      const pubmedData = state.pubmedCache[state.activeStudy];
      const article = (pubmedData?.articles || []).find(a => a.pmid === pmid);
      if (!article) return;

      // Show loading state
      btn.textContent = '\u23f3 Extracting...';
      btn.disabled = true;

      // Fetch abstract + details
      const detail = await fetchPubMedDetail(pmid);

      // Extract fields
      const extracted = extractFieldsFromPubMed(article, detail);

      state.pubmedSelected = pmid;
      state.pubmedExtracted = extracted;
      state.pubmedFieldAccepted = {};
      renderPubmedPanelOnly();
    });
  });

  // Back to results
  document.getElementById('btn-pubmed-back')?.addEventListener('click', () => {
    state.pubmedSelected = null;
    state.pubmedExtracted = null;
    state.pubmedFieldAccepted = {};
    renderPubmedPanelOnly();
  });

  // Field accept/deny/undo
  document.querySelectorAll('[data-pubmed-field-action]').forEach(btn => {
    btn.addEventListener('click', () => {
      const action = btn.dataset.pubmedFieldAction;
      const field = btn.dataset.field;
      if (action === 'accept') {
        state.pubmedFieldAccepted[field] = true;
        showToast(`Accepted: ${field} = ${state.pubmedExtracted[field]}`);
      } else if (action === 'deny') {
        state.pubmedFieldAccepted[field] = false;
      } else if (action === 'undo') {
        delete state.pubmedFieldAccepted[field];
      }
      renderPubmedPanelOnly();
    });
  });

  // Accept all remaining
  document.getElementById('btn-pubmed-accept-all')?.addEventListener('click', () => {
    const schemaFields = ['First_Author', 'Year', 'Journal', 'Country', 'DOI', 'PMID', 'Total_N', 'Design', 'Title'];
    let count = 0;
    schemaFields.forEach(f => {
      if (state.pubmedExtracted[f] != null && state.pubmedExtracted[f] !== '' && state.pubmedFieldAccepted[f] == null) {
        state.pubmedFieldAccepted[f] = true;
        count++;
      }
    });
    if (count > 0) showToast(`Accepted ${count} fields`);
    renderPubmedPanelOnly();
  });

  // Deny all remaining
  document.getElementById('btn-pubmed-deny-all')?.addEventListener('click', () => {
    const schemaFields = ['First_Author', 'Year', 'Journal', 'Country', 'DOI', 'PMID', 'Total_N', 'Design', 'Title'];
    schemaFields.forEach(f => {
      if (state.pubmedExtracted[f] != null && state.pubmedExtracted[f] !== '' && state.pubmedFieldAccepted[f] == null) {
        state.pubmedFieldAccepted[f] = false;
      }
    });
    renderPubmedPanelOnly();
  });

  // Apply individual PubMed field to cards
  document.querySelectorAll('[data-pubmed-apply]').forEach(btn => {
    btn.addEventListener('click', () => {
      const field = btn.dataset.pubmedApply;
      handleApplyPubmedField(field);
    });
  });

  // Apply all accepted PubMed fields
  document.getElementById('btn-pubmed-apply-all')?.addEventListener('click', () => {
    handleApplyAllPubmedFields();
  });

  // Retry
  document.getElementById('btn-pubmed-retry')?.addEventListener('click', () => {
    delete state.pubmedCache[state.activeStudy];
    savePubmedCache();
    handlePubmedLookup();
  });
}

function handleBulkAction(action) {
  if (!state.activeStudy) return;
  const rows = state.studyMap[state.activeStudy] || [];
  let count = 0;

  rows.forEach(row => {
    const key = getDecisionKey(state.activeStudy, row._originalIndex);
    if (state.decisions[key]) return; // already decided

    const type = row.type || '';
    let decision = null;

    switch (action) {
      case 'accept-matching':
        if (row.run1 && row.run2 && row.run1.trim() === row.run2.trim()) {
          decision = { decision: 'run1', correctedValue: null };
        }
        break;
      case 'accept-run1-all':
        if (type === 'field_mismatch' && row.run1 != null && row.run1 !== '') {
          decision = { decision: 'run1', correctedValue: null };
        }
        break;
      case 'accept-run2-all':
        if (type === 'field_mismatch' && row.run2 != null && row.run2 !== '') {
          decision = { decision: 'run2', correctedValue: null };
        }
        break;
      case 'include-run1-only':
        if (type === 'only_in_run1') {
          decision = { decision: 'include', correctedValue: null };
        }
        break;
      case 'exclude-run1-only':
        if (type === 'only_in_run1') {
          decision = { decision: 'exclude', correctedValue: null };
        }
        break;
      case 'include-run2-only':
        if (type === 'only_in_run2') {
          decision = { decision: 'include', correctedValue: null };
        }
        break;
      case 'exclude-run2-only':
        if (type === 'only_in_run2') {
          decision = { decision: 'exclude', correctedValue: null };
        }
        break;
      case 'skip-all':
        decision = { decision: 'skip', correctedValue: null };
        break;
    }

    if (decision) {
      state.decisions[key] = { ...decision, timestamp: new Date().toISOString(), auto: true };
      count++;
    }
  });

  if (count > 0) {
    saveDecisions();
    const labels = {
      'accept-matching': 'Accepted matching',
      'accept-run1-all': 'Accepted Run 1 for',
      'accept-run2-all': 'Accepted Run 2 for',
      'include-run1-only': 'Included Run1-Only',
      'exclude-run1-only': 'Excluded Run1-Only',
      'include-run2-only': 'Included Run2-Only',
      'exclude-run2-only': 'Excluded Run2-Only',
      'skip-all': 'Skipped',
    };
    showToast(`${labels[action] || action} ${count} items`);
    renderMainContentOnly();
    updateStatsInPlace();
  } else {
    showToast('No pending items match this action');
  }
}

// Apply a single PubMed extracted field to matching discrepancy cards
function handleApplyPubmedField(fieldName) {
  if (!state.pubmedExtracted || !state.activeStudy) return;
  const value = state.pubmedExtracted[fieldName];
  if (value == null || value === '') return;

  const rows = state.studyMap[state.activeStudy] || [];
  let count = 0;

  rows.forEach(row => {
    if (row.sheet === 'study' && row.field === fieldName) {
      const key = getDecisionKey(state.activeStudy, row._originalIndex);
      state.decisions[key] = {
        decision: 'corrected',
        correctedValue: String(value),
        timestamp: new Date().toISOString(),
        source: 'pubmed',
      };
      count++;
      updateCardInPlace(key);
    }
  });

  if (count > 0) {
    saveDecisions();
    showToast(`Applied PubMed ${fieldName} = "${value}" to ${count} card(s)`);
    updateStatsInPlace();
  } else {
    showToast(`No matching card found for ${fieldName}`);
  }
}

// Apply all accepted PubMed fields to matching cards
function handleApplyAllPubmedFields() {
  if (!state.pubmedExtracted || !state.activeStudy) return;
  const schemaFields = ['First_Author', 'Year', 'Journal', 'Country', 'DOI', 'PMID', 'Total_N', 'Design', 'Title'];
  let totalApplied = 0;

  schemaFields.forEach(f => {
    if (state.pubmedFieldAccepted[f] === true && state.pubmedExtracted[f] != null && state.pubmedExtracted[f] !== '') {
      const value = state.pubmedExtracted[f];
      const rows = state.studyMap[state.activeStudy] || [];
      rows.forEach(row => {
        if (row.sheet === 'study' && row.field === f) {
          const key = getDecisionKey(state.activeStudy, row._originalIndex);
          state.decisions[key] = {
            decision: 'corrected',
            correctedValue: String(value),
            timestamp: new Date().toISOString(),
            source: 'pubmed',
          };
          totalApplied++;
          updateCardInPlace(key);
        }
      });
    }
  });

  if (totalApplied > 0) {
    saveDecisions();
    showToast(`Applied ${totalApplied} PubMed value(s) to cards`);
    updateStatsInPlace();
  } else {
    showToast('No accepted fields with matching cards to apply');
  }
}

function renderMainContentOnly() {
  const mc = document.getElementById('main-content');
  if (mc) {
    // Preserve scroll position
    const scrollTop = mc.scrollTop;
    mc.innerHTML = renderMainContent();
    mc.scrollTop = scrollTop;
    // Rebind sheet tabs
    document.querySelectorAll('.sheet-tab').forEach(tab => {
      tab.addEventListener('click', () => {
        state.activeSheet = tab.dataset.sheet;
        renderMainContentOnly();
      });
    });
    // Rebind card actions & value boxes
    mc.removeEventListener('click', handleCardAction);
    mc.addEventListener('click', handleCardAction);
    mc.removeEventListener('click', handleValueBoxClick);
    mc.addEventListener('click', handleValueBoxClick);
    // PubMed panel events
    document.getElementById('btn-pubmed')?.addEventListener('click', handlePubmedLookup);
    bindPubmedPanelEvents();
    // Other buttons
    document.getElementById('btn-clear-filters')?.addEventListener('click', () => {
      state.filterSource = 'all';
      state.filterType = 'all';
      state.filterStatus = 'all';
      renderMainContentOnly();
      updateStatsInPlace();
      // Update filter chip states in sidebar
      document.querySelectorAll('.chip[data-filter]').forEach(c => {
        const fn = c.dataset.filter;
        const fv = c.dataset.value;
        const isActive = (fn === 'status' && state.filterStatus === fv)
          || (fn === 'source' && state.filterSource === fv)
          || (fn === 'type' && state.filterType === fv);
        c.classList.toggle('active', isActive);
      });
    });
    // Bulk action buttons (delegated)
    document.querySelectorAll('[data-bulk]').forEach(btn => {
      btn.addEventListener('click', () => handleBulkAction(btn.dataset.bulk));
    });
    // PDF toggle
    document.getElementById('btn-toggle-pdf')?.addEventListener('click', () => {
      state.pdfOpen = !state.pdfOpen;
      togglePdfPane();
    });
  }
}

// Targeted PubMed panel update — only swaps the pubmed panel DOM, no card flashing
function renderPubmedPanelOnly() {
  const pubmedData = state.pubmedCache[state.activeStudy];
  const newHtml = renderPubmedPanel(pubmedData);

  // Find existing panel
  const existingPanel = document.querySelector('.pubmed-panel');
  if (existingPanel) {
    const temp = document.createElement('div');
    temp.innerHTML = newHtml;
    const newPanel = temp.firstElementChild;
    if (newPanel) {
      existingPanel.replaceWith(newPanel);
    } else {
      existingPanel.remove();
    }
  } else if (newHtml) {
    // Insert panel after study-header
    const studyHeader = document.querySelector('.study-header');
    if (studyHeader) {
      const temp = document.createElement('div');
      temp.innerHTML = newHtml;
      const newPanel = temp.firstElementChild;
      if (newPanel) studyHeader.after(newPanel);
    }
  }

  // Rebind PubMed events on the new panel
  bindPubmedPanelEvents();
}

// Toggle PDF pane without re-rendering everything
function togglePdfPane() {
  const layout = document.querySelector('.app-layout');
  if (!layout) { renderApp(); return; }

  if (state.pdfOpen) {
    layout.classList.add('pdf-open');
    // Add PDF pane if not exists
    if (!document.getElementById('pdf-pane')) {
      const temp = document.createElement('div');
      temp.innerHTML = renderPdfPane();
      const pdfPane = temp.firstElementChild;
      if (pdfPane) layout.appendChild(pdfPane);
      bindPdfEvents();
    }
    // Update the button text
    const btn = document.getElementById('btn-toggle-pdf');
    if (btn) btn.innerHTML = '\ud83d\udcc4 Hide PDF';
  } else {
    layout.classList.remove('pdf-open');
    const pdfPane = document.getElementById('pdf-pane');
    if (pdfPane) pdfPane.remove();
    layout.style.gridTemplateColumns = '';
    const btn = document.getElementById('btn-toggle-pdf');
    if (btn) btn.innerHTML = '\ud83d\udcc4 View PDF';
  }
}

// ============================================================
// KEYBOARD SHORTCUTS
// ============================================================
function handleKeyboard(e) {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

  const navigateStudy = (newStudy) => {
    // Update sidebar
    document.querySelectorAll('.study-item').forEach(s => s.classList.remove('active'));
    document.querySelector(`.study-item[data-study="${newStudy}"]`)?.classList.add('active');

    state.activeStudy = newStudy;
    state.activeSheet = 'all';
    state.pubmedPanelOpen = false;
    state.pubmedSelected = null;
    state.pubmedExtracted = null;
    renderMainContentOnly();
    updateStatsInPlace();

    if (state.pdfOpen) {
      const pdfPane = document.getElementById('pdf-pane');
      if (pdfPane) {
        const temp = document.createElement('div');
        temp.innerHTML = renderPdfPane();
        const newPane = temp.firstElementChild;
        if (newPane) pdfPane.replaceWith(newPane);
        bindPdfEvents();
      }
    }

    // Scroll sidebar item into view
    document.querySelector(`.study-item[data-study="${newStudy}"]`)?.scrollIntoView({ block: 'nearest' });
  };

  if (e.key === 'ArrowDown' && e.ctrlKey) {
    e.preventDefault();
    const idx = state.studies.indexOf(state.activeStudy);
    if (idx < state.studies.length - 1) navigateStudy(state.studies[idx + 1]);
  }

  if (e.key === 'ArrowUp' && e.ctrlKey) {
    e.preventDefault();
    const idx = state.studies.indexOf(state.activeStudy);
    if (idx > 0) navigateStudy(state.studies[idx - 1]);
  }
}

// ============================================================
// PUSH / PULL
// ============================================================
const isLocal = location.hostname === 'localhost' || location.hostname === '127.0.0.1';

async function handlePush() {
  const btn = document.getElementById('btn-push');
  if (btn) { btn.textContent = '⏳ Pushing...'; btn.disabled = true; }

  try {
    // Build merged CSV with decisions
    const rows = state.rawData.map((row, idx) => {
      const result = { ...row };
      for (const [key, decision] of Object.entries(state.decisions)) {
        const [, origIdx] = key.split('::');
        if (parseInt(origIdx) === idx) {
          result.reviewer_decision = decision.decision;
          result.corrected_value = decision.correctedValue || '';
          break;
        }
      }
      delete result._originalIndex;
      return result;
    });

    const csv = Papa.unparse(rows);

    if (isLocal) {
      // Local dev: push to server API
      const res = await fetch('/api/push', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ csv }),
      });
      const data = await res.json();
      if (data.ok) {
        showToast(`✓ Pushed ${Object.keys(state.decisions).length} decisions to review-queue.csv`);
      } else {
        showToast(`Push failed: ${data.error}`, true);
      }
    } else {
      // Vercel/remote: download as CSV file
      const blob = new Blob([csv], { type: 'text/csv' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'review-queue-reviewed.csv';
      a.click();
      URL.revokeObjectURL(url);
      showToast(`⬇ Downloaded reviewed CSV (${Object.keys(state.decisions).length} decisions)`);
    }
  } catch (err) {
    showToast(`Push error: ${err.message}`, true);
  } finally {
    if (btn) { btn.textContent = '⬆ Push'; btn.disabled = false; }
  }
}

async function handlePull() {
  const btn = document.getElementById('btn-pull');
  if (btn) { btn.textContent = '⏳ Pulling...'; btn.disabled = true; }

  try {
    let csvText = null;

    if (isLocal) {
      // Local dev: pull from server API
      const res = await fetch('/api/pull');
      const data = await res.json();
      if (data.ok && data.csv) {
        csvText = data.csv;
      } else {
        showToast(`Pull failed: ${data.error}`, true);
        return;
      }
    } else {
      // Vercel/remote: reload from static CSV
      const res = await fetch('/data/review-queue.csv');
      if (res.ok) {
        csvText = await res.text();
      } else {
        showToast('Could not fetch data file', true);
        return;
      }
    }

    if (csvText) {
      parseCSV(csvText);

      // Restore existing decisions from the CSV's reviewer_decision column
      let imported = 0;
      state.rawData.forEach((row, idx) => {
        if (row.reviewer_decision && row.reviewer_decision.trim()) {
          const key = getDecisionKey(row.study_id, idx);
          if (!state.decisions[key]) {
            state.decisions[key] = {
              decision: row.reviewer_decision.trim(),
              correctedValue: row.corrected_value || null,
              timestamp: new Date().toISOString(),
              imported: true,
            };
            imported++;
          }
        }
      });
      if (imported > 0) saveDecisions();

      if (state.studies.length > 0 && !state.studies.includes(state.activeStudy)) {
        state.activeStudy = state.studies[0];
      }
      renderApp();
      showToast(`✓ Pulled ${state.rawData.length} rows${imported > 0 ? `, imported ${imported} decisions` : ''}`);
    }
  } catch (err) {
    showToast(`Pull error: ${err.message}`, true);
  } finally {
    if (btn) { btn.textContent = '⬇ Pull'; btn.disabled = false; }
  }
}

// ============================================================
// AUTO-LOAD
// ============================================================
async function tryAutoLoad() {
  try {
    const response = await fetch('/data/review-queue.csv');
    if (response.ok) {
      const text = await response.text();
      parseCSV(text);
      if (state.studies.length > 0) {
        state.activeStudy = state.studies[0];
        renderApp();
        return true;
      }
    }
  } catch { /* not available */ }
  return false;
}

// ============================================================
// INIT
// ============================================================
loadDecisions();

tryAutoLoad().then(loaded => {
  if (!loaded) {
    renderUploadScreen();
  }
});
