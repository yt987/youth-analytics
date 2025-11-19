const state = {
  sort: "country",
  order: "asc",
  page: 1,
  per_page: 50,
};

function byId(id) { return document.getElementById(id); }
function qs(sel) { return document.querySelector(sel); }

function getMultiSelectValues(selectEl) {
  return Array.from(selectEl.selectedOptions).map(o => o.value);
}

function buildQuery() {
  const params = new URLSearchParams();
  const regions = getMultiSelectValues(byId("region"));
  const incomes = getMultiSelectValues(byId("income"));
  const profiles = getMultiSelectValues(byId("profile"));
  if (regions.length) params.set("region", regions.join(","));
  if (incomes.length) params.set("income_group", incomes.join(","));
  if (profiles.length) params.set("profile", profiles.join(","));
  ["min_literacy","min_primary","min_secondary","min_spend"].forEach(id => {
    const v = byId(id).value.trim();
    if (v !== "") params.set(id, v);
  });
  params.set("sort", state.sort);
  params.set("order", state.order);
  params.set("page", state.page);
  params.set("per_page", state.per_page);
  return params.toString();
}

function fmt(v) {
  if (v === null || v === undefined || v === "") return "";
  const n = Number(v);
  if (Number.isNaN(n)) return v;
  return n.toFixed(1);
}

function showNotice(msg) {
  let box = qs(".notice");
  if (!box) {
    box = document.createElement("div");
    box.className = "notice";
    qs(".container:last-of-type")?.prepend(box);
    if (!box.parentElement) document.body.prepend(box);
  }
  box.textContent = msg;
}
function clearNotice() {
  const box = qs(".notice");
  if (box) box.remove();
}

async function fetchMeta() {
  const r = await fetch("/api/meta");
  if (!r.ok) throw new Error("Failed to load metadata");
  const meta = await r.json();
  byId("latest-year").textContent = meta.last_year ?? "—";
  const region = byId("region");
  const income = byId("income");
  const profile = byId("profile");
  function fill(select, arr) {
    select.innerHTML = "";
    arr.forEach(v => {
      const opt = document.createElement("option");
      opt.value = v; opt.textContent = v; select.appendChild(opt);
    });
  }
  fill(region, meta.regions);
  fill(income, meta.income_groups);
  fill(profile, meta.profiles);
}

async function fetchStatsAndRender() {
  const r = await fetch(`/api/stats?${buildQuery()}`);
  if (!r.ok) throw new Error("Failed to load stats");
  const data = await r.json();
  byId("kpi-literacy").textContent = data.avg.literacy ?? "—";
  byId("kpi-primary").textContent = data.avg.primary ?? "—";
  byId("kpi-secondary").textContent = data.avg.secondary ?? "—";
  byId("kpi-spend").textContent = data.avg.spend ?? "—";
  byId("kpi-count").textContent = data.counts.countries;
}

async function fetchCountriesAndRender() {
  const r = await fetch(`/api/countries?${buildQuery()}`);
  if (!r.ok) throw new Error("Failed to load countries");
  const data = await r.json();
  const tbody = qs("#countries tbody");
  tbody.innerHTML = "";
  if (!data.results.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 9;
    td.textContent = "No results. Adjust filters or reset.";
    tr.appendChild(td);
    tbody.appendChild(tr);
  } else {
    data.results.forEach(row => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${row.country}</td>
        <td>${fmt(row.youth_literacy_rate)}</td>
        <td>${fmt(row.primary_enrollment_rate)}</td>
        <td>${fmt(row.secondary_enrollment_rate)}</td>
        <td>${fmt(row.gov_education_spending_pct_gdp)}</td>
        <td>${row.latest_year ?? ""}</td>
        <td>${row.region}</td>
        <td>${row.income_group}</td>
        <td>${row.education_profile}</td>
      `;
      tbody.appendChild(tr);
    });
  }
  byId("page-info").textContent = `Page ${data.page} of ${data.pages}`;
  byId("prev").disabled = data.page <= 1;
  byId("next").disabled = data.page >= data.pages;
}

function debounce(fn, ms) {
  let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

function attachHandlers() {
  ["region","income","profile","min_literacy","min_primary","min_secondary","min_spend"].forEach(id => {
    const el = byId(id);
    el.addEventListener("change", () => { state.page = 1; refresh(); });
    if (el.tagName === "INPUT") el.addEventListener("input", debounce(() => { state.page = 1; refresh(); }, 250));
  });

  document.querySelectorAll("#countries thead th[data-sort]").forEach(th => {
    th.addEventListener("click", () => {
      const key = th.getAttribute("data-sort");
      if (state.sort === key) {
        state.order = state.order === "asc" ? "desc" : "asc";
      } else {
        state.sort = key; state.order = "asc";
      }
      refresh();
    });
  });

  byId("prev").addEventListener("click", () => {
    if (state.page > 1) { state.page--; refresh(); }
  });
  byId("next").addEventListener("click", () => {
    state.page++; refresh();
  });

  const resetBtn = byId("reset");
  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      ["region","income","profile"].forEach(id => {
        const el = byId(id);
        Array.from(el.options).forEach(o => o.selected = false);
      });
      ["min_literacy","min_primary","min_secondary","min_spend"].forEach(id => byId(id).value = "");
      state.sort = "country"; state.order = "asc"; state.page = 1;
      refresh();
    });
  }
}

async function refresh() {
  try {
    await Promise.all([
      fetchStatsAndRender(),
      fetchCountriesAndRender(),
      fetchInsightsAndRender(),
    ]);
  } catch (e) {
    showNotice(String(e.message || e));
  }
}

(async function init() {
  try {
    await fetchMeta();
    attachHandlers();
    await refresh();
  } catch (e) {
    showNotice(String(e.message || e));
  }
})();

async function fetchInsightsAndRender() {
  const r = await fetch(`/api/insights/live?${buildQuery()}`);
  if (!r.ok) return;
  const data = await r.json();
  const top = document.getElementById("top-yls");
  const imp = document.getElementById("top-improvers");
  if (!top || !imp) return;

  const fill = (el, rows, key, suffix = "") => {
    el.innerHTML = rows.length ? "" : "<li>No results</li>";
    rows.forEach(row => {
      const li = document.createElement("li");
      li.textContent = `${row.country} — ${row[key] ?? ""}${suffix}`;
      el.appendChild(li);
    });
  };

  fill(top, data.top_yls || [], "yls_score");
  fill(imp, data.top_improvers_literacy_5y || [], "lit_change_5y", " pp");
}

// call once on load
fetchInsightsAndRender().catch(()=>{});