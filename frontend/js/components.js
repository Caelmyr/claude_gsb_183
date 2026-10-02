/* Shared UI components: navigation, formatting, badges, tables, polling. */
const Components = (() => {
  const PAGES = [
    { key: 'home', href: 'index.html', label: '总览 Overview' },
    { key: 'submit', href: 'submit.html', label: '作业提交 Submit' },
    { key: 'monitor', href: 'monitor.html', label: '作业监控 Monitor' },
    { key: 'nodes', href: 'nodes.html', label: '节点管理 Nodes' },
    { key: 'shards', href: 'shards.html', label: '分片管理 Shards' },
    { key: 'shuffle', href: 'shuffle.html', label: 'Shuffle 排序' },
    { key: 'logs', href: 'logs.html', label: '日志搜索 Logs' },
    { key: 'metrics', href: 'metrics.html', label: '性能指标 Metrics' },
    { key: 'fault', href: 'fault.html', label: '故障恢复 Fault' },
    { key: 'config', href: 'config.html', label: '配置管理 Config' },
    { key: 'results', href: 'results.html', label: '结果导出 Results' },
  ];

  const LABELS = {
    PENDING: '待调度 Pending', SHARDING: '分片 Sharding', MAP: 'Map', SHUFFLE: 'Shuffle',
    REDUCE: 'Reduce', SUCCEEDED: '成功 Succeeded', FAILED: '失败 Failed', CANCELLED: '已取消 Cancelled',
    ASSIGNED: '已分配 Assigned', RUNNING: '运行中 Running', RETRYING: '重试 Retrying',
    alive: '存活 Alive', dead: '失联 Dead', ready: '就绪 Ready', done: '完成 Done',
  };
  const CLASS = {
    SUCCEEDED: 'good', FAILED: 'bad', CANCELLED: 'muted', RUNNING: 'run', MAP: 'run',
    REDUCE: 'aqua', SHUFFLE: 'warn', RETRYING: 'warn', ASSIGNED: 'aqua', PENDING: 'muted',
    SHARDING: 'muted', alive: 'good', dead: 'bad', ready: 'muted', done: 'good',
  };

  // ------------------------------------------------------------------
  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function fmtNum(n) {
    if (n == null || isNaN(n)) return '-';
    return Number(n).toLocaleString('en-US');
  }

  function fmtBytes(n) {
    if (n == null || isNaN(n)) return '-';
    if (n < 1024) return n + ' B';
    if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
    if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(2) + ' MB';
    return (n / 1024 / 1024 / 1024).toFixed(2) + ' GB';
  }

  function fmtTime(ms) {
    if (!ms) return '-';
    const d = new Date(ms);
    return d.toLocaleTimeString('zh-CN', { hour12: false });
  }

  function fmtDur(ms) {
    if (ms == null || isNaN(ms)) return '-';
    if (ms < 1000) return ms + ' ms';
    if (ms < 60000) return (ms / 1000).toFixed(1) + ' s';
    return (ms / 60000).toFixed(1) + ' min';
  }

  function fmtPct(x) {
    return (x == null || isNaN(x)) ? '-' : (Number(x).toFixed(1) + '%');
  }

  // ------------------------------------------------------------------
  function stateBadge(state, withDot) {
    const cls = CLASS[state] || 'muted';
    const label = LABELS[state] || String(state);
    return `<span class="badge ${cls}${withDot ? ' badge-dot' : ''}">${esc(label)}</span>`;
  }

  function progress(pct, label) {
    const p = Math.max(0, Math.min(100, Number(pct) || 0));
    const fillCls = p >= 100 ? 'good' : (p >= 60 ? '' : '');
    return `<div class="progress-label"><span>${esc(label || '')}</span><span class="tabular">${p.toFixed(0)}%</span></div>
      <div class="progress"><div class="fill ${fillCls}" style="width:${p}%"></div></div>`;
  }

  function meter(pct, label) {
    const p = Math.max(0, Math.min(100, Number(pct) || 0));
    const cls = p >= 90 ? 'crit' : (p >= 70 ? 'high' : (p >= 50 ? 'warn' : 'ok'));
    return `<div class="flex between small"><span class="muted">${esc(label || '')}</span><span class="tabular bold">${p.toFixed(0)}%</span></div>
      <div class="meter"><div class="fill ${cls}" style="width:${p}%"></div></div>`;
  }

  function empty(msg) {
    return `<div class="empty">${esc(msg || '暂无数据 No data')}</div>`;
  }

  // ------------------------------------------------------------------
  // Cluster health panel
  // ------------------------------------------------------------------
  const HEALTH_GRADE = {
    good: { cls: 'good', label: '健康 Healthy' },
    warn: { cls: 'warn', label: '注意 Degraded' },
    critical: { cls: 'bad', label: '异常 Critical' },
    no_data: { cls: 'muted', label: '数据不足 No data' },
    unknown: { cls: 'muted', label: '等待节点 Waiting' },
  };
  const HEALTH_DIM_SHORT = {
    availability: '可用 Availability',
    heartbeat: '心跳 Heartbeat',
    load: '负载 Load',
    reliability: '可靠 Reliability',
  };

  function healthGrade(grade) {
    return HEALTH_GRADE[grade] || HEALTH_GRADE.unknown;
  }

  // Gauge ring rendered as an SVG arc; value null -> greyed "no data".
  function healthGauge(score, grade) {
    const g = healthGrade(grade);
    const val = (score == null || isNaN(score)) ? null : Math.max(0, Math.min(100, Number(score)));
    const r = 52, c = 2 * Math.PI * r;
    const pct = val == null ? 0 : val / 100;
    const colorCls = val == null ? 'muted' : g.cls;
    return `<div class="health-gauge ${colorCls}">
      <svg viewBox="0 0 120 120" width="116" height="116">
        <circle class="gauge-track" cx="60" cy="60" r="${r}"></circle>
        <circle class="gauge-fill ${colorCls}" cx="60" cy="60" r="${r}"
          stroke-dasharray="${(c * pct).toFixed(1)} ${(c * (1 - pct)).toFixed(1)}"></circle>
      </svg>
      <div class="gauge-center">
        <div class="gauge-value">${val == null ? '—' : val.toFixed(0)}</div>
        <div class="gauge-label">${esc(g.label)}</div>
      </div>
    </div>`;
  }

  function _healthWarnings(h) {
    const cov = h.coverage || {};
    const warnings = [];
    if (cov.nodes_dead > 0) {
      warnings.push(`${cov.nodes_dead} 个节点失联 ${cov.nodes_dead} node(s) dead`);
    }
    if (cov.nodes_alive > 0 && cov.load_reporting < cov.nodes_alive) {
      warnings.push(`${cov.nodes_alive - cov.load_reporting} 个节点缺少负载数据 ${cov.nodes_alive - cov.load_reporting} node(s) missing load data`);
    }
    if (cov.reliability_source === 'none') {
      warnings.push('近期无任务结果，可靠性按满分计 No task outcomes yet');
    } else if (cov.reliability_source === 'cumulative') {
      warnings.push('近期任务样本不足，采用累计计数 Window sparse, using cumulative counters');
    }
    return warnings;
  }

  // Compact panel: gauge + per-dimension bars. `detailed` adds raw values.
  function healthPanel(h, detailed) {
    if (!h) return empty('健康数据暂不可用 Health unavailable');
    const g = healthGrade(h.grade);
    const dims = h.dimensions || {};
    const order = ['availability', 'heartbeat', 'load', 'reliability'];
    const dimRows = order.map(k => {
      const d = dims[k];
      if (!d) return '';
      const raw = d.raw || {};
      const score = d.score;
      const noData = score == null || d.status === 'no_data';
      const cls = noData ? 'muted' : (score >= 85 ? 'good' : score >= 60 ? 'warn' : 'bad');
      let detail = '';
      if (k === 'availability') {
        detail = `${raw.alive != null ? raw.alive : '-'}/${raw.total != null ? raw.total : '-'} 存活 alive`;
      } else if (k === 'heartbeat') {
        detail = noData ? '无心跳数据' : `年龄 age ${raw.age_ratio != null ? (raw.age_ratio * 100).toFixed(0) + '% 超时阈值' : '-'}`;
      } else if (k === 'load') {
        detail = noData ? '无负载数据' : `饱和度 saturation ${raw.min != null ? 'min ' + raw.min : ''}`;
      } else if (k === 'reliability') {
        detail = noData ? '无任务数据' : `失败率 fail ${(raw.failure_rate != null ? (raw.failure_rate * 100).toFixed(1) : '-')}% (${raw.source === 'window' ? '近' + Math.round((raw.window_sec || 600) / 60) + '分钟' : '累计'})`;
      }
      const pct = noData ? 0 : Math.max(0, Math.min(100, Number(score) || 0));
      return `<div class="health-dim">
        <div class="flex between small">
          <span>${esc(HEALTH_DIM_SHORT[k] || k)}<span class="muted"> · 权重 ${Math.round((d.weight || 0) * 100)}%</span></span>
          <span class="tabular bold ${cls}">${noData ? '无数据 N/A' : score.toFixed(1)}</span>
        </div>
        <div class="meter"><div class="fill ${noData ? 'na' : cls}" style="width:${pct}%"></div></div>
        <div class="small muted">${esc(detail)}</div>
      </div>`;
    }).join('');

    const warnings = _healthWarnings(h);
    const updated = h.ts_ms ? `<span class="small muted">更新 ${fmtTime(h.ts_ms)}</span>` : '';
    const penalty = h.short_board_penalty > 0.1
      ? `<div class="small muted">短板节点扣分 short-board −${h.short_board_penalty.toFixed(1)}</div>` : '';
    const warnHtml = warnings.length
      ? `<div class="health-warn">${warnings.map(x => `<div>⚠ ${esc(x)}</div>`).join('')}</div>` : '';

    return `<div class="health-panel">
      <div class="health-head">
        ${healthGauge(h.score, h.grade)}
        <div class="health-dims">${dimRows}</div>
      </div>
      <div class="flex between health-foot">
        <div>${penalty}${warnHtml}</div>
        <div>${updated}</div>
      </div>
    </div>`;
  }

  // Full per-node health table for the metrics page.
  function healthNodeTable(nodes) {
    const rows = nodes || [];
    if (!rows.length) return empty('暂无节点 No workers registered');
    return table([
      { key: 'name', label: '节点 Worker', render: r => `<b>${esc(r.name)}</b>` },
      { key: 'status', label: '状态', render: r => stateBadge(r.status, true) },
      { key: 'score', label: '健康分 Score', num: true,
        render: r => {
          if (r.score == null) return '<span class="muted">N/A</span>';
          const cls = r.score >= 85 ? 'good' : r.score >= 60 ? 'warn' : 'bad';
          return `<span class="bold ${cls}">${r.score.toFixed(1)}</span>`;
        } },
      { key: 'avail', label: '可用', num: true,
        render: r => fmtDim(r.dims, 'availability') },
      { key: 'hb', label: '心跳', num: true,
        render: r => fmtDim(r.dims, 'heartbeat', r.raw_heartbeat, 'age_ratio') },
      { key: 'load', label: '负载 (C/M/L)', num: true,
        render: r => r.raw_load
          ? `${r.raw_load.cpu_percent.toFixed(0)}% / ${r.raw_load.mem_percent.toFixed(0)}% / ${r.raw_load.load1.toFixed(2)}`
          : '<span class="muted">N/A</span>' },
      { key: 'rel', label: '失败 (窗/累)', num: true,
        render: r => {
          const rr = r.raw_reliability || {};
          const wf = rr.window_failed || 0;
          const cf = rr.cumulative_failed;
          return cf == null ? `${wf}` : `${wf} / ${cf}`;
        } },
    ], rows);
  }

  function fmtDim(dims, key, raw, rawKey) {
    const v = dims && dims[key];
    if (v == null) return '<span class="muted">N/A</span>';
    const cls = v >= 85 ? 'good' : v >= 60 ? 'warn' : 'bad';
    return `<span class="${cls}">${v.toFixed(0)}</span>`;
  }

  // headers: [{key, label, num, render(row), width}]
  function table(headers, rows, opts) {
    if (!rows || !rows.length) return empty();
    const thead = '<tr>' + headers.map(h =>
      `<th class="${h.num ? 'num' : ''}"${h.width ? ` style="width:${h.width}"` : ''}>${esc(h.label)}</th>`
    ).join('') + '</tr>';
    const tbody = rows.map((row, ri) => {
      const tds = headers.map(h => {
        const val = h.render ? h.render(row, ri) : row[h.key];
        return `<td class="${h.num ? 'num tabular' : ''}">${val == null ? '-' : val}</td>`;
      }).join('');
      const click = (opts && opts.onClick) ? ` data-row="${ri}"` : '';
      return `<tr${click}>${tds}</tr>`;
    }).join('');
    const html = `<div class="table-wrap"><table class="table"><thead>${thead}</thead><tbody>${tbody}</tbody></table></div>`;
    if (opts && opts.onClick) {
      // attach click handler by returning element instead; handled via data attribute
      return html;
    }
    return html;
  }

  // ------------------------------------------------------------------
  function renderNav(active) {
    const host = document.getElementById('app-nav');
    if (!host) return;
    host.innerHTML =
      `<span class="brand">分布式 MapReduce<small>Distributed</small></span>` +
      PAGES.map(p =>
        `<a class="nav-link${p.key === active ? ' active' : ''}" href="${p.href}">${esc(p.label)}</a>`
      ).join('') +
      `<span class="spacer"></span>` +
      `<button class="theme-toggle" id="theme-toggle" title="切换主题 Theme">◐</button>`;
    const toggle = document.getElementById('theme-toggle');
    toggle.addEventListener('click', () => {
      const root = document.documentElement;
      const cur = root.getAttribute('data-theme');
      const next = cur === 'dark' ? 'light' : 'dark';
      root.setAttribute('data-theme', next);
      try { localStorage.setItem('mr-theme', next); } catch (e) {}
      window.dispatchEvent(new Event('themechange'));
    });
  }

  function initTheme() {
    try {
      const saved = localStorage.getItem('mr-theme');
      if (saved) document.documentElement.setAttribute('data-theme', saved);
    } catch (e) {}
  }

  function init(active) {
    initTheme();
    renderNav(active);
  }

  // ------------------------------------------------------------------
  function toast(msg, type) {
    let host = document.querySelector('.toast-host');
    if (!host) { host = document.createElement('div'); host.className = 'toast-host'; document.body.appendChild(host); }
    const t = document.createElement('div');
    t.className = 'toast ' + (type || '');
    t.textContent = msg;
    host.appendChild(t);
    setTimeout(() => t.remove(), 4000);
  }

  function poll(fn, ms) {
    let timer = null;
    let stopped = false;
    async function run() {
      if (stopped) return;
      try { await fn(); } catch (e) { /* transient */ }
      if (!stopped) timer = setTimeout(run, ms);
    }
    return {
      start() { run(); },
      stop() { stopped = true; if (timer) clearTimeout(timer); },
    };
  }

  function valueCell(rec) {
    // Render a result record generically: key -> value / values.
    const keys = Object.keys(rec).filter(k => k !== 'key');
    if (keys.length === 1) return esc(rec[keys[0]]);
    return esc(keys.map(k => `${k}=${rec[k]}`).join(', '));
  }

  // Build a job <select> in hostId once, auto-select the first job.
  function jobPicker(hostId, onSelect) {
    const host = document.getElementById(hostId);
    if (!host) return;
    API.get('/api/jobs').then(d => {
      const jobs = d.jobs || [];
      let html = '<select class="job-select"><option value="">选择作业 Select job…</option>';
      jobs.forEach(j => { html += `<option value="${j.job_id}">${esc(j.name)} — ${j.status}</option>`; });
      html += '</select>';
      host.innerHTML = html;
      const sel = host.querySelector('select');
      sel.addEventListener('change', () => onSelect(sel.value));
      if (jobs.length) { sel.value = jobs[0].job_id; onSelect(sel.value); }
    }).catch(() => { host.innerHTML = empty('无法连接 Master (Cannot reach master)'); });
  }

  return {
    PAGES, LABELS, CLASS, esc, fmtNum, fmtBytes, fmtTime, fmtDur, fmtPct,
    stateBadge, progress, meter, empty, table, renderNav, init, toast, poll, valueCell, jobPicker,
    healthPanel, healthNodeTable, healthGrade, healthGauge,
  };
})();
