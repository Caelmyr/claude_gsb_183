/* 集群健康评分 Cluster health — shared widget used by the overview and nodes pages.
 *
 * Data comes from /api/cluster/health (also embedded in /api/overview):
 * one smoothed 0-100 score, four weighted dimensions with raw/smoothed values,
 * and a per-node breakdown that mirrors the raw numbers on Nodes / Metrics.
 */
const Health = (() => {
  const DIMS = [
    { key: 'availability', zh: '节点可用性', en: 'Availability' },
    { key: 'heartbeat', zh: '心跳稳定性', en: 'Heartbeat' },
    { key: 'load', zh: '资源负载', en: 'Resource load' },
    { key: 'tasks', zh: '任务质量', en: 'Task quality' },
  ];
  const GRADE_TEXT = {
    good: '健康 Healthy',
    warn: '注意 Degraded',
    bad: '异常 Critical',
    unknown: '未知 Unknown',
  };
  const MISSING_LABEL = {
    heartbeat: '心跳 Heartbeat',
    load: '负载 Load',
    tasks: '任务 Tasks',
  };
  const COLOR = { good: 'var(--good)', warn: 'var(--serious)', bad: 'var(--critical)', unknown: 'var(--muted)' };

  function clamp(v, lo = 0, hi = 100) { return Math.max(lo, Math.min(hi, Number(v) || 0)); }

  // ----------------------------------------------------------------
  function gauge(score, grade) {
    const pct = score == null ? 0 : clamp(score);
    // 240deg arc starting at 150deg (bottom-left), gap at the bottom.
    const R = 62, CIRC = 2 * Math.PI * R, ARC = (240 / 360) * CIRC;
    const dash = ARC * pct / 100;
    const color = COLOR[grade] || COLOR.unknown;
    const shown = score == null ? '—' : Math.round(score);
    return `
      <div class="health-gauge">
        <svg width="170" height="150" viewBox="0 0 170 150" role="img" aria-label="health score">
          <g transform="rotate(150 85 80)">
            <circle class="gauge-track" cx="85" cy="80" r="${R}" fill="none" stroke-width="11"
              stroke-dasharray="${ARC} ${CIRC}" stroke-linecap="round"/>
            <circle cx="85" cy="80" r="${R}" fill="none" stroke-width="11" stroke-linecap="round"
              stroke="${color}" stroke-dasharray="${dash.toFixed(1)} ${CIRC}"
              style="transition:stroke-dasharray .6s ease,stroke .6s ease"/>
          </g>
          <text x="85" y="82" text-anchor="middle" font-size="38" font-weight="700"
            fill="currentColor" class="tabular">${shown}</text>
          <text x="85" y="104" text-anchor="middle" font-size="10.5" fill="var(--muted)">/ 100</text>
        </svg>
        <span class="health-grade ${grade}">${GRADE_TEXT[grade] || GRADE_TEXT.unknown}</span>
      </div>`;
  }

  function sparkline(history) {
    if (!history || history.length < 2) return '';
    const w = 150, h = 22, pad = 1;
    const pts = history.slice(-40);
    const xs = i => pad + (w - 2 * pad) * i / (pts.length - 1);
    const ys = v => h - pad - (h - 2 * pad) * clamp(v) / 100;
    const d = pts.map((p, i) => `${i ? 'L' : 'M'}${xs(i).toFixed(1)},${ys(p.score).toFixed(1)}`).join(' ');
    return `<svg class="health-spark" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
      <path d="${d}" fill="none" stroke="var(--series-1)" stroke-width="1.5"/></svg>`;
  }

  function dimDetail(key, d) {
    const x = d.detail || {};
    if (key === 'availability') {
      return `${x.alive ?? 0}/${x.workers_total ?? 0} 存活 alive · 失联 ${x.dead ?? 0} · 滞后 ${x.stale ?? 0}`;
    }
    if (key === 'heartbeat') {
      if (!d.available) return '窗口内无心跳样本 no heartbeat samples';
      const age = x.avg_age_ms == null ? '-' : (x.avg_age_ms / 1000).toFixed(1) + 's';
      const jit = x.avg_jitter_cv == null ? '-' : (x.avg_jitter_cv * 100).toFixed(0) + '%';
      return `平均延迟 age ${age} · 抖动 jitter ${jit} · ${x.reporting_workers ?? 0} 节点上报`;
    }
    if (key === 'load') {
      if (!d.available) return '窗口内无资源样本 no resource samples';
      const f = v => (v == null ? '-' : Number(v).toFixed(0) + '%');
      return `CPU ${f(x.avg_cpu)} · 内存 MEM ${f(x.avg_mem)} · 负载/核 load/core ${x.avg_load_ratio ?? '-'} · ${x.reporting_workers ?? 0} 节点上报`;
    }
    if (key === 'tasks') {
      if (!d.available) return '窗口内无任务完成 no task events in window';
      const fr = x.failure_rate == null ? '-' : (x.failure_rate * 100).toFixed(1) + '%';
      return `成功 ${x.succeeded ?? 0} · 失败 ${x.failed ?? 0} · 失败率 ${fr}` +
             (x.low_sample ? ' · 样本少 low sample' : '');
    }
    return '';
  }

  function dimsPanel(h) {
    const dims = h.dimensions || {};
    return '<div class="health-dims">' + DIMS.map(def => {
      const d = dims[def.key];
      if (!d) return '';
      const val = d.score;
      const raw = d.raw_score;
      const pct = val == null ? 0 : clamp(val);
      const fillCls = val == null ? '' : (val >= 80 ? 'good' : (val >= 60 ? 'warn' : 'bad'));
      const weightPct = Math.round((d.weight || 0) * 100);
      const valTxt = val == null
        ? '<span class="muted">N/A</span>'
        : `<b class="tabular ${fillCls === 'bad' ? 'bad' : (fillCls === 'good' ? 'good' : '')}">${val.toFixed(0)}</b>`;
      const rawTxt = raw == null ? '' :
        `<span class="muted small"> 原始 raw ${raw.toFixed(0)}</span>`;
      return `<div class="health-dim">
        <div class="dim-head">
          <span class="dim-name">${def.zh} <span class="muted">${def.en}</span>
            <span class="muted small">×${weightPct}%</span>${d.coverage < 0.999 ?
              ` <span class="badge warn" title="该维度有效数据覆盖比例 coverage">覆盖 ${(d.coverage * 100).toFixed(0)}%</span>` : ''}
          </span>
          <span class="dim-meta">${valTxt}${rawTxt}</span>
        </div>
        <div class="meter"><div class="fill ${fillCls}" style="width:${pct}%"></div></div>
        <div class="dim-detail">${dimDetail(def.key, d)}</div>
      </div>`;
    }).join('') + '</div>';
  }

  function nodesTable(h) {
    const rows = h.nodes || [];
    if (!rows.length) return Components.empty('尚无注册节点 No workers registered');
    const f2 = v => (v == null ? '-' : Number(v).toFixed(1));
    const fr = v => (v == null ? '-' : Number(v).toFixed(2));
    const na = v => (v == null ? '<span class="muted">-</span>' : v);
    const cls = v => v == null ? '' : (v >= 80 ? 'good' : (v >= 60 ? '' : 'bad'));
    return Components.table([
      { key: 'name', label: '节点 Worker', render: r => `<b>${Components.esc(r.name)}</b>` },
      { key: 'status', label: '状态 Status', render: r => Components.stateBadge(r.status, true) },
      { key: 'score', label: '节点评分 Score', num: true,
        render: r => `<span class="bold tabular ${cls(r.score)}">${na(r.score)}</span>` },
      { key: 'availability', label: '可用性 Avail', num: true, render: r => na(f2(r.availability)) },
      { key: 'heartbeat', label: '心跳 HB', num: true, render: r => na(f2(r.heartbeat)) },
      { key: 'load', label: '负载 Load', num: true, render: r => na(f2(r.load)) },
      { key: 'cpu_avg', label: 'CPU%', num: true, render: r => na(f2(r.cpu_avg)) },
      { key: 'mem_avg', label: 'MEM%', num: true, render: r => na(f2(r.mem_avg)) },
      { key: 'load1_avg', label: 'load/core', num: true, render: r => na(fr(r.load_ratio)) },
      { key: 'tasks', label: '窗口任务 OK/失败', num: true,
        render: r => `<span class="good">${r.tasks_ok_window || 0}</span> / <span class="${r.tasks_failed_window ? 'bad' : ''}">${r.tasks_failed_window || 0}</span>` },
      { key: 'missing', label: '缺失数据 Missing',
        render: r => (r.missing && r.missing.length
          ? r.missing.map(m => `<span class="badge warn">${MISSING_LABEL[m] || m}</span>`).join(' ')
          : '<span class="muted small">完整 complete</span>') },
    ], rows);
  }

  // ----------------------------------------------------------------
  // Full card for the overview page.
  // ----------------------------------------------------------------
  function renderCard(hostId, h) {
    const host = document.getElementById(hostId);
    if (!host) return;
    if (!h) {
      host.innerHTML = `<div class="card"><h2>集群健康评分 <span class="sub">Cluster health</span></h2>
        ${Components.empty('无法获取健康评分 Cannot load health report')}</div>`;
      return;
    }
    const grade = h.grade || 'unknown';
    const covPct = (clamp((h.coverage ?? 1) * 100)).toFixed(0);
    const updated = h.ts_ms ? Components.fmtTime(h.ts_ms) : '-';
    const missing = h.missing_workers || [];
    const rawTxt = h.raw_score == null ? '' :
      `<span>原始分 Raw <b class="tabular">${h.raw_score.toFixed(0)}</b></span>`;
    host.innerHTML = `
      <div class="card">
        <h2>集群健康评分 <span class="sub">Cluster health</span>
          <span class="sub" style="float:right">${sparkline(h.history)}</span>
        </h2>
        <div class="health-grid">
          ${gauge(h.score, grade)}
          ${dimsPanel(h)}
        </div>
        <div class="health-meta">
          <span>数据覆盖 Coverage <b>${covPct}%</b></span>
          <span>平滑窗口 Smoothing <b>${h.smoothing_sec || '-'}s EWMA</b></span>
          <span>指标窗口 Window <b>${h.window_sec || '-'}s</b> / 任务 <b>${h.task_window_sec || '-'}s</b></span>
          ${rawTxt}
          <span>更新 Updated <b>${updated}</b></span>
          ${missing.length ? `<span class="bad">缺失节点 Missing: <b>${missing.map(m => Components.esc(m.name)).join(', ')}</b></span>` : ''}
        </div>
        <div class="health-nodes-wrap">
          <h3 style="margin:6px 0 8px;font-size:13.5px">节点健康明细 <span class="sub muted">Per-node breakdown</span></h3>
          ${nodesTable(h)}
        </div>
      </div>`;
  }

  // Compact tile for the nodes page header.
  function renderMini(hostId, h) {
    const host = document.getElementById(hostId);
    if (!host || !h) return;
    const grade = h.grade || 'unknown';
    const num = h.score == null ? '—' : Math.round(h.score);
    host.innerHTML = `<div class="stat">
      <div class="label">健康评分 Health</div>
      <div class="health-mini">
        <span class="dot ${grade}"></span>
        <span class="num ${grade} tabular">${num}</span>
        <span class="health-grade ${grade}" style="padding:1px 8px;font-size:11px">${GRADE_TEXT[grade] || ''}</span>
      </div>
      <div class="delta">${h.raw_score != null ? `原始 raw ${h.raw_score.toFixed(0)} · ` : ''}覆盖 coverage ${clamp((h.coverage ?? 1) * 100).toFixed(0)}%</div>
    </div>`;
  }

  return { renderCard, renderMini };
})();
