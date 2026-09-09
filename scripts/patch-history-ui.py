import io

p = 'src/main/resources/static/js/views/topics.js'
s = io.open(p, encoding='utf-8').read()

old_start = "  // ---- partition history interactions ----"
old_end_marker = """  view.querySelectorAll('th[data-matrix-partition]').forEach((th) => {
    th.addEventListener('click', () => setHistoryFilter(Number(th.dataset.matrixPartition), true));
  });
"""
i0 = s.index(old_start)
i1 = s.index(old_end_marker) + len(old_end_marker)

new_wiring = r'''  // ---- partition history: filters, per-partition tenure, grouped timeline ----
  const histBody = view.querySelector('#hist-body');
  const histSummary = view.querySelector('#hist-summary');
  const histChips = view.querySelector('#hist-chips');
  let partFilter = '';
  let typeFilter = '';

  const TYPE_META = {
    LEADER_CHANGED: { icon: '\u26a1', label: 'Leader changed', tone: 'accent' },
    LEADER_OFFLINE: { icon: '\u26a1', label: 'Leader offline', tone: 'err' },
    ISR_CHANGED: { icon: '\u21c4', label: 'ISR changed', tone: 'warn' },
    REASSIGNED: { icon: '\u21c9', label: 'Reassigned', tone: 'cyan' },
    PARTITION_ADDED: { icon: '\u2795', label: 'Partition added', tone: 'ok' },
    PARTITION_REMOVED: { icon: '\u2796', label: 'Partition removed', tone: 'neutral' },
  };

  const matchesFilters = (e) =>
    (partFilter === '' || e.partition === partFilter) &&
    (typeFilter === '' || e.type === typeFilter);

  // current leader + since-when per partition (from newest event that set a leader)
  const tenure = (() => {
    const map = {};
    for (let i = historyEvents.length - 1; i >= 0; i--) {
      const e = historyEvents[i];
      if (!(e.partition in map) && e.toLeader != null) {
        map[e.partition] = { leader: e.toLeader, since: e.ts };
      }
    }
    return map;
  })();

  function chip(label, active, onClick) {
    const b = document.createElement('button');
    b.className = 'chip' + (active ? ' active' : '');
    b.textContent = label;
    b.addEventListener('click', onClick);
    return b;
  }

  function renderHistory() {
    // summary: current leader per partition
    histSummary.innerHTML = detail.partitions.map((p) => {
      const t = tenure[p.partition];
      const leaderTxt = t ? `broker ${t.leader}` : 'unknown';
      const since = t ? ` \u00b7 since ${fmtRel(t.since)}` : '';
      const active = partFilter === '' || partFilter === p.partition;
      return `<button class="hist-tenure ${active ? '' : 'dim'}" data-p="${p.partition}"
        title="Show p${p.partition} history only">
        <span class="mono">p${p.partition}</span> \u2192 leader <b>broker ${t ? t.leader : '?'}</b>${since}
      </button>`;
    }).join('');
    histSummary.querySelectorAll('.hist-tenure').forEach((b) => {
      b.addEventListener('click', () => {
        partFilter = Number(b.dataset.p);
        typeFilter = '';
        renderHistory();
      });
    });

    // filter chips
    histChips.innerHTML = '';
    const all = chip('All events', partFilter === '' && typeFilter === '',
      () => { partFilter = ''; typeFilter = ''; renderHistory(); });
    histChips.appendChild(all);
    const counts = {};
    historyEvents.forEach((e) => { counts[e.type] = (counts[e.type] || 0) + 1; });
    Object.keys(TYPE_META).forEach((type) => {
      if (!counts[type]) return;
      const meta = TYPE_META[type];
      histChips.appendChild(chip(`${meta.icon} ${meta.label} (${counts[type]})`,
        typeFilter === type, () => { typeFilter = typeFilter === type ? '' : type; renderHistory(); }));
    });
    histChips.appendChild(chip('Elections only', typeFilter === 'LEADER_CHANGED',
      () => { typeFilter = typeFilter === 'LEADER_CHANGED' ? '' : 'LEADER_CHANGED'; renderHistory(); }));

    // timeline grouped by day
    const events = historyEvents.filter(matchesFilters);
    if (events.length === 0) {
      histBody.innerHTML = `<div class="empty-state">No matching events \u2014 leaders and ISR have been
        stable since monitoring started. Changes appear here within 15 seconds of happening.</div>`;
      return;
    }
    const parts = [];
    let lastDay = '';
    events.forEach((e) => {
      const meta = TYPE_META[e.type] || { icon: '\u2022', label: e.type, tone: 'neutral' };
      const day = new Date(e.ts).toLocaleDateString('en-GB', { weekday: 'short', day: '2-digit', month: 'short' });
      if (day !== lastDay) {
        parts.push(`<div class="tl-day">${esc(day)}</div>`);
        lastDay = day;
      }
      const leaderLine = e.type.startsWith('LEADER') && e.fromLeader != null
        ? `<div class="tl-move mono small">broker ${e.fromLeader} \u2192 ${e.toLeader != null ? e.toLeader : '(offline)'}</div>`
        : '';
      const extra = (e.isr && e.type === 'ISR_CHANGED')
        ? `<div class="mono small muted">ISR = [${e.isr.join(', ')}]</div>` : '';
      parts.push(`
        <div class="tl-item tone-${meta.tone}" data-ts="${e.ts}" data-topic="${esc(e.topic)}"
             data-partition="${e.partition}">
          <div class="tl-head"><span class="tl-icon">${meta.icon}</span> ${eventBadge(meta.label, meta.tone)}
            <span class="mono small">p${e.partition}</span>
            <span class="small muted" style="margin-left:auto">${fmtRel(e.ts)}</span></div>
          <div class="small tl-detail">${esc(e.detail || '')}</div>
          ${leaderLine}${extra}
        </div>`);
    });
    histBody.innerHTML = `<div class="timeline">${parts.join('')}</div>`;

    // click an event -> toggle full detail (replicas + ISR)
    histBody.querySelectorAll('.tl-item').forEach((item) => {
      item.addEventListener('click', () => item.classList.toggle('open'));
    });
  }

  renderHistory();

  // silent auto-refresh of history data every 15s while this page is visible
  clearInterval(window.__histTimer);
  window.__histTimer = setInterval(async () => {
    if (!document.getElementById('history-card')) { clearInterval(window.__histTimer); return; }
    try {
      historyEvents.length = 0;
      const fresh = await get(clusterPath() + `/topics/${encodeURIComponent(topic)}/history?limit=300`);
      (fresh?.events || []).forEach((e) => historyEvents.push(e));
      renderHistory();
    } catch { /* cluster briefly unreachable - keep old view */ }
  }, 15000);
'''

s = s[:i0] + new_wiring + s[i1:]
io.open(p, 'w', encoding='utf-8').write(s)
print('wiring replaced')
