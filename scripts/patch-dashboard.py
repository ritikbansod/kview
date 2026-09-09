import io

p = 'src/main/resources/static/js/views/dashboard.js'
s = io.open(p, encoding='utf-8').read()

# 1. Fix the Brokers card - it should NOT navigate to #/topics.
#    Instead it should scroll to the broker table already on the dashboard.
old = '''      <div class="kpi clickable" data-goto="#/topics"><div class="label">Brokers</div><div class="value">${fmtNum(overview.brokers?.length)}</div><div class="kpi-hint">view configs</div></div>'''
new = '''      <div class="kpi clickable" data-scroll="broker-table"><div class="label">Brokers</div><div class="value">${fmtNum(overview.brokers?.length)}</div></div>'''
assert old in s, 'Brokers card not found'
s = s.replace(old, new, 1)

# 2. Wire the scroll behavior for the Brokers card
old_wire = '''  // clickable KPI cards route to their pages
  view.querySelectorAll('.kpi[data-goto]').forEach((card) => {
    card.addEventListener('click', () => { location.hash = card.dataset.goto; });
  });'''
new_wire = '''  // clickable KPI cards route to their pages
  view.querySelectorAll('.kpi[data-goto]').forEach((card) => {
    card.addEventListener('click', () => { location.hash = card.dataset.goto; });
  });
  // brokers card scrolls to the broker table on the same page
  view.querySelectorAll('.kpi[data-scroll]').forEach((card) => {
    card.addEventListener('click', () => {
      document.querySelector('[data-broker-table]')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  });'''
assert old_wire in s, 'old wiring not found'
s = s.replace(old_wire, new_wire, 1)

# 3. Add data-broker-table to the brokers table wrapper
old_table = '''        <div class="card">
          <div class="card-title"><h2>Brokers</h2><span class="faint small">click a broker for its config</span></div>'''
new_table = '''        <div class="card" data-broker-table>
          <div class="card-title"><h2>Brokers</h2><span class="faint small">click a broker for its config</span></div>'''
assert old_table in s, 'broker table not found'
s = s.replace(old_table, new_table, 1)

io.open(p, 'w', encoding='utf-8').write(s)
print('dashboard routing fixed')
