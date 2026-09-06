// ===== Connections: manage cluster profiles + security (PLAINTEXT / mTLS / SASL / OAuth) =====
import { get, post, put, del, setActiveCluster, store } from '../api.js';
import { esc, badge, protocolBadge, spinner, toast, confirmDialog, modal } from '../ui.js';

export async function renderConnections(view) {
  view.innerHTML = spinner();
  const clusters = await get('/clusters');

  view.innerHTML = `
    <div class="toolbar" style="align-items:center">
      <h1 style="margin:0">Cluster Connections</h1>
      <div class="spacer" style="flex:1"></div>
      <button class="btn primary" id="add-cluster-btn">+ Add cluster</button>
    </div>
    <p class="page-sub">Connect to any number of Kafka clusters over PLAINTEXT, TLS/mTLS or
    SASL (PLAIN, SCRAM, OAUTHBEARER). Profiles are stored server-side; secrets never travel back to the UI.</p>
    <div id="cluster-cards" class="grid-kpi" style="grid-template-columns:repeat(auto-fill,minmax(330px,1fr))">
      ${clusters.map((c) => clusterCard(c)).join('')}
    </div>`;

  view.querySelectorAll('[data-use]').forEach((btn) => btn.addEventListener('click', () => {
    setActiveCluster(btn.dataset.use);
    toast(`Active cluster: ${btn.dataset.use}`);
    renderConnections(view);
  }));

  view.querySelectorAll('[data-test]').forEach((btn) => btn.addEventListener('click', async () => {
    const id = btn.dataset.test;
    const resultEl = view.querySelector(`[data-test-result="${id}"]`);
    resultEl.innerHTML = '<span class="muted">Testing…</span>';
    const cluster = clusters.find((c) => c.id === id);
    const payload = cluster.builtIn
      ? { name: cluster.name, bootstrapServers: cluster.bootstrapServers, security: { protocol: 'PLAINTEXT' } }
      : cluster;
    const result = await post('/clusters/test', profileFromCard(payload)).catch((e) => ({ ok: false, error: e.message }));
    resultEl.innerHTML = result.ok
      ? `<div class="test-result ok">✓ Connected — cluster <span class="mono">${esc(result.clusterId)}</span>,
           ${result.nodes.length} broker(s): ${result.nodes.map((n) => esc(`${n.host}:${n.port}`)).join(', ')}</div>`
      : `<div class="test-result err">✗ ${esc(result.error || 'Connection failed')}</div>`;
  }));

  view.querySelectorAll('[data-reconnect]').forEach((btn) => btn.addEventListener('click', async () => {
    try {
      await post(`/clusters/${encodeURIComponent(btn.dataset.reconnect)}/reconnect`);
      toast('Reconnected');
    } catch (err) { toast(err.message, 'err'); }
  }));

  view.querySelectorAll('[data-delete]').forEach((btn) => btn.addEventListener('click', () => {
    confirmDialog({
      title: `Delete connection "${btn.dataset.delete}"?`,
      message: 'The stored profile is removed. Kafka itself is not touched.',
      onConfirm: async () => {
        try {
          await del(`/clusters/${encodeURIComponent(btn.dataset.delete)}`);
          toast('Connection deleted');
          renderConnections(view);
        } catch (err) { toast(err.message, 'err'); }
      },
    });
  }));

  view.querySelector('#add-cluster-btn').addEventListener('click', () => profileModal(null, () => renderConnections(view)));
  view.querySelectorAll('[data-sr-only]').forEach((btn) => btn.addEventListener('click', () => {
    const cluster = clusters.find((c) => c.id === btn.dataset.srOnly);
    registryOnlyModal(cluster, () => renderConnections(view));
  }));
  view.querySelectorAll('[data-edit]').forEach((btn) => btn.addEventListener('click', () => {
    const cluster = clusters.find((c) => c.id === btn.dataset.edit);
    profileModal(cluster, () => renderConnections(view));
  }));
}

function clusterCard(c) {
  const sec = c.security || {};
  const features = [
    sec.protocol === 'SSL' || sec.protocol === 'SASL_SSL' ? badge('TLS', 'cyan') : '',
    (sec.keystoreCertificateChainPem || sec.keystoreLocation) ? badge('client cert', 'cyan') : '',
    sec.saslMechanism === 'OAUTHBEARER' ? badge('OAuth2', 'purple') : '',
    sec.saslMechanism && sec.saslMechanism.startsWith('SCRAM') ? badge(sec.saslMechanism, 'purple') : '',
    sec.endpointVerificationEnabled === false ? badge('hostname check off', 'warn') : '',
  ].filter(Boolean).join(' ');

  return `
    <div class="kpi" style="display:flex;flex-direction:column;gap:8px;${c.id === store.clusterId ? 'border-color:var(--accent);' : ''}">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <b style="font-size:15px">${esc(c.name)}</b>
        ${c.id === store.clusterId ? badge('★ active', 'accent') : ''}
        ${c.builtIn ? badge('built-in', 'neutral') : ''}
      </div>
      <div class="mono small muted">${c.bootstrapServers.map(esc).join('<br>')}</div>
      <div>${protocolBadge(sec)} ${features}</div>
      <div class="btn-row" style="margin-top:auto">
        ${c.id === store.clusterId
          ? `<button class="btn sm primary" disabled>Active</button>`
          : `<button class="btn sm primary" data-use="${esc(c.id)}">Use</button>`}
        <button class="btn sm ghost" data-test="${esc(c.id)}">Test</button>
        <button class="btn sm ghost" data-reconnect="${esc(c.id)}">Reconnect</button>
        ${c.builtIn ? `<button class="btn sm ghost" data-sr-only="${esc(c.id)}">Schema registry…</button>` : ''}
        ${c.builtIn ? '' : `<button class="btn sm ghost" data-edit="${esc(c.id)}">Edit</button>`}
        ${c.builtIn ? '' : `<button class="btn sm danger" data-delete="${esc(c.id)}">Delete</button>`}
      </div>
      <div data-test-result="${esc(c.id)}"></div>
    </div>`;
}

/** Cards return masked secrets; the test endpoint tolerates them as-is. */
function profileFromCard(cluster) {
  return {
    id: cluster.id,
    name: cluster.name,
    bootstrapServers: cluster.bootstrapServers,
    security: cluster.security,
  };
}

// ================= Profile form =================

function profileModal(existing, done) {
  const sec = existing?.security || {};
  const reg = existing?.schemaRegistry || {};
  const srUser = reg.authType === 'BASIC' ? (reg.username || '') : '';
  const srPassPlaceholder = reg.password ? '•••••• (unchanged)' : '';
  const proto = sec.protocol || 'PLAINTEXT';
  modal({
    title: existing ? `Edit connection "${existing.name}"` : 'Add cluster connection',
    wide: true,
    body: `
      <div class="form-grid">
        <div class="field"><label>Name</label>
          <input type="text" id="pf-name" value="${esc(existing?.name || '')}" placeholder="e.g. prod-eu" /></div>
        <div class="field"><label>Bootstrap servers <span class="hint">(comma separated)</span></label>
          <input type="text" id="pf-bootstrap" value="${esc((existing?.bootstrapServers || ['localhost:9092']).join(','))}" /></div>
      </div>

      <fieldset class="section"><legend>Security protocol</legend>
        <div class="field"><select id="pf-protocol">
          ${['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'].map((p) =>
            `<option value="${p}" ${p === proto ? 'selected' : ''}>${p}</option>`).join('')}
        </select></div>
      </fieldset>

      <fieldset class="section" id="pf-sasl-section" style="display:${proto.startsWith('SASL') ? '' : 'none'}">
        <legend>SASL authentication</legend>
        <div class="form-grid">
          <div class="field"><label>Mechanism</label>
            <select id="pf-mechanism">
              ${['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'OAUTHBEARER'].map((m) =>
                `<option value="${m}" ${m === (sec.saslMechanism || 'PLAIN') ? 'selected' : ''}>${m}</option>`).join('')}
            </select></div>
          <div id="pf-user-field" class="field"><label>Username</label>
            <input type="text" id="pf-username" value="${esc(sec.saslUsername || '')}" /></div>
          <div id="pf-pass-field" class="field"><label>Password</label>
            <input type="password" id="pf-password" value="${esc(sec.saslPassword || '')}" placeholder="${sec.saslPassword ? '•••••• (unchanged)' : ''}" /></div>
        </div>
        <div id="pf-oauth" style="display:none">
          <div class="form-grid">
            <div class="field full"><label>Token endpoint URL <span class="hint">(OAuth2 client-credentials)</span></label>
              <input type="text" id="pf-token-url" value="${esc(sec.oauthTokenUrl || '')}" placeholder="https://idp.example.com/realms/kafka/protocol/openid-connect/token" /></div>
            <div class="field"><label>Client ID</label>
              <input type="text" id="pf-client-id" value="${esc(sec.oauthClientId || '')}" /></div>
            <div class="field"><label>Client secret</label>
              <input type="password" id="pf-client-secret" value="${esc(sec.oauthClientSecret || '')}" placeholder="${sec.oauthClientSecret ? '•••••• (unchanged)' : ''}" /></div>
            <div class="field full"><label>Scope <span class="hint">(optional)</span></label>
              <input type="text" id="pf-scope" value="${esc(sec.oauthScope || '')}" /></div>
          </div>
        </div>
      </fieldset>

      <fieldset class="section" id="pf-tls-section" style="display:${(proto === 'SSL' || proto === 'SASL_SSL') ? '' : 'none'}">
        <legend>TLS / certificates</legend>
        <label class="checkbox mb16"><input type="checkbox" id="pf-verify" ${sec.endpointVerificationEnabled === false ? '' : 'checked'} />
          verify broker hostname</label>
        <div class="form-grid">
          <div>
            <div class="field"><label>Client certificate (mTLS)</label>
              <select id="pf-keystore-mode">
                <option value="none" ${!sec.keystoreLocation && !sec.keystoreCertificateChainPem ? 'selected' : ''}>none</option>
                <option value="file" ${sec.keystoreLocation ? 'selected' : ''}>keystore file</option>
                <option value="pem" ${sec.keystoreCertificateChainPem ? 'selected' : ''}>paste PEM</option>
              </select></div>
            <div id="pf-keystore-file">
              <div class="field"><label>Keystore path</label>
                <input type="text" id="pf-ks-path" value="${esc(sec.keystoreLocation || '')}" placeholder="/certs/keystore.p12" /></div>
              <div class="form-grid">
                <div class="field"><label>Keystore password</label>
                  <input type="password" id="pf-ks-pass" value="${esc(sec.keystorePassword || '')}" placeholder="${sec.keystorePassword ? '•••••• (unchanged)' : ''}" /></div>
                <div class="field"><label>Type</label>
                  <select id="pf-ks-type">
                    ${['PKCS12', 'JKS'].map((t) => `<option ${t === (sec.keystoreType || 'PKCS12') ? 'selected' : ''}>${t}</option>`).join('')}
                  </select></div>
              </div>
            </div>
            <div id="pf-keystore-pem" style="display:none">
              <div class="field"><label>Certificate chain (PEM)</label>
                <textarea id="pf-ks-chain" rows="4" placeholder="${sec.keystoreCertificateChainPem ? '•••••• (configured — leave empty to keep)' : '-----BEGIN CERTIFICATE-----…'}"></textarea></div>
              <div class="field"><label>Private key (PEM)</label>
                <textarea id="pf-ks-key" rows="4" placeholder="${sec.keystoreKeyPem ? '•••••• (configured — leave empty to keep)' : '-----BEGIN PRIVATE KEY-----…'}"></textarea></div>
              <div class="field"><label>Key password <span class="hint">(encrypted keys only)</span></label>
                <input type="password" id="pf-ks-key-pass" value="" placeholder="${sec.keystorePassword && sec.keystoreCertificateChainPem ? '•••••• (unchanged)' : ''}" /></div>
            </div>
          </div>
          <div>
            <div class="field"><label>Trust of brokers (server CA)</label>
              <select id="pf-truststore-mode">
                <option value="none" ${!sec.truststoreLocation && !sec.truststoreCertificatesPem ? 'selected' : ''}>system default</option>
                <option value="file" ${sec.truststoreLocation ? 'selected' : ''}>truststore file</option>
                <option value="pem" ${sec.truststoreCertificatesPem ? 'selected' : ''}>paste PEM</option>
              </select></div>
            <div id="pf-truststore-file" style="display:none">
              <div class="field"><label>Truststore path</label>
                <input type="text" id="pf-ts-path" value="${esc(sec.truststoreLocation || '')}" placeholder="/certs/truststore.p12" /></div>
              <div class="form-grid">
                <div class="field"><label>Truststore password</label>
                  <input type="password" id="pf-ts-pass" value="${esc(sec.truststorePassword || '')}" placeholder="${sec.truststorePassword ? '•••••• (unchanged)' : ''}" /></div>
                <div class="field"><label>Type</label>
                  <select id="pf-ts-type">
                    ${['PKCS12', 'JKS'].map((t) => `<option ${t === (sec.truststoreType || 'PKCS12') ? 'selected' : ''}>${t}</option>`).join('')}
                  </select></div>
              </div>
            </div>
            <div id="pf-truststore-pem" style="display:none">
              <div class="field"><label>CA certificates (PEM)</label>
                <textarea id="pf-ts-certs" rows="6" placeholder="${sec.truststoreCertificatesPem ? '•••••• (configured — leave empty to keep)' : '-----BEGIN CERTIFICATE-----…'}"></textarea></div>
            </div>
          </div>
        </div>
      </fieldset>
      <fieldset class="section"><legend>Schema registry (optional)</legend>
        <div class="form-grid">
          <div class="field"><label>Type</label>
            <select id="pf-sr-type">
              <option value="">none</option>
              <option value="CONFLUENT">Confluent-compatible (Confluent SR/Cloud, Redpanda, Karapace, Apicurio)</option>
            </select></div>
          <div class="field"><label>Registry URL</label>
            <input type="text" id="pf-sr-url" value="${esc(reg.url || '')}" placeholder="http://schema-registry:8081" /></div>
          <div id="pf-sr-auth" style="display:none">
            <div class="form-grid">
              <div class="field"><label>Username</label>
                <input type="text" id="pf-sr-user" value="${esc(srUser)}" /></div>
              <div class="field"><label>Password</label>
                <input type="password" id="pf-sr-pass" value="" placeholder="${srPassPlaceholder}" /></div>
            </div>
          </div>
        </div>
        <p class="hint" style="margin-top:-6px">Decodes schema-registry encoded messages in the Data Explorer (Avro, JSON Schema; Protobuf soon).</p>
      </fieldset>
      <div id="pf-test-result"></div>`,
    actions: [
      { label: 'Test connection', class: 'ghost', onClick: (o) => testInModal(o) },
      { label: 'Cancel', class: 'ghost', onClick: (o, close) => close() },
      { label: 'Save connection', class: 'primary', onClick: async (o, close) => { if (await save(o)) { close(); done(); } } },
    ],
    onMount: (overlay) => wireForm(overlay),
  });

  function collect(overlay) {
    const protocol = overlay.querySelector('#pf-protocol').value;
    const usesSasl = protocol.startsWith('SASL');
    const usesTls = protocol === 'SSL' || protocol === 'SASL_SSL';
    const mechanism = overlay.querySelector('#pf-mechanism')?.value;
    const security = { protocol };

    if (usesSasl) {
      security.saslMechanism = mechanism;
      if (mechanism === 'OAUTHBEARER') {
        security.oauthTokenUrl = val('#pf-token-url');
        security.oauthClientId = val('#pf-client-id');
        security.oauthClientSecret = secret('#pf-client-secret');
        security.oauthScope = val('#pf-scope') || null;
      } else {
        security.saslUsername = val('#pf-username');
        security.saslPassword = secret('#pf-password');
      }
    }
    if (usesTls) {
      security.endpointVerificationEnabled = overlay.querySelector('#pf-verify').checked;
      const ksMode = overlay.querySelector('#pf-keystore-mode').value;
      if (ksMode === 'file') {
        security.keystoreLocation = val('#pf-ks-path');
        security.keystorePassword = secret('#pf-ks-pass');
        security.keystoreType = overlay.querySelector('#pf-ks-type').value;
      } else if (ksMode === 'pem') {
        const chain = secret('#pf-ks-chain');
        const key = secret('#pf-ks-key');
        if (chain) security.keystoreCertificateChainPem = chain;
        if (key) security.keystoreKeyPem = key;
        const keyPass = secret('#pf-ks-key-pass');
        if (keyPass && keyPass !== '••••••') security.keystorePassword = keyPass;
      }
      const tsMode = overlay.querySelector('#pf-truststore-mode').value;
      if (tsMode === 'file') {
        security.truststoreLocation = val('#pf-ts-path');
        security.truststorePassword = secret('#pf-ts-pass');
        security.truststoreType = overlay.querySelector('#pf-ts-type').value;
      } else if (tsMode === 'pem') {
        const certs = secret('#pf-ts-certs');
        if (certs) security.truststoreCertificatesPem = certs;
      }
    }
    const srType = overlay.querySelector('#pf-sr-type')?.value || '';
    const schemaRegistry = srType ? {
      type: srType,
      url: val('#pf-sr-url'),
      authType: val('#pf-sr-user') || val('#pf-sr-pass') ? 'BASIC' : 'NONE',
      username: val('#pf-sr-user') || null,
      password: secret('#pf-sr-pass') || null,
    } : null;
    return {
      id: existing?.id || null,
      name: val('#pf-name') || null,
      bootstrapServers: val('#pf-bootstrap').split(',').map((s) => s.trim()).filter(Boolean),
      security,
      schemaRegistry,
    };

    function val(sel) { return overlay.querySelector(sel)?.value.trim() || ''; }
    /** Masked "keep as is" values are sent verbatim; the backend swaps them for stored secrets. */
    function secret(sel) {
      const v = overlay.querySelector(sel)?.value ?? '';
      return v === '' ? (overlay.querySelector(sel)?.placeholder.startsWith('••••••') ? '••••••' : '') : v;
    }
  }

  async function testInModal(overlay) {
    const resultEl = overlay.querySelector('#pf-test-result');
    resultEl.innerHTML = '<span class="muted">Testing…</span>';
    const payload = collect(overlay);
    if (payload.bootstrapServers.length === 0) {
      resultEl.innerHTML = '<div class="test-result err">Bootstrap servers are required</div>';
      return;
    }
    const result = await post('/clusters/test', payload).catch((e) => ({ ok: false, error: e.message }));
    let registryPart = '';
    if (result.ok && payload.schemaRegistry) {
      const sr = await post('/registries/test', payload.schemaRegistry).catch((e) => ({ ok: false, error: e.message }));
      registryPart = sr.ok
        ? `<div class="test-result ok">✓ Schema registry reachable — ${sr.subjectCount} subject(s)</div>`
        : `<div class="test-result err">✗ Schema registry: ${esc(sr.error || 'unreachable')}</div>`;
    }
    resultEl.innerHTML = result.ok
      ? `<div class="test-result ok">✓ Connected — cluster <span class="mono">${esc(result.clusterId)}</span>,
           ${result.nodes.length} broker(s): ${result.nodes.map((n) => esc(`${n.host}:${n.port}`)).join(', ')}</div>${registryPart}`
      : `<div class="test-result err">✗ ${esc(result.error || 'Connection failed')}</div>`;
  }

  async function save(overlay) {
    const payload = collect(overlay);
    if (!payload.name) { toast('Name is required', 'warn'); return false; }
    if (payload.bootstrapServers.length === 0) { toast('Bootstrap servers are required', 'warn'); return false; }
    try {
      let clusterId = payload.id;
      if (clusterId) {
        await put(`/clusters/${encodeURIComponent(clusterId)}`, payload);
        toast('Connection updated');
      } else {
        const created = await post('/clusters', payload);
        clusterId = created.id;
        toast(`Connection "${created.name}" added`);
      }
      // registry attachment is stored separately (registries.json), including for the default cluster
      if (payload.schemaRegistry) {
        await put(`/clusters/${encodeURIComponent(clusterId)}/registry`, payload.schemaRegistry);
      } else {
        await del(`/clusters/${encodeURIComponent(clusterId)}/registry`).catch(() => {});
      }
      return true;
    } catch (err) {
      toast(err.message, 'err');
      return false;
    }
  }
}

function wireForm(overlay) {
  const protocol = overlay.querySelector('#pf-protocol');
  const applyRegistry = () => {
    const type = overlay.querySelector('#pf-sr-type')?.value;
    const authVisible = !!type;
    overlay.querySelector('#pf-sr-auth').style.display = authVisible ? '' : 'none';
  };
  overlay.querySelector('#pf-sr-type')?.addEventListener('change', applyRegistry);
  const applyVisibility = () => {
    const p = protocol.value;
    overlay.querySelector('#pf-sasl-section').style.display = p.startsWith('SASL') ? '' : 'none';
    overlay.querySelector('#pf-tls-section').style.display = (p === 'SSL' || p === 'SASL_SSL') ? '' : 'none';
    applyMechanism();
  };
  const applyMechanism = () => {
    const isOAuth = overlay.querySelector('#pf-mechanism')?.value === 'OAUTHBEARER';
    overlay.querySelector('#pf-oauth').style.display = isOAuth ? '' : 'none';
    overlay.querySelector('#pf-user-field').style.display = isOAuth ? 'none' : '';
    overlay.querySelector('#pf-pass-field').style.display = isOAuth ? 'none' : '';
  };
  protocol.addEventListener('change', applyVisibility);
  overlay.querySelector('#pf-mechanism')?.addEventListener('change', applyMechanism);

  const keystoreMode = overlay.querySelector('#pf-keystore-mode');
  keystoreMode?.addEventListener('change', () => {
    overlay.querySelector('#pf-keystore-file').style.display = keystoreMode.value === 'file' ? '' : 'none';
    overlay.querySelector('#pf-keystore-pem').style.display = keystoreMode.value === 'pem' ? '' : 'none';
  });
  const trustMode = overlay.querySelector('#pf-truststore-mode');
  trustMode?.addEventListener('change', () => {
    overlay.querySelector('#pf-truststore-file').style.display = trustMode.value === 'file' ? '' : 'none';
    overlay.querySelector('#pf-truststore-pem').style.display = trustMode.value === 'pem' ? '' : 'none';
  });

  // initial visibility for edit mode
  const srUrl = overlay.querySelector('#pf-sr-url');
  if (srUrl && srUrl.value) overlay.querySelector('#pf-sr-type').value = 'CONFLUENT';
  applyRegistry();
  overlay.querySelector('#pf-keystore-file') &&
    (overlay.querySelector('#pf-keystore-file').style.display = keystoreMode.value === 'file' ? '' : 'none');
  overlay.querySelector('#pf-keystore-pem') &&
    (overlay.querySelector('#pf-keystore-pem').style.display = keystoreMode.value === 'pem' ? '' : 'none');
  overlay.querySelector('#pf-truststore-file') &&
    (overlay.querySelector('#pf-truststore-file').style.display = trustMode.value === 'file' ? '' : 'none');
  overlay.querySelector('#pf-truststore-pem') &&
    (overlay.querySelector('#pf-truststore-pem').style.display = trustMode.value === 'pem' ? '' : 'none');
  applyVisibility();
}

/** Registry-only modal — used for the built-in default cluster (no editable profile). */
function registryOnlyModal(cluster, done) {
  let current = null;
  modal({
    title: `Schema registry — ${cluster.name}`,
    body: `
      <p class="muted small">Attach a schema registry to decode schema-encoded messages in the Data Explorer.</p>
      <div class="form-grid">
        <div class="field"><label>Type</label>
          <select id="ro-type">
            <option value="">none (detach)</option>
            <option value="CONFLUENT">Confluent-compatible</option>
          </select></div>
        <div class="field"><label>Registry URL</label>
          <input type="text" id="ro-url" placeholder="http://schema-registry:8081" /></div>
      </div>
      <div id="ro-auth" style="display:none">
        <div class="form-grid">
          <div class="field"><label>Username</label>
            <input type="text" id="ro-user" /></div>
          <div class="field"><label>Password</label>
            <input type="password" id="ro-pass" placeholder="" /></div>
        </div>
      </div>
      <div id="ro-result"></div>`,
    actions: [
      { label: 'Test', class: 'ghost', onClick: (o) => testRegistryOnly(o) },
      { label: 'Detach', class: 'danger', onClick: async (o, close) => {
          try { await del(`/clusters/${encodeURIComponent(cluster.id)}/registry`); toast('Registry detached'); close(); done(); }
          catch (err) { toast(err.message, 'err'); }
        } },
      { label: 'Cancel', class: 'ghost', onClick: (o, close) => close() },
      { label: 'Save', class: 'primary', onClick: async (o, close) => {
          const type = o.querySelector('#ro-type').value;
          if (!type) { toast('Pick a registry type or use Detach', 'warn'); return; }
          const url = o.querySelector('#ro-url').value.trim();
          if (!url) { toast('Registry URL is required', 'warn'); return; }
          const authType = o.querySelector('#ro-user').value || o.querySelector('#ro-pass').value ? 'BASIC' : 'NONE';
          try {
            await put(`/clusters/${encodeURIComponent(cluster.id)}/registry`, {
              type, url, authType,
              username: o.querySelector('#ro-user').value.trim() || null,
              password: o.querySelector('#ro-pass').value || null,
            });
            toast('Schema registry attached');
            close(); done();
          } catch (err) { toast(err.message, 'err'); }
        } },
    ],
    onMount: async (overlay) => {
      const apply = () => {
        overlay.querySelector('#ro-auth').style.display =
          overlay.querySelector('#ro-type').value ? '' : 'none';
      };
      overlay.querySelector('#ro-type').addEventListener('change', apply);
      try {
        const res = await get(`/clusters/${encodeURIComponent(cluster.id)}/registry`);
        current = res.registry;
        overlay.querySelector('#ro-type').value = current.type || 'CONFLUENT';
        overlay.querySelector('#ro-url').value = current.url || '';
        overlay.querySelector('#ro-user').value = current.username || '';
        overlay.querySelector('#ro-pass').placeholder = current.password ? '•••••• (unchanged)' : '';
      } catch { /* not attached yet */ }
      apply();
    },
  });

  async function testRegistryOnly(overlay) {
    const resultEl = overlay.querySelector('#ro-result');
    resultEl.innerHTML = '<span class="muted">Testing…</span>';
    const type = overlay.querySelector('#ro-type').value;
    if (!type) { resultEl.innerHTML = '<div class="test-result err">Pick a registry type</div>'; return; }
    const payload = {
      type,
      url: overlay.querySelector('#ro-url').value.trim(),
      authType: overlay.querySelector('#ro-user').value || overlay.querySelector('#ro-pass').value ? 'BASIC' : 'NONE',
      username: overlay.querySelector('#ro-user').value.trim() || null,
      password: overlay.querySelector('#ro-pass').value || null,
    };
    const result = await post('/registries/test', payload).catch((e) => ({ ok: false, error: e.message }));
    resultEl.innerHTML = result.ok
      ? `<div class="test-result ok">✓ Registry reachable — ${result.subjectCount} subject(s)</div>`
      : `<div class="test-result err">✗ ${esc(result.error || 'unreachable')}</div>`;
  }
}
