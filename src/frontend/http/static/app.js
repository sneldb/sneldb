(() => {
  const $ = (id) => document.getElementById(id);
  const cmd = $('command');
  const out = $('output');
  const runBtn = $('run');
  const clrBtn = $('clear');
  const fmt = $('fmt');
  const status = $('status');
  const hl = $('hl');

  function setStatus(text) { status.textContent = text; }
  function setOutput(text) {
    // Update output and (optionally) syntax highlight JSON
    if (fmt.checked && window.hljs) {
      try {
        const json = JSON.parse(text);
        const pretty = JSON.stringify(json, null, 2)
          .replaceAll('&', '&amp;')
          .replaceAll('<', '&lt;')
          .replaceAll('>', '&gt;');
        out.innerHTML = pretty;
        // decorate with highlight.js
        hljs.highlightElement(out);
        return;
      } catch (_) {
        // fall through to plain text
      }
    }
    out.textContent = text;
  }

  async function run() {
    const input = cmd.value.trim();
    if (!input) { setStatus('Empty command'); return; }
    setStatus('Running...');
    const headers = { 'Content-Type': 'text/plain' };
    if (window.SNELDB_AUTH_TOKEN) {
      headers['Authorization'] = 'Bearer ' + window.SNELDB_AUTH_TOKEN;
    }

    const lines = input.split(/\n+/).map(s => s.trim()).filter(Boolean);
    const outputs = [];
    for (const line of lines) {
      try {
        const res = await fetch('/command', { method: 'POST', headers, body: line });
        const text = await res.text();
        const rendered = fmt.checked ? (() => { try { return JSON.stringify(JSON.parse(text), null, 2); } catch { return text; } })() : text;
        outputs.push(rendered);
        if (!res.ok) { outputs.push(`-- Error ${res.status}`); break; }
      } catch (e) {
        outputs.push(String(e));
        break;
      }
    }
    setOutput(outputs.join('\n'));
    setStatus('Done');
  }

  runBtn.addEventListener('click', run);
  clrBtn.addEventListener('click', () => setOutput(''));
  cmd.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault(); run();
    }
  });

  function syncHighlight() {
    // Escape HTML then set code text
    const escaped = cmd.value
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;');
    hl.innerHTML = escaped;
    if (window.hljs) {
      if (typeof hljs.highlightElement === 'function') {
        hljs.highlightElement(hl);
      } else if (typeof hljs.highlightBlock === 'function') {
        hljs.highlightBlock(hl);
      }
    }
  }
  cmd.addEventListener('input', syncHighlight);
  // initialize highlighting on load
  syncHighlight();
})();


