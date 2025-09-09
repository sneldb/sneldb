// theme/book.js
document.addEventListener('DOMContentLoaded', function () {
    var hl = window.hljs;
    if (!hl) return;

    // Optional: quick debug
    try { console.log('hljs version:', hl.versionString || '(unknown)'); } catch(e) {}

    document.querySelectorAll('pre code.language-sneldb').forEach(function (block) {
      if (typeof hl.highlightElement === 'function') {
        // hljs >= 11
        hl.highlightElement(block);
      } else if (typeof hl.highlightBlock === 'function') {
        // hljs <= 10
        hl.highlightBlock(block);
      } else if (typeof hl.highlight === 'function') {
        // very old fallback
        var res = hl.highlight(block.textContent, { language: 'sneldb', ignoreIllegals: true });
        block.innerHTML = res.value;
        block.classList.add('hljs');
      }
    });
  });