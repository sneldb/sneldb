// theme/book.js
document.addEventListener('DOMContentLoaded', function () {
  var hl = window.hljs;
  if (hl) {
    document.querySelectorAll('pre code.language-sneldb').forEach(function (block) {
      if (typeof hl.highlightElement === 'function') {
        hl.highlightElement(block);
      } else if (typeof hl.highlightBlock === 'function') {
        hl.highlightBlock(block);
      } else if (typeof hl.highlight === 'function') {
        var res = hl.highlight(block.textContent, { language: 'sneldb', ignoreIllegals: true });
        block.innerHTML = res.value;
        block.classList.add('hljs');
      }
    });
  }

  var searchWrapper = document.getElementById('search-wrapper');
  var sidebar = document.querySelector('nav.sidebar');
  if (searchWrapper && sidebar) {
    searchWrapper.classList.remove('hidden');
    var scrollbox = sidebar.querySelector('.sidebar-scrollbox');
    if (scrollbox && searchWrapper.parentElement !== sidebar) {
      sidebar.insertBefore(searchWrapper, scrollbox);
    }

    var searchInput = searchWrapper.querySelector('#searchbar');
    if (searchInput) {
      searchInput.setAttribute('placeholder', 'Search documentation...');
    }
  }
});
