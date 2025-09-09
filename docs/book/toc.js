// Populate the sidebar
//
// This is a script, and not included directly in the page, to control the total size of the book.
// The TOC contains an entry for each page, so if each page includes a copy of the TOC,
// the total size of the page becomes O(n**2).
class MDBookSidebarScrollbox extends HTMLElement {
    constructor() {
        super();
    }
    connectedCallback() {
        this.innerHTML = '<ol class="chapter"><li class="chapter-item expanded "><a href="quickstart.html"><strong aria-hidden="true">1.</strong> Quickstart</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="quickstart/installation.html"><strong aria-hidden="true">1.1.</strong> Installation</a></li><li class="chapter-item expanded "><a href="quickstart/running.html"><strong aria-hidden="true">1.2.</strong> Running SnelDB</a></li><li class="chapter-item expanded "><a href="quickstart/gentle_intro.html"><strong aria-hidden="true">1.3.</strong> A Gentle Guide</a></li><li class="chapter-item expanded "><a href="quickstart/philosophy.html"><strong aria-hidden="true">1.4.</strong> Design Philosophy</a></li></ol></li><li class="chapter-item expanded "><a href="introduction.html"><strong aria-hidden="true">2.</strong> Introduction</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="introduction/why_sneldb.html"><strong aria-hidden="true">2.1.</strong> Why SnelDB?</a></li><li class="chapter-item expanded "><a href="introduction/what_is_sneldb.html"><strong aria-hidden="true">2.2.</strong> What is SnelDB?</a></li><li class="chapter-item expanded "><a href="introduction/key_features.html"><strong aria-hidden="true">2.3.</strong> Key Features</a></li></ol></li><li class="chapter-item expanded "><a href="commands.html"><strong aria-hidden="true">3.</strong> Commands</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="commands/syntax.html"><strong aria-hidden="true">3.1.</strong> Syntax</a></li><li class="chapter-item expanded "><a href="commands/define.html"><strong aria-hidden="true">3.2.</strong> Define</a></li><li class="chapter-item expanded "><a href="commands/store.html"><strong aria-hidden="true">3.3.</strong> Store</a></li><li class="chapter-item expanded "><a href="commands/query.html"><strong aria-hidden="true">3.4.</strong> Query</a></li><li class="chapter-item expanded "><a href="commands/replay.html"><strong aria-hidden="true">3.5.</strong> Replay</a></li><li class="chapter-item expanded "><a href="commands/flush.html"><strong aria-hidden="true">3.6.</strong> Flush</a></li></ol></li><li class="chapter-item expanded "><a href="design.html"><strong aria-hidden="true">4.</strong> Design</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="design/overview.html"><strong aria-hidden="true">4.1.</strong> Overview</a></li><li class="chapter-item expanded "><a href="design/storage_engine.html"><strong aria-hidden="true">4.2.</strong> Storage Engine</a></li><li class="chapter-item expanded "><a href="design/query_replay.html"><strong aria-hidden="true">4.3.</strong> Query and Replay</a></li><li class="chapter-item expanded "><a href="design/sharding.html"><strong aria-hidden="true">4.4.</strong> Sharding</a></li><li class="chapter-item expanded "><a href="design/infrastructure.html"><strong aria-hidden="true">4.5.</strong> Infrastructure</a></li><li class="chapter-item expanded "><a href="design/compaction.html"><strong aria-hidden="true">4.6.</strong> Compaction</a></li><li class="chapter-item expanded "><a href="design/layering_strategy.html"><strong aria-hidden="true">4.7.</strong> Layering Strategy</a></li><li class="chapter-item expanded "><a href="design/threading_and_async.html"><strong aria-hidden="true">4.8.</strong> Threading and Async</a></li><li class="chapter-item expanded "><a href="design/logging.html"><strong aria-hidden="true">4.9.</strong> Logging</a></li><li class="chapter-item expanded "><a href="design/file_formats.html"><strong aria-hidden="true">4.10.</strong> File Formats and Data Layout</a></li></ol></li></ol>';
        // Set the current, active page, and reveal it if it's hidden
        let current_page = document.location.href.toString().split("#")[0].split("?")[0];
        if (current_page.endsWith("/")) {
            current_page += "index.html";
        }
        var links = Array.prototype.slice.call(this.querySelectorAll("a"));
        var l = links.length;
        for (var i = 0; i < l; ++i) {
            var link = links[i];
            var href = link.getAttribute("href");
            if (href && !href.startsWith("#") && !/^(?:[a-z+]+:)?\/\//.test(href)) {
                link.href = path_to_root + href;
            }
            // The "index" page is supposed to alias the first chapter in the book.
            if (link.href === current_page || (i === 0 && path_to_root === "" && current_page.endsWith("/index.html"))) {
                link.classList.add("active");
                var parent = link.parentElement;
                if (parent && parent.classList.contains("chapter-item")) {
                    parent.classList.add("expanded");
                }
                while (parent) {
                    if (parent.tagName === "LI" && parent.previousElementSibling) {
                        if (parent.previousElementSibling.classList.contains("chapter-item")) {
                            parent.previousElementSibling.classList.add("expanded");
                        }
                    }
                    parent = parent.parentElement;
                }
            }
        }
        // Track and set sidebar scroll position
        this.addEventListener('click', function(e) {
            if (e.target.tagName === 'A') {
                sessionStorage.setItem('sidebar-scroll', this.scrollTop);
            }
        }, { passive: true });
        var sidebarScrollTop = sessionStorage.getItem('sidebar-scroll');
        sessionStorage.removeItem('sidebar-scroll');
        if (sidebarScrollTop) {
            // preserve sidebar scroll position when navigating via links within sidebar
            this.scrollTop = sidebarScrollTop;
        } else {
            // scroll sidebar to current active section when navigating via "next/previous chapter" buttons
            var activeSection = document.querySelector('#sidebar .active');
            if (activeSection) {
                activeSection.scrollIntoView({ block: 'center' });
            }
        }
        // Toggle buttons
        var sidebarAnchorToggles = document.querySelectorAll('#sidebar a.toggle');
        function toggleSection(ev) {
            ev.currentTarget.parentElement.classList.toggle('expanded');
        }
        Array.from(sidebarAnchorToggles).forEach(function (el) {
            el.addEventListener('click', toggleSection);
        });
    }
}
window.customElements.define("mdbook-sidebar-scrollbox", MDBookSidebarScrollbox);
