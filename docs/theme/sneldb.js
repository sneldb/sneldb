(function () {
  function sneldb(hljs) {
    const IDENT = /[A-Za-z][A-Za-z0-9_.-]*/;
    const KW_RE = /\b(?:DEFINE|AS|FIELDS|STORE|FOR|PAYLOAD|QUERY|SINCE|WHERE|LIMIT|AND|OR|NOT|REPLAY)\b/;

    return {
      name: 'SnelDB',
      aliases: ['sneldb'],
      contains: [
        // strings
        hljs.QUOTE_STRING_MODE,

        // "key":
        { className: 'attr', begin: /"[^"\\]*(\\.[^"\\]*)*(?=\s*:)/, relevance: 10 },
        // unquoted key:
        { className: 'attr', begin: /\b[A-Za-z][A-Za-z0-9_.-]*\b(?=\s*:)/, relevance: 5 },

        // numbers
        { className: 'number', begin: /\b\d+(?:\.\d+)?\b/ },

        // operators & punctuation
        { className: 'operator',    begin: /!=|>=|<=|=|>|</ },
        { className: 'punctuation', begin: /[{}\[\],:]/ },

        // --- keywords (must come BEFORE identifiers) ---
        { className: 'keyword', begin: KW_RE },

        // --- event type right after verbs (optional: give it a special class) ---
        {
          // e.g. "DEFINE order_created" / "STORE order_created" / "QUERY order_created" / "REPLAY order_created"
          begin: /\b(?:DEFINE|STORE|QUERY|REPLAY)\b\s+/,
          end: /\s|$|[{"]/,
          excludeBegin: true,
          excludeEnd: true,
          contains: [
            { className: 'event-type',
              begin: /(?!DEFINE|AS|FIELDS|STORE|FOR|PAYLOAD|QUERY|SINCE|WHERE|LIMIT|AND|OR|NOT|REPLAY|true|false|null)\b[A-Za-z][A-Za-z0-9_.-]*\b/
            }
          ]
        },

        // --- generic identifiers (NOT keywords or literals) ---
        {
          className: 'identifier',
          // negative lookahead prevents matching keywords/literals
          begin: /(?!DEFINE|AS|FIELDS|STORE|FOR|PAYLOAD|QUERY|SINCE|WHERE|LIMIT|AND|OR|NOT|REPLAY|true|false|null)\b[A-Za-z][A-Za-z0-9_.-]*\b/,
          relevance: 0
        }
      ]
    };
  }

  if (typeof window !== 'undefined' && window.hljs) {
    window.hljs.registerLanguage('sneldb', sneldb);
  } else if (typeof module !== 'undefined') {
    module.exports = sneldb;
  }
})();