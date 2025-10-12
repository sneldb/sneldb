(function () {
  function sneldb(hljs) {
    // Case-insensitive keyword matching using character classes
    const KW_RE = /\b(?:[Dd][Ee][Ff][Ii][Nn][Ee]|[Aa][Ss]|[Ff][Ii][Ee][Ll][Dd][Ss]|[Ss][Tt][Oo][Rr][Ee]|[Ff][Oo][Rr]|[Pp][Aa][Yy][Ll][Oo][Aa][Dd]|[Qq][Uu][Ee][Rr][Yy]|[Ff][Ii][Nn][Dd]|[Rr][Ee][Pp][Ll][Aa][Yy]|[Pp][Ii][Nn][Gg]|[Ff][Ll][Uu][Ss][Hh]|[Ss][Ii][Nn][Cc][Ee]|[Ww][Hh][Ee][Rr][Ee]|[Ll][Ii][Mm][Ii][Tt]|[Oo][Ff][Ff][Ss][Ee][Tt]|[Oo][Rr][Dd][Ee][Rr]|[Aa][Nn][Dd]|[Oo][Rr]|[Nn][Oo][Tt]|[Rr][Ee][Tt][Uu][Rr][Nn]|[Ll][Ii][Nn][Kk][Ee][Dd]|[Uu][Ss][Ii][Nn][Gg]|[Pp][Ee][Rr]|[Bb][Yy]|[Ff][Oo][Ll][Ll][Oo][Ww][Ee][Dd]|[Pp][Rr][Ee][Cc][Ee][Dd][Ee][Dd]|[Cc][Oo][Uu][Nn][Tt]|[Uu][Nn][Ii][Qq][Uu][Ee]|[Tt][Oo][Tt][Aa][Ll]|[Aa][Vv][Gg]|[Mm][Ii][Nn]|[Mm][Aa][Xx]|[Hh][Oo][Uu][Rr]|[Dd][Aa][Yy]|[Ww][Ee][Ee][Kk]|[Mm][Oo][Nn][Tt][Hh]|[Aa][Ss][Cc]|[Dd][Ee][Ss][Cc])\b/;

    const CMD_RE = /\b(?:[Dd][Ee][Ff][Ii][Nn][Ee]|[Ss][Tt][Oo][Rr][Ee]|[Qq][Uu][Ee][Rr][Yy]|[Ff][Ii][Nn][Dd]|[Rr][Ee][Pp][Ll][Aa][Yy]|[Pp][Ii][Nn][Gg]|[Ff][Ll][Uu][Ss][Hh])\b\s+/;

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
          begin: CMD_RE,
          end: /\s|$|[{"]/,
          excludeBegin: true,
          excludeEnd: true,
          contains: [
            { className: 'event-type',
              begin: /(?![Dd][Ee][Ff][Ii][Nn][Ee]|[Aa][Ss]|[Ff][Ii][Ee][Ll][Dd][Ss]|[Ss][Tt][Oo][Rr][Ee]|[Ff][Oo][Rr]|[Pp][Aa][Yy][Ll][Oo][Aa][Dd]|[Qq][Uu][Ee][Rr][Yy]|[Ff][Ii][Nn][Dd]|[Rr][Ee][Pp][Ll][Aa][Yy]|[Pp][Ii][Nn][Gg]|[Ff][Ll][Uu][Ss][Hh]|[Ss][Ii][Nn][Cc][Ee]|[Ww][Hh][Ee][Rr][Ee]|[Ll][Ii][Mm][Ii][Tt]|[Oo][Ff][Ff][Ss][Ee][Tt]|[Oo][Rr][Dd][Ee][Rr]|[Aa][Nn][Dd]|[Oo][Rr]|[Nn][Oo][Tt]|[Rr][Ee][Tt][Uu][Rr][Nn]|[Ll][Ii][Nn][Kk][Ee][Dd]|[Uu][Ss][Ii][Nn][Gg]|[Pp][Ee][Rr]|[Bb][Yy]|[Ff][Oo][Ll][Ll][Oo][Ww][Ee][Dd]|[Pp][Rr][Ee][Cc][Ee][Dd][Ee][Dd]|[Cc][Oo][Uu][Nn][Tt]|[Uu][Nn][Ii][Qq][Uu][Ee]|[Tt][Oo][Tt][Aa][Ll]|[Aa][Vv][Gg]|[Mm][Ii][Nn]|[Mm][Aa][Xx]|[Hh][Oo][Uu][Rr]|[Dd][Aa][Yy]|[Ww][Ee][Ee][Kk]|[Mm][Oo][Nn][Tt][Hh]|[Aa][Ss][Cc]|[Dd][Ee][Ss][Cc]|[Tt][Rr][Uu][Ee]|[Ff][Aa][Ll][Ss][Ee]|[Nn][Uu][Ll][Ll])\b[A-Za-z][A-Za-z0-9_.-]*\b/
            }
          ]
        },

        // --- generic identifiers (NOT keywords or literals) ---
        {
          className: 'identifier',
          // negative lookahead prevents matching keywords/literals
          begin: /(?![Dd][Ee][Ff][Ii][Nn][Ee]|[Aa][Ss]|[Ff][Ii][Ee][Ll][Dd][Ss]|[Ss][Tt][Oo][Rr][Ee]|[Ff][Oo][Rr]|[Pp][Aa][Yy][Ll][Oo][Aa][Dd]|[Qq][Uu][Ee][Rr][Yy]|[Ff][Ii][Nn][Dd]|[Rr][Ee][Pp][Ll][Aa][Yy]|[Pp][Ii][Nn][Gg]|[Ff][Ll][Uu][Ss][Hh]|[Ss][Ii][Nn][Cc][Ee]|[Ww][Hh][Ee][Rr][Ee]|[Ll][Ii][Mm][Ii][Tt]|[Oo][Ff][Ff][Ss][Ee][Tt]|[Oo][Rr][Dd][Ee][Rr]|[Aa][Nn][Dd]|[Oo][Rr]|[Nn][Oo][Tt]|[Rr][Ee][Tt][Uu][Rr][Nn]|[Ll][Ii][Nn][Kk][Ee][Dd]|[Uu][Ss][Ii][Nn][Gg]|[Pp][Ee][Rr]|[Bb][Yy]|[Ff][Oo][Ll][Ll][Oo][Ww][Ee][Dd]|[Pp][Rr][Ee][Cc][Ee][Dd][Ee][Dd]|[Cc][Oo][Uu][Nn][Tt]|[Uu][Nn][Ii][Qq][Uu][Ee]|[Tt][Oo][Tt][Aa][Ll]|[Aa][Vv][Gg]|[Mm][Ii][Nn]|[Mm][Aa][Xx]|[Hh][Oo][Uu][Rr]|[Dd][Aa][Yy]|[Ww][Ee][Ee][Kk]|[Mm][Oo][Nn][Tt][Hh]|[Aa][Ss][Cc]|[Dd][Ee][Ss][Cc]|[Tt][Rr][Uu][Ee]|[Ff][Aa][Ll][Ss][Ee]|[Nn][Uu][Ll][Ll])\b[A-Za-z][A-Za-z0-9_.-]*\b/,
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
