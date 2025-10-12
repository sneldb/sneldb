(function () {
  function sneldb(hljs) {
    // Case-insensitive keyword matching using character classes
    const KW_RE = /\b(?:[Dd][Ee][Ff][Ii][Nn][Ee]|[Aa][Ss]|[Ff][Ii][Ee][Ll][Dd][Ss]|[Ss][Tt][Oo][Rr][Ee]|[Ff][Oo][Rr]|[Pp][Aa][Yy][Ll][Oo][Aa][Dd]|[Qq][Uu][Ee][Rr][Yy]|[Ff][Ii][Nn][Dd]|[Rr][Ee][Pp][Ll][Aa][Yy]|[Pp][Ii][Nn][Gg]|[Ff][Ll][Uu][Ss][Hh]|[Ss][Ii][Nn][Cc][Ee]|[Ww][Hh][Ee][Rr][Ee]|[Ll][Ii][Mm][Ii][Tt]|[Oo][Ff][Ff][Ss][Ee][Tt]|[Oo][Rr][Dd][Ee][Rr]|[Aa][Nn][Dd]|[Oo][Rr]|[Nn][Oo][Tt]|[Rr][Ee][Tt][Uu][Rr][Nn]|[Ll][Ii][Nn][Kk][Ee][Dd]|[Uu][Ss][Ii][Nn][Gg]|[Pp][Ee][Rr]|[Bb][Yy]|[Ff][Oo][Ll][Ll][Oo][Ww][Ee][Dd]|[Pp][Rr][Ee][Cc][Ee][Dd][Ee][Dd]|[Cc][Oo][Uu][Nn][Tt]|[Uu][Nn][Ii][Qq][Uu][Ee]|[Tt][Oo][Tt][Aa][Ll]|[Aa][Vv][Gg]|[Mm][Ii][Nn]|[Mm][Aa][Xx]|[Hh][Oo][Uu][Rr]|[Dd][Aa][Yy]|[Ww][Ee][Ee][Kk]|[Mm][Oo][Nn][Tt][Hh]|[Aa][Ss][Cc]|[Dd][Ee][Ss][Cc])\b/;

    const CMD_RE = /\b(?:[Dd][Ee][Ff][Ii][Nn][Ee]|[Ss][Tt][Oo][Rr][Ee]|[Qq][Uu][Ee][Rr][Yy]|[Ff][Ii][Nn][Dd]|[Rr][Ee][Pp][Ll][Aa][Yy]|[Pp][Ii][Nn][Gg]|[Ff][Ll][Uu][Ss][Hh])\b\s+/;

    return {
      name: 'SnelDB',
      aliases: ['sneldb'],
      contains: [
        hljs.QUOTE_STRING_MODE,
        { className: 'attr', begin: /"[^"\\]*(\\.[^"\\]*)*(?=\s*:\s*)/, relevance: 10 },
        { className: 'attr', begin: /\b[A-Za-z][A-Za-z0-9_.-]*\b(?=\s*:\s*)/, relevance: 5 },
        { className: 'number', begin: /\b\d+(?:\.\d+)?\b/ },
        { className: 'operator', begin: /!=|>=|<=|=|>|</ },
        { className: 'punctuation', begin: /[{}\[\],:]/ },
        { className: 'keyword', begin: KW_RE },
        {
          begin: CMD_RE, end: /\s|$|[{"]/, excludeBegin: true, excludeEnd: true,
          contains: [ { className: 'event-type', begin: /(?![Dd][Ee][Ff][Ii][Nn][Ee]|[Aa][Ss]|[Ff][Ii][Ee][Ll][Dd][Ss]|[Ss][Tt][Oo][Rr][Ee]|[Ff][Oo][Rr]|[Pp][Aa][Yy][Ll][Oo][Aa][Dd]|[Qq][Uu][Ee][Rr][Yy]|[Ff][Ii][Nn][Dd]|[Rr][Ee][Pp][Ll][Aa][Yy]|[Pp][Ii][Nn][Gg]|[Ff][Ll][Uu][Ss][Hh]|[Ss][Ii][Nn][Cc][Ee]|[Ww][Hh][Ee][Rr][Ee]|[Ll][Ii][Mm][Ii][Tt]|[Oo][Ff][Ff][Ss][Ee][Tt]|[Oo][Rr][Dd][Ee][Rr]|[Aa][Nn][Dd]|[Oo][Rr]|[Nn][Oo][Tt]|[Rr][Ee][Tt][Uu][Rr][Nn]|[Ll][Ii][Nn][Kk][Ee][Dd]|[Uu][Ss][Ii][Nn][Gg]|[Pp][Ee][Rr]|[Bb][Yy]|[Ff][Oo][Ll][Ll][Oo][Ww][Ee][Dd]|[Pp][Rr][Ee][Cc][Ee][Dd][Ee][Dd]|[Cc][Oo][Uu][Nn][Tt]|[Uu][Nn][Ii][Qq][Uu][Ee]|[Tt][Oo][Tt][Aa][Ll]|[Aa][Vv][Gg]|[Mm][Ii][Nn]|[Mm][Aa][Xx]|[Hh][Oo][Uu][Rr]|[Dd][Aa][Yy]|[Ww][Ee][Ee][Kk]|[Mm][Oo][Nn][Tt][Hh]|[Aa][Ss][Cc]|[Dd][Ee][Ss][Cc]|[Tt][Rr][Uu][Ee]|[Ff][Aa][Ll][Ss][Ee]|[Nn][Uu][Ll][Ll])\b[A-Za-z][A-Za-z0-9_.-]*\b/ } ]
        },
        { className: 'identifier', begin: /(?![Dd][Ee][Ff][Ii][Nn][Ee]|[Aa][Ss]|[Ff][Ii][Ee][Ll][Dd][Ss]|[Ss][Tt][Oo][Rr][Ee]|[Ff][Oo][Rr]|[Pp][Aa][Yy][Ll][Oo][Aa][Dd]|[Qq][Uu][Ee][Rr][Yy]|[Ff][Ii][Nn][Dd]|[Rr][Ee][Pp][Ll][Aa][Yy]|[Pp][Ii][Nn][Gg]|[Ff][Ll][Uu][Ss][Hh]|[Ss][Ii][Nn][Cc][Ee]|[Ww][Hh][Ee][Rr][Ee]|[Ll][Ii][Mm][Ii][Tt]|[Oo][Ff][Ff][Ss][Ee][Tt]|[Oo][Rr][Dd][Ee][Rr]|[Aa][Nn][Dd]|[Oo][Rr]|[Nn][Oo][Tt]|[Rr][Ee][Tt][Uu][Rr][Nn]|[Ll][Ii][Nn][Kk][Ee][Dd]|[Uu][Ss][Ii][Nn][Gg]|[Pp][Ee][Rr]|[Bb][Yy]|[Ff][Oo][Ll][Ll][Oo][Ww][Ee][Dd]|[Pp][Rr][Ee][Cc][Ee][Dd][Ee][Dd]|[Cc][Oo][Uu][Nn][Tt]|[Uu][Nn][Ii][Qq][Uu][Ee]|[Tt][Oo][Tt][Aa][Ll]|[Aa][Vv][Gg]|[Mm][Ii][Nn]|[Mm][Aa][Xx]|[Hh][Oo][Uu][Rr]|[Dd][Aa][Yy]|[Ww][Ee][Ee][Kk]|[Mm][Oo][Nn][Tt][Hh]|[Aa][Ss][Cc]|[Dd][Ee][Ss][Cc]|[Tt][Rr][Uu][Ee]|[Ff][Aa][Ll][Ss][Ee]|[Nn][Uu][Ll][Ll])\b[A-Za-z][A-Za-z0-9_.-]*\b/, relevance: 0 }
      ]
    };
  }

  if (typeof window !== 'undefined' && window.hljs) {
    window.hljs.registerLanguage('sneldb', sneldb);
  } else if (typeof module !== 'undefined') {
    module.exports = sneldb;
  }
})();


