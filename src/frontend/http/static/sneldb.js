(function () {
  function sneldb(hljs) {
    const IDENT = /[A-Za-z][A-Za-z0-9_.-]*/;
    const KW_RE = /\b(?:DEFINE|AS|FIELDS|STORE|FOR|PAYLOAD|QUERY|SINCE|WHERE|LIMIT|AND|OR|NOT|REPLAY)\b/;

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
          begin: /\b(?:DEFINE|STORE|QUERY|REPLAY)\b\s+/, end: /\s|$|[{"]/, excludeBegin: true, excludeEnd: true,
          contains: [ { className: 'event-type', begin: /(?!DEFINE|AS|FIELDS|STORE|FOR|PAYLOAD|QUERY|SINCE|WHERE|LIMIT|AND|OR|NOT|REPLAY|true|false|null)\b[A-Za-z][A-Za-z0-9_.-]*\b/ } ]
        },
        { className: 'identifier', begin: /(?!DEFINE|AS|FIELDS|STORE|FOR|PAYLOAD|QUERY|SINCE|WHERE|LIMIT|AND|OR|NOT|REPLAY|true|false|null)\b[A-Za-z][A-Za-z0-9_.-]*\b/, relevance: 0 }
      ]
    };
  }

  if (typeof window !== 'undefined' && window.hljs) {
    window.hljs.registerLanguage('sneldb', sneldb);
  } else if (typeof module !== 'undefined') {
    module.exports = sneldb;
  }
})();


