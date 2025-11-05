# Running SnelDB

The easiest way to get hands-on is the embedded Playground.

- Start SnelDB (dev config enables the Playground by default):
  - `server.http_addr = "127.0.0.1:8085"`
  - `[playground] enabled = true`
- Open `http://127.0.0.1:8085/` in your browser.
- Type commands like:

```sneldb
DEFINE subscription FIELDS { "id": "int", "plan": "string" }
STORE subscription FOR ctx1 PAYLOAD {"id":1,"plan":"free"}
STORE subscription FOR ctx2 PAYLOAD {"id":2,"plan":"pro"}
QUERY subscription WHERE id=1
```

Notes

- The UI posts raw command lines to `POST /command` (no JSON API required).
- Set `server.output_format` to `text` (terminal-like), `json`, or `arrow` (Apache Arrow IPC stream).
- To disable the Playground, set `[playground] enabled = false`.
