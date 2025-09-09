# SnelDB Design Philosophy

## Hassle-free by design

SnelDB is small on purpose. You don’t need to learn dozens of commands, fiddle with query planners, or manage indexes. Four simple verbs—`DEFINE`, `STORE`, `QUERY`, `REPLAY`—cover almost everything you need. Less to remember, less to break.

## Immutability at the core

Facts don’t change once written. Immutability makes your history reliable and auditable. If things evolve, you add new event types or new schema versions. Old events remain intact; new ones live alongside them.

## Evolution over correction

Rather than patching or rewriting, you let the story grow. Each new event is another page in the log. That makes timelines honest, reproducible, and easy to debug.

## Performance without knobs

SnelDB is built for performance, but you don’t need to manage any of it. Internally, it uses shards to spread load, an LSM-tree design to keep writes fast, and columnar storage with enum bitmaps and XOR filters to make queries efficient. You never have to tune these parts yourself—they just work in the background so you can focus on your application.

## Universal patterns

Two simple movements cover most use cases:

- **Replay** one context’s timeline to rebuild its state.
- **Query** across many contexts with filters.

This model is the same whether you’re preparing order data in retail, collecting device signals in IoT, managing subscriptions in SaaS, or feeding clean event streams into data and AI/ML teams for training and analysis.

## Staying in its lane

SnelDB doesn’t do business logic, aggregations, or access control. Those belong in your services and tools. The database’s job is to keep track of everything faithfully and give it back quickly.
