# System Design Notes

Personal learning notes on backend system design.

Code is **pseudo-code shaped like FastAPI + MySQL + Redis** — real route decorators, real Redis commands, real SQL, with the plumbing stripped out so the design decision stays visible. Runnable demos live in `demos/` (coming).

## Contents

- [Caching Strategies](docs/caching-strategies.md)

  Organized around two orthogonal axes rather than a flat pattern list:

  - **Part I — Strategies.** Read axis (cache-aside vs. read-through) × write axis (write-through, write-behind, write-around), plus how they combine. The unit of choice is the entity, not the service.
  - **Part II — Failure modes.** Stampede, hot keys, invalidation & eviction, negative caching. Not strategies — these arrive regardless of which cell of the matrix you picked.
  - **Part III — Reference.** Comparison tables and a decision guide.

  ![The two axes](docs/diagrams/two-axes.png)

## Diagrams

Sources are `.excalidraw` files in [`docs/diagrams/`](docs/diagrams) — open them at [excalidraw.com](https://excalidraw.com) to edit. The committed `.png` and `.svg` next to each are generated.
