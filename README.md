# System Design Notes

Personal learning notes on backend system design, with runnable-shaped code examples.

Stack used in examples: **FastAPI + MySQL + Redis**.

## Contents

- [Caching Strategies](docs/caching-strategies.md)

  Organized around two orthogonal axes rather than a flat pattern list:

  - **Part I — Strategies.** Read axis (cache-aside vs. read-through) × write axis (write-through, write-behind, write-around), plus how they combine. The unit of choice is the entity, not the service.
  - **Part II — Failure modes.** Stampede, hot keys, invalidation & eviction, negative caching. Not strategies — these arrive regardless of which cell of the matrix you picked.
  - **Part III — Reference.** Comparison tables and a decision guide.
