# Distributed transactions

> Commit work that spans more than one service, or return the fast part of that work while the rest continues.

A workflow can run each call and remember that it ran. It cannot enlist those services in one database transaction. These patterns are how you still get a defined outcome.

## Patterns in this section

- [Saga](saga.md). Pair each step with a compensation, and run the compensations in reverse when a later step fails.
- [Early return](early-return.md). Hand the caller a result as soon as the first phase is saved, and keep processing after the caller has gone.

## Choosing a pattern

A later step can fail and the earlier steps must be undone: use a [saga](saga.md).

The caller only needs the first phase, and the rest can finish in the background: use [early return](early-return.md).
