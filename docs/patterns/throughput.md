# Throughput

> Limit how fast work starts, run urgent work first, and stop one tenant from taking every slot.

These three controls live on a queue. They apply when a workflow is admitted, not to each step inside it.

## Patterns in this section

- [Rate limiting](rate-limiting.md). At most N starts per period, across every process that uses the queue.
- [Priority](priority.md). A lower priority number is admitted first. Work with no priority is admitted before work that has one.
- [Fairness](fairness.md). `partition_concurrency` caps how many workflows from one tenant run at once.

## Choosing a pattern

The downstream service allows a fixed number of calls per second: [rate limiting](rate-limiting.md).

Some jobs should jump the queue: [priority](priority.md).

One customer must not fill the queue during a burst: [fairness](fairness.md).

Use more than one of them on the same queue when you need all three.
