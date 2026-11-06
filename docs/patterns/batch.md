# Batch processing

> Spread a large set of records across child workflows, and decide how many of those children run at once.

The parent, or a queue, is what keeps the set durable. A crash resumes the children that were already started and starts only the ones that were not saved.

## Patterns in this section

- [Fan-out](fan-out.md). One child per chunk, started together. Use it when you know the chunk count up front and you want them all in flight.
- [Batch iterator](batch-iterator.md). Page through a set that does not fit in one run. Not supported yet, because there is no continue-as-new.
- [Sliding window](sliding-window.md). Keep a fixed number of children running. The queue form holds that cap across every copy of the program.
- [Map-reduce](mapreduce.md). Split the set into a tree of child workflows, process the leaves, and fold the results back up.

## Choosing a pattern

You can count the chunks before you start, and the downstream service can take them all: [fan-out](fan-out.md).

The set is unbounded and one workflow must not store every page: a [batch iterator](batch-iterator.md). Today that means a new workflow ID per page.

You need a concurrency cap, especially across more than one process: a [sliding window](sliding-window.md).

You need a single aggregated result and the fan-out is wide enough to warrant a tree: [map-reduce](mapreduce.md).
