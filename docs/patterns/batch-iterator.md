# Batch iterator

> Page through a set that may be unbounded. Each page is durable, and no single run stores every page.

:::warning[Not supported yet]

Continue-as-new is what keeps a batch iterator on one workflow ID. There is no call for that. One run can page until its step list is too long, or each page can start the next page under a new ID.

:::

## Overview

A batch iterator walks a dataset that does not fit in one workflow history. It processes one page, keeps the cursor, and continues in a fresh run so recovery never replays the pages already finished.

The cursor has to be durable. The next run has to be started exactly once.

## Problem

An export walks every row newer than a cursor. Stuffing each page into one workflow works until recovery takes longer than the job. Starting the next page from a process-local loop loses the cursor if the process dies after the page is done and before the next start is recorded.

## Solution

Make "start the next page" the last saved step of this page. The next page has its own workflow ID.

```python
from rust_dwos import DBOS, SetWorkflowID


@DBOS.workflow()
def export_page(cursor: str | None) -> str | None:
    rows, next_cursor = load_page(cursor)
    for row in rows:
        write_row(row)
    if next_cursor is None:
        return cursor
    with SetWorkflowID(f"export-{next_cursor}"):
        DBOS.start_workflow(export_page, next_cursor)
    return next_cursor
```

`load_page` is a step, so replay sees the same `next_cursor` and the same child ID. A second start of that ID attaches to the run that already exists.

Callers follow the chain by workflow ID if they need the latest page. There is no single ID that stays open for the whole export.

## When to use

Use a new run per page when the set is larger than you want to replay and you can name each page by its cursor.

Use [fan-out](fan-out.md) when you know the chunks in advance and want them concurrent.

Use one workflow when the set is small enough that the step list stays cheap to recover.

## Trade-offs

Each page is a finished workflow plus a successor. The UI shows many runs. A crash resumes the page that was in progress and does not redo pages that already started their successor. You give up a single workflow ID for the whole job.

## Pitfalls

- Start the successor after the page's writes are saved, and only once. Starting it before `write_row` finishes can export the next page while this one is incomplete.
- The last page must not start another run. The `next_cursor is None` branch is that stop.
- Two exports that share a cursor value share a workflow ID. Include the export's own ID in the child ID if two exports can overlap.

## Related

- [Continue-as-new](continue-as-new.md)
- [Fan-out](fan-out.md)
- [Sliding window](sliding-window.md)
