# Resume after a correction

> When retries are exhausted, park the workflow and wait for a person to send a corrected input. Then continue from that point.

:::note[Supported]

Catch `DBOSMaxStepRetriesExceeded`, publish a status event, and `DBOS.recv` the correction. `recv` has a timeout, so loop when the wait should stay open.

:::

## Overview

A resumable step fails until a person can change the input. The workflow does not fail with it. It records that it is waiting, accepts one message with the fix, and calls the step again with that fix. Everything before the failure stays saved.

## Problem

A shipping step rejects an address the carrier does not serve. Retrying the same address will not help. Failing the workflow forces the caller to start over and can repeat the charge that already succeeded. The person who can fix the address is not in the process that ran the step.

## Solution

```python
from rust_dwos import DBOS, DBOSMaxStepRetriesExceeded


@DBOS.workflow()
def fulfil(order_id: str, address: str) -> str:
    charge(order_id)
    try:
        return ship(address)
    except DBOSMaxStepRetriesExceeded:
        DBOS.set_event("status", "AWAITING_CORRECTION")
        fixed = None
        while fixed is None:
            fixed = DBOS.recv("correction", timeout_seconds=86400)
        return ship(fixed)
```

The support tool sends the new address:

```python
DBOS.send(workflow_id, new_address, topic="correction", idempotency_key=f"{workflow_id}:address")
```

`ship` is a step. The first call's failure is saved, so recovery does not ship the bad address again. The second call is a new step, with the corrected address.

## When to use

Use a parked workflow when a person can supply a better input and the work already done should stand.

Use [approval](approval.md) when the person is accepting or rejecting, not replacing an argument.

Let the workflow fail when nobody should be asked and the caller should see the error.

## Trade-offs

The run stays `PENDING` until the correction arrives or you cancel it. That is visible in the [UI](../production.md#see-your-workflows) and in `get_event(..., "status")`. It is also a run that occupies no CPU while it waits, and that will wait as long as the loop does.

## Pitfalls

- One `recv` returns `None` at the timeout. The loop is what keeps the workflow parked.
- A correction that arrives as `None` cannot be told apart from a timeout. Send the new address as a string or a dict.
- The second `ship` sits outside the `try`. A bad correction fails the workflow. Wrap that call in the same handler if a bad correction should park the workflow again.

## Related

- [Approval](approval.md)
- [Fixed number of retries](fixed-retries.md)
- [Entity workflow](entity-workflow.md)
