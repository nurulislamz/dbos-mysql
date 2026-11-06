# External interaction

> Ask an outside system for status, keep a long step alive, start work later, receive a callback, or wait for a person.

The workflow stays durable. The outside system stays an ordinary API, a webhook, or a person sending a message.

## Patterns in this section

- [Polling an external service](polling.md). A short poll heartbeats inside one step. A long poll sleeps in the workflow between attempts.
- [Long-running steps](long-running-step.md). A step calls `DBOS.heartbeat()` so a stuck attempt is noticed. The next attempt cannot read what the heartbeat sent.
- [Delayed start](delayed-start.md). The workflow row exists now and starts after a delay.
- [Delayed callback](delayed-callback.md). Sleep, call out, and wait for the other side to send the result back.
- [Approval](approval.md). Block until a person approves or the wait times out.

## Choosing a pattern

You have to ask the other system whether it is done: [poll](polling.md).

The step itself runs for a long time and should be failed if it goes silent: a [long-running step](long-running-step.md).

The work should exist now and run later: a [delayed start](delayed-start.md).

The other system will call you back: a [delayed callback](delayed-callback.md).

A person decides: an [approval](approval.md).
