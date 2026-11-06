# Messaging

> Start a workflow by sending to it, answer a caller, or collect a stream of events until the stream goes quiet.

These patterns carry data into a running workflow. A message is delivered once, in order, on a topic. An event is a named value anyone can read.

## Patterns in this section

- [Signal with start](signal-with-start.md). Start a workflow under a chosen ID if it is not already running, then send it a message.
- [Request and response](request-response.md). Validate a request, change workflow state, and return a result to the caller in one round trip. Not supported yet.
- [Event accumulator](event-accumulator.md). Collect messages from many senders and close the batch after a quiet period.

## Choosing a pattern

The caller has a command and the workflow should exist to receive it: use [signal with start](signal-with-start.md).

The caller needs the workflow's answer before it returns, with the request and the answer in one operation: that is [request and response](request-response.md). Today that is three calls.

Many producers emit events and the workflow should process them as a batch: use an [event accumulator](event-accumulator.md).
