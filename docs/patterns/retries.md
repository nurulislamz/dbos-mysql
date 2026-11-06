# Retries

> Bound how many times a step runs, which errors are worth retrying, how long you are willing to wait, and what a person can do when retries run out.

Retries are off unless you turn them on. The attempt count includes the first try. When the count runs out, the step saves `DBOSMaxStepRetriesExceeded` and raises it.

## Patterns in this section

- [Fixed number of retries](fixed-retries.md). Cap the number of attempts.
- [A time budget across retries](wall-time-retries.md). Cap the sum of every attempt. Not supported yet. A child workflow timeout is the workaround.
- [Errors that must not be retried](non-retryable.md). `should_retry` returns false for errors that will never succeed.
- [An error that sets the next wait](delayed-retry.md). Sleep for a delay the error reports, then try again. Not supported yet as a step option.
- [Fast retries, then slow ones](fast-slow-retries.md). A short interval first, then a long one, so a long outage does not exhaust the count.
- [Alert when retries pile up](retry-alerting.md). Increment a metric once `DBOS.step_status.current_attempt` passes a threshold.
- [Resume after a correction](resumable.md). After retries are exhausted, wait for a person to send a fixed input.

## Choosing a pattern

You know the most you will pay for one step: use a [fixed count](fixed-retries.md).

The business rule is a deadline, whatever the attempt count: use a [time budget](wall-time-retries.md).

Some errors are permanent: mark them [not retryable](non-retryable.md).

The service tells you when to come back: use a [delayed retry](delayed-retry.md).

A short blip should retry quickly, and a long outage should not give up: use [fast then slow](fast-slow-retries.md).

On-call should hear about it before the count runs out: [alert](retry-alerting.md).

A person can fix the input: [resume after a correction](resumable.md).
