# Adapted from dbos-transact-py tests/test_failures.py at commit 236142b. See MANIFEST.md.
import asyncio

import pytest

from rust_dwos import DBOS, DBOSException, DBOSMaxStepRetriesExceeded


class ShouldRetryFatalError(Exception):
    pass



class ShouldRetryRetryableError(Exception):
    pass



def test_step_should_retry(dbos: DBOS) -> None:
    # A step that fails with a non-retryable exception should bail out on the
    # first failure instead of exhausting retries.
    step_counter = 0
    max_attempts = 4

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=max_attempts,
        should_retry=lambda e: not isinstance(e, ShouldRetryFatalError),
    )
    def fatal_step() -> None:
        nonlocal step_counter
        step_counter += 1
        raise ShouldRetryFatalError("no retry")

    @DBOS.workflow()
    def fatal_workflow() -> None:
        fatal_step()

    # should_retry returns False -> the original exception propagates, not
    # DBOSMaxStepRetriesExceeded.
    with pytest.raises(ShouldRetryFatalError, match="no retry"):
        fatal_workflow()
    assert step_counter == 1

    # Sanity check: a retryable exception exhausts max_attempts as usual.
    retryable_counter = 0

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=max_attempts,
        should_retry=lambda e: not isinstance(e, ShouldRetryFatalError),
    )
    def retryable_step() -> None:
        nonlocal retryable_counter
        retryable_counter += 1
        raise ShouldRetryRetryableError("retry")

    @DBOS.workflow()
    def retryable_workflow() -> None:
        retryable_step()

    with pytest.raises(DBOSMaxStepRetriesExceeded):
        retryable_workflow()
    assert retryable_counter == max_attempts



async def test_step_should_retry_async_validator(dbos: DBOS) -> None:
    # Async step paired with an async validator: the validator is awaited,
    # and returning False short-circuits retries.
    step_counter = 0
    max_attempts = 4

    async def is_retryable(e: BaseException) -> bool:
        await asyncio.sleep(0)  # actually async
        return not isinstance(e, ShouldRetryFatalError)

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=max_attempts,
        should_retry=is_retryable,
    )
    async def fatal_step_async() -> None:
        nonlocal step_counter
        step_counter += 1
        raise ShouldRetryFatalError("no retry")

    @DBOS.workflow()
    async def wf() -> None:
        await fatal_step_async()

    with pytest.raises(ShouldRetryFatalError, match="no retry"):
        await wf()
    assert step_counter == 1

    # And an async validator that returns True still exhausts retries.
    retry_counter = 0

    async def always_retry(e: BaseException) -> bool:
        return True

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=max_attempts,
        should_retry=always_retry,
    )
    async def retry_step_async() -> None:
        nonlocal retry_counter
        retry_counter += 1
        raise Exception("retry")

    @DBOS.workflow()
    async def retry_wf() -> None:
        await retry_step_async()

    with pytest.raises(DBOSMaxStepRetriesExceeded):
        await retry_wf()
    assert retry_counter == max_attempts


def test_step_should_retry_sync_step_async_validator_rejected(dbos: DBOS) -> None:
    # A sync step paired with an async validator should be rejected when
    # invoked as a step, since the coroutine can't be awaited from a sync context.
    async def is_retryable(e: BaseException) -> bool:
        return True

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=2,
        should_retry=is_retryable,
    )
    def bad_step() -> None:
        raise Exception("boom")

    @DBOS.workflow()
    def wf() -> None:
        bad_step()

    with pytest.raises(Exception, match="sync but should_retry is async"):
        wf()


def test_step_should_retry_on_last_attempt(dbos: DBOS) -> None:
    # If should_retry returns False on the final attempt, the caught exception
    # should propagate instead of being wrapped as DBOSMaxStepRetriesExceeded.
    step_counter = 0
    max_attempts = 3

    @DBOS.step(
        retries_allowed=True,
        interval_seconds=0,
        max_attempts=max_attempts,
        should_retry=lambda e: not isinstance(e, ShouldRetryFatalError),
    )
    def mixed_step() -> None:
        nonlocal step_counter
        step_counter += 1
        if step_counter < max_attempts:
            raise Exception("retry me")
        raise ShouldRetryFatalError("stop now")

    @DBOS.workflow()
    def mixed_workflow() -> None:
        mixed_step()

    with pytest.raises(ShouldRetryFatalError, match="stop now"):
        mixed_workflow()
    assert step_counter == max_attempts



def test_sync_step_rejects_callback_awaitables(dbos: DBOS) -> None:
    async def is_retryable(error: BaseException) -> bool:
        return True

    @DBOS.step(retries_allowed=True, should_retry=lambda error: is_retryable(error))
    def bad_step() -> None:
        raise ValueError("boom")

    @DBOS.workflow()
    def workflow() -> None:
        bad_step()

    with pytest.raises(DBOSException, match="sync but should_retry is async"):
        workflow()
