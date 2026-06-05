import asyncio
import inspect

import pytest

import asyncio_primitives
from asyncio_primitives.countdown_latch import CountdownLatch


async def wait_for_latch(latch: CountdownLatch, timeout: float = 0.5):
    return await asyncio.wait_for(latch.wait(), timeout=timeout)


@pytest.mark.asyncio
async def test_countdown_latch_is_exported_from_package():
    assert asyncio_primitives.CountdownLatch is CountdownLatch


@pytest.mark.asyncio
async def test_countdown_latch_has_public_wait_coroutine():
    latch = CountdownLatch()

    assert hasattr(latch, "wait")
    assert inspect.iscoroutinefunction(latch.wait)


@pytest.mark.asyncio
async def test_countdown_latch_has_public_countdown_coroutine():
    latch = CountdownLatch()

    assert hasattr(latch, "countdown")
    assert inspect.iscoroutinefunction(latch.countdown)


@pytest.mark.asyncio
async def test_countdown_latch_rejects_non_positive_count():
    with pytest.raises(ValueError):
        CountdownLatch(0)

    with pytest.raises(ValueError):
        CountdownLatch(-1)


@pytest.mark.asyncio
async def test_countdown_latch_defaults_to_one():
    latch = CountdownLatch()

    assert latch.count == 1


@pytest.mark.asyncio
async def test_countdown_latch_exposes_current_count():
    latch = CountdownLatch(3)

    assert latch.count == 3

    await latch.countdown()

    assert latch.count == 2


@pytest.mark.asyncio
async def test_countdown_latch_wait_blocks_until_count_reaches_zero():
    latch = CountdownLatch(2)
    events = []

    async def waiter():
        events.append("waiting")
        await latch.wait()
        events.append("released")

    waiter_task = asyncio.create_task(waiter())
    await asyncio.sleep(0.03)

    assert waiter_task.done() is False
    assert events == ["waiting"]

    await latch.countdown()
    await asyncio.sleep(0.03)

    assert latch.count == 1
    assert waiter_task.done() is False
    assert events == ["waiting"]

    await latch.countdown()
    await asyncio.wait_for(waiter_task, timeout=0.5)

    assert latch.count == 0
    assert events == ["waiting", "released"]


@pytest.mark.asyncio
async def test_countdown_latch_countdown_to_zero_releases_all_waiters():
    latch = CountdownLatch(3)
    released = []

    async def waiter(number):
        await latch.wait()
        released.append(number)

    waiters = [asyncio.create_task(waiter(number)) for number in range(5)]
    await asyncio.sleep(0.03)

    assert all(task.done() is False for task in waiters)

    await latch.countdown()
    await latch.countdown()
    await asyncio.sleep(0.03)

    assert released == []
    assert all(task.done() is False for task in waiters)

    await latch.countdown()
    await asyncio.wait_for(asyncio.gather(*waiters), timeout=0.5)

    assert sorted(released) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_countdown_latch_wait_returns_immediately_after_count_reaches_zero():
    latch = CountdownLatch(1)

    await latch.countdown()

    await wait_for_latch(latch)


@pytest.mark.asyncio
async def test_countdown_latch_waiters_created_after_zero_do_not_block():
    latch = CountdownLatch(1)
    released = []

    await latch.countdown()

    async def waiter(number):
        await latch.wait()
        released.append(number)

    await asyncio.wait_for(
        asyncio.gather(*(waiter(number) for number in range(5))),
        timeout=0.5,
    )

    assert sorted(released) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_countdown_latch_concurrent_countdowns_release_waiters_once_zero():
    count = 10
    latch = CountdownLatch(count)
    released = []

    async def waiter():
        await latch.wait()
        released.append("released")

    async def worker():
        await asyncio.sleep(0)
        await latch.countdown()

    waiter_task = asyncio.create_task(waiter())
    await asyncio.sleep(0.03)

    assert waiter_task.done() is False

    await asyncio.gather(*(worker() for _ in range(count - 1)))
    await asyncio.sleep(0.03)

    assert latch.count == 1
    assert waiter_task.done() is False

    await worker()
    await asyncio.wait_for(waiter_task, timeout=0.5)

    assert latch.count == 0
    assert released == ["released"]


@pytest.mark.asyncio
async def test_countdown_latch_cancelled_waiter_does_not_block_other_waiters():
    latch = CountdownLatch(1)
    released = []

    async def waiter(label):
        await latch.wait()
        released.append(label)

    cancelled_waiter = asyncio.create_task(waiter("cancelled"))
    remaining_waiter = asyncio.create_task(waiter("remaining"))
    await asyncio.sleep(0.03)

    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter

    assert remaining_waiter.done() is False

    await latch.countdown()
    await asyncio.wait_for(remaining_waiter, timeout=0.5)

    assert released == ["remaining"]


@pytest.mark.asyncio
async def test_countdown_latch_cancelled_wait_can_wait_again_later():
    latch = CountdownLatch(1)

    cancelled_waiter = asyncio.create_task(latch.wait())
    await asyncio.sleep(0.03)

    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter

    next_waiter = asyncio.create_task(latch.wait())
    await asyncio.sleep(0.03)

    assert next_waiter.done() is False

    await latch.countdown()
    await asyncio.wait_for(next_waiter, timeout=0.5)


@pytest.mark.asyncio
async def test_countdown_latch_extra_countdowns_after_zero_are_noops():
    latch = CountdownLatch(1)

    await latch.countdown()
    await latch.countdown()
    await latch.countdown()

    assert latch.count == 0
    await wait_for_latch(latch)
