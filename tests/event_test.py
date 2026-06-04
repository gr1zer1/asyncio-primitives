import asyncio

import pytest

import asyncio_primitives
from asyncio_primitives.event import Event


@pytest.mark.asyncio
async def test_event_is_exported_from_package():
    assert asyncio_primitives.Event is Event


@pytest.mark.asyncio
async def test_event_wait_blocks_until_set():
    event = Event()
    events = []

    async def waiter():
        events.append("waiting")
        await event.wait()
        events.append("released")

    waiter_task = asyncio.create_task(waiter())
    await asyncio.sleep(0.03)

    assert waiter_task.done() is False
    assert events == ["waiting"]

    event.set()
    await asyncio.wait_for(waiter_task, timeout=0.5)

    assert events == ["waiting", "released"]


@pytest.mark.asyncio
async def test_event_wait_returns_immediately_after_set():
    event = Event()

    event.set()

    await asyncio.wait_for(event.wait(), timeout=0.5)


@pytest.mark.asyncio
async def test_event_set_releases_all_current_waiters():
    event = Event()
    results = []

    async def waiter(number):
        await event.wait()
        results.append(number)

    waiters = [asyncio.create_task(waiter(number)) for number in range(5)]
    await asyncio.sleep(0.03)

    assert all(task.done() is False for task in waiters)

    event.set()
    await asyncio.wait_for(asyncio.gather(*waiters), timeout=0.5)

    assert sorted(results) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_event_set_is_idempotent():
    event = Event()

    event.set()
    event.set()

    await asyncio.wait_for(event.wait(), timeout=0.5)


@pytest.mark.asyncio
async def test_event_clear_resets_event_for_future_waiters():
    event = Event()
    events = []

    event.set()
    await asyncio.wait_for(event.wait(), timeout=0.5)

    event.clear()

    async def waiter():
        events.append("waiting")
        await event.wait()
        events.append("released")

    waiter_task = asyncio.create_task(waiter())
    await asyncio.sleep(0.03)

    assert waiter_task.done() is False
    assert events == ["waiting"]

    event.set()
    await asyncio.wait_for(waiter_task, timeout=0.5)

    assert events == ["waiting", "released"]


@pytest.mark.asyncio
async def test_event_can_be_reused_across_set_clear_cycles():
    event = Event()
    results = []

    async def waiter(label):
        await event.wait()
        results.append(label)

    first_waiter = asyncio.create_task(waiter("first"))
    await asyncio.sleep(0.03)

    event.set()
    await asyncio.wait_for(first_waiter, timeout=0.5)

    event.clear()
    second_waiter = asyncio.create_task(waiter("second"))
    await asyncio.sleep(0.03)

    assert second_waiter.done() is False

    event.set()
    await asyncio.wait_for(second_waiter, timeout=0.5)

    assert results == ["first", "second"]


@pytest.mark.asyncio
async def test_event_waiters_created_after_set_do_not_block():
    event = Event()
    results = []

    event.set()

    async def waiter(number):
        await event.wait()
        results.append(number)

    await asyncio.wait_for(
        asyncio.gather(*(waiter(number) for number in range(3))),
        timeout=0.5,
    )

    assert sorted(results) == [0, 1, 2]


@pytest.mark.asyncio
async def test_event_cancelled_wait_does_not_prevent_later_waiters():
    event = Event()

    cancelled_waiter = asyncio.create_task(event.wait())
    await asyncio.sleep(0.03)

    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter

    next_waiter = asyncio.create_task(event.wait())
    await asyncio.sleep(0.03)

    assert next_waiter.done() is False

    event.set()
    await asyncio.wait_for(next_waiter, timeout=0.5)


@pytest.mark.asyncio
async def test_event_cancelled_waiter_does_not_cancel_other_waiters():
    event = Event()
    results = []

    async def waiter(label):
        await event.wait()
        results.append(label)

    cancelled_waiter = asyncio.create_task(waiter("cancelled"))
    remaining_waiter = asyncio.create_task(waiter("remaining"))
    await asyncio.sleep(0.03)

    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter

    assert remaining_waiter.done() is False

    event.set()
    await asyncio.wait_for(remaining_waiter, timeout=0.5)

    assert results == ["remaining"]

