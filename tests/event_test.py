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
    assert event._flag is True


@pytest.mark.asyncio
async def test_event_wait_returns_immediately_after_set():
    event = Event()

    event.set()

    await asyncio.wait_for(event.wait(), timeout=0.5)
    assert event._flag is True


@pytest.mark.asyncio
async def test_event_set_releases_all_waiters():
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
    first_future = event._future
    event.set()

    assert event._flag is True
    assert first_future.done() is True
    assert event._future is not first_future
    assert event._future.done() is True
    await asyncio.wait_for(event.wait(), timeout=0.5)


@pytest.mark.asyncio
async def test_event_clear_makes_future_waiters_block_again():
    event = Event()

    event.set()
    await asyncio.wait_for(event.wait(), timeout=0.5)

    await event.clear()
    waiter_task = asyncio.create_task(event.wait())
    await asyncio.sleep(0.03)

    assert event._flag is False
    assert waiter_task.done() is False

    event.set()
    await asyncio.wait_for(waiter_task, timeout=0.5)


@pytest.mark.asyncio
async def test_event_clear_replaces_completed_future():
    event = Event()

    old_future = event._future
    event.set()
    await event.clear()

    assert event._flag is False
    assert event._future is not old_future
    assert event._future.done() is False


@pytest.mark.asyncio
async def test_event_clear_keeps_current_waiters_waiting_for_next_set():
    event = Event()
    results = []

    async def waiter():
        await event.wait()
        results.append("released")

    waiter_task = asyncio.create_task(waiter())
    await asyncio.sleep(0.03)

    current_future = event._future
    await event.clear()
    await asyncio.sleep(0.03)

    assert event._future is current_future
    assert waiter_task.done() is False
    assert results == []

    event.set()
    await asyncio.wait_for(waiter_task, timeout=0.5)

    assert results == ["released"]


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
    await event.clear()

    second_waiter = asyncio.create_task(waiter("second"))
    await asyncio.sleep(0.03)

    assert second_waiter.done() is False

    event.set()
    await asyncio.wait_for(second_waiter, timeout=0.5)

    assert results == ["first", "second"]


@pytest.mark.asyncio
async def test_event_cancelled_wait_does_not_prevent_later_set():
    event = Event()

    waiter_task = asyncio.create_task(event.wait())
    await asyncio.sleep(0.03)

    assert waiter_task.done() is False

    waiter_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter_task

    event.set()

    assert event._flag is True
    await asyncio.wait_for(event.wait(), timeout=0.5)


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
