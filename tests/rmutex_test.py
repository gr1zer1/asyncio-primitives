import asyncio

import pytest

from asyncio_primitives import RMutex


class Value:
    def __init__(self, value):
        self.value = value


@pytest.mark.asyncio
async def test_rmutex_get_allows_mutating_wrapped_object():
    mutex = RMutex(Value(12))

    async with mutex.get() as guard:
        guard.value = 99

    async with mutex.get() as guard:
        assert guard.value == 99


@pytest.mark.asyncio
async def test_rmutex_guard_can_replace_wrapped_object():
    mutex = RMutex(Value(12))

    async with mutex.get() as guard:
        guard.replace(Value(99))

    async with mutex.get() as guard:
        assert guard.value == 99


@pytest.mark.asyncio
async def test_rmutex_same_task_can_reenter_get():
    mutex = RMutex(Value(0))

    async with mutex.get() as first:
        assert mutex._counter == 1
        first.value += 1

        async with mutex.get() as second:
            assert mutex._counter == 2
            second.value += 1

        assert mutex._counter == 1
        first.value += 1

    assert mutex._counter == 0
    assert mutex._current_task is None

    async with mutex.get() as guard:
        assert guard.value == 3


@pytest.mark.asyncio
async def test_rmutex_same_task_can_reenter_lock():
    mutex = RMutex()
    state = []

    async with mutex.lock():
        assert mutex._counter == 1
        state.append("outer")

        async with mutex.lock():
            assert mutex._counter == 2
            state.append("inner")

        assert mutex._counter == 1
        state.append("outer_after_inner")

    assert state == ["outer", "inner", "outer_after_inner"]
    assert mutex._counter == 0
    assert mutex._current_task is None


@pytest.mark.asyncio
async def test_rmutex_reentrant_get_and_lock_share_ownership():
    mutex = RMutex(Value(0))

    async with mutex.get() as guard:
        assert mutex._counter == 1
        guard.value += 1

        async with mutex.lock():
            assert mutex._counter == 2
            guard.value += 1

        assert mutex._counter == 1

    assert mutex._counter == 0
    async with mutex.get() as guard:
        assert guard.value == 2


@pytest.mark.asyncio
async def test_rmutex_other_task_waits_until_all_reentrant_levels_are_released():
    mutex = RMutex(Value(0))
    events = []
    entered_nested = asyncio.Event()
    allow_outer_release = asyncio.Event()

    async def owner():
        async with mutex.get() as guard:
            events.append("owner_outer_in")
            guard.value = 1

            async with mutex.get() as nested:
                events.append("owner_inner_in")
                nested.value = 2
                entered_nested.set()
                await asyncio.sleep(0.02)

            events.append("owner_inner_out")
            await allow_outer_release.wait()
            events.append("owner_outer_out")

    async def waiter():
        await entered_nested.wait()
        events.append("waiter_waiting")

        async with mutex.get() as guard:
            events.append("waiter_in")
            assert guard.value == 2
            guard.value = 3

    owner_task = asyncio.create_task(owner())
    waiter_task = asyncio.create_task(waiter())

    await entered_nested.wait()
    await asyncio.sleep(0.04)

    assert events == [
        "owner_outer_in",
        "owner_inner_in",
        "waiter_waiting",
        "owner_inner_out",
    ]

    allow_outer_release.set()
    await asyncio.wait_for(asyncio.gather(owner_task, waiter_task), timeout=0.5)

    assert events == [
        "owner_outer_in",
        "owner_inner_in",
        "waiter_waiting",
        "owner_inner_out",
        "owner_outer_out",
        "waiter_in",
    ]
    async with mutex.get() as guard:
        assert guard.value == 3


@pytest.mark.asyncio
async def test_rmutex_is_exclusive_between_different_tasks():
    mutex = RMutex(Value(0))
    active_holders = 0
    max_active_holders = 0

    async def worker():
        nonlocal active_holders, max_active_holders

        async with mutex.get() as guard:
            active_holders += 1
            max_active_holders = max(max_active_holders, active_holders)
            current = guard.value
            await asyncio.sleep(0.01)
            guard.value = current + 1
            active_holders -= 1

    await asyncio.wait_for(
        asyncio.gather(*(worker() for _ in range(10))),
        timeout=1.0,
    )

    assert max_active_holders == 1
    async with mutex.get() as guard:
        assert guard.value == 10


@pytest.mark.asyncio
async def test_rmutex_lock_protects_external_state():
    mutex = RMutex()
    state = []

    async def worker(number):
        async with mutex.lock():
            current = list(state)
            await asyncio.sleep(0.01)
            current.append(number)
            state[:] = current

    await asyncio.wait_for(
        asyncio.gather(*(worker(i) for i in range(10))),
        timeout=1.0,
    )

    assert sorted(state) == list(range(10))


@pytest.mark.asyncio
async def test_rmutex_close_from_non_owner_does_not_unlock():
    mutex = RMutex(Value(0))
    owner_entered = asyncio.Event()
    release_owner = asyncio.Event()
    events = []

    async def owner():
        async with mutex.get() as guard:
            events.append("owner_in")
            guard.value = 1
            owner_entered.set()
            await release_owner.wait()
            events.append("owner_out")

    async def non_owner_close():
        await owner_entered.wait()
        await mutex.close()
        events.append("non_owner_close")

    async def waiter():
        await owner_entered.wait()
        async with mutex.get() as guard:
            events.append("waiter_in")
            guard.value = 2

    owner_task = asyncio.create_task(owner())
    close_task = asyncio.create_task(non_owner_close())
    waiter_task = asyncio.create_task(waiter())

    await owner_entered.wait()
    await close_task
    await asyncio.sleep(0.03)

    assert events == ["owner_in", "non_owner_close"]
    assert mutex._counter == 1

    release_owner.set()
    await asyncio.wait_for(asyncio.gather(owner_task, waiter_task), timeout=0.5)

    assert events == ["owner_in", "non_owner_close", "owner_out", "waiter_in"]
    async with mutex.get() as guard:
        assert guard.value == 2


@pytest.mark.asyncio
async def test_rmutex_releases_one_level_after_inner_exception():
    mutex = RMutex(Value(0))

    async with mutex.get() as guard:
        assert mutex._counter == 1

        with pytest.raises(RuntimeError):
            async with mutex.get():
                assert mutex._counter == 2
                raise RuntimeError("boom")

        assert mutex._counter == 1
        guard.value = 99

    assert mutex._counter == 0
    assert mutex._current_task is None

    async with mutex.get() as guard:
        assert guard.value == 99


@pytest.mark.asyncio
async def test_rmutex_releases_after_outer_exception():
    mutex = RMutex(Value(12))

    with pytest.raises(RuntimeError):
        async with mutex.get():
            raise RuntimeError("boom")

    assert mutex._counter == 0
    assert mutex._current_task is None

    async with mutex.get() as guard:
        guard.value = 99

    async with mutex.get() as guard:
        assert guard.value == 99


@pytest.mark.asyncio
async def test_rmutex_manual_acquire_and_close_are_reentrant():
    mutex = RMutex(Value(0))
    guard = mutex.get()

    await guard.acquire()
    assert mutex._counter == 1

    await guard.acquire()
    assert mutex._counter == 2

    guard.value = 99
    await guard.close()
    assert mutex._counter == 1
    assert mutex._current_task is asyncio.current_task()

    await guard.close()
    assert mutex._counter == 0
    assert mutex._current_task is None

    async with mutex.get() as next_guard:
        assert next_guard.value == 99


@pytest.mark.asyncio
async def test_rmutex_waiting_task_can_be_cancelled_without_taking_ownership():
    mutex = RMutex(Value(0))
    owner_entered = asyncio.Event()
    release_owner = asyncio.Event()
    waiter_started = asyncio.Event()

    async def owner():
        async with mutex.get() as guard:
            guard.value = 1
            owner_entered.set()
            await release_owner.wait()

    async def waiter():
        waiter_started.set()
        async with mutex.get() as guard:
            guard.value = 2

    owner_task = asyncio.create_task(owner())
    await owner_entered.wait()

    waiter_task = asyncio.create_task(waiter())
    await waiter_started.wait()
    await asyncio.sleep(0.03)

    waiter_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter_task

    assert mutex._counter == 1
    assert mutex._current_task is owner_task

    release_owner.set()
    await asyncio.wait_for(owner_task, timeout=0.5)

    assert mutex._counter == 0
    assert mutex._current_task is None
    async with mutex.get() as guard:
        assert guard.value == 1


@pytest.mark.asyncio
async def test_rmutex_close_is_idempotent_when_unlocked():
    mutex = RMutex()

    await mutex.close()
    await mutex.close()

    async with mutex.lock():
        assert mutex._counter == 1
        assert mutex._current_task is asyncio.current_task()

    assert mutex._counter == 0
    assert mutex._current_task is None
