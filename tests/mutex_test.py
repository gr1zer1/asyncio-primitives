import asyncio

import pytest

from asyncio_primitives import Mutex


class Value:
    def __init__(self, value):
        self.value = value


@pytest.mark.asyncio
async def test_mutex_get_allows_mutating_wrapped_object():
    mutex = Mutex(Value(12))

    async with mutex.get() as guard:
        guard.value = 99

    async with mutex.get() as guard:
        assert guard.value == 99


@pytest.mark.asyncio
async def test_mutex_guard_can_replace_wrapped_object():
    mutex = Mutex(Value(12))

    async with mutex.get() as guard:
        guard.replace(Value(99))

    async with mutex.get() as guard:
        assert guard.value == 99


@pytest.mark.asyncio
async def test_mutex_get_is_exclusive():
    mutex = Mutex(Value(0))
    active_holders = 0
    max_active_holders = 0
    results = []

    async def worker(number):
        nonlocal active_holders, max_active_holders

        async with mutex.get() as guard:
            active_holders += 1
            max_active_holders = max(max_active_holders, active_holders)
            current = guard.value
            await asyncio.sleep(0.01)
            guard.value = current + 1
            results.append(number)
            active_holders -= 1

    await asyncio.gather(*(worker(i) for i in range(5)))

    assert len(results) == 5
    assert max_active_holders == 1

    async with mutex.get() as guard:
        assert guard.value == 5


@pytest.mark.asyncio
async def test_mutex_lock_protects_external_state():
    mutex = Mutex()
    state = []

    async def worker(number):
        async with mutex.lock():
            current = list(state)
            await asyncio.sleep(0.01)
            current.append(number)
            state[:] = current

    await asyncio.gather(*(worker(i) for i in range(5)))

    assert sorted(state) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_mutex_releases_after_exception():
    mutex = Mutex(Value(12))

    with pytest.raises(RuntimeError):
        async with mutex.get():
            raise RuntimeError("boom")

    async with mutex.get() as guard:
        guard.value = 99

    async with mutex.get() as guard:
        assert guard.value == 99


@pytest.mark.asyncio
async def test_mutex_guard_manual_acquire_and_close():
    mutex = Mutex(Value(12))
    guard = mutex.get()

    await guard.acquire()
    guard.value = 99
    await guard.close()

    async with mutex.get() as guard:
        assert guard.value == 99


@pytest.mark.asyncio
async def test_mutex_close_is_idempotent_when_unlocked():
    mutex = Mutex()

    await mutex.close()
    async with mutex.lock():
        assert mutex._counter == 1

    assert mutex._counter == 0
