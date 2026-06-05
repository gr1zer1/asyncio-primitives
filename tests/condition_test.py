import asyncio

import pytest

from asyncio_primitives import Condition, Lock


@pytest.mark.asyncio
async def test_lock_is_exclusive():
    lock = Lock()
    active_holders = 0
    max_active_holders = 0

    async def worker():
        nonlocal active_holders, max_active_holders

        async with lock:
            active_holders += 1
            max_active_holders = max(max_active_holders, active_holders)
            await asyncio.sleep(0.01)
            active_holders -= 1

    await asyncio.gather(*(worker() for _ in range(5)))

    assert max_active_holders == 1


@pytest.mark.asyncio
async def test_condition_notify_wakes_one_waiter():
    condition = Condition()
    ready = []

    async def waiter(number):
        async with condition:
            await condition.wait()
            ready.append(number)

    tasks = [asyncio.create_task(waiter(i)) for i in range(2)]
    await asyncio.sleep(0)

    async with condition:
        condition.notify()

    await asyncio.sleep(0)

    assert len(ready) == 1

    async with condition:
        condition.notify_all()

    await asyncio.gather(*tasks)
    assert sorted(ready) == [0, 1]


@pytest.mark.asyncio
async def test_condition_notify_all_wakes_all_waiters():
    condition = Condition()
    ready = []

    async def waiter(number):
        async with condition:
            await condition.wait()
            ready.append(number)

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0)

    async with condition:
        condition.notify_all()

    await asyncio.gather(*tasks)

    assert sorted(ready) == [0, 1, 2]


@pytest.mark.asyncio
async def test_condition_wait_reacquires_lock_before_returning():
    condition = Condition()
    locked_after_wait = False

    async def waiter():
        nonlocal locked_after_wait

        async with condition:
            await condition.wait()
            locked_after_wait = condition.locked()

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0)

    async with condition:
        condition.notify()

    await task

    assert locked_after_wait is True


@pytest.mark.asyncio
async def test_condition_requires_lock_for_wait_and_notify():
    condition = Condition()

    with pytest.raises(RuntimeError):
        await condition.wait()

    with pytest.raises(RuntimeError):
        condition.notify()
