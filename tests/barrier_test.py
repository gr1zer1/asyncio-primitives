import asyncio
import inspect

import pytest

from asyncio_primitives import Barrier


async def wait_for_barrier(barrier: Barrier, timeout: float = 0.5):
    return await asyncio.wait_for(barrier.wait(), timeout=timeout)


@pytest.mark.asyncio
async def test_barrier_has_public_wait_coroutine():
    barrier = Barrier(1)

    assert hasattr(barrier, "wait")
    assert inspect.iscoroutinefunction(barrier.wait)


@pytest.mark.asyncio
async def test_barrier_rejects_non_positive_capacity():
    with pytest.raises(ValueError):
        Barrier(0)

    with pytest.raises(ValueError):
        Barrier(-1)


@pytest.mark.asyncio
async def test_barrier_capacity_one_passes_immediately():
    barrier = Barrier(1)

    await wait_for_barrier(barrier)

    assert barrier._coroutines == 0


@pytest.mark.asyncio
async def test_barrier_waits_until_all_parties_arrive():
    barrier = Barrier(3)
    passed = []

    async def worker(number):
        passed.append(f"before_{number}")
        await barrier.wait()
        passed.append(f"after_{number}")

    first = asyncio.create_task(worker(1))
    second = asyncio.create_task(worker(2))

    await asyncio.sleep(0.03)

    assert not first.done()
    assert not second.done()
    assert passed == ["before_1", "before_2"]
    assert barrier._coroutines == 2

    third = asyncio.create_task(worker(3))
    await asyncio.wait_for(asyncio.gather(first, second, third), timeout=0.5)

    assert passed[:3] == ["before_1", "before_2", "before_3"]
    assert sorted(passed[3:]) == ["after_1", "after_2", "after_3"]
    assert barrier._coroutines == 0


@pytest.mark.asyncio
async def test_barrier_can_be_reused_for_multiple_generations():
    barrier = Barrier(2)
    events = []

    async def worker(generation, number):
        events.append(f"before_{generation}_{number}")
        await barrier.wait()
        events.append(f"after_{generation}_{number}")

    for generation in range(3):
        await asyncio.wait_for(
            asyncio.gather(
                worker(generation, 1),
                worker(generation, 2),
            ),
            timeout=0.5,
        )

    for generation in range(3):
        before = [f"before_{generation}_1", f"before_{generation}_2"]
        after = [f"after_{generation}_1", f"after_{generation}_2"]

        assert all(event in events for event in before)
        assert all(event in events for event in after)
        assert max(events.index(event) for event in before) < min(
            events.index(event) for event in after
        )

    assert len(events) == 12
    assert barrier._coroutines == 0


@pytest.mark.asyncio
async def test_barrier_keeps_extra_task_waiting_for_next_generation():
    barrier = Barrier(2)
    events = []

    async def worker(number):
        events.append(f"before_{number}")
        await barrier.wait()
        events.append(f"after_{number}")

    first = asyncio.create_task(worker(1))
    second = asyncio.create_task(worker(2))

    await asyncio.wait_for(asyncio.gather(first, second), timeout=0.5)

    third = asyncio.create_task(worker(3))
    await asyncio.sleep(0.03)

    assert third.done() is False
    assert sorted(events) == ["after_1", "after_2", "before_1", "before_2", "before_3"]
    assert barrier._coroutines == 1

    fourth = asyncio.create_task(worker(4))
    await asyncio.wait_for(asyncio.gather(third, fourth), timeout=0.5)

    assert sorted(events) == [
        "after_1",
        "after_2",
        "after_3",
        "after_4",
        "before_1",
        "before_2",
        "before_3",
        "before_4",
    ]
    assert barrier._coroutines == 0


@pytest.mark.asyncio
async def test_barrier_async_context_manager_waits_on_enter():
    barrier = Barrier(2)
    events = []

    async def worker(number):
        events.append(f"before_{number}")
        async with barrier:
            events.append(f"inside_{number}")
        events.append(f"after_{number}")

    first = asyncio.create_task(worker(1))
    await asyncio.sleep(0.03)

    assert first.done() is False
    assert events == ["before_1"]

    second = asyncio.create_task(worker(2))
    await asyncio.wait_for(asyncio.gather(first, second), timeout=0.5)

    assert events[:2] == ["before_1", "before_2"]
    assert sorted(events[2:]) == [
        "after_1",
        "after_2",
        "inside_1",
        "inside_2",
    ]
    assert barrier._coroutines == 0


@pytest.mark.asyncio
async def test_barrier_releases_all_waiters_under_concurrency_stress():
    parties = 5
    rounds = 10
    barrier = Barrier(5)
    completed = []

    async def worker(number):
        for round_number in range(rounds):
            await barrier.wait()
            completed.append((round_number, number))

    await asyncio.wait_for(
        asyncio.gather(*(worker(number) for number in range(parties))),
        timeout=2.0,
    )

    assert len(completed) == parties * rounds
    for round_number in range(rounds):
        assert sorted(
            number for completed_round, number in completed
            if completed_round == round_number
        ) == list(range(parties))
    assert barrier._coroutines == 0


@pytest.mark.asyncio
async def test_barrier_cancelled_waiter_does_not_leave_stale_count():
    barrier = Barrier(3)

    first = asyncio.create_task(barrier.wait())
    second = asyncio.create_task(barrier.wait())

    await asyncio.sleep(0.03)
    assert barrier._coroutines == 2

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    await asyncio.sleep(0.03)
    assert barrier._coroutines == 1
    assert second.done() is False

    third = asyncio.create_task(barrier.wait())
    fourth = asyncio.create_task(barrier.wait())

    await asyncio.wait_for(asyncio.gather(second, third, fourth), timeout=0.5)
    assert barrier._coroutines == 0


@pytest.mark.asyncio
async def test_barrier_cancelled_generation_can_be_reused():
    barrier = Barrier(2)

    first = asyncio.create_task(barrier.wait())
    await asyncio.sleep(0.03)

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    assert barrier._coroutines == 0

    await asyncio.wait_for(
        asyncio.gather(barrier.wait(), barrier.wait()),
        timeout=0.5,
    )
    assert barrier._coroutines == 0
