import asyncio

import pytest

import asyncio_primitives
from asyncio_primitives.priority_queue import Instance, PriorityQueue


class NonComparableItem:
    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        raise AssertionError("queue must not compare stored items")


@pytest.mark.asyncio
async def test_priority_queue_is_exported_from_package():
    assert asyncio_primitives.PriorityQueue is PriorityQueue


@pytest.mark.asyncio
async def test_priority_queue_starts_empty():
    queue = PriorityQueue()

    assert queue.is_empty() is True
    assert queue._queue == []


@pytest.mark.asyncio
async def test_priority_queue_push_stores_instance_with_priority_and_item():
    queue = PriorityQueue()

    await queue.push("item", priority=10)

    assert queue.is_empty() is False
    assert queue._queue == [Instance(priority=10, item="item")]


@pytest.mark.asyncio
async def test_priority_queue_pop_returns_single_item_and_empties_queue():
    queue = PriorityQueue()

    await queue.push("item", priority=1)

    assert await asyncio.wait_for(queue.pop(), timeout=0.5) == "item"
    assert queue.is_empty() is True
    assert queue._queue == []


@pytest.mark.asyncio
async def test_priority_queue_pops_lowest_numeric_priority_first():
    queue = PriorityQueue()

    await queue.push("medium", priority=5)
    await queue.push("lowest", priority=-1)
    await queue.push("highest", priority=20)
    await queue.push("low", priority=0)

    assert await asyncio.wait_for(queue.pop(), timeout=0.5) == "lowest"
    assert await asyncio.wait_for(queue.pop(), timeout=0.5) == "low"
    assert await asyncio.wait_for(queue.pop(), timeout=0.5) == "medium"
    assert await asyncio.wait_for(queue.pop(), timeout=0.5) == "highest"
    assert queue.is_empty() is True


@pytest.mark.asyncio
async def test_priority_queue_allows_duplicate_priorities_without_comparing_items():
    queue = PriorityQueue()
    items = [NonComparableItem("first"), NonComparableItem("second")]

    await queue.push(items[0], priority=1)
    await queue.push(items[1], priority=1)

    popped = [
        await asyncio.wait_for(queue.pop(), timeout=0.5),
        await asyncio.wait_for(queue.pop(), timeout=0.5),
    ]

    assert sorted(item.value for item in popped) == ["first", "second"]
    assert queue.is_empty() is True


@pytest.mark.asyncio
async def test_priority_queue_pop_waits_until_item_is_available():
    queue = PriorityQueue()
    events = []

    async def consumer():
        events.append("consumer_waiting")
        item = await queue.pop()
        events.append(f"consumer_got_{item}")

    consumer_task = asyncio.create_task(consumer())
    await asyncio.sleep(0.03)

    assert consumer_task.done() is False
    assert events == ["consumer_waiting"]

    await queue.push("item", priority=1)
    await asyncio.wait_for(consumer_task, timeout=0.5)

    assert events == ["consumer_waiting", "consumer_got_item"]
    assert queue.is_empty() is True


@pytest.mark.asyncio
async def test_priority_queue_multiple_consumers_receive_one_item_each_by_priority():
    queue = PriorityQueue()
    results = []

    async def consumer(number):
        item = await queue.pop()
        results.append((number, item))

    consumers = [asyncio.create_task(consumer(number)) for number in range(3)]
    await asyncio.sleep(0.03)

    assert all(task.done() is False for task in consumers)

    await queue.push("last", priority=30)
    await queue.push("first", priority=10)
    await queue.push("second", priority=20)

    await asyncio.wait_for(asyncio.gather(*consumers), timeout=0.5)

    assert [item for _, item in sorted(results)] != []
    assert sorted(item for _, item in results) == ["first", "last", "second"]
    assert queue.is_empty() is True


@pytest.mark.asyncio
async def test_priority_queue_items_pushed_before_consumers_are_popped_in_priority_order():
    queue = PriorityQueue()
    results = []

    await queue.push("third", priority=3)
    await queue.push("first", priority=1)
    await queue.push("second", priority=2)

    async def consumer():
        results.append(await queue.pop())

    await asyncio.wait_for(
        asyncio.gather(*(consumer() for _ in range(3))),
        timeout=0.5,
    )

    assert results == ["first", "second", "third"]
    assert queue.is_empty() is True


@pytest.mark.asyncio
async def test_priority_queue_concurrent_producers_are_all_consumed_by_priority():
    queue = PriorityQueue()

    async def producer(item, priority):
        await queue.push(item, priority=priority)

    await asyncio.gather(
        producer("medium", 10),
        producer("first", 1),
        producer("last", 99),
        producer("second", 2),
    )

    assert [await asyncio.wait_for(queue.pop(), timeout=0.5) for _ in range(4)] == [
        "first",
        "second",
        "medium",
        "last",
    ]
    assert queue.is_empty() is True


@pytest.mark.asyncio
async def test_priority_queue_cancelled_pop_does_not_consume_future_item():
    queue = PriorityQueue()

    consumer = asyncio.create_task(queue.pop())
    await asyncio.sleep(0.03)

    assert consumer.done() is False

    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer

    await queue.push("item", priority=1)

    assert await asyncio.wait_for(queue.pop(), timeout=0.5) == "item"
    assert queue.is_empty() is True


@pytest.mark.asyncio
async def test_priority_queue_waiting_consumer_gets_highest_priority_available_at_wakeup():
    queue = PriorityQueue()
    results = []

    async def consumer():
        results.append(await queue.pop())

    consumer_task = asyncio.create_task(consumer())
    await asyncio.sleep(0.03)

    await queue.push("slow", priority=50)
    await queue.push("fast", priority=1)
    await asyncio.wait_for(consumer_task, timeout=0.5)

    assert results == ["fast"]
    assert await asyncio.wait_for(queue.pop(), timeout=0.5) == "slow"
    assert queue.is_empty() is True

