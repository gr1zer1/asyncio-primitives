import asyncio

import pytest

import asyncio_primitives
from asyncio_primitives.bounded_queue import BoundedQueue


@pytest.mark.asyncio
async def test_bounded_queue_is_exported_from_package():
    assert asyncio_primitives.BoundedQueue is BoundedQueue


@pytest.mark.asyncio
async def test_bounded_queue_rejects_non_positive_capacity():
    with pytest.raises(ValueError):
        BoundedQueue(0)

    with pytest.raises(ValueError):
        BoundedQueue(-1)


@pytest.mark.asyncio
async def test_bounded_queue_put_and_get_single_item():
    queue = BoundedQueue(capacity=1)

    await queue.put("item")

    assert await asyncio.wait_for(queue.get(), timeout=0.5) == "item"
    assert len(queue._queue) == 0


@pytest.mark.asyncio
async def test_bounded_queue_preserves_fifo_order():
    queue = BoundedQueue(capacity=3)

    await queue.put("first")
    await queue.put("second")
    await queue.put("third")

    assert await asyncio.wait_for(queue.get(), timeout=0.5) == "first"
    assert await asyncio.wait_for(queue.get(), timeout=0.5) == "second"
    assert await asyncio.wait_for(queue.get(), timeout=0.5) == "third"


@pytest.mark.asyncio
async def test_bounded_queue_get_waits_until_item_is_available():
    queue = BoundedQueue(capacity=1)
    events = []

    async def consumer():
        events.append("consumer_waiting")
        item = await queue.get()
        events.append(f"consumer_got_{item}")

    consumer_task = asyncio.create_task(consumer())
    await asyncio.sleep(0.03)

    assert consumer_task.done() is False
    assert events == ["consumer_waiting"]

    await queue.put("item")
    await asyncio.wait_for(consumer_task, timeout=0.5)

    assert events == ["consumer_waiting", "consumer_got_item"]
    assert len(queue._queue) == 0


@pytest.mark.asyncio
async def test_bounded_queue_put_waits_when_queue_is_full():
    queue = BoundedQueue(capacity=1)
    events = []

    await queue.put("first")

    async def producer():
        events.append("producer_waiting")
        await queue.put("second")
        events.append("producer_done")

    producer_task = asyncio.create_task(producer())
    await asyncio.sleep(0.03)

    assert producer_task.done() is False
    assert events == ["producer_waiting"]
    assert list(queue._queue) == ["first"]

    assert await asyncio.wait_for(queue.get(), timeout=0.5) == "first"
    await asyncio.wait_for(producer_task, timeout=0.5)

    assert events == ["producer_waiting", "producer_done"]
    assert list(queue._queue) == ["second"]


@pytest.mark.asyncio
async def test_bounded_queue_multiple_consumers_receive_one_item_each():
    queue = BoundedQueue(capacity=3)
    results = []

    async def consumer(number):
        item = await queue.get()
        results.append((number, item))

    consumers = [asyncio.create_task(consumer(number)) for number in range(3)]
    await asyncio.sleep(0.03)

    assert all(task.done() is False for task in consumers)

    await queue.put("a")
    await queue.put("b")
    await queue.put("c")

    await asyncio.wait_for(asyncio.gather(*consumers), timeout=0.5)

    assert sorted(item for _, item in results) == ["a", "b", "c"]
    assert len(results) == 3
    assert len(queue._queue) == 0


@pytest.mark.asyncio
async def test_bounded_queue_multiple_producers_resume_when_space_is_available():
    queue = BoundedQueue(capacity=2)
    events = []

    await queue.put("initial_1")
    await queue.put("initial_2")

    async def producer(item):
        events.append(f"waiting_{item}")
        await queue.put(item)
        events.append(f"done_{item}")

    producers = [
        asyncio.create_task(producer("later_1")),
        asyncio.create_task(producer("later_2")),
    ]
    await asyncio.sleep(0.03)

    assert all(task.done() is False for task in producers)
    assert sorted(events) == ["waiting_later_1", "waiting_later_2"]
    assert list(queue._queue) == ["initial_1", "initial_2"]

    assert await asyncio.wait_for(queue.get(), timeout=0.5) == "initial_1"
    await asyncio.sleep(0.03)

    assert len(queue._queue) == 2
    assert sum(event.startswith("done_") for event in events) == 1

    assert await asyncio.wait_for(queue.get(), timeout=0.5) == "initial_2"
    await asyncio.wait_for(asyncio.gather(*producers), timeout=0.5)

    assert sorted(events) == [
        "done_later_1",
        "done_later_2",
        "waiting_later_1",
        "waiting_later_2",
    ]
    assert sorted(queue._queue) == ["later_1", "later_2"]


@pytest.mark.asyncio
async def test_bounded_queue_unbounded_mode_never_blocks_put():
    queue = BoundedQueue()

    for item in range(100):
        await asyncio.wait_for(queue.put(item), timeout=0.5)

    assert len(queue._queue) == 100
    assert [await asyncio.wait_for(queue.get(), timeout=0.5) for _ in range(100)] == list(range(100))


@pytest.mark.asyncio
async def test_bounded_queue_cancelled_get_does_not_consume_future_item():
    queue = BoundedQueue(capacity=1)

    consumer = asyncio.create_task(queue.get())
    await asyncio.sleep(0.03)

    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer

    await queue.put("item")

    assert await asyncio.wait_for(queue.get(), timeout=0.5) == "item"
    assert len(queue._queue) == 0


@pytest.mark.asyncio
async def test_bounded_queue_cancelled_put_does_not_enqueue_item_later():
    queue = BoundedQueue(capacity=1)

    await queue.put("first")
    producer = asyncio.create_task(queue.put("second"))
    await asyncio.sleep(0.03)

    assert producer.done() is False

    producer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await producer

    assert await asyncio.wait_for(queue.get(), timeout=0.5) == "first"
    await asyncio.sleep(0.03)

    assert len(queue._queue) == 0

    await queue.put("third")
    assert await asyncio.wait_for(queue.get(), timeout=0.5) == "third"
