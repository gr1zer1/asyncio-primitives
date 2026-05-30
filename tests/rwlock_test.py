import asyncio

import pytest

from asyncio_primitives import RWLock


class Value:
    def __init__(self, value):
        self.value = value


@pytest.mark.asyncio
async def test_basic_read():
    obj = Value(12)
    rwlock = RWLock(obj)

    async with rwlock.read() as guard:
        assert guard.value == 12


@pytest.mark.asyncio
async def test_basic_write():
    obj = Value(12)
    rwlock = RWLock(obj)

    async with rwlock.write() as guard:
        guard.value = 99

    async with rwlock.read() as guard:
        assert guard.value == 99


@pytest.mark.asyncio
async def test_write_can_store_nested_value():
    obj = Value(12)
    rwlock = RWLock(obj)

    async with rwlock.write() as guard:
        guard.value = Value(99)

    async with rwlock.read() as guard:
        assert guard.value.value == 99


@pytest.mark.asyncio
async def test_reader_writer_guard_usage():
    rwlock = RWLock()
    value = Value(12)
    log = []

    async def writer():
        async with rwlock.writer():
            log.append("writer_in")
            await asyncio.sleep(0.05)
            value.value = 99
            log.append("writer_out")

    async def reader():
        await asyncio.sleep(0.01)
        async with rwlock.reader():
            log.append("reader_in")
            assert value.value == 99

    await asyncio.gather(writer(), reader())

    assert log == ["writer_in", "writer_out", "reader_in"]


@pytest.mark.asyncio
async def test_reader_writer_counts_reset():
    rwlock = RWLock()

    async with rwlock.reader():
        assert rwlock._readers == 1
    assert rwlock._readers == 0

    async with rwlock.writer():
        assert rwlock._readers == -1
    assert rwlock._readers == 0


@pytest.mark.asyncio
async def test_read_is_readonly():
    obj = Value(12)
    rwlock = RWLock(obj)

    with pytest.raises(Exception):
        async with rwlock.read() as guard:
            guard.value = Value(99)


@pytest.mark.asyncio
async def test_multiple_concurrent_readers():
    obj = Value(0)
    rwlock = RWLock(obj)
    results = []

    async def reader(n):
        async with rwlock.read():
            await asyncio.sleep(0.05)
            results.append(n)

    await asyncio.gather(*[reader(i) for i in range(5)])

    assert len(results) == 5


@pytest.mark.asyncio
async def test_writer_is_exclusive():
    obj = Value(0)
    rwlock = RWLock(obj)
    log = []

    async def writer():
        async with rwlock.write() as guard:
            log.append("writer_in")
            await asyncio.sleep(0.1)
            guard.value = Value(42)
            log.append("writer_out")

    async def reader():
        await asyncio.sleep(0.02)
        async with rwlock.read() as guard:
            log.append("reader_in")
            assert guard.value.value == 42

    await asyncio.gather(writer(), reader())

    assert log == ["writer_in", "writer_out", "reader_in"]


@pytest.mark.asyncio
async def test_readers_count_resets():
    obj = Value(0)
    rwlock = RWLock(obj)

    async with rwlock.read():
        pass
    assert rwlock._readers == 0

    async with rwlock.write():
        pass
    assert rwlock._readers == 0


@pytest.mark.asyncio
async def test_multiple_concurrent_writers():
    obj = Value(0)
    rwlock = RWLock(obj)
    results = []
    active_writers = 0
    max_active_writers = 0

    async def writer(n):
        nonlocal active_writers, max_active_writers

        async with rwlock.write() as guard:
            active_writers += 1
            max_active_writers = max(max_active_writers, active_writers)
            await asyncio.sleep(0.05)
            results.append(n)
            active_writers -= 1

    await asyncio.gather(*[writer(i) for i in range(5)])

    assert len(results) == 5
    assert max_active_writers == 1


@pytest.mark.asyncio
async def test_writer_priority():
    obj = Value(0)
    rwlock = RWLock(obj)
    results = []

    async def slow_reader():
        async with rwlock.read():
            await asyncio.sleep(0.1)  
            results.append("slow_reader")

    async def writer():
        await asyncio.sleep(0.03)  
        async with rwlock.write():
            results.append("writer")

    async def late_reader():
        await asyncio.sleep(0.05)  
        async with rwlock.read():
            results.append("late_reader")

    await asyncio.gather(slow_reader(), writer(), late_reader())

    assert results == ["slow_reader", "writer", "late_reader"]
