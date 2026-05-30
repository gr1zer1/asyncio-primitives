# asyncio-primitives

A collection of additional asynchronous primitives for `asyncio`.

The package currently provides `RWLock`: a read/write lock that allows multiple readers at the same time, while writers get exclusive access.

## Development Setup

```bash
pip install -r requirements.txt
```

## Running Tests

```bash
pytest
```

## RWLock

`RWLock` can be used as an object wrapper, similar to a Rust-style lock:

```python
from asyncio_primitives import RWLock


class Value:
    def __init__(self, value):
        self.value = value


rwlock = RWLock(Value(12))

async with rwlock.read() as value:
    print(value.value)

async with rwlock.write() as value:
    value.value = 99
```

`RWLock` can also be used as a regular lock for protecting an arbitrary code section:

```python
rwlock = RWLock()

async with rwlock.reader():
    # read-only section
    ...

async with rwlock.writer():
    # write section
    ...
```

## Behavior

- Multiple readers can hold the lock at the same time.
- A writer gets exclusive access.
- If a writer is already waiting for the lock, new readers wait so the writer does not starve.
- `read()` returns a read proxy and prevents assigning object attributes through that proxy.
- `write()` returns a write proxy and allows assigning object attributes through that proxy.
- `reader()` and `writer()` are used when the lock is needed as a code-section guard rather than as an object wrapper.

## Future Primitives

### AsyncCell

A container for a single value with asynchronous access.

API idea:

```python
cell = AsyncCell(10)

value = await cell.get()
await cell.set(20)

async with cell.write() as value:
    ...
```

Purpose: safely store and update one shared value between coroutines.

### AsyncOnce

A primitive that guarantees an asynchronous initializer runs only once.

API idea:

```python
once = AsyncOnce()

await once.run(init_database)
```

Purpose: lazy initialization, database connections, config loading, cache warm-up.

### AsyncBarrier

A barrier that waits until a configured number of tasks reaches the same point.

API idea:

```python
barrier = AsyncBarrier(3)

await barrier.wait()
```

Purpose: synchronize multiple worker tasks before moving to the next phase.

### AsyncCountdownEvent

An event that becomes set when a counter reaches zero.

API idea:

```python
event = AsyncCountdownEvent(5)

event.decrement()
await event.wait()
```

Purpose: wait for a set of independent operations to finish.

### AsyncRateLimiter

A rate limiter for asynchronous operations.

API idea:

```python
limiter = AsyncRateLimiter(rate=10, per=1.0)

async with limiter:
    await call_api()
```

Purpose: limit requests to APIs, task queues, and background operations.

### AsyncResourcePool

A pool of reusable resources.

API idea:

```python
pool = AsyncResourcePool(create_connection, max_size=10)

async with pool.acquire() as connection:
    await connection.execute(...)
```

Purpose: manage connections, clients, sessions, and other expensive objects.

### AsyncPriorityQueueLock

A lock with a priority-based wait queue.

API idea:

```python
lock = AsyncPriorityQueueLock()

async with lock.acquire(priority=10):
    ...
```

Purpose: let more important tasks acquire access before normal tasks.

## Roadmap

1. Stabilize the `RWLock` API.
2. Add stronger typing for proxy and guard objects.
3. Add tests for task cancellation while waiting for the lock.
4. Add `AsyncCell` as the next small primitive.
5. Add `AsyncOnce`, `AsyncBarrier`, and `AsyncRateLimiter` after that.
