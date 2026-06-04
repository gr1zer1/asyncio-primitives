# asyncio-primitives

A collection of additional asynchronous primitives for `asyncio`.

The package currently provides:

- `RWLock`: a read/write lock that allows multiple readers at the same time, while writers get exclusive access.
- `Mutex`: an exclusive async lock that can wrap an object or protect an arbitrary code section.
- `RMutex`: a reentrant exclusive async lock for cases where the same task must be able to acquire the lock multiple times.
- `Barrier`: a reusable synchronization point that releases tasks in groups.
- `Event`: an async event flag that wakes waiters when it is set.
- `BoundedQueue`: an async FIFO queue with optional capacity limits.
- `PriorityQueue`: an async queue that returns items by numeric priority.

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

## Mutex

`Mutex` provides exclusive access. Only one coroutine can hold it at a time.

Like `RWLock`, it can be used as an object wrapper:

```python
from asyncio_primitives import Mutex


class Value:
    def __init__(self, value):
        self.value = value


mutex = Mutex(Value(12))

async with mutex.get() as value:
    value.value = 99
```

It can also be used as a regular lock for protecting a code section:

```python
mutex = Mutex()

async with mutex.lock():
    # exclusive section
    ...
```

### Mutex API

- `Mutex(obj=None)` creates a mutex. `obj` is optional and is used by `get()`.
- `get()` returns a guard that locks the mutex and proxies attributes of the wrapped object.
- `lock()` returns a guard that only locks and unlocks the mutex.
- `close()` releases the mutex if it is currently locked.

### Mutex Behavior

- Only one holder can enter a `Mutex`-protected section at a time.
- `get()` allows reading and assigning attributes on the wrapped object.
- `replace(new_obj)` on the guard replaces the wrapped object.
- `lock()` is useful when the protected state is stored outside the mutex.

## RMutex

`RMutex` is a reentrant mutex. It provides exclusive access like `Mutex`, but the owning `asyncio.Task` can acquire it multiple times without blocking itself.

Use it when a protected operation can call another protected operation from the same task:

```python
from asyncio_primitives import RMutex


class Value:
    def __init__(self, value):
        self.value = value


rmutex = RMutex(Value(0))

async with rmutex.get() as value:
    value.value += 1

    async with rmutex.get() as same_value:
        same_value.value += 1
```

`RMutex` can also protect an external state block:

```python
rmutex = RMutex()
items = []

async with rmutex.lock():
    items.append(1)

    async with rmutex.lock():
        items.append(2)
```

### RMutex API

- `RMutex(obj=None)` creates a reentrant mutex. `obj` is optional and is used by `get()`.
- `get()` returns a guard that locks the mutex and proxies attributes of the wrapped object.
- `lock()` returns a guard that only locks and unlocks the mutex.
- `close()` releases one reentrant level when called by the owning task.

### RMutex Behavior

- Only the owning task can reenter the lock.
- Other tasks wait until the owner releases all reentrant levels.
- Each successful acquire increments an internal counter.
- Each `close()` or `async with` exit decrements one level.
- The mutex is fully released only when the counter reaches zero.
- Calling `close()` from a non-owner task does not unlock the mutex.
- `get()` allows reading and assigning attributes on the wrapped object.
- `replace(new_obj)` on the guard replaces the wrapped object.

## Barrier

`Barrier` waits until a fixed number of coroutines reach the same point. When the required number of waiters arrives, all waiters in that generation are released together and the barrier becomes reusable for the next generation.

```python
import asyncio

from asyncio_primitives import Barrier


barrier = Barrier(3)


async def worker(number):
    print("before", number)
    await barrier.wait()
    print("after", number)


await asyncio.gather(worker(1), worker(2), worker(3))
```

`Barrier` can also be used as an async context manager. Entering the context waits for the current generation to fill:

```python
barrier = Barrier(2)

async with barrier:
    # runs after two coroutines have entered the barrier
    ...
```

### Barrier API

- `Barrier(n)` creates a reusable barrier for `n` coroutines.
- `wait()` waits until `n` coroutines have reached the barrier.
- `async with barrier` calls `wait()` on enter and does nothing special on exit.

### Barrier Behavior

- `n` must be greater than zero.
- Waiters are released by generation.
- After a generation is released, the barrier can be used again.
- If a waiting task is cancelled before the generation is released, it should not leave stale waiter state behind.
- The barrier is intended for synchronization inside one event loop.

## Event

`Event` is an asynchronous flag. Coroutines can wait until the flag is set, and `set()` wakes all current waiters.

```python
import asyncio

from asyncio_primitives import Event


event = Event()


async def worker():
    await event.wait()
    print("event is set")


async def controller():
    event.set()


await asyncio.gather(worker(), controller())
```

After the event is set, future waiters return immediately until the event is cleared:

```python
event = Event()

event.set()
await event.wait()

event.clear()
```

### Event API

- `Event()` creates an unset event.
- `wait()` waits until the event is set.
- `set()` sets the event and wakes all current waiters.
- `clear()` clears the event so future waiters block again.

### Event Behavior

- `wait()` returns immediately while the event is set.
- `wait()` blocks while the event is unset.
- `set()` is synchronous and wakes all current waiters.
- `clear()` is synchronous and resets the event.
- Current waiters are cancelled when `clear()` is called while they are waiting.
- The event is intended for synchronization inside one event loop.

## BoundedQueue

`BoundedQueue` is an asynchronous FIFO queue. Consumers wait when the queue is empty. Producers wait when the queue is full and a capacity limit is configured.

```python
import asyncio

from asyncio_primitives import BoundedQueue


queue = BoundedQueue(capacity=2)


async def producer():
    await queue.put("first")
    await queue.put("second")


async def consumer():
    item = await queue.get()
    print(item)


await asyncio.gather(producer(), consumer())
```

The queue can also be created without a capacity limit:

```python
queue = BoundedQueue()

await queue.put("item")
item = await queue.get()
```

### BoundedQueue API

- `BoundedQueue(capacity=None)` creates a queue. If `capacity` is `None`, the queue is unbounded.
- `put(item)` adds an item to the tail of the queue.
- `get()` removes and returns the oldest item from the head of the queue.

### BoundedQueue Behavior

- Items are returned in FIFO order.
- `capacity` must be greater than zero when provided.
- `get()` waits while the queue is empty.
- `put()` waits while the queue is full.
- Waiting producers are notified when consumers remove items.
- Waiting consumers are notified when producers add items.
- The queue is intended for synchronization inside one event loop.

## PriorityQueue

`PriorityQueue` is an asynchronous queue that returns the item with the lowest numeric priority first. Consumers wait when the queue is empty.

```python
import asyncio

from asyncio_primitives import PriorityQueue


queue = PriorityQueue()


async def producer():
    await queue.push("normal", priority=10)
    await queue.push("urgent", priority=1)


async def consumer():
    item = await queue.pop()
    print(item)  # urgent


await asyncio.gather(producer(), consumer())
```

You can inspect the next item without removing it:

```python
queue = PriorityQueue()

await queue.push("first", priority=1)

next_item = await queue.peek()
same_item = await queue.pop()
```

### PriorityQueue API

- `PriorityQueue()` creates an empty priority queue.
- `push(item, priority)` adds an item with a numeric priority.
- `pop()` removes and returns the item with the lowest numeric priority.
- `peek()` returns the item with the lowest numeric priority without removing it.
- `is_empty()` returns `True` when the queue has no items.

### PriorityQueue Behavior

- Lower numeric priority values are returned before higher values.
- `pop()` waits while the queue is empty.
- `peek()` waits while the queue is empty and does not remove the item.
- Items with the same priority are both allowed, but their relative order is not guaranteed.
- Stored items do not need to be comparable with each other.
- The queue is intended for synchronization inside one event loop.

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
