from collections import deque
from typing import Generic, TypeVar

from asyncio.locks import Condition


T = TypeVar("T")


class BoundedQueue(Generic[T]):
    """Asynchronous FIFO queue with an optional capacity limit.

    `BoundedQueue` coordinates producers and consumers:

    - `get()` waits while the queue is empty.
    - `put()` waits while the queue is full.
    - items are returned in FIFO order.

    If `capacity` is `None`, the queue is unbounded and `put()` never waits
    because of size.

    Example:
        ```python
        queue = BoundedQueue[int](capacity=2)

        await queue.put(1)
        item = await queue.get()
        ```
    """

    def __init__(self, capacity: int | None = None):
        """Create a queue.

        Args:
            capacity: Maximum number of items allowed in the queue. Use `None`
                for an unbounded queue.

        Raises:
            ValueError: If `capacity` is less than or equal to zero.
        """
        if capacity is not None and capacity <= 0:
            raise ValueError
        self.capacity: int | None = capacity
        self._queue: deque[T] = deque(maxlen=capacity)
        self._cond: Condition = Condition()
    

    async def get(self) -> T:
        """Remove and return the oldest item from the queue.

        If the queue is empty, this method waits until a producer adds an item.
        After removing an item, waiting producers are notified because space may
        be available.
        """
        async with self._cond:
            while len(self._queue) == 0:
                await self._cond.wait()
            item = self._queue.popleft()
            self._cond.notify_all()
            return item
    
    async def put(self, item: T) -> None:
        """Add an item to the tail of the queue.

        If a capacity limit is configured and the queue is full, this method
        waits until a consumer removes an item. After adding the item, waiting
        consumers are notified.
        """
        async with self._cond:
            if self.capacity is not None:
                while len(self._queue) >= self.capacity:
                    await self._cond.wait()
            self._queue.append(item)
            self._cond.notify_all()
            return 
