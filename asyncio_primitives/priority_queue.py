import heapq
from dataclasses import dataclass, field
from typing import Any

from .condition import Condition


@dataclass(order=True)
class Instance:
    """Internal heap entry for `PriorityQueue`.

    Only `priority` participates in ordering. The stored `item` is excluded
    from comparisons, so queue entries can hold non-comparable objects.
    """

    priority: int
    item: Any = field(compare=False)


class PriorityQueue:
    """Asynchronous priority queue.

    Items are returned by ascending numeric priority: lower priority values are
    popped before higher priority values. Consumers wait while the queue is
    empty and are notified when producers add new items.
    """

    def __init__(self):
        """Create an empty priority queue."""
        self._queue: list[Instance] = []
        self._cond: Condition = Condition()

    async def push(self, item: Any, priority: int):
        """Add `item` to the queue with the given priority.

        Args:
            item: Value to store in the queue.
            priority: Numeric priority. Lower values are returned first.
        """
        async with self._cond:
            inst = self._create_instance(item, priority)
            heapq.heappush(self._queue, inst)
            self._cond.notify_all()

    async def pop(self) -> Any:
        """Remove and return the item with the lowest numeric priority.

        If the queue is empty, this method waits until an item is pushed.
        """
        async with self._cond:
            while self.is_empty():
                await self._cond.wait()
            inst = heapq.heappop(self._queue)
            return inst.item

    async def peek(self) -> Any:
        """Return the next item without removing it from the queue.

        If the queue is empty, this method waits until an item is pushed.
        """
        async with self._cond:
            while self.is_empty():
                await self._cond.wait()
            return self._queue[0].item

    def is_empty(self) -> bool:
        """Return `True` when the queue has no items."""
        return len(self._queue) == 0

    def _create_instance(self, item: Any, priority: int) -> Instance:
        return Instance(priority, item)
