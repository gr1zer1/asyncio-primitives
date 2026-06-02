from asyncio.locks import Condition
from collections import deque
from typing import Any


class BoundedQueue:
    def __init__(self, capacity: int | None = None):
        if capacity is not None and capacity <= 0:
            raise ValueError
        self.capacity: int | None = capacity
        self._queue: deque = deque(maxlen=capacity)
        self._cond: Condition = Condition()
    

    async def get(self) -> Any:
        async with self._cond:
            while len(self._queue) == 0:
                await self._cond.wait()
            item = self._queue.popleft()
            self._cond.notify_all()
            return item
    
    async def put(self, item: Any) -> None:
        async with self._cond:
            if self.capacity is not None:
                while len(self._queue) >= self.capacity:
                    await self._cond.wait()
            self._queue.append(item)
            self._cond.notify_all()
            return 