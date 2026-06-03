import heapq
from dataclasses import dataclass, field
from typing import Any
from asyncio.locks import Condition




@dataclass(order=True)    
class Instance:
    priority: int
    item: Any=field(compare=False)


class PriorityQueue:
    def __init__(self):
        self._queue: list[Instance] = []
        self._cond: Condition = Condition()


    async def push(self, item: Any, priority: int):
        async with self._cond: 
            inst = self._create_instance(item, priority)

            heapq.heappush(self._queue, inst)

            self._cond.notify_all()
    

    async def pop(self) -> Any:
        async with self._cond:
            while self.is_empty():
                await self._cond.wait()
            inst = heapq.heappop(self._queue)


            return inst.item
    
    async def peek(self) -> Any:
        async with self._cond:
            while self.is_empty():
                await self._cond.wait()
            return self._queue[0].item
    



    def is_empty(self) -> bool:
        return len(self._queue) == 0
        
    
    def _create_instance(self, item: Any, priority: int) -> Instance:
        return Instance(priority, item)