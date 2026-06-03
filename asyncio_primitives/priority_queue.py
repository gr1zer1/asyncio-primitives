import heapq
from dataclasses import dataclass, field
from typing import Any


@dataclass(order=True)    
class Instance:
    priority: int
    item: Any=field(compare=False)


class PriorityQueue:
    def __init__(self):
        self._queue: list[Instance] = []


    def push(self, item: Any, priority: int):
        inst = self._create_instance(item, priority)

        heapq.heappush(self._queue, inst)
    

    def pop(self) -> Any:
        if self.is_empty():
            raise IndexError
        return heapq.heappop(self._queue)
    
    def is_empty(self) -> bool:
        return len(self._queue) == 0
        
    
    def _create_instance(self, item: Any, priority: int) -> Instance:
        return Instance(priority, item)