from typing import Any
from asyncio.locks import Condition


class RWLock:


    def __init__(self, obj: Any):
        self.obj: Any = obj
        self._readers: int = 0
        self._condition: Condition = Condition()
    

    async def _acquire_read(self) -> ReadGuard:
        async with self._condition:
            while self._readers < 0:
                await self._condition.wait()
            self._readers += 1


    async def _release_read(self):
        async with self._condition:
            self._readers -= 1
            if self._readers == 0:
                self._condition.notify_all()


    async def _acquire_write(self) -> WriteGuard:
        async with self._condition:
            while self._readers > 0:
                await self._condition.wait()
            self._readers = -1
    

    async def _release_write(self):
        async with self._condition:
            self._readers = 0
            self._condition.notify_all()
    

class ReadGuard:
    ...


class WriteGuard:
    ...