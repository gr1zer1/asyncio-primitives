from __future__ import annotations
from typing import Any
from asyncio.locks import Condition
from abc import ABC, abstractmethod




class RWLock:


    def __init__(self, obj: Any):
        self.obj: Any = obj
        self._readers: int = 0
        self._condition: Condition = Condition()
    

    async def _acquire_read(self):
        async with self._condition:
            while self._readers < 0:
                await self._condition.wait()
            self._readers += 1


    async def _release_read(self):
        async with self._condition:
            self._readers -= 1
            if self._readers == 0:
                self._condition.notify_all()


    async def _acquire_write(self):
        async with self._condition:
            while self._readers != 0:
                await self._condition.wait()
            self._readers = -1
    

    async def _release_write(self):
        async with self._condition:
            self._readers = 0
            self._condition.notify_all()
    

    async def read(self) -> ReadGuard:
        await self._acquire_read()

        return ReadGuard(self)
    

    async def write(self) -> WriteGuard:
        await self._acquire_write()

        return WriteGuard(self)
    
    
class Guard(ABC):

    @abstractmethod
    async def close(self):...

    async def __aenter__(self):
        return self
    

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
    




class ReadGuard(Guard):
    def __init__(self, lock: RWLock):
        self.lock = lock

    
    @property
    def value(self):
        return self.lock.obj
    
    @value.setter
    def value(self, value: Any):
        raise Exception
    

    async def close(self):
        await self.lock._release_read()


class WriteGuard(Guard):
    def __init__(self, lock: RWLock):
        self.lock = lock

    
    @property
    def value(self):
        return self.lock.obj
    
    @value.setter
    def value(self, value: Any):
        self.lock.obj = value
    

    async def close(self):
        await self.lock._release_write()


