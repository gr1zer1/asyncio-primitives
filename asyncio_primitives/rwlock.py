from __future__ import annotations
from typing import Any, TypeVar, Generic
from asyncio.locks import Condition
from abc import ABC, abstractmethod
from enum import Enum



class RWLock:


    def __init__(self, obj: Any | None = None):
        self._obj: Any | None= obj
        self._readers: int = 0
        self._condition: Condition = Condition()
        self._waiting_writers: int = 0
    

    async def _acquire_read(self):
        async with self._condition:
            while self._readers < 0 or self._waiting_writers > 0:
                await self._condition.wait()
            self._readers += 1


    async def _release_read(self):
        async with self._condition:
            self._readers -= 1
            if self._readers == 0:
                self._condition.notify_all()


    async def _acquire_write(self):
        async with self._condition:
            self._waiting_writers += 1
            while self._readers != 0:
                await self._condition.wait()
            self._waiting_writers -= 1
            self._readers = -1
    

    async def _release_write(self):
        async with self._condition:
            self._readers = 0
            self._condition.notify_all()
    

    def read(self) -> ReadProxy:

        return ReadProxy(self)
    

    def write(self) -> WriteProxy:

        return WriteProxy(self)
    
    def reader(self):
       

        return RWLockGuard(lock=self, roll=RWRoll.Reader)
    
    def writer(self):
        
        return RWLockGuard(lock = self, roll=RWRoll.Writer)
    


    
class Proxy(ABC):

    @abstractmethod
    async def close(self):...

    async def __aenter__(self):
        return self
    

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
    



T = TypeVar("T")


class ReadProxy(Proxy, Generic[T]):
    def __init__(self, lock: RWLock):

        object.__setattr__(self, "lock", lock)
        object.__setattr__(self, "_obj", lock._obj)    

    
    def __getattr__(self, name):
        
        target = object.__getattribute__(self, "_obj")
        return getattr(target, name)

    def __setattr__(self, name, value):

        raise AttributeError("Читатель не имеет прав на модификацию!")

    async def __aenter__(self):
        await self.lock._acquire_read()
        return self
    

    async def close(self):
        await self.lock._release_read()
    

    def replace(self, new_obj: Any):
        lock = object.__getattribute__(self, "lock")

        object.__setattr__(self, "_obj", new_obj)   
        lock._obj = new_obj
    
    


class WriteProxy(Proxy, Generic[T]):
    def __init__(self, lock: RWLock):

        object.__setattr__(self, "lock", lock)
        object.__setattr__(self, "_obj", lock._obj)    

    
    def __getattr__(self, name):
        
        target = object.__getattribute__(self, "_obj")
        return getattr(target, name)

    def __setattr__(self, name, value):

        target = object.__getattribute__(self, "_obj")
        setattr(target, name, value)
       
    async def __aenter__(self):
        await self.lock._acquire_write()
        return self
    


    async def close(self):
        await self.lock._release_write()


class RWLockGuard:
    def __init__(self, lock: RWLock, roll: RWRoll):
        self.roll = roll
        self.lock = lock
    

    async def __aenter__(self):
        match self.roll:
            case RWRoll.Reader:
                await self.lock._acquire_read()
            case RWRoll.Writer:
                await self.lock._acquire_write()
    

    async def __aexit__(self, exc_type, exc, tb):
        match self.roll:
            case RWRoll.Reader:
                await self.lock._release_read()
            case RWRoll.Writer:
                await self.lock._release_write()



class RWRoll(Enum):
    Reader = 1
    Writer = -1