from typing import Any
from asyncio.locks import Condition


class Mutex:

    def __init__(self, obj: Any | None = None):
        self._obj = obj
        self._cond: Condition = Condition()
        self._counter: int = 0
    

    async def _increment(self):
        async with self._cond:
            while self._counter > 0:
                await self._cond.wait()
            self._counter = 1
    

    def get(self):
        return MutexGuard(self)


    def lock(self):
        return MutexLock(self)


    async def close(self):
        async with self._cond:
            if self._counter == 0:
                return 
            self._counter = 0
            self._cond.notify(1)        


class MutexLock:
    def __init__(self, mutex: Mutex):
        self._mutex = mutex

    async def __aenter__(self):
        await self._mutex._increment()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._mutex.close()


class MutexGuard:

    _mutex: Mutex

    def __init__(self, mutex: Mutex):
        object.__setattr__(self, "_mutex", mutex)


    async def acquire(self):
        await self._mutex._increment()

        return self

    async def close(self):
        await self._mutex.close()


    async def __aenter__(self):
        return await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        return await self.close()


    def __getattr__(self, name):
        target = object.__getattribute__(self, "_mutex")
        return getattr(target._obj, name)
    
    def __setattr__(self, name, value):
        target = object.__getattribute__(self, "_mutex")
        obj = target._obj
        setattr(obj, name, value)
    

    def replace(self, new_obj: Any):
        target = object.__getattribute__(self, "_mutex")
        target._obj = new_obj

