import asyncio
from asyncio.locks import Condition

class Barrier:
    def __init__(self, n: int):
        if n <= 0: 
            raise ValueError
        self.capacity: int = n
        
        self._coroutines: int = 0
        self._cond: Condition = Condition()
        
        self._generation: int = 0  

    async def wait(self):
        async with self._cond:

            my_generation = self._generation
            
            self._coroutines += 1
            
            if self._coroutines == self.capacity:

                self._coroutines = 0
                self._generation += 1  

                self._cond.notify_all()
            else:
                try:
                    while my_generation == self._generation:

                        await self._cond.wait()

                except asyncio.CancelledError:
                    if my_generation == self._generation:
                        self._coroutines -= 1

                        if self._coroutines == 0:
                            self._cond.notify_all()

                    raise

    async def __aenter__(self):
        await self.wait()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass