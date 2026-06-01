from asyncio.locks import Condition



class Barrier:

    def __init__(self,n: int):
        self._coroutines: int = 0
        self.capacity: int = n
        self._cond: Condition = Condition()

    
async def _wait(self):
    async with self._cond:
        self._coroutines += 1
        if self._coroutines == self.capacity:

            self._coroutines = 0  
            self._cond.notify_all()
        else:

            while self._coroutines < self.capacity and self._coroutines != 0:
                await self._cond.wait()
    

    async def __aenter__(self):
        await self._wait()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        ...