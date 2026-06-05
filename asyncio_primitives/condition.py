from mutex import Mutex
import asyncio


class Condition:

    def __init__(self):
        self._waiters: list[asyncio.Future] = []
        self._mutex = Mutex()
    
    

    async def wait(self):
        future = asyncio.get_event_loop().create_future()
        
        self._waiters.append(future)

        await self._mutex.close()

        try:
            asyncio.shield(future)
        finally:
            await self._mutex._increment()
            
            self._waiters.remove(future)

    

    def notify(self, n: int = 1):
        for _ in range(n):
            if self._waiters:
                future = self._waiters.pop(0)
                future.set_result(None)
    


    def notify_all(self):
        self.notify(len(self._waiters))
    

    async def __aenter__(self):
        await self._mutex._increment()
    

    async def __aexit__(self, exc_type, exc, tb):
        await self._mutex.close()