from .condition import Condition


class CountdownLatch:


    def __init__(self, n: int = 1):
        if n <= 0:
            raise ValueError("n must be greater than 0")
        
        self._n: int = n
        self._cond: Condition = Condition()
    

    async def wait(self):
        async with self._cond:
            while self._n > 0:
                await self._cond.wait()
            self._cond.notify_all()
    

    async def countdown(self):
        async with self._cond:
            self._n -= 1 if self._n > 0 else 0
            if self._n == 0:
                self._cond.notify_all()
    

    @property
    def count(self) -> int:
        return self._n