from .condition import Condition


class CountdownLatch:
    """One-shot latch that opens when its counter reaches zero.

    `CountdownLatch` is useful when one or more coroutines need to wait until a
    fixed number of independent operations have completed. Each `countdown()`
    decrements the counter. When the counter reaches zero, all current waiters
    are released and future `wait()` calls return immediately.
    """

    def __init__(self, n: int = 1):
        """Create a latch with an initial count.

        Args:
            n: Number of `countdown()` calls required to open the latch.

        Raises:
            ValueError: If `n` is less than or equal to zero.
        """
        if n <= 0:
            raise ValueError("n must be greater than 0")
        
        self._n: int = n
        self._cond: Condition = Condition()
    

    async def wait(self):
        """Wait until the counter reaches zero.

        If the latch is already open, this method returns immediately.
        """
        async with self._cond:
            while self._n > 0:
                await self._cond.wait()
    

    async def countdown(self):
        """Decrement the counter and release waiters when it reaches zero.

        Calling this method after the latch has already opened leaves the count
        at zero.
        """
        async with self._cond:
            self._n -= 1 if self._n > 0 else 0
            if self._n == 0:
                self._cond.notify_all()
    

    @property
    def count(self) -> int:
        """Return the current counter value."""
        return self._n
