import asyncio
from types import TracebackType

from asyncio.locks import Condition


class Barrier:
    """Reusable asynchronous barrier.

    `Barrier` waits until a fixed number of coroutines reach the same point.
    When `capacity` coroutines are waiting, all waiters in the current
    generation are released and the barrier can be reused for the next
    generation.

    Example:
        ```python
        barrier = Barrier(3)

        await barrier.wait()
        ```

    The barrier can also be used as an async context manager:

        ```python
        async with barrier:
            ...
        ```
    """

    def __init__(self, n: int):
        """Create a barrier for `n` coroutines.

        Args:
            n: Number of coroutines required to release one barrier generation.

        Raises:
            ValueError: If `n` is less than or equal to zero.
        """
        if n <= 0: 
            raise ValueError
        self.capacity: int = n
        
        self._coroutines: int = 0
        self._cond: Condition = Condition()
        
        self._generation: int = 0  

    async def wait(self) -> None:
        """Wait until the current barrier generation is full.

        The calling coroutine blocks until `capacity` coroutines have entered
        the same generation. The last arriving coroutine releases all waiters,
        resets the waiter count, and advances the generation.

        If a waiting coroutine is cancelled before the generation is released,
        the waiter count is decremented for that generation.
        """
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

    async def __aenter__(self) -> "Barrier":
        """Wait on the barrier when entering an `async with` block."""
        await self.wait()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Leave the context manager.

        The barrier is synchronized on enter, so exit does not perform any
        additional work.
        """
        pass
