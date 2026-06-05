import asyncio
from types import TracebackType
from typing import Any

from .condition import Condition


class RMutex:
    """Reentrant asynchronous mutex.

    `RMutex` provides exclusive access like `Mutex`, but allows the owning
    `asyncio.Task` to acquire the lock multiple times. Every successful acquire
    increments an internal counter, and every close decrements one level. Other
    tasks can enter only after the counter reaches zero.

    The lock can be used in two modes:

    1. As an object wrapper through `get()`:

       ```python
       mutex = RMutex(value)

       async with mutex.get() as value:
           value.name = "new name"
       ```

    2. As a code-section guard through `lock()`:

       ```python
       mutex = RMutex()

       async with mutex.lock():
           ...
       ```
    """

    def __init__(self, obj: Any | None = None):
        """Create an `RMutex`.

        Args:
            obj: Optional object exposed through `get()`. Leave it unset when
                the mutex is used only as a code-section guard.
        """
        self._obj = obj
        self._cond: Condition = Condition()
        self._counter: int = 0
        self._current_task: asyncio.Task | None = None 
    

    async def _increment(self) -> None:
        """Acquire one reentrant level for the current task."""
        async with self._cond:
            
            
            while self._current_task != asyncio.current_task() and self._current_task is not None:
                await self._cond.wait()
            
            if self._current_task is None:
                self._current_task = asyncio.current_task()
            
            self._counter += 1
    

    def get(self) -> "RMutexGuard":
        """Return a guard that locks the mutex and proxies the wrapped object.

        The same task may enter this guard multiple times. Other tasks wait
        until the owning task releases all reentrant levels.
        """
        return RMutexGuard(self)


    def lock(self) -> "RMutexLock":
        """Return a guard for protecting an arbitrary code section.

        This mode does not expose the wrapped object. It only acquires and
        releases the reentrant mutex around the `async with` block.
        """
        return RMutexLock(self)


    async def close(self) -> None:
        """Release one reentrant level held by the current task.

        If the current task does not own the mutex, this method does nothing.
        When the counter reaches zero, ownership is cleared and one waiter is
        notified.
        """
        async with self._cond:
            if self._current_task != asyncio.current_task():
                return
            if self._counter == 0:
                return 
            self._counter -= 1
            if self._counter == 0:
                self._current_task = None
            self._cond.notify(1)        


class RMutexLock:
    """Async context manager returned by `RMutex.lock()`.

    It protects a code section without exposing a wrapped object.
    """

    def __init__(self, mutex: RMutex):
        """Create a code-section guard for `mutex`."""
        self._mutex = mutex

    async def __aenter__(self) -> "RMutexLock":
        """Acquire the mutex and return this guard."""
        await self._mutex._increment()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Release one reentrant level when leaving `async with`."""
        await self._mutex.close()


class RMutexGuard:
    """Guard returned by `RMutex.get()`.

    The guard acquires the reentrant mutex and proxies attribute access to the
    wrapped object. Reads and writes are forwarded to that object.
    """

    _mutex: RMutex

    def __init__(self, mutex: RMutex):
        """Create an object proxy guard for `mutex`."""
        object.__setattr__(self, "_mutex", mutex)


    async def acquire(self) -> "RMutexGuard":
        """Acquire one reentrant level and return this guard."""
        await self._mutex._increment()

        return self

    async def close(self) -> None:
        """Release one reentrant level held by the current task."""
        await self._mutex.close()


    async def __aenter__(self) -> "RMutexGuard":
        """Acquire the mutex and return this object proxy guard."""
        return await self.acquire()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Release one reentrant level when leaving `async with`."""
        return await self.close()


    def __getattr__(self, name: str) -> Any:
        """Read an attribute from the wrapped object."""
        target = object.__getattribute__(self, "_mutex")
        return getattr(target._obj, name)
    
    def __setattr__(self, name: str, value: Any) -> None:
        """Set an attribute on the wrapped object."""
        target = object.__getattribute__(self, "_mutex")
        obj = target._obj
        setattr(obj, name, value)
    

    def replace(self, new_obj: Any) -> None:
        """Replace the wrapped object stored in the mutex."""
        target = object.__getattribute__(self, "_mutex")
        target._obj = new_obj
