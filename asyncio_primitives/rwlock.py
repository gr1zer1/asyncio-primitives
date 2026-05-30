from __future__ import annotations
from abc import ABC, abstractmethod
from asyncio.locks import Condition
from enum import Enum
from types import TracebackType
from typing import Any, Generic, TypeVar


T = TypeVar("T")


class RWLock(Generic[T]):
    """Asynchronous read/write lock.

    `RWLock` can be used in two ways:

    1. As an object wrapper:

       ```python
       lock = RWLock(value)

       async with lock.read() as value:
           ...

       async with lock.write() as value:
           ...
       ```

    2. As a regular lock for protecting a code section:

       ```python
       lock = RWLock()

       async with lock.reader():
           ...

       async with lock.writer():
           ...
       ```

    Multiple readers can hold the lock at the same time. A writer gets
    exclusive access. If a writer is already waiting, new readers wait so the
    writer does not starve.
    """


    def __init__(self, obj: T | None = None):
        """Create an `RWLock`.

        Args:
            obj: Object exposed through `read()` and `write()`. If no wrapped
                object is needed, leave this unset and use only `reader()` /
                `writer()`.
        """
        self._obj: T | None = obj
        self._readers: int = 0
        self._condition: Condition = Condition()
        self._waiting_writers: int = 0
    

    async def _acquire_read(self) -> None:
        async with self._condition:
            while self._readers < 0 or self._waiting_writers > 0:
                await self._condition.wait()
            self._readers += 1


    async def _release_read(self) -> None:
        async with self._condition:
            self._readers -= 1
            if self._readers == 0:
                self._condition.notify_all()


    async def _acquire_write(self) -> None:
        async with self._condition:
            self._waiting_writers += 1
            while self._readers != 0:
                await self._condition.wait()
            self._waiting_writers -= 1
            self._readers = -1
    

    async def _release_write(self) -> None:
        async with self._condition:
            self._readers = 0
            self._condition.notify_all()
    

    def read(self) -> ReadProxy[T]:
        """Return an async context manager for reading the wrapped object.

        Inside `async with`, this returns a `ReadProxy`. It proxies attribute
        reads from the wrapped object and blocks attribute assignment.

        Example:
            ```python
            async with lock.read() as value:
                print(value.name)
            ```
        """
        return ReadProxy(self)
    

    def write(self) -> WriteProxy[T]:
        """Return an async context manager for mutating the wrapped object.

        Inside `async with`, this returns a `WriteProxy`. It proxies attribute
        reads from the wrapped object and allows attribute assignment.

        Example:
            ```python
            async with lock.write() as value:
                value.name = "new name"
            ```
        """
        return WriteProxy(self)
    

    def reader(self) -> RWLockGuard:
        """Return a read guard for protecting an arbitrary code section.

        Use this method when the object is stored outside the `RWLock` and the
        lock is needed only for synchronization.

        Example:
            ```python
            async with lock.reader():
                print(shared_state)
            ```
        """
        return RWLockGuard(lock=self, roll=RWRoll.Reader)
    

    def writer(self) -> RWLockGuard:
        """Return a write guard for protecting an arbitrary code section.

        While the write guard holds the lock, new readers and writers wait.

        Example:
            ```python
            async with lock.writer():
                shared_state.append(item)
            ```
        """
        return RWLockGuard(lock=self, roll=RWRoll.Writer)
    


class Proxy(ABC):
    """Base class for `ReadProxy` and `WriteProxy` objects."""

    @abstractmethod
    async def close(self) -> None:
        """Release the lock held by this proxy."""
        ...

    async def __aenter__(self) -> Proxy:
        """Return the proxy object when entering `async with`."""
        return self
    

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release the lock when leaving `async with`."""
        await self.close()
    


class ReadProxy(Proxy, Generic[T]):
    """Read-only proxy for the object stored inside `RWLock`.

    `ReadProxy` allows reading attributes from the wrapped object, but blocks
    attribute assignment through the proxy.

    Usually this should not be created directly. Use `RWLock.read()`.
    """

    def __init__(self, lock: RWLock[T]):
        """Create a read proxy for the lock."""
        object.__setattr__(self, "lock", lock)
        object.__setattr__(self, "_obj", lock._obj)    

    
    def __getattr__(self, name: str) -> Any:
        """Return an attribute from the wrapped object."""
        target = object.__getattribute__(self, "_obj")
        return getattr(target, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Block attribute mutation through the read proxy."""
        raise AttributeError("A reader cannot modify the wrapped object.")

    async def __aenter__(self) -> ReadProxy[T]:
        """Acquire the read lock and return the read proxy."""
        await self.lock._acquire_read()
        return self
    

    async def close(self) -> None:
        """Release the read lock."""
        await self.lock._release_read()
    

    def replace(self, new_obj: T) -> None:
        """Replace the wrapped object.

        Warning:
            This method is kept for current compatibility, but semantically it
            is a write operation. Prefer changing the object through `write()`
            in new code.
        """
        lock = object.__getattribute__(self, "lock")

        object.__setattr__(self, "_obj", new_obj)   
        lock._obj = new_obj
    
    


class WriteProxy(Proxy, Generic[T]):
    """Writable proxy for the object stored inside `RWLock`.

    `WriteProxy` allows reading and mutating attributes of the wrapped object.

    Usually this should not be created directly. Use `RWLock.write()`.
    """

    def __init__(self, lock: RWLock[T]):
        """Create a write proxy for the lock."""
        object.__setattr__(self, "lock", lock)
        object.__setattr__(self, "_obj", lock._obj)    

    
    def __getattr__(self, name: str) -> Any:
        """Return an attribute from the wrapped object."""
        target = object.__getattribute__(self, "_obj")
        return getattr(target, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Set an attribute on the wrapped object."""
        target = object.__getattribute__(self, "_obj")
        setattr(target, name, value)
       
    async def __aenter__(self) -> WriteProxy[T]:
        """Acquire the write lock and return the write proxy."""
        await self.lock._acquire_write()
        return self
    


    async def close(self) -> None:
        """Release the write lock."""
        await self.lock._release_write()


class RWLockGuard:
    """Async context manager for `reader()` and `writer()`.

    The guard does not return a wrapped object. It only acquires and releases
    the lock around a user-provided code block.
    """

    def __init__(self, lock: RWLock[Any], roll: RWRoll):
        """Create a guard for the reader or writer role."""
        self.roll = roll
        self.lock = lock
    

    async def __aenter__(self) -> None:
        """Acquire the read or write lock depending on the guard role."""
        match self.roll:
            case RWRoll.Reader:
                await self.lock._acquire_read()
            case RWRoll.Writer:
                await self.lock._acquire_write()
    

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Release the read or write lock depending on the guard role."""
        match self.roll:
            case RWRoll.Reader:
                await self.lock._release_read()
            case RWRoll.Writer:
                await self.lock._release_write()



class RWRoll(Enum):
    """Role of the guard object inside `RWLockGuard`."""

    Reader = 1
    """Read-lock role."""

    Writer = -1
    """Write-lock role."""
