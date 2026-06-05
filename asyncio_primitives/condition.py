import asyncio
from collections import deque


class Lock:
    """Small FIFO async lock used as a base primitive for Condition."""

    def __init__(self):
        self._locked = False
        self._waiters: deque[asyncio.Future] = deque()

    def locked(self) -> bool:
        return self._locked

    async def acquire(self) -> bool:
        if not self._locked and not self._waiters:
            self._locked = True
            return True

        future = asyncio.get_running_loop().create_future()
        self._waiters.append(future)

        try:
            while self._locked or self._waiters[0] is not future:
                await future
            self._waiters.popleft()
            self._locked = True
            return True
        except BaseException:
            if future in self._waiters:
                self._waiters.remove(future)
            if not self._locked:
                self._wake_up_first()
            raise

    def release(self) -> None:
        if not self._locked:
            raise RuntimeError("Lock is not acquired")

        self._locked = False
        self._wake_up_first()

    def _wake_up_first(self) -> None:
        while self._waiters:
            future = self._waiters[0]
            if not future.done():
                future.set_result(True)
                return
            self._waiters.popleft()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()


class Condition:
    def __init__(self, lock: Lock | None = None):
        self._lock = lock if lock is not None else Lock()
        self._waiters: deque[asyncio.Future] = deque()

    def locked(self) -> bool:
        return self._lock.locked()

    async def acquire(self) -> bool:
        return await self._lock.acquire()

    def release(self) -> None:
        self._lock.release()

    async def wait(self) -> bool:
        if not self.locked():
            raise RuntimeError("cannot wait on un-acquired lock")

        future = asyncio.get_running_loop().create_future()
        self._waiters.append(future)
        self.release()

        try:
            await future
            return True
        finally:
            await self.acquire()
            if future in self._waiters:
                self._waiters.remove(future)

    def notify(self, n: int = 1) -> None:
        if not self.locked():
            raise RuntimeError("cannot notify on un-acquired lock")

        if n < 0:
            raise ValueError("n must be non-negative")

        notified = 0
        for future in tuple(self._waiters):
            if notified >= n:
                break
            if not future.done():
                future.set_result(True)
                notified += 1

    def notify_all(self) -> None:
        self.notify(len(self._waiters))

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()
