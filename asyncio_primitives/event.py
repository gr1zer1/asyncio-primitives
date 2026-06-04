import asyncio


class Event:
    """Asynchronous event flag.

    `Event` lets coroutines wait until a shared flag is set. Calling `set()`
    wakes current waiters and lets future `wait()` calls return immediately
    until `clear()` resets the flag.
    """

    def __init__(self):
        """Create an unset event."""
        self._flag: bool = False
        self._future: asyncio.Future | None = None

    def _get_future(self) -> asyncio.Future:
        if self._future is None or self._future.done():
            self._future = asyncio.get_event_loop().create_future()

        return self._future

    async def wait(self):
        """Wait until the event is set.

        If the event is already set, this method returns immediately. If the
        event is cleared while this coroutine is waiting, the wait is cancelled
        and `asyncio.CancelledError` is raised.
        """
        if self._flag:
            return
        future = self._get_future()
        await asyncio.shield(future)

    def set(self):
        """Set the event and wake all current waiters."""
        self._flag = True
        future = self._get_future()
        if not future.done():
            future.set_result(None)

    def clear(self):
        """Clear the event.

        Future `wait()` calls will block again. Current waiters are cancelled.
        """
        self._flag = False
        if self._future is not None and not self._future.done():
            self._future.cancel()
        self._future = None
