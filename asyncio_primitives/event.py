import asyncio


class Event:

    def __init__(self):
        self._flag: bool = False
        self._future: asyncio.Future | None = None

        self._get_future()
    

    def _get_future(self) -> asyncio.Future:
        if self._future is None or self._future.done():
            self._future = asyncio.get_event_loop().create_future()

        return self._future
    

    async def wait(self):
        if self._flag:
            return
        future = self._get_future()
        await asyncio.shield(future)
    

    def set(self):
        self._flag = True
        future = self._get_future()
        if not future.done():
            future.set_result(None)
    

    async def clear(self):
        self._flag = False
        if self._future is None or self._future.done():
            self._get_future()