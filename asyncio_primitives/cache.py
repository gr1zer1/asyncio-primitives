from typing import Any, Callable
from .event import Event
import inspect
from inspect import iscoroutinefunction


class Cache:

    def __init__(self):
        self._cache: dict[str, Any]
        self._events: dict[str, Event]
    

    async def push(self, key: str, value: Any):
        self._cache[key] = value
    

    async def get(self, key: str, value_fn: Callable) -> Any:

        value = self._cache.get(key)

        if value is not None:
            return value
        
        event = self._events.get(key)

        if event is not None:
            await event.wait()
            return self._cache.get(key)


        self._events[key] = Event()
        if iscoroutinefunction(value_fn):
            value = await value_fn()
        else:
            value = value_fn()
        
        self._cache[key] = value
        return value