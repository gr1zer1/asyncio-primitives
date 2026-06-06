from typing import Any, Callable
from .event import Event
from inspect import iscoroutine
import functools
import inspect


class Cache:
    def __init__(self):
        self._cache: dict[str, Any] = {}
        self._events: dict[str, Event] = {}

    def push(self, key: str, value: Any) -> None:
        self._cache[key] = value

    async def get(self, key: str, value_fn: Callable) -> Any:
        if key in self._cache:
            return self._cache[key]

        if key in self._events:
            await self._events[key].wait()
            return self._cache.get(key)

        event = Event()
        self._events[key] = event
        try:
            result = value_fn()
            if iscoroutine(result): 
                result = await result
            self._cache[key] = result
            return result
        finally:
            event.set()
            del self._events[key]

    def wrapper(self, fn: Callable):
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        
        async def inner(*args,**kwargs):
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            key = f"{fn.__name__}:{bound.arguments}"

            return await self.get(key, lambda: fn(*args, **kwargs))
        
        return inner
