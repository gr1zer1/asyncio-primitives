import asyncio

import pytest

from asyncio_primitives import Cache


async def make_fetcher(value, delay: float = 0.0, counter: list | None = None):
    """Helper: returns an async fetch function."""
    async def fetch():
        if counter is not None:
            counter.append(1)
        if delay:
            await asyncio.sleep(delay)
        return value
    return fetch


# ---------------------------------------------------------------------------
# Basic get / push
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cache_get_calls_fetch_fn_on_miss():
    cache = Cache()
    fetcher = await make_fetcher("result")

    value = await cache.get("key", fetcher)

    assert value == "result"


@pytest.mark.asyncio
async def test_cache_get_returns_cached_value_on_hit():
    cache = Cache()
    counter = []
    fetcher = await make_fetcher("result", counter=counter)

    await cache.get("key", fetcher)
    await cache.get("key", fetcher)

    assert len(counter) == 1  # fetch_fn called only once


@pytest.mark.asyncio
async def test_cache_push_stores_value():
    cache = Cache()

    cache.push("key", "pushed")
    value = await cache.get("key", lambda: "should_not_be_called")

    assert value == "pushed"


@pytest.mark.asyncio
async def test_cache_different_keys_stored_independently():
    cache = Cache()

    await cache.get("a", await make_fetcher("value_a"))
    await cache.get("b", await make_fetcher("value_b"))

    assert await cache.get("a", await make_fetcher("wrong")) == "value_a"
    assert await cache.get("b", await make_fetcher("wrong")) == "value_b"


# ---------------------------------------------------------------------------
# Thundering Herd protection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cache_fetch_fn_called_once_under_concurrent_load():
    cache = Cache()
    counter = []
    fetcher = await make_fetcher("result", delay=0.05, counter=counter)

    results = await asyncio.gather(*[cache.get("key", fetcher) for _ in range(50)])

    assert len(counter) == 1
    assert all(r == "result" for r in results)


@pytest.mark.asyncio
async def test_cache_all_waiters_receive_correct_value():
    cache = Cache()
    fetcher = await make_fetcher(42, delay=0.05)

    results = await asyncio.gather(*[cache.get("key", fetcher) for _ in range(20)])

    assert results == [42] * 20


@pytest.mark.asyncio
async def test_cache_concurrent_requests_for_different_keys():
    cache = Cache()
    counter_a, counter_b = [], []

    fetcher_a = await make_fetcher("a", delay=0.05, counter=counter_a)
    fetcher_b = await make_fetcher("b", delay=0.05, counter=counter_b)

    tasks = [
        *[cache.get("key_a", fetcher_a) for _ in range(10)],
        *[cache.get("key_b", fetcher_b) for _ in range(10)],
    ]
    results = await asyncio.gather(*tasks)

    assert len(counter_a) == 1
    assert len(counter_b) == 1
    assert results[:10] == ["a"] * 10
    assert results[10:] == ["b"] * 10


# ---------------------------------------------------------------------------
# wrapper decorator
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wrapper_caches_by_args():
    cache = Cache()
    counter = []

    @cache.wrapper
    async def fetch(x: int):
        counter.append(x)
        await asyncio.sleep(0.01)
        return x * 2

    results = await asyncio.gather(*[fetch(5) for _ in range(20)])

    assert len(counter) == 1
    assert all(r == 10 for r in results)


@pytest.mark.asyncio
async def test_wrapper_different_args_cached_separately():
    cache = Cache()
    counter = []

    @cache.wrapper
    async def fetch(x: int):
        counter.append(x)
        return x * 10

    r1 = await fetch(1)
    r2 = await fetch(2)
    r3 = await fetch(1)  # должен взяться из кэша

    assert r1 == 10
    assert r2 == 20
    assert r3 == 10
    assert len(counter) == 2  # fetch вызван только для 1 и 2


@pytest.mark.asyncio
async def test_wrapper_kwargs_equivalent_to_positional():
    cache = Cache()
    counter = []

    @cache.wrapper
    async def fetch(x: int, y: int = 0):
        counter.append(1)
        return x + y

    r1 = await fetch(3)
    r2 = await fetch(3, y=0)  # эквивалентный вызов — должен попасть в кэш
    r3 = await fetch(x=3)     # тоже эквивалентный

    assert r1 == r2 == r3 == 3
    assert len(counter) == 1


@pytest.mark.asyncio
async def test_wrapper_preserves_function_name():
    cache = Cache()

    @cache.wrapper
    async def my_function():
        pass

    assert my_function.__name__ == "my_function"


@pytest.mark.asyncio
async def test_wrapper_thundering_herd_under_concurrency():
    cache = Cache()
    counter = []

    @cache.wrapper
    async def fetch(user_id: int):
        counter.append(user_id)
        await asyncio.sleep(0.05)
        return f"user_{user_id}"

    results = await asyncio.gather(*[fetch(42) for _ in range(100)])

    assert len(counter) == 1
    assert all(r == "user_42" for r in results)