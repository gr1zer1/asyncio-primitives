import asyncio
from asyncio_primitives import RWLock
import pytest
 
 
class Value:
    def __init__(self, value):
        self.value = value
 
@pytest.mark.asyncio
async def test_basic_read():
    obj = Value(12)
    rwlock = RWLock(obj)
    async with await rwlock.read() as guard:
        assert guard.value.value == 12
