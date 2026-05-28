from asyncio_primitives import RWLock
import asyncio


class Value:
    def __init__(self, value):
        self.value = value



async def main():
    obj = Value(12)

    rwlock = RWLock(obj)

    async with await rwlock.read() as guard:
        print(guard.value)
    

asyncio.run(main())