from asyncio_primitives import RWLock, Mutex
import asyncio


class Value:
    def __init__(self, value):
        self.value = value


obj = Value(12)

mutex_obj = Mutex(obj)

async def change():
    async with mutex_obj.get() as guard:
        guard = Value(16)


async def read():
    async with mutex_obj.get() as guard:
        print(guard.value)



async def main():


    asyncio.gather(change(), read())

asyncio.run(main())