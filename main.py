from asyncio_primitives import Cache
import asyncio


cache = Cache()
db = {2:"user_2"}

call_count = 0

@cache.wrapper
async def get_data_from_db(user_id: int):
    global call_count
    call_count += 1
    await asyncio.sleep(0.1) 
    return db.get(user_id)

async def main():
    results = await asyncio.gather(*[get_data_from_db(2) for _ in range(100)])
    print(f"DB вызвана: {call_count} раз") 
    print(f"Результаты одинаковые: {len(set(results)) == 1}")


asyncio.run(main())