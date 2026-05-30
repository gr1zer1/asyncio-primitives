# asyncio-primitives

Набор дополнительных асинхронных примитивов для `asyncio`.

Сейчас в пакете есть `RWLock` - read/write lock, который позволяет нескольким читателям работать одновременно, но пускает только одного писателя эксклюзивно.

## Установка для разработки

```bash
pip install -r requirements.txt
```

## Запуск тестов

```bash
pytest
```

## RWLock

`RWLock` можно использовать как обертку над объектом( **Rust style** ):

```python
from asyncio_primitives import RWLock


class Value:
    def __init__(self, value):
        self.value = value


rwlock = RWLock(Value(12))

async with rwlock.read() as value:
    print(value.value)

async with rwlock.write() as value:
    value.value = 99
```

Также `RWLock` можно использовать как обычный lock для защиты произвольного участка кода:

```python
rwlock = RWLock()

async with rwlock.reader():
    # read-only section
    ...

async with rwlock.writer():
    # write section
    ...
```

## Поведение

- Несколько читателей могут держать lock одновременно.
- Писатель получает эксклюзивный доступ.
- Если писатель уже ждет lock, новые читатели ждут, чтобы писатель не голодал.
- `read()` возвращает read-proxy и запрещает менять поля объекта.
- `write()` возвращает write-proxy и позволяет менять поля объекта.
- `reader()` и `writer()` используются, когда lock нужен не как обертка над объектом, а как защита блока кода.

## Что стоит добавить дальше

### AsyncCell

Контейнер для одного значения с асинхронным доступом.

Идея API:

```python
cell = AsyncCell(10)

value = await cell.get()
await cell.set(20)

async with cell.write() as value:
    ...
```

Зачем нужен: безопасно хранить и менять одно разделяемое значение между coroutines.

### AsyncOnce

Примитив, который гарантирует, что асинхронная инициализация выполнится только один раз.

Идея API:

```python
once = AsyncOnce()

await once.run(init_database)
```

Зачем нужен: lazy init, подключение к базе, загрузка конфигов, прогрев кеша.

### AsyncBarrier

Барьер, который ждет, пока заданное количество задач дойдет до одной точки.

Идея API:

```python
barrier = AsyncBarrier(3)

await barrier.wait()
```

Зачем нужен: синхронизация нескольких worker-задач перед переходом к следующей фазе.

### AsyncCountdownEvent

Event, который срабатывает, когда счетчик дошел до нуля.

Идея API:

```python
event = AsyncCountdownEvent(5)

event.decrement()
await event.wait()
```

Зачем нужен: дождаться завершения набора независимых операций.

### AsyncRateLimiter

Ограничитель скорости выполнения операций.

Идея API:

```python
limiter = AsyncRateLimiter(rate=10, per=1.0)

async with limiter:
    await call_api()
```

Зачем нужен: ограничивать запросы к API, очереди задач и фоновые операции.

### AsyncResourcePool

Пул переиспользуемых ресурсов.

Идея API:

```python
pool = AsyncResourcePool(create_connection, max_size=10)

async with pool.acquire() as connection:
    await connection.execute(...)
```

Зачем нужен: управлять соединениями, клиентами, сессиями и другими дорогими объектами.

### AsyncPriorityQueueLock

Lock с очередью ожидания по приоритету.

Идея API:

```python
lock = AsyncPriorityQueueLock()

async with lock.acquire(priority=10):
    ...
```

Зачем нужен: давать более важным задачам доступ раньше обычных.

## План развития

1. Довести `RWLock` до стабильного API.
2. Добавить типизацию для proxy/guard объектов.
3. Добавить тесты на отмену задач во время ожидания lock.
4. Добавить `AsyncCell` как следующий небольшой примитив.
5. После этого добавить `AsyncOnce`, `AsyncBarrier` и `AsyncRateLimiter`.
