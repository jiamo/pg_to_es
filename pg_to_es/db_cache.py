import re
import types
import pickle
import aioredis
from aiocache.serializers import PickleSerializer

pickleSerializer = PickleSerializer()


def decode(obj):
    """ change bytes to str """
    if type(obj) is bytes:
        return obj.decode()
    elif type(obj) is dict:
        return {decode(k): decode(v) for k, v in obj.items()}
    elif type(obj) is list:
        return [decode(v) for v in obj]
    elif type(obj) is set:
        return {decode(v) for v in obj}
    else:
        return obj


class DBCache:
    """
    only get don't set
    """
    key_string = "table:{}:{}"
    table_re = re.compile('([A-Za-z])([A-Z][a-z])')

    def __init__(self, memory):
        self.pool = None
        self.memory = memory

    async def connect(self, redis_url):
        host, _, port = redis_url.partition(':')
        if not port:
            port = 6379
        try:
            port = int(port)
        except ValueError:
            raise ValueError(port)
        self.pool = await aioredis.create_redis_pool(
            (host, port), minsize=10, maxsize=60)
        print("redis_pool", self.pool, id(self.pool))

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()
        self.pool = None

    def load_object(self, value):
        return decode(value)

    async def _get(self, key):
        with await self.pool as redis:
            value = await redis.get(key.encode())
            if key.startswith("table"):
                return pickleSerializer.loads(value)  # the table value is using
            return self.load_object(value)

    async def get(self, key):
        value = await self.memory.get(key)
        if value is None:
            value = await self._get(key)
            await self.memory.set(key, value, 3)
            return value
        return value

    async def mget(self, key, *keys):
        # Notice this is not for get as table, need write a new
        key_list = [key, *keys]
        with await self.pool as redis:
            values = await redis.mget(*key_list)
            values = [self.load_object(v) for v in values]
        return values

    async def hget(self, key, field):
        # don't using memory cache here.
        with await self.pool as redis:
            value = await redis.hget(key, field)
            return self.load_object(value)

    async def hset(self, key, field, value):
        # don't using memory cache here.
        with await self.pool as redis:
            value = await redis.hset(key, field, value)
            return self.load_object(value)

    async def hmget(self, key, field, *fields):
        # don't using memory cache here.
        with await self.pool as redis:
            values = await redis.hmget(key, field, *fields)
            return [self.load_object(v) for v in values]

    # add for model object

    def _value2namespace(self, value, ident):
        """"""
        if type(value) is dict:
            return types.SimpleNamespace(**value)
        return value

    def _get_cache_key(self, table_cls_name, ident):
        table_name = self.table_re.sub('\\1_\\2', table_cls_name).lower()
        return self.key_string.format(table_name, ident)

    async def table_get(self, table_cls_name, ident):
        cache_key = self._get_cache_key(table_cls_name, ident)
        value = await self.get(cache_key)
        return self._value2namespace(value, ident)

        # if we don't get from redis we can get from database
