import os
import random
import aiocache
from aiocache.serializers import PickleSerializer
from pg_to_es.config import user_config
from pg_to_es import db_cache
import asyncio
pg_slave_pool = None
pg_slave_objects = None
pg_objects = None
REDIS_URL = user_config['REDIS_URL']
REDIS_URL_SLAVE = user_config['REDIS_URL_SLAVE']
if isinstance(REDIS_URL_SLAVE, list):
    REDIS_URL_SLAVE = random.choice(REDIS_URL_SLAVE)
aio_redis_cache = aiocache.RedisCache(
    endpoint=REDIS_URL.split(":")[0],
    port=REDIS_URL.split(":")[1],
    serializer=PickleSerializer()
)  # writeable

pgdb_slave_config = {
    'host': user_config['PGDB_SLAVE_HOST'],
    'user': user_config['PGDB_USER'],
    'password': user_config['PGDB_PASSWORD'],
    'database': user_config['PGDB_DATABASE'],
    'port': user_config['PGDB_PORT'],
}
memory = aiocache.SimpleMemoryCache()
redis_slave_cache = db_cache.DBCache(memory)



async def clean():
    await redis_slave_cache.close()
    await aio_redis_cache.close()

loop = asyncio.get_event_loop()