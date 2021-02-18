import asyncio
import signal
from ppsqlreplication.util import int_lsn_to_str
import os
import sys
import redis
import pytz
from ppsqlreplication.logic_stream import LogicStreamReader
from ppsqlreplication.row_event import (
    UpdateRowEvent,
    WriteRowEvent,
    DeleteRowEvent
)

import traceback
import click
import threading
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import time
import logging.config
from pg_to_es.yqueue import Queuey
from pg_to_es import models, globals
from pg_to_es.config import stage, user_config
from pg_to_es import misc
from multiprocessing import Queue


psql_repl_setting_str = "dbname={} user={} password={} host={}".format(
    user_config["PGDB_DATABASE"],
    user_config["PGDB_USER"],
    user_config["PGDB_PASSWORD"],
    user_config["PGDB_HOST"]  # master
)
slot_name = user_config["SLOT_NAME"]

logging.LOG_PREFIX = stage
logging.FLUENTD_HOST = os.environ.get(
    'FLUENTD_HOST', user_config["FLUENTD_HOST"])
logging.FLUENTD_PORT = os.environ.get(
    'FLUENTD_PORT', user_config["FLUENTD_PORT"])
logging.config.fileConfig('logging.ini')
logger = logging.getLogger('debug')
PSQL_START_LSN_KEY = "PSQL_START_LSN_KEY"
PG_TO_ES_PROGRAM_KEEP_ALIVE = "PG_TO_ES_PROGRAM_KEEP_ALIVE"
los_tz = pytz.timezone("America/Los_Angeles")
loop = asyncio.get_event_loop()


class ElasticThread(threading.Thread):

    def __init__(self, queue, es_url, es_port, redis_url, loop,):
        super(ElasticThread, self).__init__()
        self.success_count = 0
        self.daemon = True
        self.queue = queue
        self.es_url = es_url
        self.es_port = es_port
        self.redis_url = redis_url
        self.gap_click = 10  # (second)
        self.loop = loop

    async def es_hub(self, event):
        if event["table"] == "album":
            return await self.es_album(event)
        else:
            return await self.get_update_info(event)

    async def es_album(self, event):
        info_class = models.mapping[event["table"]]
        index = "{}-{}".format("album", stage)

        if event["type"] == "UpdateRowEvent":
            b = event["row"]["before_values"]
            o = event["row"]["after_values"]
            if info_class.same_value(b, o):
                return None
            return {
                "_op_type": "update",
                "_id": o['id'],
                "_index": index,
                "_type": "album",
                "doc": info_class.changed_value(b, o)
            }

        elif event["type"] == "WriteRowEvent":
            o = event["row"]["values"]
            info = info_class(**o)
            if info.has_extra_info:
                await info.extra_info()
            info = dict(info)
            return {
                "_op_type": "create",
                "_id": o['id'],
                "_index": index,
                "_type": "album",
                "_source": info
            }

        elif event["type"] == "DeleteRowEvent":
            o = event["row"]["values"]
            return {
                "_op_type": "delete",
                "_id": o["id"],
                "_index": index,
                "_type": "album"
            }
        else:
            logger.warning("Type classify error in es_album_info")


    async def get_update_info(self, event):
        # different table have different name
        info_class = models.mapping[event["table"]]
        index = "{}-{}".format(event["table"], stage)
        if event["type"] == "UpdateRowEvent":
            b = event["row"]["before_values"]
            o = event["row"]["after_values"]
            if info_class.same_value(b, o):
                return None
        elif event["type"] == "WriteRowEvent":
            o = event["row"]["values"]
        elif event["type"] == "DeleteRowEvent":
            o = event["row"]["values"]
            return {
                "_op_type": "delete",
                "_id": o["id"],
                "_index": index,
                "_type": "_doc"
            }
        else:
            return None

        info = info_class(**o)
        if info.has_extra_info:
            await info.extra_info()

        # print("info is ", info)
        action = {
            "_id": o["id"],
            "_index": index,
            "_type": "_doc",
            "_source": info
        }
        # print(action)
        return action

    def bulk_update_offer(self, actions):
        try:
            success, _ = bulk(self.es, actions,
                              raise_on_error=True,
                              refresh="")
            self.success_count += success
            logger.info("success bulk {}".format(success))
        except Exception as e:
            msg = str(e) + ' ' + traceback.format_exc()
            logger.error(msg)

    async def work(self):
        # we need a thread to handle running process
        self.es = Elasticsearch([self.es_url], port=self.es_port)
        self.cache = redis.StrictRedis(host=self.redis_url, port=6379, db=0)
        tmp_list = []

        try:
            while True:

                event = await self.queue.get_async(wait=False)
                if asyncio.iscoroutine(event):
                    # we don't get some but we need insert before
                    if tmp_list:
                        logger.info("future len tmp_list is {}".format(
                            len(tmp_list)))
                        self.bulk_update_offer(tmp_list)
                        tmp_list = []
                    logger.info("queue {} getters {} putters {}".format(
                        len(self.queue.items), len(self.queue.getters),
                        len(self.queue.putters)))

                    # if don't we have in queue. after insert we await
                    logger.info("queue begin to wait......")
                    event = await event

                # here event must have be ok
                if event is None:
                    logger.info("ES process got None")
                    break

                info = await self.es_hub(event)

                if info:
                    tmp_list.append(info)

                if len(tmp_list) > 50:
                    logger.info("tmp_list is > 50")
                    self.bulk_update_offer(tmp_list)
                    tmp_list = []

        except Exception as e:
            msg = str(e) + ' ' + traceback.format_exc()
            print(msg)
            logger.error(msg)

        if tmp_list:
            self.bulk_update_offer(tmp_list)

        # self.queue.task_done()
        logger.info("Process insert es finish {}".format(self.success_count))
        return self.success_count

    def run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.work())


class BinlogManager(threading.Thread):
    def __init__(self, cache, stream, es_queue, es_thread, loop, start_lsn):
        super().__init__()
        self.stop_read = False
        self.cache = cache
        self.stream = stream
        self.es_queue = es_queue
        self.es_thread = es_thread
        self.loop = loop
        self.last_lsn = start_lsn

    def goodbye(self, bad_happened=False):
        logger.info("I got goodbye")
        self.stream.send_feedback()
        self.stream.close()
        logger.info(
            "end at {}".format(self.stream.flush_lsn))

        if self.es_thread.is_alive():
            self.es_queue.put(None)
            self.es_thread.join()  # why join can't
            logger.info("Main Process es_process join")
        self.cache.delete(PG_TO_ES_PROGRAM_KEEP_ALIVE)
        return

    async def work(self):
        try:
            # every 10s to set cache I am live
            sleep_time = 0
            while True:
                for binlog_events in self.stream:
                    for binlog_event in binlog_events:
                        event = {
                            "schema": binlog_event.schema,
                            "table": binlog_event.table,
                            "type": type(binlog_event).__name__,
                            "row": binlog_event.row
                        }
                        await self.es_queue.put_async(event)

                    self.last_lsn = self.stream.flush_lsn
                    self.cache.set(PSQL_START_LSN_KEY, self.last_lsn)

                if self.stop_read:
                    logger.info("In stop_read got")
                    self.goodbye()
                    break

                await asyncio.sleep(0.2)
                sleep_time += 0.2
                if sleep_time > 10:
                    self.stream.send_feedback(keep_live=True)
                    logger.info(
                        "sleep wal_end {} hex {} next_lsn {} hex {} ".format(
                            self.last_lsn,
                            int_lsn_to_str(self.last_lsn),
                            self.stream.next_lsn,
                            int_lsn_to_str(self.stream.next_lsn)))

                    self.cache.setex(PG_TO_ES_PROGRAM_KEEP_ALIVE, 15, 1)
                    self.cache.set(PSQL_START_LSN_KEY, self.stream.flush_lsn)
                    sleep_time = 0

        except Exception as e:
            msg = str(e) + ' ' + traceback.format_exc()
            logger.error(msg)
            self.goodbye(bad_happened=True)

    def run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.work())


# cache need to bee used in lock
redis_url, redis_port = user_config["REDIS_URL"].split(":")
cache = redis.StrictRedis(host=redis_url, port=redis_port, db=0)


@click.command()
@click.option('--last/--no-last', default=True)
def main(last):
    # TODO may be need use int(pg_current_wal_lsn) as old_start
    # This make it work?
    if last:
        old_start_lsn = cache.get(PSQL_START_LSN_KEY)
        old_start_lsn = int(old_start_lsn) if old_start_lsn else 0
    else:
        old_start_lsn = 0

    run(old_start_lsn)


def run(old_start_lsn):
    logger.info("Main Process {} begin at {}".format(
        os.getpid(), old_start_lsn))
    only_events = [WriteRowEvent, UpdateRowEvent, DeleteRowEvent]
    only_tables = list(models.mapping.keys())
    stream = LogicStreamReader(
        connection_settings=psql_repl_setting_str,
        only_schemas=["public"],
        only_tables=only_tables,
        only_events=only_events,
        start_lsn=old_start_lsn,
        slot_name=slot_name,
        use_add_table_option=False
    )
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    es_queue = Queuey(1000)
    work_loop = misc.loop
    manager_loop = asyncio.new_event_loop()
    es_thread = ElasticThread(
        es_queue,
        user_config["ES_URL"],
        user_config["ES_PORT"],
        redis_url,
        work_loop,
    )

    es_thread.start()
    binlog_manager = BinlogManager(
        cache, stream, es_queue, es_thread, manager_loop, old_start_lsn)

    def goodbye(signo, frame):
        logger.info("MainThread got goodbye")
        binlog_manager.stop_read = True
        binlog_manager.join()
        logger.info("MainThread got end")
        sys.exit()

    signal.signal(signal.SIGINT, goodbye)
    signal.signal(signal.SIGTERM, goodbye)
    binlog_manager.start()
    binlog_manager.join()


if __name__ == "__main__":
    while not cache.setnx(PG_TO_ES_PROGRAM_KEEP_ALIVE, 1):
        time.sleep(1)

    # set the key to expire for some case
    cache.setex(PG_TO_ES_PROGRAM_KEEP_ALIVE, 15, 1)
    logger.info("Get the redis locker")
    try:
        main()
    except Exception as e:
        msg = str(e) + ' ' + traceback.format_exc()
        logger.error(msg)
    finally:
        logger.info("Finish main process")
