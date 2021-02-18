from collections import deque
from concurrent.futures import Future
from threading import Thread, Lock
import time
from asyncio import wait_for, wrap_future
import asyncio
import sys


def from_coroutine():
    return sys._getframe(2).f_code.co_flags & 0x380


def which_pill():
    if from_coroutine():
        print("Red")
    else:
        print("Blue")


def spam():
    which_pill()


async def async_spam():
    which_pill()

# loop.run_until_complete(async_spam())  -> Red


class Queuey:
    def __init__(self, maxsize):
        self.maxsize = maxsize
        self.mutex = Lock()
        self.items = deque()
        self.getters = deque()
        self.putters = deque()

    def get_noblock(self):
        with self.mutex:
            if self.items:
                # Wake a putters here we can't just put the
                # item we can't put before
                if self.putters:
                    self.putters.popleft().set_result(True)
                return self.items.popleft(), None
            else:
                fut = Future()
                self.getters.append(fut)
                return None, fut   # if have future must have no item

    def put_noblock(self, item):
        with self.mutex:
            if len(self.items) < self.maxsize:
                self.items.append(item)
                # Wake a getters
                if self.getters:
                    self.getters.popleft().set_result(self.items.popleft())
            else:
                fut = Future()
                self.putters.append(fut)
                return fut

    async def get_async(self, wait=True):
        item, fut = self.get_noblock()
        if fut:
            if wait:
                item = await wait_for(wrap_future(fut), None)
            else:
                # we wait in client
                item = wait_for(wrap_future(fut), None)
        return item

    def get_sync(self):
        item, fut = self.get_noblock()
        if fut:
            item = fut.result()
        return item

    def get(self):
        if from_coroutine():
            return self.get_async()
        else:
            return self.get_sync()

    async def put_async(self, item):
        while True:
            fut = self.put_noblock(item)
            if fut is None:
                return True
            # when it is future it can't put
            # so we wait until we can put
            # if we can't handle this logic we return can't return
            # empty future when it is get future can get
            # but we can get the result
            # here put item, we should not lose the item

            await wait_for(wrap_future(fut), None)
            await asyncio.sleep(0.2)  # take a sleep

    def put_sync(self, item):
        while True:
            fut = self.put_noblock(item)
            if fut is None:
                return
            fut.result()

    async def put_async_nowait(self, item):
        while True:
            fut = self.put_noblock(item)
            if fut is None:
                return True
            await wait_for(wrap_future(fut), None)

    def put(self, item):
        if from_coroutine():
            return self.put_async(item)
        else:
            return self.put_sync(item)


def producer(q, n):
    for i in range(n):
        q.put(i)
    q.put(None)


def consumer(q):
    while True:
        item = q.get()
        if item is None:
            break
        print("Got:", item)


async def async_consumer(q):
    # loop = asyncio.get_event_loop()
    while True:
        # run a thread in loop or async using q.get_async
        # item = await loop.run_in_executor(None, q.get)
        # This make asyncio.Queue can't work
        item = await q.get()
        if item is None:
            break
        print("Async Got:", item)


    # print("at last begin")
    # item = await q.get_async(wait=False)
    # print("at last end", item,  asyncio.iscoroutine(item))


async def async_producer(q, n):
    for i in range(n):
        await q.put(i)
    await q.put(None)


if __name__ == "__main__":
    q = Queuey(2)
    Thread(target=producer, args=(q, 10)).start()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_consumer(q))

    # other
    q = Queuey(2)
    time.sleep(1)
    Thread(target=consumer, args=(q,)).start()
    loop.run_until_complete(async_producer(q, 10))

    time.sleep(1)
    from queue import Queue
    q = Queue()
    Thread(target=producer, args=(q, 10)).start()
    Thread(target=consumer, args=(q,)).start()

    time.sleep(1)
    from asyncio import Queue
    q = Queue()
    loop.create_task(async_consumer(q))
    loop.run_until_complete(async_producer(q, 10))

    time.sleep(1)
