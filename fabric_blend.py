import asyncio

FLUSH_TIMEOUT = 1

class TaskWait:

    def __init__(self,loop,*args,**kwargs):
        self._loop = loop
        self._waiter = asyncio.Event()
        self._flush_future = self._loop.create_task(self.flush_task())

    @asyncio.coroutine
    def flush_task(self):
        print('dfdfd')
        while True:
            try:
                yield from asyncio.wait_for(self._waiter.wait(),timeout=10, loop=self.loop)
                

            except asyncio.TimeoutError as e:
                print(e,'dfdf')
            self._waiter.clear()

    def force_flush():
        print('dfdffd')
        self._waiter.set()




loop = asyncio.get_event_loop()
foo = TaskWait(loop)
loop.run_forever()
loop.close()