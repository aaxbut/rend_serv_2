import asyncio
from datetime import datetime, timezone
import os

def utc_now():
    # utcnow returns a naive datetime, so we have to set the timezone manually <sigh>
    return datetime.utcnow().replace(tzinfo=timezone.utc)
  
class Terminator:
  pass

_unittest = os.environ.get('UNITTEST')


class AsyncWorkerPool:
    def __init__(self, loop, num_workers: int, name: str, logger, worker_co, load_factor: int=1, job_accept_duration: int=None, max_task_time: int=None, return_futures: bool=False):
        """
        This class will create `num_workers` asyncio tasks to work against a queue of
        `num_workers * load_factor` items of back-pressure (IOW we will block after such
        number of items of work is in the queue).  `worker_co` will be called
        against each item retrieved from the queue. If any exceptions are raised out of
        worker_co, self.exceptions will be set to True.
        @param loop: asyncio loop to use
        @param num_workers: number of async tasks which will pull from the internal queue
        @param name: name of the worker pool (used for logging)
        @param logger: logger to use
        @param worker_co: async coroutine to call when an item is retrieved from the queue
        @param load_factor: multiplier used for number of items in queue
        @param job_accept_duration: maximum number of seconds from first push to last push before a TimeoutError will be thrown.
                Set to None for no limit.  Note this does not get reset on aenter/aexit.
        @param max_task_time: maximum time allowed for each task before a CancelledError is raised in the task.
            Set to None for no limit.
        @param return_futures: set to reture to return a future for each `push` (imposes CPU overhead)
        @return: instance of AsyncWorkerPool
        """
        loop = loop or asyncio.get_event_loop()
        self._loop = loop
        self._num_workers = num_workers
        self._logger = logger
        self._queue = asyncio.Queue(num_workers * load_factor)
        self._workers = None
        self._exceptions = False
        self._job_accept_duration = job_accept_duration
        self._first_push_dt = None
        self._max_task_time = max_task_time
        self._return_futures = return_futures
        self._name = name
        self._worker_co = worker_co
        self._total_queued = 0

    async def _worker_loop(self):
        while True:
            got_obj = False
            future = None

            try:
                item = await self._queue.get()
                got_obj = True

                if item.__class__ is Terminator:
                    break

                future, args, kwargs = item
                # the wait_for will cancel the task (task sees CancelledError) and raises a TimeoutError from here
                # so be wary of catching TimeoutErrors in this loop
                result = await asyncio.wait_for(self._worker_co(*args, **kwargs), self._max_task_time, loop=self._loop)

                if future:
                    future.set_result(result)
            except (KeyboardInterrupt, MemoryError, SystemExit) as e:
                if future:
                    future.set_exception(e)
                self._exceptions = True
                raise
            except BaseException as e:
                if future:
                    future.set_exception(e)
                self._exceptions = True

                if not _unittest:
                    self._logger.exception('Worker call failed')
            finally:
                if got_obj:
                    self._queue.task_done()

    @property
    def exceptions(self):
        return self._exceptions

    @property
    def total_queued(self):
        return self._total_queued

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.join()

    async def push(self, *args, **kwargs) -> asyncio.Future:
        """ Method to push work to `worker_co` passed to `__init__`.
        :param args: position arguments to be passed to `worker_co`
        :param kwargs: keyword arguments to be passed to `worker_co`
        :return: future of result """
        if self._first_push_dt is None:
            self._first_push_dt = utc_now()

        if self._job_accept_duration is not None and (utc_now() - self._first_push_dt) > self._job_accept_duration:
            raise TimeoutError("Maximum lifetime of {} seconds of AsyncWorkerPool: {} exceeded".format(self._job_accept_duration, self._name))

        future = asyncio.futures.Future(loop=self._loop) if self._return_futures else None
        await self._queue.put((future, args, kwargs))
        self._total_queued += 1
        return future

    def start(self):
        assert self._workers is None

        self._workers = [asyncio.ensure_future(self._worker_loop(), loop=self._loop) for _ in range(self._num_workers)]

    async def join(self):
        # no-op if workers aren't running
        if not self._workers:
            return

        self._logger.debug('Joining {}'.format(self._name))
        # The Terminators will kick each worker from being blocked against the _queue.get() and allow
        # each one to exit
        for _ in range(self._num_workers):
            await self._queue.put(Terminator())

        try:
            await asyncio.gather(*self._workers, loop=self._loop)
            self._workers = None
        except:
            self._logger.exception('Exception joining {}'.format(self._name))
            raise
        finally:
            self._logger.debug('Completed {}'.format(self._name))