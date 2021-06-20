# Since the executable fortran program may fail to launch for some reason, we will restart it few times

from warnings import warn
from multiprocessing.dummy import Pool
import multiprocessing.dummy
from queue import Queue
from threading import Thread

class YPoolFailed(Exception):
    pass

class FAIL:
    def __init__(self, error):
        self.error=error

class WAIT:
    pass

class STOP:
    pass


def ypool_worker(thread_idx, in_queue, out_queue):
    error = None
    while True:
        _in = in_queue.get()
        if isinstance(_in, STOP):
            return
        idx, quota, func, arg = _in
        for used_quota in range(quota):
            try:
                res = func(arg)
            except Exception as e: # TODO: Add some limitation
                warn(f"worker {thread_idx} task: {idx} (quota:{quota-used_quota-1}/{quota}) fail due to: {e}")
                error = e
                continue
            out_queue.put((idx, res))
            break
        else:
            out_queue.put((idx, FAIL(error)))


class YPool:
    # dedicated to a shity implemented fortran program
    def __init__(self, pool_size, quota=3, use_sequential=False, use_dummy_pool=False, use_process=False):
        self.pool_size = pool_size
        self.quota = quota

        self.use_sequential = use_sequential
        self.use_dummy_pool = use_dummy_pool
        if use_process:
            raise NotImplementedError
        self.use_process = use_process

    def map_dummy_pool(self, func, iterable):
        pool = multiprocessing.dummy.Pool(self.pool_size)
        return pool.map(func, iterable)

    def map_sequential(self, func, iterable):
        res_list = []
        for idx, arg in enumerate(iterable):
            for used_quota in range(self.quota):
                try:
                    res = func(arg)
                    res_list.append(res_list)
                except Exception as e:
                    warn(f"sequential ({self.quota-used_quota-1}/{self.quota}) fail at {idx} due to {e}")
            else:
                raise YPoolFailed(f"YPool (sequential) failed: {res.error}")
        return res_list

    def map_threading(self, func, iterable):
        in_queue = Queue()
        out_queue = Queue()
        res_list = [WAIT() for _ in iterable]

        thread_list = []
        for thread_idx in range(self.pool_size):
            thread = Thread(target=ypool_worker, args=(thread_idx, in_queue, out_queue))
            thread.start()
            thread_list.append(thread)

        for idx, arg in enumerate(iterable):
            in_queue.put((idx, self.quota, func, arg)) # queue size is infinite

        try:
            completed = 0
            while completed < len(iterable):
                idx, res = out_queue.get()
                if isinstance(res, FAIL):
                    raise YPoolFailed(f"YPool failed: {res.error}")
                res_list[idx] = res
                completed += 1
        finally:

            for _ in range(self.pool_size):
                in_queue.put(STOP())
            
            #"""
            for thread in thread_list:
                thread: Thread
                thread.join()
            #"""

        return res_list

    def map(self, func, iterable):
        if self.use_dummy_pool:
            return self.map_dummy_pool(func, iterable)
        elif self.use_sequential:
            return self.map_sequential(func, iterable)
        else:
            return self.map_threading(func, iterable)
                
