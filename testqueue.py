# -*- coding: utf-8 -*-

#! /usr/bin/env python3
import multiprocessing
import multiprocessing as mp
from threading import Thread
from queue import Queue


class HendlerMidleware:
    index = 1
    def __init__(self, id_task,f):
        self.index+=self.index
        self.task_id = f


    def print_info(self):
        print('none',self.index)
        return self.index, self.task_id

f = []

c = HendlerMidleware(3,f)

f.append(c)

c1 = HendlerMidleware(3,f)

print(c.index,c1.task_id)


import multiprocessing as mp

def worker(q):
    q.put()
   # while True:
    ##   
      #  print(item)

       # q.task_done()

q = Queue()


for loop_main in range(10):pass
   # q.put(loop_main)

    #t = mp.Process(target=worker,args=(q,))
    #t.daemon= True
    #t.start()
    

q.join()

from functools import partial

partial1 = partial

def partial(func, *args, **kwargs):
    def run_before(*fargs, **fkwargs):

        run_kwargs = kwargs.copy()
        run_kwargs.update(fkwargs)

        return func(*(args+fargs), **run_kwargs)
        
    run_before.func = func
    run_before.args = args
    run_before.kwargs = kwargs

    return run_before

t = partial1(int, base=2)

#print(t)

t.__doc__= 'Convert base 2 string to an int.'

print(t('100100'))

from functools import lru_cache


@lru_cache(maxsize=None)
def fib(n):
    if n <2:
        return n
    return fib(n-1) + fib(n-2)


#print([fib(n) for n in range(100)])