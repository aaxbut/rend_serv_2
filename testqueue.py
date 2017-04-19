# -*- coding: utf-8 -*-

#! /user/bin/env python3
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


for loop_main in range(10):
   # q.put(loop_main)

    t = mp.Process(target=worker,args=(q,))
    t.daemon= True
    t.start()
    

q.join()