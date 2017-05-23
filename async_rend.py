# -*- coding: utf-8 -*-

from multiprocessing import (
    Queue,
    Process,
    Lock,
    JoinableQueue,
    )


class RenderIt:

    def __init__(self):
        self.queue = Queue()
        self.proc = [Process for x in range(2)]
        self.data = []

    def set_queue(self, data):
        #self.data.append(data)
        print('Add item to queue')
        if data:
            self.queue.put(data)
            return 1

    def get_status(self):
        print('Status of process')
        print(self.proc)
#        for p in self.proc:
#            print(p.pid, p.is_alive())


    def get_run(self):
        p = []
        print('Run work')





rt = RenderIt()
rt.set_queue({'redd':'sdsd'})
rt.get_status()
rt.get_run()