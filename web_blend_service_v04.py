from aiohttp import web
import asyncio
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

 

import logging
import json

import aiohttp_session
from aiohttp_session import setup, get_session, session_middleware
#from aiohttp_session.cookie_storage import EncryptedCookieStorage

import multiprocessing as mp
import multiprocessing.pool, threading, random, time

from multiprocessing import Process, freeze_support
from threading import Thread
from queue import Queue
import signal
import functools
import queue

from queue import Empty

import bpy
from bpy.app.handlers import persistent


from datetime import datetime
import time
 
# import config settings
import configparser


conf = configparser.RawConfigParser()
conf.read('wb.conf')
BLEND_DIR = conf.get('bl_path','BLEND_DIR')
USERS_DIR = conf.get('bl_path','USERS_DIR')
dbconnectionhost = conf.get('base','dbconnectionhost')
dbname = conf.get('base','dbname')
dbusername = conf.get('base','dbusername')
dbpassword = conf.get('base','dbpassword')
u_ugid = conf.get('usr_permission','uid')
u_gguid = conf.get('usr_permission','gid')
# base connect
import MySQLdb as mysql
import random


# end import config settings

class TaskWait:

    def __init__(self,loop,*args,**kwargs):

        self._loop = loop
        self._waiter = asyncio.Event()
        self._flush_future = self._loop.create_task(self.flush_task())
        self._pool = pool
        self._log = logging.getLogger(self.__class__.__name__)
        self._queue = q_priority_job


    
    def worker(self, next_job):

        #next_job.data=dict(next_job.data)
        task = json.loads(next_job.data)


        self._log.info('worker job : {} '.format(task['project_name']))


        try:

            bpy.ops.wm.open_mainfile(filepath=task['project_name'])
            
            bpy.context.scene.frame_start = 0
            bpy.context.scene.frame_end = 10

            bpy.context.scene.render.filepath ='{}{}.mp4'.format(str(task['result_dir'])+'/'+str('roller_video'),random.randint(1,2000))
            bpy.context.scene.render.engine = 'CYCLES'
            bpy.context.scene.cycles.device='CPU'
            bpy.context.scene.render.ffmpeg.format = 'MPEG4'
            bpy.context.scene.render.ffmpeg.video_bitrate=750
            bpy.context.scene.render.ffmpeg.audio_bitrate=124

            bpy.ops.render.render(animation=True,scene=bpy.context.scene.name)

            os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
            os.chmod(bpy.context.scene.render.filepath, 0o777)
         
            bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
            bpy.context.scene.render.filepath ='{}.jpg'.format(str(task['result_dir'])+'/'+str('roller_video'))

            bpy.ops.render.render(write_still=True)

            os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
            os.chmod(bpy.context.scene.render.filepath, 0o777)

        

        except Empty: pass

        finally:
            self._loop.stop()




        return {'ok':'ok'}



    @asyncio.coroutine
    def run_jobs(self):
        

        #next_job = q.get()
        #data = self._queue.get()
        #self._log.info('Current: {} '.format(type(self._queue)))
        
        lt = []        

        #data = q_priority_job.get()
        self._log.info('{} '.format(datetime.now().strftime('%c')))
        while not self._queue.empty():

            self._log.info('Current queue : {} '.format('NOT EMPTY '))
            data = self._queue.get()
            lt.append(data)
            #map(self.worker, data)
            #run = self._pool.submit(self.worker, (data,))
            #run.start()
            
           # self._log.info('Current queue : {} '.format(run))
            #self._loop.run_until_complete(self.worker(data))
            #t = threading.Thread(target=self.worker, args=(data,))
            #t.start()
            #try:

                #self.worker(data)
            #except Exception as e:
            #    self._log.info('Exception Current queue : {} '.format(e))
            #res = self._pool.apply_async(self.worker, (data,))

        self._log.info('{} Coutn in queue '.format(datetime.now().strftime('%c'), lt.__len__()))
        procs = (mp.Process(target=self.worker, args=(data,)) for _ in lt)

        for proc in procs:
            proc.daemon = True
            proc.start()
        
        for proc in procs:
            proc.join()
           # p.close()
            #p.join()

            #test = yield from self.worker(data)


            self._queue.task_done()

            






        #next_job.task_done()

        #return {'o@@@@@@@@@@@@@@@@@k':'o@@@@@@@@@@@@@k'}
        #self._log.info('Current queweweweweeeeweweue name: {} '.format(type(q)))

        #for x in range(10):
        #    self._log.info('Current queweweweweeeeweweue name: {} ######{}'.format(next_job,str(x)))
        
        #while True:
        #   next_job = q.get()
        #    self._log.info('Current queue name: {} '.format(next_job))
        #    self._log.info('!!!!!!!!!!!!!!!!!!!!! {} ******************'.format(procs))
    
            

        #next_job.task_done()





    @asyncio.coroutine
    def flush_task(self):

        #start_flush = datetime.now().strftime('%c')
        
       # self._log.info('Time Flush_TASK {}'.format(datetime.now().strftime('%c')))

        while True:
            try:
                
                #self._log.info('{} Current process name: {} '.format(self._queue, mp.current_process().name))

                #data = self.run_jobs(self._queue)
                #data = self._pool.apply_async(self.run_jobs,  self._queue)

                #self._log.info('Current queue name: {} '.format(next_job))

                self._log.info( datetime.now().strftime('%c'))
                task = self._loop.create_task(self.run_jobs())
                self._log.info('Date {} , TASK status {}'.format(datetime.now().strftime('%c'),task))

                data = yield from asyncio.wait_for(self._waiter.wait(),timeout=2.0, loop=self._loop)
         
            except asyncio.TimeoutError:
                pass


            self._waiter.clear()

    def force_flush():
        #self._log.info('{} Force flush Current process name: {}  '.format(__name__, mp.current_process().name))
       # self._waiter.clear()
        self._waiter.set()


    def submit(self,callbacks,*args, **kwargs):
        
        print(args,kwargs)
        future = self._pool.submit(self.flush_task)
     #   future2 = self._pool.submit(self.run_jobs)


        return future


@functools.total_ordering
class Job:

    def __init__(self, priority, description, data):
        self.priority = priority
        self.description = description
        self.data = data
        print('New job:', description)

        return 

    def __eq__(self,other):
        try:
            return self.priority == other.priority
        except AttributeError:
            return NotImplemented

    def __lt__(self,other):
        try:
            return self.priority < other.priority
        except AttributeError:
            return NotImplemented



 
def render_proc_cr1(task):

    logging.info(' {} ************* **'.format(task))
    
    
    try:

        bpy.ops.wm.open_mainfile(filepath=task['project_name'])
        bpy.context.scene.frame_start = 0
        bpy.context.scene.frame_end = 10

        bpy.context.scene.render.filepath ='{}{}.mp4'.format(str(task['result_dir'])+'/'+str('roller_video'),random.randint(1,2000))
        bpy.context.scene.render.engine = 'CYCLES'
        bpy.context.scene.cycles.device='CPU'
        bpy.context.scene.render.ffmpeg.format = 'MPEG4'
        bpy.context.scene.render.ffmpeg.video_bitrate=750
        bpy.context.scene.render.ffmpeg.audio_bitrate=124

        bpy.ops.render.render(animation=True,scene=bpy.context.scene.name)

        os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
        os.chmod(bpy.context.scene.render.filepath, 0o777)
         
        bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
        bpy.context.scene.render.filepath ='{}.jpg'.format(str(task['result_dir'])+'/'+str('roller_video'))

        bpy.ops.render.render(write_still=True)

        os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
        os.chmod(bpy.context.scene.render.filepath, 0o777)

        

    except Empty: pass

@asyncio.coroutine 
def render_proc_cr23(task):

    logging.info('**{}######################'.format(task))

    next_job = q_priority_job.get()

    logging.info('**$$$$$$$$$$$$$$$$ {}################{}######'.format(next_job.description['message'],type(next_job.description) ))

    try:

        bpy.ops.wm.open_mainfile(filepath=task['project_name'])
        bpy.context.scene.frame_start = 0
        bpy.context.scene.frame_end = 10

        bpy.context.scene.render.filepath ='{}{}.mp4'.format(str(task['result_dir'])+'/'+str('roller_video'),random.randint(1,2000))
        bpy.context.scene.render.engine = 'CYCLES'
        bpy.context.scene.cycles.device='CPU'
        bpy.context.scene.render.ffmpeg.format = 'MPEG4'
        bpy.context.scene.render.ffmpeg.video_bitrate=750
        bpy.context.scene.render.ffmpeg.audio_bitrate=124

        bpy.ops.render.render(animation=True,scene=bpy.context.scene.name)

        os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
        os.chmod(bpy.context.scene.render.filepath, 0o777)
         
        bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
        bpy.context.scene.render.filepath ='{}.jpg'.format(str(task['result_dir'])+'/'+str('roller_video'))

        bpy.ops.render.render(write_still=True)

        os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
        os.chmod(bpy.context.scene.render.filepath, 0o777)

        

    except Empty: pass



    #while True:
     #   next_job = q_priority_job.get()
        
       # d = dict(next_job.description)
        
      #  logging.info('**$$$$$$$$$$$$$$$$ {}###$$$$$$$$$$$$####'.format(next_job.description))


        #q_priority_job.task_done()


#    logging.info(' Queue status {} ****: {} '.format(
 #                                                       q_priority_job,
  #                                                      q_priority_job.get()
##                                                   )
  #                                                  )

    #q_priority_job.task_done()

    #return

def render_proc_cr(task):

    logging.info('###$$$$${}$$$$$$$$$$###'.format(task.data))
    #if q_priority_job.emtpy():

    logging.info('#: QUEUE ##{},::###:: {}'.format(task,q_priority_job))





queue_taska = Queue(3)
q_priority_job = queue.PriorityQueue()


def calback_from_pool(t):
    
    #item = yield from queue_taska.get()
    logging.info('**** {} Current process name: :  '.format('__name__, mp.current_process().name , q_priority_job'))
    #mp.current_process().name

    


def transmit(request):
    
    data = yield from request.text()
    

    mp.freeze_support()

   # pool = mp.Pool(1)

    req_json = json.loads(data)
    logging.info('{} :::'.format(datetime.now().strftime('%c')))
    

    if request.content_type == 'application/json':

        joq_data = Job(3, request.method, data)
        q_priority_job.put(joq_data)
        logging.info('Job description {}'.format(q_priority_job))
        #logging.info('user :{} ::'.format(req_json['user']))
        l = [joq_data]
      #  l.append([joq_data])
        #res = pool.apply_async(render_proc_cr,  l, callback=calback_from_pool)
        #queue_taska.put(res)
        logging.info('!!!!!$#$####!!!!!!!! {} ******d ***********'.format(l))

        #pool.close()
        #pool.join()


        
        return web.Response(body=json.dumps({'ok': req_json}).encode('utf-8'), content_type='application/json')
    return web.json_response({'ok':'ok_transmit'})    

 

 

##
@persistent
def render_complete(scene):
    logging.info('#### RENDER COMPLETE #######{} ################{}#########{}####'.format(scene,bpy.data.filepath,bpy.context.scene.render.filepath))
    logging.info('#####{}####{}##'.format(os.path.abspath(bpy.data.filepath),bpy.data.filepath))

    try:
        #ins()
        h = bpy.context.scene.render.filepath
        with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
            try:
                db.execute('update users_rollers set is_ready=1,filename_video=%s where id=%s',('video/roller_video.mp4',h.split('/')[7]))
                db.execute('update users_rollers set is_ready=1,filename_screen=%s where id=%s',('video/roller_video.jpg',h.split('/')[7]))
            except Exception as e:
                logging.info('Base err : {}'.format(e))
            finally:
                db.close()
            
         
        os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
        
    except:
        pass


@persistent
def render_begin(scene):
    logging.info('###### RENDER BEGIN  ###{}################{}#########{}####'.format(scene,bpy.data.filepath,bpy.context.scene.render.filepath))
    logging.info('#####{}####{}##'.format(os.path.abspath(bpy.data.filepath),bpy.data.filepath))

    try:
        #ins()
        h = bpy.context.scene.render.filepath
        with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
            try:
                cur.execute('update users_rollers set is_ready=1,filename_video=%s where id=%s',('video/roller_video.mp4',h.split('/')[7]))
            except Exception as e:
                logging.info('Base err : {}'.format(e))
            finally:
                db.close()
     
        os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
    except:
        pass


### server info
import sys, os, platform, shutil


def server_info():
    sysname, nodename, release, version, machine, processor = platform.uname()
    logging.info('{} SRV: {} '.format(datetime.now().strftime('%c'), nodename))


test_calback_task = []



@asyncio.coroutine
def test_calback(item, handler):

    #test_calback_task = queue.put(item)

    def middle_handler(request):
        response = yield from handler(request)

        logging.info(':::{}::::::Time now: {} handler status {}'.format(item, datetime.now().strftime('%c'), str(handler)))
       # logging.info('::In Task:{}::::::Tim'.format(test_calback_task))
     
        return response

    return middle_handler


     

def handler_signal(loop):
    loop.remove_signal_handler(signal.SIGTERM)
    loop.stop()


 
def start_bk_task():

    try:
        logging.info('*'*10000)
    except Exception as e:
        logging.info('{}'.format(e))

@asyncio.coroutine
def shutdown(server, app, handler):
    server.close()
    yield from server.wait_closed()
    
    
    yield from app.shutdown()
    yield from handler.finish_connections(10.0)
    yield from app.cleanup()



 
def init(loop):
    logging.basicConfig(level=logging.DEBUG)
 
    bpy.app.handlers.render_complete.append(render_complete)
     
    app = web.Application(loop=loop, middlewares=[test_calback])
    #app = web.Application(loop=loop)
   
    app.router.add_post('/tr', transmit)

    #app.on_startup.append(start_bk_task)


    server = yield from loop.create_server(app.make_handler(),'0.0.0.0',781)
    return server


if __name__ == '__main__':

    pool = ThreadPoolExecutor(4)
    #queue = asyncio.Queue()

    policy = asyncio.get_event_loop_policy()
    policy.set_event_loop(policy.new_event_loop())
    loop = asyncio.get_event_loop()

    task_wait_test = TaskWait(loop, pool, logging, q_priority_job)

    loop.run_in_executor(task_wait_test, start_bk_task)

    loop.add_signal_handler(signal.SIGTERM,handler_signal,loop)

    #loop.call_soon_threadsafe(print, 'test')

    #loop.run_in_executor(pool,)

    srv = loop.run_until_complete(init(loop))

    
    try:
        logging.info('{} SRV: {} '.format(datetime.now().strftime('%c'), srv.sockets[0].getsockname())) 
        loop.run_forever()
    
    except KeyboardInterrupt:
        logging.info('{} SRV: closing  {} '.format(datetime.now().strftime('%c'), srv.sockets[0].getsockname())) 
        #loop.close()  
        #pass
    finally:
        loop.run_until_complete(shutdown(srv, app, handler))
        loop.close()
