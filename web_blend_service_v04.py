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

from multiprocessing import Process, freeze_support
from threading import Thread
from queue import Queue
import signal

from queue import Empty
import bpy
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
        self._queue = queue

    @asyncio.coroutine
    def flush_task(self):

        #start_flush = datetime.now().strftime('%c')
        
#        logging.info('Time Flush_TASK {}'.format(datetime.now().strftime('%c')))

        while True:
            try:
                
                self._log.info('{} Current process name: {} '.format(self._queue,mp.current_process().name))
                data = yield from asyncio.wait_for(self._waiter.wait(),timeout=2.0, loop=self._loop)
         
            except asyncio.TimeoutError:
                pass

            self._waiter.clear()

    def force_flush():
        #self._log.info('{} Force flush Current process name: {}  '.format(__name__, mp.current_process().name))
        self._waiter.set()


    def submit(self,callbacks,*args, **kwargs):
        print(args,kwargs)

        return self._pool.submit(self.flush_task)
        




 
def render_proc_cr(task):

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

queue_taska = Queue(3)


def calback_from_pool(t):

   # item = queue_taska.get()

    logging.info('{} Current process name: {}, task {}:  '.format(__name__, mp.current_process().name ,queue_taska))
    #mp.current_process().name

    

@asyncio.coroutine
def transmit(request):
    
    data = yield from request.text()
    

    mp.freeze_support()
    pool = mp.Pool(4)
    req_json = json.loads(data)

    logging.info('{} ::: {}'.format(datetime.now().strftime('%c'),pool ))

    #logging.info('Session method : {}, session type : {}, messages is : {} : {}'.format(request.method, request, request, req_json))

    if request.content_type == 'application/json':
        
        #logging.info('user :{} ::'.format(req_json['user']))
        l = [req_json]
        res = pool.apply_async(render_proc_cr,  l, callback=calback_from_pool)
        #queue_taska.put(res)
        #logging.info('!!!!!$#$####!!!!!!!! {} ******d ***********'.format(res))

        #pool.close()
        #pool.join()


        
        return web.json_response(req_json)

 

 
from bpy.app.handlers import persistent
##
@persistent
def render_complete(scene):
    logging.info('################{} ################{}#########{}####'.format(scene,bpy.data.filepath,bpy.context.scene.render.filepath))
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
    logging.info('################{}################{}#########{}####'.format(scene,bpy.data.filepath,bpy.context.scene.render.filepath))
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
def test_calback(item,handler):

    test_calback_task = queue.put(item)

    def middle_handler(request):
        hand_data = yield from handler(request)

        logging.info(':::{}::::::Time now: {} handler status {}'.format(item, datetime.now().strftime('%c'), str(hand_data)))
        logging.info('::In Task:{}::::::Tim'.format(test_calback_task))
     
        return web.json_response({'ok':str(hand_data)})

    return middle_handler


    return 

def handler_signal(loop):
    loop.remove_signal_handler(signal.SIGTERM)
    loop.stop()


 
def start_bk_task(app):
    try:
        logging.info('*'*180)
    except Exception as e:
        logging.info('{}'.format(e))
        

 
def init(loop):
    logging.basicConfig(level=logging.DEBUG)
 
    bpy.app.handlers.render_complete.append(render_complete)
     
    app = web.Application(loop=loop, middlewares=[test_calback])
    app.router.add_post('/tr', transmit)

    #app.on_startup.append(start_bk_task)


    server = yield from loop.create_server(app.make_handler(),'0.0.0.0',781)
    return server


pool = ThreadPoolExecutor(max_workers=mp.cpu_count())


queue = asyncio.Queue()
policy = asyncio.get_event_loop_policy()
policy.set_event_loop(policy.new_event_loop())
loop = asyncio.get_event_loop()

task_wait_test = TaskWait(loop, pool, logging,queue)

loop.run_in_executor(task_wait_test,start_bk_task)

loop.add_signal_handler(signal.SIGTERM,handler_signal,loop)

#loop.call_soon_threadsafe(print, 'test')

#loop.run_in_executor(pool,)

srv = loop.run_until_complete(init(loop))

    
try:
    logging.info('{} SRV: {} '.format(datetime.now().strftime('%c'), srv.sockets[0].getsockname())) 
    loop.run_forever()
    
except KeyboardInterrupt:
    loop.close()  
    #pass
