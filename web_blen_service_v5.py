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

LOG_FILENAME = '/var/tmp/render_blender_server.log'


#q_priority_job = asyncio.Queue(maxsize=12)
#queue.PriorityQueue(2)
# base connect
import MySQLdb as mysql
import random


# end import config settings

class TaskWait:
    #queue = asyncio.Queue(maxsize=12)


    def __init__(self,loop,*args,**kwargs):

        self._loop = loop
        self._waiter = asyncio.Event()
        self._flush_future = self._loop.create_task(self.flush_task())
        self._pool = pool
        self._log = logging.getLogger(self.__class__.__name__)
        #self._queue = 


    
    def worker(self, next_job):

        #next_job.data=dict(next_job.data)
        time_start = time.time()
        task = json.loads(next_job[0].data)
        #self._log.info('worker job !!!!!!!!!!!: {} '.format(task))


        #self._log.info('worker job : {} '.format(task['project_name']))
        bpy.ops.wm.open_mainfile(filepath=task['project_name'])
        context_frame_start = bpy.context.scene.frame_start
        context_frame_end = bpy.context.scene.frame_end
        self._log.info(':::Priview ::')
        self._log.info('{}:::Priview :: {} Picture ::{}, Full::{}'.format(
                                                            datetime.now().strftime('%c'), 
                                                            task['moview_priview'], 
                                                            task['moview_picture'],
                                                            task['moview_full']
                                                            ))




        if task['moview_priview']:
            try:
                #self._log.info('worker job !!!!!!!!!!!: {} '.format(task))
                with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
                    try:
                        self._log.info(':::Priview ::### before append to database %s' %(datetime.now().strftime('%c')))
                        db.execute('update users_rollers set is_render_demo=1,filename_video_demo=%s where id=%s',('video/roller_video_demo.mp4',str(task['user_roller_id'])))
                        db.execute('update users_rollers set is_render_demo=1,filename_screen_demo=%s where id=%s',('video/roller_video_demo.jpg',str(task['user_roller_id'])))
                        self._log.info(':::Priview ::### after  update to database %s' %(datetime.now().strftime('%c')))
                    except Exception as e:
                        logging.info('Base err : {}'.format(e))
                    finally:
                        self._log.info(':::Priview ::### finally phase update to database %s' %(datetime.now().strftime('%c')))
                        db.close()
                

                bpy.context.scene.frame_start = 0
                bpy.context.scene.frame_end = 25

                bpy.context.scene.render.filepath ='{}.mp4'.format(str(task['result_dir'])+'/'+str('roller_video_demo'))

                bpy.context.scene.render.engine = 'CYCLES'
                bpy.context.scene.cycles.device='CPU'
                bpy.context.scene.render.ffmpeg.format = 'MPEG4'
                bpy.context.scene.render.ffmpeg.video_bitrate=750
                bpy.context.scene.render.ffmpeg.audio_bitrate=124
            
                bpy.ops.render.render(animation=True,scene=bpy.context.scene.name)
                os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
                os.chmod(bpy.context.scene.render.filepath, 0o777)
         
        
                bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
            
                bpy.context.scene.render.filepath ='{}.jpg'.format(str(task['result_dir'])+'/'+str('roller_video_demo'))
                bpy.ops.render.render(write_still=True)
                os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
                os.chmod(bpy.context.scene.render.filepath, 0o777)


                with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
                    try:
                        self._log.info(':::Priview ::### before append to database %s' %(datetime.now().strftime('%c')))
                        db.execute('update users_rollers set is_ready_demo=1,filename_video_demo=%s where id=%s',('video/roller_video_demo.mp4',str(task['user_roller_id'])))
                        db.execute('update users_rollers set is_ready_demo=1,filename_screen_demo=%s where id=%s',('video/roller_video_demo.jpg',str(task['user_roller_id'])))
                        self._log.info(':::Priview ::### after  update to database %s' %(datetime.now().strftime('%c')))

                    except Exception as e:
                        logging.info('Base err : {}'.format(e))
                    finally:
                        self._log.info(':::Priview ::### finally phase update to database %s' %(datetime.now().strftime('%c')))
                        db.close()

        

            except Empty: pass
        if task['moview_picture']:
            try:
                with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
                    try:
                        db.execute('update users_rollers set is_ready=1,filename_screen=%s where id=%s',('video/roller_video_demo.jpg',str(task['user_roller_id'])))
                    except Exception as e:
                        logging.info('Base err : {}'.format(e))
                    finally:
                        db.close()
       
                bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
            
                bpy.context.scene.render.filepath ='{}.jpg'.format(str(task['result_dir'])+'/'+str('roller_video'))
                bpy.ops.render.render(write_still=True)
                os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
                os.chmod(bpy.context.scene.render.filepath, 0o777)


        

            except Empty: pass

        if task['moview_full']:
            try:
                self._log.info('worker job !!!!!!!!!!!: {} '.format(task))
                with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
                    try:
                        db.execute('update users_rollers set is_render=1,filename_video=%s where id=%s',('video/roller_video_demo.mp4',str(task['user_roller_id'])))
                        db.execute('update users_rollers set is_render=1,filename_screen=%s where id=%s',('video/roller_video_demo.jpg',str(task['user_roller_id'])))
                    except Exception as e:
                        logging.info('Base err : {}'.format(e))
                    finally:
                        db.close()


                bpy.ops.wm.open_mainfile(filepath=task['project_name'])
                bpy.context.scene.frame_start = 0
                bpy.context.scene.frame_end = 50

                

                bpy.context.scene.render.filepath ='{}.mp4'.format(str(task['result_dir'])+'/'+str('roller_video'))

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
                with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
                    try:
                        db.execute('update users_rollers set is_render=1,filename_video=%s where id=%s',('video/roller_video_demo.mp4',str(task['user_roller_id'])))
                        db.execute('update users_rollers set is_render=1,filename_screen=%s where id=%s',('video/roller_video_demo.jpg',str(task['user_roller_id'])))
                    except Exception as e:
                        logging.info('Base err : {}'.format(e))
                    finally:
                        db.close()



        

            except Empty: pass




        #finally:
           # self._loop.stop()



        end_time = time.time() - time_start
        self._log.info('worker job : {} TIME {}'.format(' s',end_time))
        return {'ok':end_time }



    @asyncio.coroutine
    def run_jobs(self):
        lt = []        


       # self._log.info('{} '.format(datetime.now().strftime('%c')))
        while not self._queue.empty():

            self._log.info('Current queue : {} {}'.format('NOT EMPTY',self._queue))

            data = self._queue.get()
            

            lt.append(data)
            self._log.info('Current queue : {} DATA {} ##### '.format('$$##$@  ',lt))
            self._queue.task_done()
            


        self._log.info('{} Count in queue {} status queu ::{}'.format(datetime.now().strftime('%c'), lt, self._queue.empty()))
        procs = (Thread(target=self.worker, args=(lt,)) for _ in lt)

        for proc in procs:
            proc.setDaemon(True)
            proc.start()
            self._log.info('{} Proc started {} status queu ::{}'.format(datetime.now().strftime('%c'), proc, self._queue.empty()))
        
        for proc in procs:
            proc.join()
            self._log.info('{} Proc join {} status queu ::{}'.format(datetime.now().strftime('%c'), proc, self._queue.empty()))

            
        
        #return 'Status run_job'

    
    

    @asyncio.coroutine
    def flush_task(self):

    
        while True:
            try:
                self._log.info('#With out JOBS run')
    
               #self._log.info('#With out JOBS run #{} ## QUEUE empty {} ## jobs type'.format(datetime.now().strftime('%c'), queue.empty()))

               # task = self._loop.create_task(self.run_jobs())

                self._log.info('{} , loop time : {},   loop is working : {}'.format(datetime.now().strftime('%c'), self._loop.time(), self._loop.is_running()))

                data = yield from asyncio.wait_for(self._waiter.wait(),timeout=2.0, loop=self._loop)
         
            except asyncio.TimeoutError:

                self._log.info('{} EXCEPTION {} '.format(datetime.now().strftime('%c'),str(e)))
                


            #self._waiter.clear()

    def force_flush():
        self._waiter.set()

    #def _set_in_q(self, job):
     #   self._log.info('::: _set_in QUEUE ::')
     #   TaskWait.queue.put(job)






    def submit(self,callbacks,*args, **kwargs):
        
    
        future = self._pool.submit(self.flush_task)
    
        return future


@functools.total_ordering
class Job:

    def __init__(self, priority, description, data):

        self.priority = priority
        self.description = description
        self.data = data
        logging.info('**{}### priority : {}###############'.format(datetime.now().strftime('%c'), self.priority ))
       # TaskWait.queue._set_in_q(self.data)

        #print('New job:', description)

        return 
    #def __call__(self):   https://pythoness.pp.ua/catalog/article/veb-pauk-s-ispolzovaniem-aiohttp-i-asyncio/
      #  TaskWait._queue.put(self.data)

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








def calback_from_pool(t):
    
    #item = yield from queue_taska.get()
    logging.info('**** {} Current process name: :  '.format('__name__, mp.current_process().name , q_priority_job'))
    #mp.current_process().name

    

@asyncio.coroutine
def transmit(request):
    
    data = yield from request.text()
    

    #mp.freeze_support()

   # pool = mp.Pool(1)

    req_json = json.loads(data)
    logging.info('{} :::'.format(datetime.now().strftime('%c')))
    

    if request.content_type == 'application/json':

     #   joq_data = Job(3, request.method, data)

       # q_priority_job.put(joq_data)

       # logging.info('Job description {} ########## {} ##########'.format('sdcsdcsdcsdvwrby3y3yb5y45by45b',q_priority_job.__dict__))
        #logging.info('user :{} ::'.format(req_json['user']))
        #l = [joq_data]
        #logging.info('!!!!!$#$####!!!!!!!! {} ******d ***********'.format(l))
        
        #return web.Response(body=json.dumps({'ok': req_json}).encode('utf-8'), content_type='application/json')
        return web.json_response({'ok':'ok_transmit'}) 
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
    #logging.info('32312312312312312313')
   # logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
 
    #bpy.app.handlers.render_complete.append(render_complete)
     
    app = web.Application()
    #app = web.Application(loop=loop)
   
    app.router.add_post('/tr', transmit)

    logging.info('32312312312312312313')

    #app.on_startup.append(start_bk_task)


    server = yield from loop.create_server(app.make_handler(),'0.0.0.0',7811)
    return server


if __name__ == '__main__':

   # global q_priority_job
   # hdlr = logging.FileHandler('/var/tmp/render_blender_server.log')
   # logging.addHandler(hdlr)

    
    pool = ThreadPoolExecutor(4)
    print('+++++1')
    
    policy = asyncio.get_event_loop_policy()
    print('+++++2')
    policy.set_event_loop(policy.new_event_loop())
    print('+++++3')
    
    loop = asyncio.get_event_loop()
    print('+++++4')
    loop.set_debug(True)
    print('+++++5')
    logging.info('32312312312312312313')
    print('+++++6')

    srv = loop.run_until_complete(init(loop))
    print('+++++7')

    #queue = asyncio.Queue(maxsize=12, loop=loop)

    
   # logging.info('{} SRV: Status taskWait  ####'.format(datetime.now().strftime('%c')))

        #task_wait_test = TaskWait(loop, pool, logging)
       # loop.set_debug(True)
        #loop.run_in_executor(task_wait_test, start_bk_task)
       # logging.info('{} SRV: {} Status taskWait {} ####'.format(datetime.now().strftime('%c'),task_wait_test)) 


   # logging.info('{} SRV:  Exception   ####'.format(datetime.now().strftime('%c'))) 


    #loop.add_signal_handler(signal.SIGTERM, handler_signal, loop)

    #loop.call_soon_threadsafe(print, 'test')

    #loop.run_in_executor(pool,)

  #  srv = loop.run_until_complete(init(loop))

    
    try:
        logging.info('{} SRV: {} '.format(datetime.now().strftime('%c'),'srv.sockets[0].getsockname()')) 
        loop.run_forever()
    
    except KeyboardInterrupt:
      #  logging.info('{} SRV: closing  {} '.format(datetime.now().strftime('%c'), srv.sockets[0].getsockname())) 
      #  asyncio.gather(*asyncio.Task.all_tasks()).cancel()
      #  loop.stop()
        loop.close()  
        #pass
    #finally:
    #    loop.run_until_complete(shutdown(srv, app, handler))
    #    loop.close()
