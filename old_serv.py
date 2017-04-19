from aiohttp import web
import asyncio
import logging
import json

import aiohttp_session
from aiohttp_session import setup, get_session, session_middleware
#from aiohttp_session.cookie_storage import EncryptedCookieStorage

import multiprocessing as mp

from multiprocessing import Process, freeze_support
from threading import Thread

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


# end import config settings






 
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



@asyncio.coroutine
def transmit(request):
    
    data = yield from request.text()
    

    mp.freeze_support()
    pool = mp.Pool()

    



    req_json = json.loads(data)

    logging.info('Session method : {}, session type : {}, messages is : {} : {}'.format(request.method, request, request, req_json))

    if request.content_type == 'application/json':
        
        logging.info('user :{} ::'.format(req_json['user']))



        l = [req_json]
        res = pool.apply_async(render_proc_cr,  l)

     #   logging.info('!!!!!$#$####!!!!!!!! {} ******d ***********'.format(pool))

        pool.close()
        #pool.join()


       # task = loop.create_task(render_proc_cr(req_json))
       # ret_value = yield from task

       # asyncio.ensure_future(render_proc_cr(req_json))
        
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
    logging.info('################{} ################{}#########{}####'.format(scene,bpy.data.filepath,bpy.context.scene.render.filepath))
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




@asyncio.coroutine
def run_render_multi1(data_for_render):
    
    tasks = []
    g=[]
    server_info()
   
    if data_for_render['message'] =='We did it!':
        
        logging.info('render file name {} start at {} CPU count {}'.format(data_for_render['project_name'],datetime.now().strftime('%c'),os.cpu_count()))
        freeze_support()
        num_procs = os.cpu_count();
        q =  mp.JoinableQueue()
        
        
        tasks.append(data_for_render)
        logging.info('!!!!!len of TASK {}:  QUE SIZE {}'.format(len(tasks), q.qsize()))
        for task in tasks:
                q.put(task)
                
                    #logging.info('task name {} and file name {}'.format(task,x['file_name']))

        procs = (mp.Process(target=worker, args=(q,task,)) for _ in range(5))
        logging.info('!!!!!!!!!!!!!!!!!!!!! {} ******************'.format(procs))
        #procs[0].start()
        #time.sleep(1)
        for p in procs:
            p.daemon = True
            #g.append(1)
            logging.info('!!!!!!!!!!!!!!!!!!!!! {} ******************'.format(p))
            p.start()
           #p.join()
           
           
        
           # p.join()
        for p in procs: 
            p.join()
           # time.sleep(1)
            if not p.is_alive():
                logging.info('!!!!!!!!!!!!!!!!!!!!! {} ******************'.format(p))
                sys.stdout.flush()
           
        

    return data_for_render

### end render module 
import sys

def render_proc( task):
    #q.put(task)
    task=task[0]
    try:
        o = find_before(task)
        bpy.context.scene.frame_start = 0
        bpy.context.scene.frame_end = 10
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
        s = sys.stdout

        f = open('/dev/null', 'w')
        sys.stdout = f

        bpy.ops.render.render(write_still=True)

        os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
        os.chmod(bpy.context.scene.render.filepath, 0o777)

        sys.stdout = s

    except Empty: pass

import random

def render_proc_crd(task):
    logging.info('!!!!!!!!!!!!!!!!!!!!! {} ************* **'.format(task))
    
    task=task[0]
    try:
        o = find_before(task)
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
        s = sys.stdout

        f = open('/dev/null', 'w')
        sys.stdout = f

        bpy.ops.render.render(write_still=True)

        os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
        os.chmod(bpy.context.scene.render.filepath, 0o777)

        sys.stdout = s

    except Empty: pass




import random, sys

def test_wrk(q,t):
    q.put(t)
    r = random.randint(1,250)
     
    logging.info('!!!!!!!!!!!!!!!!!!!!! {} *************** {}***'.format(t['message'], r))

     
     



@asyncio.coroutine
def run_render_multi55(data_for_render):
    proc = []
    ctx = mp.get_context()

    q = ctx.Queue()
    
    num_procs = 4

    for x in range(num_procs):

        proc.append(mp.Process(target=render_proc_cr, args=(q,data_for_render)))
        logging.info('!!!!!!!!!!!!!!!!!!!!! {} *************** {} **'.format(data_for_render,q))
    
    
    for p in range(num_procs):
        proc[p].start()
        #print(q.get(),proc[p])
    
    
    for p in range(num_procs):
        proc[p].join()
       # print('+',proc[p].is_alive())

@asyncio.coroutine
def run_render_multi1(data_for_render):
    proc = []
    ctx = mp.get_context()

    q = ctx.Queue()
    
    num_procs = 4

     
    logging.info('!!!!!!!!!!!!!!!!!!!!! {} *************** {} **'.format(data_for_render,q))
    
    proc = mp.Process(target=render_proc_cr, args=([[data_for_render]]))
    proc.start()
    proc.join() 



    

@asyncio.coroutine
def run_render_multig(data_for_render):
    mp.freeze_support()
    pool = mp.Pool()

    l = [[data_for_render]]
    res = pool.map(render_proc_cr,  l)

    logging.info('!!!!!$#$####!!!!!!!! {} ******d ***********'.format(pool))

    pool.close()
    pool.join()
 
    return data_for_render

### server info
import sys, os, platform, shutil


def server_info():
    sysname, nodename, release, version, machine, processor = platform.uname()
    logging.info('{} SRV: {} '.format(datetime.now().strftime('%c'), nodename))



 
def init(loop):
    logging.basicConfig(level=logging.DEBUG)
    bpy.app.handlers.render_complete.append(render_complete)

     
    app = web.Application()


    
    app.router.add_post('/tr', transmit)
  
 
    _ = yield from loop.create_server(app.make_handler(),'0.0.0.0',781)
    return _

 
loop = asyncio.get_event_loop()
srv = loop.run_until_complete(init(loop))

    
try: 
    loop.run_forever()
    print('serving on', srv.sockets[0].getsockname())
except KeyboardInterrupt:
    loop.close()  
    #pass