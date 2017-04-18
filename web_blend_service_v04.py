from aiohttp import web
import asyncio
import logging
import json

import aiohttp_session
from aiohttp_session import setup, get_session, session_middleware
#from aiohttp_session.cookie_storage import EncryptedCookieStorage

import multiprocessing as mp
from multiprocessing import Process, freeze_support
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

@asyncio.coroutine
def handle(request):
    #pass
    logging.info('in test module {0}'.format(request.json))
    if request.method == 'POST':
        print('post!!!!!')
    name = request.match_info.get('name', "Anonymous")
    text = "Hello, " + name
    return web.Response(text=text)


@asyncio.coroutine
def wshandler(request):
    if request.method == 'POST':
        print('post!!!!!')
    logging.info('in wshandler module {0}'.format(request.json))
    ws = web.WebSocketResponse()
    ws.prepare(request)
    for msg in ws:
        if msg.type == web.MsgType.text:
            ws.send_str("Hello, {}".format(msg.data))
        elif msg.type == web.MsgType.binary:
            ws.send_bytes(msg.data)
        elif msg.type == web.MsgType.close:
            break
    return ws


@asyncio.coroutine
def check_data(data):
    data['start_frame']=0
    data['end_frame']=50

    return data


@asyncio.coroutine
def transmit11(request):
    ts = []
    data = yield from request.text()
    req_json = json.loads(data)
    logging.info('Session method : {}, session type : {}, messages is : {} : {}'.format(request.method, request, request, req_json))
    if request.content_type == 'application/json':
        run_render_multi(req_json)
        logging.info('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!YIELD FROM REND_BLRND_MULTI RETURN MESSAGES : {}'.format(req_json['user']))

        return web.json_response(req_json)
    return web.Response(body=json.dumps({'ok': req_json}).encode('utf-8'),
        content_type='application/json')#(yield from request.text())


@asyncio.coroutine
def render_proc_cr(task):
    logging.info('!!!!!!!!!!!!!!!!!!!!! {} ************* **'.format(task))
    
    #task=task[0]
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

@asyncio.coroutine
def transmit(request):
    ts = []
    data = yield from request.text()

    req_json = json.loads(data)

    logging.info('Session method : {}, session type : {}, messages is : {} : {}'.format(request.method, request, request, req_json))

    if request.content_type == 'application/json':

        #data2 = yield from render_proc_cr(req_json)
        logging.info('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!YIELD FROM REND_BLRND_MULTI RETURN MESSAGES : {}'.format(req_json['user']))

        asyncio.ensure_future(render_proc_cr(req_json))
         
         
         
        
        return web.json_response(req_json)
    


    #return web.Response(body=json.dumps({'ok': req_json}).encode('utf-8'),
     #   content_type='application/json')#(yield from request.text())





def find_before(task):
    #name_file=''


    #for entry in os.scandir(os.path.join(BLEND_DIR, task['project_name']+str('/project/'))):

    #    if not entry.name.startswith('.') and entry.is_file():
    #        logging.info('!!!!!!!!!!!!!!find_before ENTRY NAME : {}'.format(entry))

    #        if entry.name == task['project_name'] +'.blend':
           #     print ('found file project  : {} '.format(entry.name))
                #bpy.path = os.path.join(BLEND_DIR, task['name'])
    #            logging.info('!!!!!!!!!!!find_before USER TASK : {}'.format(task['user']))
             #   print ('directory in : ',os.getcwd())
                # set directory where file place    
    #            os.chdir(os.path.abspath(os.path.join(BLEND_DIR, task['project_name']+str('/project/'))))

               # print ('directory in :2 : ',os.getcwd())    
    #task['project_name']

    logging.info('TEST' * 100)
    logging.info('file !!!!!!!!!!!!!!{}'.format(os.path.abspath(os.path.join(BLEND_DIR,task['project_name']))))


    bpy.ops.wm.open_mainfile(filepath=task['project_name'])

    name_file =task['user'] +'_'+ task['project_name'].split('/')[-1]
    #bpy.ops.wm.save_as_mainfile(filepath=name_file)

    
    #bpy.ops.wm.open_mainfile(filepath=name_file)


    #for entry1 in os.listdir(os.path.join(USERS_DIR, task['user'])):
    #    for entry2 in os.listdir(os.path.join(USERS_DIR, task['user'], entry1)):
    #        for x in bpy.data.scenes['Scene'].sequence_editor.sequences_all:
    #            if x.type == 'IMAGE':
    #                seq_elem = x.strip_elem_from_frame(0)
    #                #print(x.name.split('.')[0])
    #                if x.name.split('.')[0] in task['files_png']:
    #                    logging.info('!!##FILES PNG### {}  : ####find_before : {}'.format(task['files_png'], seq_elem))#

                       # print('test', task['files_png'][x.name.split('.')[0]])
                       # print('dfddfdf',os.path.join(USERS_DIR, task['user'], entry1))


    #                    seq_elem.filename = task['files_png'][x.name.split('.')[0]]

    #                       x.directory = os.path.join(USERS_DIR, task['user'], entry1)

    #bpy.ops.wm.save_as_mainfile(filepath=name_file)
    return os.path.abspath(os.path.join(BLEND_DIR,task['project_name']))



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
            
        #cur.execute('update users_rollers set is_ready=1, filename_video={} where id={}'.format(bpy.context.scene.render.filepath,h.split('/')[7]))
        os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
        #os.remove(os.path.abspath(bpy.data.filepath))
        #os.remove(os.path.abspath(bpy.data.filepath+'1'))
    except:
        pass

#def

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
     
        #cur.execute('update users_rollers set is_ready=1, filename_video={} where id={}'.format(bpy.context.scene.render.filepath,h.split('/')[7]))
        os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
        #os.remove(os.path.abspath(bpy.data.filepath))
        #os.remove(os.path.abspath(bpy.data.filepath+'1'))
    except:
        pass



#@asyncio.coroutine
def worker(q,data_for_render):
    
    task = data_for_render
    logging.info('{}: TASK:{} Q: {}'.format(datetime.now().strftime('%c'),task,type(q)))
    while True:
        try:
            
            q.get_nowait()
            logging.info('{} : WORKER: {} and file name {}'.format(datetime.now().strftime('%c'), task,task['project_name']))
            ## before render we create new file project of blender, and run render it self

            #bpy.ops.wm.open_mainfile(filepath=task['file_name'])
            o = find_before(task)
            bpy.context.scene.render.filepath ='{}.mp4'.format(str(task['result_dir'])+'/'+str('roller_video'))
            bpy.context.scene.render.engine = 'CYCLES'
            bpy.context.scene.cycles.device='CPU'
            bpy.context.scene.render.ffmpeg.format = 'MPEG4'
            bpy.context.scene.render.ffmpeg.video_bitrate=750
            bpy.context.scene.render.ffmpeg.audio_bitrate=124


           # with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
           #     try:
           #         db.execute('update users_rollers set is_render=1 where id=%s',(str(task['user_roller_id']),))#

                #except Exception as e:
                #    logging.info('Base err : {}'.format(e))
                #finally:
                #    db.close()
           
            #bpy.context.scene.frame_start = 100
            #bpy.context.scene.frame_end = 150

            
            #os.chown(bpy.context.scene.render.filepath, 500, 500)

            bpy.ops.render.render(animation=True,scene=bpy.context.scene.name)

            os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
            os.chmod(bpy.context.scene.render.filepath, 0o777)
            bpy.context.scene.frame_start = 100
            bpy.context.scene.frame_end = 101
            bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
            bpy.context.scene.render.filepath ='{}.jpg'.format(str(task['result_dir'])+'/'+str('roller_video'))
            bpy.ops.render.render(write_still=True)
            os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
            os.chmod(bpy.context.scene.render.filepath, 0o777)
            #logging.info(' ###########{} ###################: render  name {} '.format(g,task['project_name']))
           # yield from p
            #    logging.info(' {} ###################: render  name {} path {}: {}'.format(q.get(),task['project_name'],datetime.now().strftime('%c'),o))
            #    try:
            #        os.remove(o)
            #    except: pass

            q.task_done()
           ## logging.info('render file name {} complete at {}'.format(task['file_name'],datetime.now().strftime('%c')))
        except Empty: break
           # logging.info('in worker have exception: {} and file name {}'.format(task,task['file_name']))
           # logging.info('render file name {} complete at {}'.format(task['file_name'],datetime.now().strftime('%c')))
            #logging.info(' render  name {} complete at {}'.format(task['project_name'],datetime.now().strftime('%c')))
            
            

# --
def worker1( task):
     

    
    #task = data_for_render
    
    while True:
        try:
            
            
            #logging.info('{} : WORKER: {} and file name {}'.format(datetime.now().strftime('%c'), task,task['project_name']))
            ## before render we create new file project of blender, and run render it self

            #bpy.ops.wm.open_mainfile(filepath=task['file_name'])
            o = find_before(task)
            bpy.context.scene.render.filepath ='{}.mp4'.format(str(task['result_dir'])+'/'+str('roller_video'))
            bpy.context.scene.render.engine = 'CYCLES'
            bpy.context.scene.cycles.device='CPU'
            bpy.context.scene.render.ffmpeg.format = 'MPEG4'
            bpy.context.scene.render.ffmpeg.video_bitrate=750
            bpy.context.scene.render.ffmpeg.audio_bitrate=124


           # with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
           #     try:
           #         db.execute('update users_rollers set is_render=1 where id=%s',(str(task['user_roller_id']),))#

                #except Exception as e:
                #    logging.info('Base err : {}'.format(e))
                #finally:
                #    db.close()
           
            #bpy.context.scene.frame_start = 100
            #bpy.context.scene.frame_end = 150

            
            #os.chown(bpy.context.scene.render.filepath, 500, 500)

            bpy.ops.render.render(animation=True,scene=bpy.context.scene.name)

            os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
            os.chmod(bpy.context.scene.render.filepath, 0o777)
            bpy.context.scene.frame_start = 100
            bpy.context.scene.frame_end = 101
            bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
            bpy.context.scene.render.filepath ='{}.jpg'.format(str(task['result_dir'])+'/'+str('roller_video'))
            bpy.ops.render.render(write_still=True)
            os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
            os.chmod(bpy.context.scene.render.filepath, 0o777)
            #logging.info(' ###########{} ###################: render  name {} '.format(g,task['project_name']))
           # yield from p
            #    logging.info(' {} ###################: render  name {} path {}: {}'.format(q.get(),task['project_name'],datetime.now().strftime('%c'),o))
            #    try:
            #        os.remove(o)
            #    except: pass

             
           ## logging.info('render file name {} complete at {}'.format(task['file_name'],datetime.now().strftime('%c')))
        except Empty: break


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




### end server info 

#if __name__ == '__main__':
# log level debug
def init(loop):
    logging.basicConfig(level=logging.DEBUG)
    bpy.app.handlers.render_complete.append(render_complete)

   # fernet_key = fernet.Fernet.generate_key()
   # secret_key = base64.urlsafe_b64decode(fernet_key)
    
    app = web.Application()


    app.router.add_route('GET','/echo', wshandler)
    app.router.add_post('/tr', transmit)
   # app.router.add_route('POST','/tr', transmit)
    app.router.add_route('GET','/', handle)
    app.router.add_route('POST','/', handle)
    app.router.add_route('GET','/{name}', handle)
    #setup(app, EncryptedCookieStorage(secret_key))
    #setup(app, EncryptedCookieStorage(secret_key))
    
    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


    #loop = asyncio.get_event_loop()
    f = yield from loop.create_server(app.make_handler(),'0.0.0.0',781)
    return f


app = web.Application()
loop = asyncio.get_event_loop()
srv = loop.run_until_complete(init(loop))
#srv = loop.run_until_complete(f)
    
try: 
    loop.run_forever()
    print('serving on', srv.sockets[0].getsockname())
except KeyboardInterrupt:
    loop.close()  
    #pass