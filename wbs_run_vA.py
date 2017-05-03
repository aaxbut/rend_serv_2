# -*- coding: utf-8 -*-
import asyncio
from datetime import datetime
import logging 
import json
from aiohttp import web

import itertools
import multiprocessing as mp

from queue import Empty
from collections import deque

import subprocess as sp


import bpy
import os
import time

import MySQLdb as mysql


# import config settings
import configparser


conf = configparser.RawConfigParser()

conf.read('wb1.conf')
FFMPEG_BIN = 'ffmpeg'


BLEND_DIR = conf.get('bl_path','BLEND_DIR')
USERS_DIR = conf.get('bl_path','USERS_DIR')
dbconnectionhost = conf.get('base','dbconnectionhost')
dbname = conf.get('base','dbname')
dbusername = conf.get('base','dbusername')
dbpassword = conf.get('base','dbpassword')
u_ugid = conf.get('usr_permission','uid')
u_gguid = conf.get('usr_permission','gid')

MAX_SIZE_QUEUE = 6
LOG_FILENAME = '/var/tmp/render_blender_server_test.log'
# base connect




class DecoWithArgs(object):

    
    def __init__(self,args1,args2,args3):
        
        print('inside class __init__')
        #additional agrs in decorator
        self.args1 = str(args1)
        self.args2 = str(args2)
        self.args3 = str(args3)
        #self.q_list = []

       # self.q_list.append([x for x in itertools.chain(self.args1,self.args2,self.args3)])

        #self.q_dict = {}
    

    def __call__(self, func):
        print('inside __call__  %s' % str(func))

        def warap(*args, **kwagrs):
            #args.append(12)
            #print(args, kwagrs)
            #print(func.__name__)
            ##  change args, kwargs on function   
            
           # self.q_dict.update(kwagrs)
           # self.q_list.append(args)

            func1 = yield from func(*args)

            print(func1.text)

            # run task update in list,  

            t1 = loop.create_task(corobas_barabas('23',func1.text))

            return func1
        return warap


class DecoWithArgsMysqlUpd(object):

    
    def __init__(self,args1,args2,args3):
        
       #$ print('inside class __init__')
        #additional agrs in decorator
        self.args1 = str(args1)
        self.args2 = str(args2)
        self.args3 = str(args3)
        #self.q_list = []

       # self.q_list.append([x for x in itertools.chain(self.args1,self.args2,self.args3)])

        #self.q_dict = {}
    

    def __call__(self, func):
       # print('inside __call__  %s' % str(func))

        def warap(*args):

            # args[0]['render_type']
            # args[0]['user_roller_id']
            render_type = args[0]['render_type']
            user_roller_id = args[0]['user_roller_id']
            if render_type == 1:
                with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
                    try:
                        db.execute('update users_rollers set is_render=1,filename_video=%s where id=%s',('video/roller_video.mp4',user_roller_id))
                        db.execute('update users_rollers set is_render=1,filename_screen=%s where id=%s',('video/roller_video.jpg',user_roller_id))
                    except Exception as e:
                        logging.info('Base err : {}'.format(e))
                    finally:
                        db.close()

            if render_type == 4:
                with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
                    try:
                        db.execute('update users_rollers set is_render=1,filename_video_demo=%s where id=%s',('video/roller_video_demo.mp4',user_roller_id))
                        db.execute('update users_rollers set is_render=1,filename_screen_demo=%s where id=%s',('video/roller_video_demo.jpg',user_roller_id))
                    except Exception as e:
                        logging.info('Base err : {}'.format(e))
                    finally:
                        db.close()

            if render_type == 2:
                with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
                    try:
                        db.execute('update users_rollers set is_render=1,filename_screen=%s where id=%s',('video/roller_video.jpg',user_roller_id))
                    except Exception as e:
                        logging.info('Base err : {}'.format(e))
                    finally:
                        db.close()


            logging.info('WRAP KWARGS BEFORE: {}'.format(args[0]['render_type']))


            #args.append(12)
            #print(args, kwagrs)
            #print(func.__name__)
            ##  change args, kwargs on function   
            
           # self.q_dict.update(kwagrs)
           # self.q_list.append(args)

            func1 = func(*args)

            if render_type == 1:
                with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
                    try:
                        db.execute('update users_rollers set is_ready=1,filename_video=%s where id=%s',('video/roller_video.mp4',user_roller_id))
                        db.execute('update users_rollers set is_ready=1,filename_screen=%s where id=%s',('video/roller_video.jpg',user_roller_id))
                    except Exception as e:
                        logging.info('Base err : {}'.format(e))
                    finally:
                        db.close()

            if render_type == 4:
                with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
                    try:
                        db.execute('update users_rollers set is_ready_demo=1,filename_video_demo=%s where id=%s',('video/roller_video_demo.mp4',user_roller_id))
                        db.execute('update users_rollers set is_ready_demo=1,filename_screen_demo=%s where id=%s',('video/roller_video_demo.jpg',user_roller_id))
                    except Exception as e:
                        logging.info('Base err : {}'.format(e))
                    finally:
                        db.close()

            if render_type == 2:
                with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
                    try:
                        db.execute('update users_rollers set is_ready=1,filename_screen=%s where id=%s',('video/roller_video.jpg',user_roller_id))
                    except Exception as e:
                        logging.info('Base err : {}'.format(e))
                    finally:
                        db.close()            
            logging.info('WRAP KWARGS BEFORE: {}'.format(args[0]['render_type']))

            

            # run task update in list,  

            #t1 = loop.create_task(corobas_barabas('23',func1.text))

            return func1
        return warap



def timit(func):
    def wrapper(*args,**kwargs):
 #       d = {}
  #      d.update(kwargs)
        start_time = time.time()

        logging.info('before run')

        func(*args,**kwargs)
        end_time = time.time()-start_time
        #logging.info('after ARGS KWARGS {}      TIME:'.format(args))
        logging.info('after run {}      TIME:{}'.format(func.__name__, end_time))
    return wrapper

def timit2(func):
    def wrapper(*args,**kwargs):
 #       d = {}
  #      d.update(kwargs)
        start_time = time.time()

        logging.info('before run')

        func(*args)
        end_time = time.time()-start_time
        logging.info('after ARGS KWARGS {}      TIME:'.format(args))
        logging.info('after run {}      TIME:{}'.format(func.__name__, end_time))
    return wrapper
    



#@DecoWithArgsMysqlUpd('aaxbut','@','gmail.com')
#@asyncio.coroutine
#@timit
def rend_priview(*args, **kwargs):


    priview_frames_start, priview_frames_end = args[0]
    task = kwargs

    logging.info('REND_PRIVIEW def {} ::: {} '.format(priview_frames_start,priview_frames_end))
    logging.info('REND_PRIVIEW def {} :: '.format(kwargs))



    try:
        bpy.context.scene.frame_start = priview_frames_start
        bpy.context.scene.frame_end = priview_frames_end


            #bpy.context.scene.render.threads_mode ='FIXED'
            #bpy.context.scene.render.threads = 4


        bpy.context.scene.render.filepath ='{}_{}.mp4'.format(str(task['result_dir'])+'/'+str('roller_video_demo'),str(priview_frames_start))

        bpy.context.scene.render.engine = 'CYCLES'
        #bpy.context.scene.render.engine = 'BLENDER_RENDER'
            

        bpy.context.scene.cycles.device='CPU'
        bpy.context.scene.render.ffmpeg.format = 'MPEG4'
        bpy.context.scene.render.ffmpeg.codec = 'MPEG4'

          #  bpy.context.scene.render.ffmpeg.format = 'MPEG4'

        bpy.context.scene.render.ffmpeg.video_bitrate=6000
        bpy.context.scene.render.ffmpeg.maxrate = 9000
           # bpy.context.scene.render.ffmpeg.buffersize = 2000
        bpy.context.scene.render.ffmpeg.packetsize = 4096
        bpy.context.scene.render.resolution_percentage = 60
            #bpy.context.scene.render.ffmpeg.use_autosplit=True
            #bpy.context.scene.render.ffmpeg.gopsize=100


        bpy.context.scene.render.ffmpeg.audio_bitrate=384
            
           # s= base_rec_rend4(task)

        bpy.ops.render.render(animation=True,scene=bpy.context.scene.name)
        os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
        os.chmod(bpy.context.scene.render.filepath, 0o777)
    except Exception as e:
        logging.info('REND_PRIVIEW def {} :::  '.format(str(e)))
    return '1'
            
#@timit       
def rend_task(task):


    try:
        bpy.ops.wm.open_mainfile(filepath=task['project_name'])
        context_frame_start = bpy.context.scene.frame_start
        context_frame_end = bpy.context.scene.frame_end
        
    except Exception as e:

        logging.info('{} Render TASK{} ########## {} ##########'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'), str(e),'KY KY KY BACKGROUND '))
        pass


    if task['render_type'] is 2: # render picture 

        start_time=time.time()

        logging.info('{} Render TASK{} render picture  {} ##########'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'), '','KY KY KY BACKGROUND '))

        #base_rec_rend4
        try:
#s= base_rec_rend2(task)

            bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
            bpy.context.scene.render.filepath ='{}.jpg'.format(str(task['result_dir'])+'/'+str('roller_video'))
            bpy.ops.render.render(write_still=True)
            os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
            os.chmod(bpy.context.scene.render.filepath, 0o777)

     #       s= base_rec_rend_of2(task)
            
            end_time = time.time() - start_time
            logging.info('{} TASK description {} moview_picture  {} ### TIME :{}'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'),task['result_dir'],'',end_time))

        except Empty: pass

        
    if task['render_type'] is 4: #render priview 



        try:
            start_time=time.time()
            logging.info('{} Render TASK{} ##   priview   ### {} ####'.format(datetime.now().strftime('%D:: %H:%M:%S'), task['render_type'],' '))

 
            bpy.context.scene.frame_start = 0
            bpy.context.scene.frame_end = 500

            #bpy.context.scene.render.threads_mode ='FIXED'
            #bpy.context.scene.render.threads = 4


            bpy.context.scene.render.filepath ='{}.mp4'.format(str(task['result_dir'])+'/'+str('roller_video_demo'))

            bpy.context.scene.render.engine = 'CYCLES'
            #bpy.context.scene.render.engine = 'BLENDER_RENDER'
            

            bpy.context.scene.cycles.device='CPU'
        #    bpy.context.scene.render.ffmpeg.format = 'XVID'
            bpy.context.scene.render.ffmpeg.format = 'MPEG4'
            bpy.context.scene.render.ffmpeg.codec = 'MPEG4'

            

            bpy.context.scene.render.ffmpeg.video_bitrate=6000
            bpy.context.scene.render.ffmpeg.maxrate = 9000
           # bpy.context.scene.render.ffmpeg.buffersize = 2000
            bpy.context.scene.render.ffmpeg.packetsize = 4096
            #bpy.context.scene.render.resolution_percentage = 35
            #bpy.context.scene.render.ffmpeg.use_autosplit=True
            #bpy.context.scene.render.ffmpeg.gopsize=100


            bpy.context.scene.render.ffmpeg.audio_bitrate=384
            
           # s= base_rec_rend4(task)

            bpy.ops.render.render(animation=True,scene=bpy.context.scene.name)
            os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
            os.chmod(bpy.context.scene.render.filepath, 0o777)
            
            bpy.context.scene.frame_start = 100
            bpy.context.scene.frame_end = 101
        
            bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
            
            bpy.context.scene.render.filepath ='{}.jpg'.format(str(task['result_dir'])+'/'+str('roller_video_demo'))
            bpy.ops.render.render(write_still=True)
            os.chown(bpy.context.scene.render.filepath, int(u_ugid), int(u_gguid))
            os.chmod(bpy.context.scene.render.filepath, 0o777)


           # s= base_rec_rend_of4(task)
            
            end_time = time.time() - start_time
            

            logging.info('{} TASK description {} render priview   {} ### TIME :{}'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'),task['result_dir'],'',end_time))

        except Exception as e:
#            logging.info('{} Render TASK  moview_priview{} ########## {} ##########'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'), str(e),'KY KY KY BACKGROUND '))
            pass
    if task['render_type'] is 41:
        start_time=time.time()
     #render priview 

    
        try:
            start_time=time.time()
            logging.info('{} Render TASK{} ##   priview   ### {} ####'.format(datetime.now().strftime('%D:: %H:%M:%S'), task['render_type'],' '))

            #l_1 = [[0,100],[101,200],[201,300],[301,400],[401,500]]
            #l_1 = [[0,250],[251,500]]
            l_1 =[[0,10],[11,20]]
            logging.info('{} TASK des'.format(l_1))
            #p = mp.Process(target=rend_priview, args=(l_1[0],), kwargs=task)
            procs = [mp.Process(target=rend_priview, args=(x,), kwargs=task) for x in l_1]

           # p.start()
           # p.join()
            

            #procs = mp.Process(target=rend_priview, args=(lt[0],)).start()


            for p in procs:

                p.daemon = True
        #        logging.info('!!!!!!!!!!!!!!!!!!!!! {} ******************'.format(p))
                p.start()
        #        p.join()

           # end_time = time.time() - start_time
           # logging.info('DSFSDFSDFSDFSDFSDFSDFSDF'.format(end_time))

            for p in procs:
            #    logging.info('!!!!!!!!!!!!!!!!!!!!! {} ******41 ************'.format(p))
                p.join()
             #   logging.info('!!!!!!!!!!!!!!!!!!!!! {} ******41 ************'.format(p))

 

        except Exception as e:
       
            logging.info('{} Render TASK  moview_priview{} ########## {} ##########'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'), str(e),start_time))


       # finally:
        file_txt_for_concat =str(task['result_dir'])+'/'+'tst.txt' 
        with open(file_txt_for_concat,'w') as f:
            for x in l_1:

                f.write('file roller_video_demo_{}.mp4\n'.format(x[0]))

        out_file = str(task['result_dir'])+'/'+'roller_video_demo.mp4'


        command = [FFMPEG_BIN,
                    '-y',
    
                    '-f','concat',
                    '-safe',str(0),
                    '-i', file_txt_for_concat,
                    '-c','copy',
                    out_file,
                    ]

        
        pipe = sp.Popen(command)
        os.chown(out_file, int(u_ugid), int(u_gguid))
        os.chmod(out_file, 0o777)
        end_time = time.time() - start_time
        
        #logging.info('22222222222222222 {}'.format(end_time))




        logging.info('{} TASK description  @@! {} render priview {} ### TIME :{}'.format(datetime.now().strftime('%D:: %H:%M:%S'),task['result_dir'],'',end_time))


    if task['render_type'] is 1: #render full video
        try:
            logging.info('{} Render TASK{} ##   moview_full   ### {} ####'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'), task['render_type'],' '))



            start_time=time.time()
            bpy.ops.wm.open_mainfile(filepath=task['project_name'])
             

            bpy.context.scene.render.filepath ='{}.mp4'.format(str(task['result_dir'])+'/'+str('roller_video'))

            bpy.context.scene.render.engine = 'CYCLES'
            bpy.context.scene.cycles.device='CPU'
            bpy.context.scene.render.ffmpeg.format = 'MPEG4'
            bpy.context.scene.render.ffmpeg.video_bitrate=750
            bpy.context.scene.render.ffmpeg.audio_bitrate=124

            #res = base_rec_rend(task)
          #  logging.info('{} TASK description {} ########## {} ### TIME :{}'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'),task['result_dir'],'res ',end_time))
            #db =  mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname)
            
           # db.execute('update users_rollers set is_render=1,filename_video=%s where id=%s',('video/roller_video.mp4',str(task['user_roller_id'])))
           # db.execute('update users_rollers set is_render=1,filename_screen=%s where id=%s',('video/roller_video.jpg',str(task['user_roller_id'])))



            
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
            end_time = time.time() - start_time

            
#            db.execute('update users_rollers set is_ready=1,filename_video=%s where id=%s',('video/roller_video.mp4',str(task['user_roller_id'])))
#            db.execute('update users_rollers set is_ready=1,filename_screen=%s where id=%s',('video/roller_video.jpg',str(task['user_roller_id'])))
            
   #         res = base_rec_rend_of(task)
            

#            with mysql.connect(host=dbconnectionhost,user=dbusername,passwd=dbpassword,db=dbname) as db:
#                try:
#                    db.execute('update users_rollers set is_ready=1,filename_video=%s where id=%s',('video/roller_video.mp4',str(task['user_roller_id'])))
#                    db.execute('update users_rollers set is_ready=1,filename_screen=%s where id=%s',('video/roller_video.jpg',str(task['user_roller_id'])))
#                except Exception as e:
#                    logging.info('Base err : {}'.format(e))
#                finally:
#                    db.close()
            logging.info('{} TASK description {} ########## {} ### TIME :{}'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'),task['result_dir'],'res' ,end_time))
 #           db.close()

        except Exception as e:
            pass








@asyncio.coroutine
def check_queue ():
    while True:


        #logging.info('Job description {} ########## {} #  #'.format(' $$ ','KY KY KY BACKGROUND '))
        yield from asyncio.sleep(5)

        
        loop.call_soon_threadsafe(start_background_tasks)

         
def start_background_tasks():

    runing_task =  len(queue_of_run_tasks)
    
    #logging.info('{} ##  Objects len  in runningtask: {} ##########'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'), runing_task ))

    if runing_task >= MAX_SIZE_QUEUE:
        i = MAX_SIZE_QUEUE
    else:
        i = runing_task
    #queue_of_suspend
    #logging.info('{} ##  Objects len : {} ##########'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'), i ))
    while i:
        try:
            logging.info('{} ##  Object len: {} ##########'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'), i ))

            sec, task = queue_of_run_tasks.pop()
            
            task = json.loads(task)
            logging.info('{} ##  Object name: {} ##########'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'), task))

        #self._log.info('{} Count in queue {} status queu ::'.format(datetime.now().strftime('%c'), len(TaskWait.tst_list)))



        #    with mp.Pool(processes=os.cpu_count()) as pool:
        #        pool.map(rend_task,[task])

            #if 
            procs = mp.Process(target=rend_task, args=(task,)).start()
       
       # self._log.info('{} Proc started {} status queu :: '.format(datetime.now().strftime('%c'), procs ))

        #for proc in procs:
           # proc.setDaemon(True)
        #    proc.start()
        #    self._log.info('{} Proc started {} status queu ::{}'.format(datetime.now().strftime('%c'), proc, TaskWait.__queue))
        
        #for proc in procs:
        #    proc.join()
         #   self._log.info('{} Proc join {} status queu ::{}'.format(datetime.now().strftime('%c'), proc, TaskWait.__queue))



 
       # end_time = time.time() - start_time 
       # logging.info('Task description {} ########## {} ### TIME :{}'.format(task['project_name'],'KY KY KY BACKGROUND ',end_time))


        except Exception as e:
            pass

        #logging.info('Job description {} ########## {} ##########'.format(' $$ ','KY KY KY BACKGROUND '))
        #data = asyncio.wait_for(print('sds'), timeout=2.0)
        i-=1
        #end_time = time.time() - start_time 
    #logging.info('Task description {} ########## {} ### TIME :{}'.format('#$$#','KY KY KY BACKGROUND ',end_time))



@asyncio.coroutine
@DecoWithArgs('aaxbut','@','gmail.com')
def transmit(request):    
    data = yield from request.text()
    req_json = json.loads(data)
    #logging.info('{} :::'.format(datetime.now().strftime('%c')))

    if request.content_type == 'application/json':

        logging.info('Transmit description time : {} ########## Data : {} ##########'.format(datetime.now().strftime('%c'),req_json.__class__.__name__))
        ### some short work mby here
    return web.json_response(req_json)   


@asyncio.coroutine
def corobas_barabas(*args, **kwargs):
    #print(' <==XUBA BUBA KARABAS==> '*7,args)

    logging.info('{} ##  Object name: {} ##########'.format(datetime.now().strftime('%D:: %HH:%MM:%SS'), corobas_barabas.__name__))

    queue_of_run_tasks.insert(0,args)

    yield from asyncio.sleep(1)



@asyncio.coroutine
def corobas_1():

    yield from asyncio.sleep(1)

   # print(' <==XUBA BUBA==> '*7)
    #return {'ok':'ok'}

def main_loop(loop):
    #set logging  
    #logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
     
    app = web.Application(loop=loop)
    
    hello_corobas = loop.create_task(corobas_1()) # 

    loop_check_queue = loop.create_task(check_queue())



   
    app.router.add_post('/tr', transmit)
    
    server = yield from loop.create_server(app.make_handler(),'0.0.0.0',7812)
    return server


if __name__ == '__main__':
    #pool = ThreadPoolExecutor(4)
    queue_of_run_tasks = [] 
    #deque()
    
    policy = asyncio.get_event_loop_policy()
    policy.set_event_loop(policy.new_event_loop())
    loop = asyncio.get_event_loop()
    #loop.set_debug(True)
   
   # loop.call_soon_threadsafe(start_background_tasks)
   
    srv = loop.run_until_complete(main_loop(loop))

    
    try:
        logging.info('{} SRV: {} '.format(
                                            datetime.now().strftime('%c'), 
                                            srv.sockets[0].getsockname())) 
        loop.run_forever()
    
    except KeyboardInterrupt:
        logging.info('{} SRV: closing  {} '.format(
                                                    datetime.now().strftime('%c'), 
                                                    srv.sockets[0].getsockname())) 
        asyncio.gather(*asyncio.Task.all_tasks()).cancel()
        loop.stop()
        loop.close()  