# -*- coding: utf-8 -*-
import asyncio
from datetime import datetime
import logging
import json
from aiohttp import web
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

import asyncio.subprocess
import bpy
import os
import time
import MySQLdb as mysql
import math
import configparser
import random


conf = configparser.RawConfigParser()
conf.read('/etc/wb4.conf')
FFMPEG_BIN = 'ffmpeg'
BLEND_DIR = conf.get('bl_path', 'BLEND_DIR')
USERS_DIR = conf.get('bl_path', 'USERS_DIR')
dbconnectionhost = conf.get('base', 'dbconnectionhost')
dbname = conf.get('base', 'dbname')
dbusername = conf.get('base', 'dbusername')
dbpassword = conf.get('base', 'dbpassword')
u_ugid = conf.get('usr_permission', 'uid')
u_gguid = conf.get('usr_permission', 'gid')
MAX_SIZE_QUEUE = 6
LOG_FILENAME = '/var/tmp/render_blender_server_test.log'

p_rend_type = {
                1: {
                        'file_video': 'video/roller_video.mp4',
                        'file_screen': 'video/roller_video.jpg',
                        'render_type_video': 'filename_video',
                        'render_type_screen': 'filename_screen',
                        'status_start': 'is_render',
                        'status_end': 'is_ready'

                },
                2: {
                        'file_screen': 'video/roller_video.jpg',
                        'render_type_screen': 'filename_screen',
                        'status_start': 'is_render',
                        'status_end': 'is_ready'
                    },
                4: {
                        'file_video': 'video/roller_video_demo.mp4',
                        'file_screen': 'video/roller_video_demo.jpg',
                        'render_type_video': 'filename_video_demo',
                        'render_type_screen': 'filename_screen_demo',
                        'status_start': 'is_render_demo',
                        'status_end': 'is_ready_demo'

                },
            }


def data_update(**kwargs):
    render_type = int(kwargs['render_type'])
    cond = kwargs['cond']
    user_rollerid = kwargs['user_roller_id']

    if cond is False:
        try:
            with mysql.connect(host=dbconnectionhost, user=dbusername, passwd=dbpassword, db=dbname) as db:
                if render_type != 2:
                    db.execute('update users_rollers set {}=1,{}="{}", {}="{}" where id={}'.format(
                        p_rend_type[render_type]['status_start'],

                        p_rend_type[render_type]['render_type_video'],
                        p_rend_type[render_type]['file_video'],
                        p_rend_type[render_type]['render_type_screen'],
                        p_rend_type[render_type]['file_screen'],
                        user_rollerid
                    ))
                else:
                    db.execute('update users_rollers set {}=1, {}="{}" where id={}'.format(
                        p_rend_type[render_type]['status_start'],
                        p_rend_type[render_type]['render_type_screen'],
                        p_rend_type[render_type]['file_screen'],

                        user_rollerid
                    ))
        except mysql.Error as e:
            logging.error('{}'.format(str(e)))

    elif cond is True:
        with mysql.connect(host=dbconnectionhost, user=dbusername, passwd=dbpassword, db=dbname) as db:
            try:
                if kwargs['render_type'] == 1:
                    db.execute('update users_rollers set {}=1,{}="{}", {}="{}" where id={}'.format(
                        p_rend_type[render_type]['status_end'],
                        p_rend_type[render_type]['render_type_video'],
                        p_rend_type[render_type]['file_video'],
                        p_rend_type[render_type]['render_type_screen'],
                        p_rend_type[render_type]['file_screen'],
                        user_rollerid
                    ))
                elif kwargs['render_type'] == 4:
                    db.execute('update users_rollers set {}=1,{}="{}", {}="{}" where id={}'.format(
                        p_rend_type[render_type]['status_end'],
                        p_rend_type[render_type]['render_type_video'],
                        p_rend_type[render_type]['file_video'],
                        p_rend_type[render_type]['render_type_screen'],
                        p_rend_type[render_type]['file_screen'],
                        user_rollerid
                    ))
                elif kwargs['render_type'] == 2:
                    db.execute('update users_rollers set {}=1, {}="{}" where id={}'.format(
                        p_rend_type[render_type]['status_end'],
                        p_rend_type[render_type]['render_type_screen'],
                        p_rend_type[render_type]['file_screen'],
                        user_rollerid
                    ))
            except mysql.Error as e:
                logging.error('{}'.format(str(e)))


def return_list_of_parts(len_frames, parts):
    """  function make parts from size 
    :rtype: list
    """
    chunk = len_frames / parts
    floor_chunk = math.floor(chunk)
    chunk_all = floor_chunk * parts
    i = 0
    parts_list = []
    for x in range(parts):
        i += floor_chunk
        parts_list.append([i - floor_chunk, i - 1])
    if parts_list[-1][1] != len_frames:
        i += len_frames - chunk_all
        x1, y1 = parts_list.pop()
        parts_list.append([x1, i])
    return parts_list


@asyncio.coroutine
def great_split(task, parts):
    rend_type = int(task['render_type'])
    rend_result_dir = task['result_dir']
    file_name = p_rend_type[rend_type]['file_video'].split('/')[1].split('.')[0]
    file_txt_for_concat = '{}/tst.txt'.format(rend_result_dir)
    with open(file_txt_for_concat, 'w') as f:
        for x in parts:
            f.write('file {}_{}.mp4 \n'.format(file_name, x[0]))

    out_file = '{}/{}.mp4'.format(rend_result_dir, file_name)
    logging.info('IN GreatSplit :{}'.format(out_file))
    command = [
        FFMPEG_BIN,
        '-y',
        '-f', 'concat',
        '-safe', '0',
        '-i', file_txt_for_concat,
        '-c', 'copy',
        out_file,
    ]
    try:
        process = yield from asyncio.create_subprocess_exec(
                    *command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT
                )
        stdout, _ = yield from process.communicate()
        # logging.info('{}'.format(stdout))
    except Exception as e:
        logging.info('{}'.format(str(e)))

    try:
        os.chown(out_file, int(u_ugid), int(u_gguid))
        os.chmod(out_file, 0o777)
    except Exception as e:
        pass
    with open(file_txt_for_concat, 'r') as f:
        for x in f.readlines():
            param = x.strip('file').strip()
            pth_for_del = '{}/{}'.format(rend_result_dir, param)
            try:
                os.remove(pth_for_del)
            except Exception as e:
                logging.info('ERR Del {}'.format(e))
                pass
            logging.info('Del {}'.format(pth_for_del))
    try:
        os.remove(file_txt_for_concat)
    except Exception as e:
        logging.info('ERR Del {}'.format(e))






@asyncio.coroutine
def rend_picture(task):
    frame_set, task_set = task[0]
    logging.info('FILE opened {}'.format(task))
#    frame_start, frame_end = frame_set

    try:
        bpy.ops.wm.open_mainfile(filepath=task_set['project_name'])
        logging.info('FILE opened {}'.format(str(bpy.ops.wm.open_mainfile)))
    except Exception as e:
        logging.info(' OPEN FILE ERR{}'.format(str(e)))
    try:
        scn = bpy.context.scene
        bpy.data.scenes[scn.name].render.image_settings.file_format = 'JPEG'
        scn.render.filepath = '{}.mp40000.jpg{}'.format(
            str(task_set['result_dir']) + '/' + str('roller_video'),random.randint(1,10000))
        bpy.ops.render.render(write_still=True)
#        os.chown(scn.render.filepath, int(u_ugid), int(u_gguid))
#        os.chmod(scn.render.filepath, 0o777)
    except Exception as e:
        logging.info('ERR PIC {}'.format(str(e)))

    #yield {'ok'}


def rend_preview(task):
    frame_set, task_set = task
    frame_start, frame_end = frame_set
    # logging.info('REND_PRIVIEW {} ::: {} in queue {} '.format(frames_start, frames_end, len(queue_of_run_tasks)))
    #    logging.info('REND_PRIVIEW def {} :: '.format(kwargs))
    try:
        bpy.ops.wm.open_mainfile(filepath=task_set['project_name'])
    except Exception as e:
        logging.info('{}'.format(str(e)))

    try:
        scn = bpy.context.scene
        scn.frame_start = frame_start
        scn.frame_end = frame_end
        scn.render.filepath = '{}_{}.mp4'.format(
            str(task_set['result_dir']) + '/' + str('roller_video_demo'),
            str(frame_start))
        scn.render.engine = 'CYCLES'
        scn.cycles.device = 'CPU'
        scn.render.ffmpeg.format = 'MPEG4'
        scn.render.ffmpeg.codec = 'MPEG4'
        scn.render.ffmpeg.video_bitrate = 750
#        scn.render.ffmpeg.maxrate = 9000
#        scn.render.ffmpeg.packetsize = 4096
        scn.render.resolution_percentage = 60
        scn.render.ffmpeg.audio_bitrate = 124
        bpy.ops.render.render(animation=True, scene=scn.name)
        os.chown(scn.render.filepath, int(u_ugid), int(u_gguid))
        os.chmod(scn.render.filepath, 0o777)

    except Exception as e:
        logging.info('REND_PRIVIEW def {} :::  '.format(str(e)))

    return '1'


def rend_full_movie(task):
    frame_set, task_set = task
    frame_start, frame_end = frame_set
    logging.info('REND_full movie {} ::: {} '.format(frame_start, frame_end))
    #  logging.info('REND_full movie {} :: '.format(kwargs))
    try:
        bpy.ops.wm.open_mainfile(filepath=task_set['project_name'])
    except Exception as e:
        logging.info('{}'.format(str(e)))

    try:
        scn = bpy.context.scene
        scn.frame_start = frame_start
        scn.frame_end = frame_end
        scn.render.filepath = '{}_{}.mp4'.format(
            str(task_set['result_dir']) + '/' + str('roller_video'),
            str(frame_start))
        scn.render.engine = 'CYCLES'
        scn.cycles.device = 'CPU'
        scn.render.ffmpeg.format = 'MPEG4'
        scn.render.ffmpeg.codec = 'MPEG4'
        scn.render.ffmpeg.video_bitrate = 750
#        scn.render.ffmpeg.maxrate = 9000
#        scn.render.ffmpeg.packetsize = 4096
        scn.render.ffmpeg.audio_bitrate = 124
        bpy.ops.render.render(animation=True, scene=scn.name)
        os.chown(scn.render.filepath, int(u_ugid), int(u_gguid))
        os.chmod(scn.render.filepath, 0o777)
    except Exception as e:
        logging.info('{}'.format(str(e)))
    return '1'


@asyncio.coroutine
def check_queue():
    """ check queue mby need another variant to make waiter """
    while True:
        logging.info('Awaiting task {} : {} : {} :'.format(threading.current_thread(),threading.active_count(),threading.get_ident))
        yield from asyncio.sleep(5)
        #yield from loop.create_task((start_background_tasks()))
#        loop.call_soon_threadsafe((start_background_tasks()))


def bframes_count(**kwargs) -> int:
    """
        look frames count in dict frames_count if None open
        file and take it from blend file

    :rtype: int
    """
    path_project = kwargs['project_name']
    project_name = path_project.split('/')[-1].strip('.')
    if project_name in frames_count:
        return frames_count[project_name]['count']
    else:
        bpy.ops.wm.open_mainfile(filepath=path_project)
        count_frames = bpy.context.scene.frame_end
        frames_count[project_name] = {'project_name': project_name, 'count': count_frames}
        return count_frames


def screens_maker(task):
    """
    screens_maker make screens for full and preview movie
    
    :param task - kwargs of task  
    :return: int 
    """
    rend_type = int(task['render_type'])
    rend_project = task['project_name']
    rend_result_dir = task['result_dir']
    file_name = p_rend_type[rend_type]['file_screen'].split('/')[1]
    logging.info('IN SCREEN Maker {}'.format(task))
    try:
        bpy.ops.wm.open_mainfile(filepath=rend_project)
        scn = bpy.context.scene
        scn.frame_start = 100
        scn.frame_end = 101
        bpy.data.scenes[scn.name].render.image_settings.file_format = 'JPEG'
        scn.render.filepath = '{}'.format(str(rend_result_dir) + '/' + str(file_name))
        bpy.ops.render.render(write_still=True)
        try:
            os.chown(scn.render.filepath, int(u_ugid), int(u_gguid))
            os.chmod(scn.render.filepath, 0o777)
        except Exception as e:
            logging.info('err SCREEN MAKER rights{}'.format(str(e)))
    except Exception as e:
        logging.info('ERR IN SCREEN Maker {}'.format(str(e)))

    return 1


def tester1(task):
    frame_set, task_set = task
    frame_start, frame_end = frame_set
    asyncio.sleep(0.001)
    logging.info('BEFORE tester run #: {}, #{},{}:'.format(task, frame_start, frame_end))

    try:
        bpy.ops.wm.open_mainfile(filepath=task_set['project_name'])
    except Exception as e:
        logging.info('{}'.format(str(e)))
    try:
        scn = bpy.context.scene
        scn.frame_start = frame_start
        scn.frame_end = frame_end
        scn.render.filepath = '{}_{}.mp4'.format(
            str(task_set['result_dir']) + '/' + str('roller_video'),
            str(frame_start),
        )
        scn.render.engine = 'CYCLES'
        scn.cycles.device = 'CPU'
        scn.render.ffmpeg.format = 'MPEG4'
        scn.render.ffmpeg.codec = 'MPEG4'
        scn.render.ffmpeg.video_bitrate = 6000
        scn.render.ffmpeg.maxrate = 9000
        scn.render.ffmpeg.packetsize = 4096
        scn.render.resolution_percentage = 60
        scn.render.ffmpeg.audio_bitrate = 384
        bpy.ops.render.render(animation=True, scene=bpy.context.scene.name)
        os.chown(scn.render.filepath, int(u_ugid), int(u_gguid))
        os.chmod(scn.render.filepath, 0o777)
    except Exception as e:
        logging.info('EXCEPTION : {} #'.format(str(e)))
    logging.info('After tester run')

import concurrent.futures
@asyncio.coroutine
def starter_works(task):
    time_start = time.time()
    logging.info('{} #############!!!!!!!!#########'.format(task))
    rend_type = int(task['render_type'])
    user_roller_id = task['user_roller_id']
    p = []

    #$logging.info('{} #############!!!!!!!!#########'.format(task[0]['render_type']))
    # before run need update base for task started
#    yield from data_update(
#        render_type=rend_type,
#        user_roller_id=user_roller_id,
#        cond=False
#    )

    with ProcessPoolExecutor(max_workers=2) as executor:

        #  before run need split dict with task settings and list with parts
        #  function look in dict have part if full movie, if movie pri run 500 // 5 parts
        try:
            if rend_type == 1:
                f_count = 50
                    # bframes_count(project_name=task['project_name'])
                parts = return_list_of_parts(f_count, 2)
                p = parts
                parts_tasks = yield from [(x, task) for x in parts]
                yield from executor.map(rend_full_movie, parts_tasks)
                yield from executor.map(screens_maker, parts_tasks[0])
                logging.info('{} ####EXECUTOR RUN ###'.format('1'))

            elif rend_type == 4:
                f_count = 50
                parts = return_list_of_parts(f_count, 2)
                p = parts
                parts_tasks = [(x, task) for x in parts]
                #yield from executor.map(rend_preview, parts_tasks)
                yield from executor.map(screens_maker, parts_tasks[0])
                logging.info('{} ####EXECUTOR RUN PRIVIEW ###'.format('1'))

            elif rend_type == 2:
                logging.info('{} ####EXECUTOR RUN SCREEN ###'.format('2'))
                parts = return_list_of_parts(5, 1)
                parts_tasks = [(x, task) for x in parts]
                #rend_picture(*parts_tasks)

                #asyncio.Task(rend_picture(parts_tasks))
                #l = yield from rend_picture(parts_tasks)
                #th = threading.Thread(target=rend_picture, args=(parts_tasks))
                #th.start()
                #th.join()
                yield from loop.create_task(rend_picture(parts_tasks))
                #concurrent.futures.wait(rend_picture(parts_tasks),return_when=ALL_COMPLETED)
                logging.info('{} ####EXECUTOR RUN SCREEN ###'.format('res'))
        except Exception as e:
            logging.info('POOL err  {}'.format(str(e)))

    logging.info('{} #####################{} $#'.format(task['render_type'], ''))
    # here nee add function for split of projects and set rights
    if rend_type == 1 or rend_type == 4:
        logging.info('{} #########{}#########{} $# {} '.format(rend_type, 'BEFORE SPLIT', p, task))
     #   yield from great_split(task, p)

    #try:
    #    yield from data_update(
    #        render_type=rend_type,
    #        user_roller_id=user_roller_id,
    #        cond=True)
    #except mysql.Error as e:
    #    logging.info('POOL err{}'.format(str(e)))

    # function to update in base complited task
    time_end = time.time() - time_start
    logging.info('# {} complete TIME: {}'.format(task['result_dir'], time_end))


 
def fg(er):
    logging.info('FFFFFFFFFFFFFFGGGGGGGGGGGGGGGGGGG{} complete'.format(er))
    #fut = asyncio.async(starter_works(er))

   # t = Tread(target=starter_works,args=er)
   # t.start()
    #loop.create_task(starter_works(er))

    return 0
from threading import Thread

@asyncio.coroutine
def low_level(*args,loop=None):
     #asyncio.async(low_level())
     logging.info('FFFFFFFFFFFFFFGGGGGGGGwefwefwfGGGGGGGGGGG{} complete'.format('saff'))
     #fut = asyncio.async(starter_works(args[0]))

import threading    



@asyncio.coroutine
def start_background_tasks(future):
    # len of queue in lists
    len_queue = len(queue_of_run_tasks)
    sub_tasks = []
    #new_loop = asyncio.new_event_loop()
    #t = mp.Process(target=new_loop.run_until_complete, args=(low_level(queue_of_run_tasks.pop(),loop=new_loop),))
    #t.start()
    #t.join()
    

    logging.info('{} ##  Object len: {}'.format(queue_of_run_tasks, len(queue_of_run_tasks)))
    # 2 tasks in 1 click !!! not more or slow down rend
    if len_queue != 0 and len_queue > 4:
        for x in range(2):
            logging.info('{}'.format(str(x)))
            sub_tasks.append(starter_works(queue_of_run_tasks.pop()))
            sub_tasks.append(queue_of_run_tasks.pop())
            yield from starter_works(sub_tasks.pop())
            logging.info('{} ##  sub_tasks Object len: {}'.format(sub_tasks, len(sub_tasks)))
           # new_loop = asyncio.new_event_loop()

          #  t = threading.Thread(target=new_loop.run_until_complete, args=(low_level(sub_tasks.pop(),loop=new_loop),))
         #   t.start()
        #    t.join()
           # new_loop.run_until_complete(low_level(sub_tasks.pop(),loop=new_loop))
     #   procs = [mp.Process(target=starter_works,args=[task,]) for task in sub_tasks]
     #   for proc in procs:
     #       proc.start()

 #       yield from asyncio.gather(*sub_tasks)
        future.set_result('Future is done!')
        # logging.info('{} len >2 {}'.format(asyncio.gather.__name__, len_queue))
    else:
        for x in range(len_queue):
            logging.info('{}'.format(str(x)))
            
            #sub_tasks.append(queue_of_run_tasks.pop())
            logging.info('{}PROC  sub_tasks Object len: {}'.format(sub_tasks, type(sub_tasks)))
#        yield from asyncio.gather(*sub_tasks)
        try:
            sub_tasks.append(queue_of_run_tasks.pop())
            #mp.Process(target=starter_works,args=[taska,]).start()
            yield from starter_works(sub_tasks.pop())
            #proc = mp.Process(target=starter_works,args=[queue_of_run_tasks.pop(),]).start()
            #yield from proc.join()
            

        except Exception as e:
            logging.info('PROC {}'.format(e))


        future.set_result('Future is done!')
    

            

            

         
#        try:
#            #loop.call_soon_threadsafe(starter_works(sub_tasks))#

            #with mp.Pool() as p:
            #    p.map_async(starter_works,sub_tasks)
            #    logging.info('{}PROC  sub_tasks Object len: {}'.format(sub_tasks, pool_1))
       # except Exception as e:
       #     logging.info('{}'.format(e))
        #with ProcessPoolExecutor(max_workers=1) as executor:
        #    executor.map(list(starter_works), sub_tasks)

        
#        procs = [mp.Process(target=starter_works,args=[task,]) for task in sub_tasks]
#        
#        logging.info('PROC  sub_tasks Object len: {}'.format(procs))
#        for proc in procs:
 #           logging.info('PROC  sub_tasks Object len: {}'.format(proc))
            #proc.daemon =True
 #           proc.start()
 #       for proc in procs:
 #           logging.info('PROC  sub_tasks Object len: {}'.format(proc))
 #           #proc.daemon =True
 #           proc.join()
#

        #yield from asyncio.gather(*sub_tasks)


@asyncio.coroutine
def transmit(request):
    data = yield from request.text()
    #yield from asyncio.sleep(1)
    req_json = json.loads(data)
    # logging.info('{} :::'.format(datetime.now().strftime('%c')))
    if request.content_type == 'application/json':
        logging.info('Transmit description time : {} ## Data : {} ###'.format(
                                    datetime.now().strftime('%c'),
                                    req_json.__class__.__name__
                                    ))
    # some short work mby here
    queue_of_run_tasks.append(req_json)
    return web.json_response(req_json)


@asyncio.coroutine
def greet_every_two_seconds(loop):
    while True:
        #loop.create_task(start_background_tasks())
        #print('Hello World', threading.current_thread(), threading.active_count(),threading.get_ident())
        print('in proc : ', os.getppid(),__name__,os.getpid(), len(queue_of_run_tasks))
        if len(queue_of_run_tasks) > 0:
            print('Hello World!!!!!!!!!!!!!!!!!!!',len(queue_of_run_tasks))
            #bkp_loop = asyncio.new_event_loop()
            future1 = asyncio.Future(loop=loop)

            future = asyncio.run_coroutine_threadsafe(start_background_tasks(future1),loop)
            #yield from future
#            try:
#                loop.create_task(start_background_tasks(future1))
#            except Exception as e:
#                print(e)

#            tasks = [
#                start_background_tasks(future1),
#            ]
#            yield from asyncio.gather(*tasks)

            #yield from asyncio.ensure_future(start_background_tasks(future1))
            #loop.run_until_complete(future1)
            #bkp_loop.close()
            

        yield from asyncio.sleep(2)


def loop_in_thread(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(greet_every_two_seconds(loop))


def main_loop(loop):
    # set logging
    # logging.basicConfig(level=logging.DEBUG)
    FORMAT = '%(asctime)-15s %(processName)s %(message)s'
    logging.basicConfig(
                        filename='sds',
                        format=FORMAT,
                        level=logging.INFO
                        )
    app = web.Application(loop=loop)

    loop_check_queue = loop.create_task(check_queue())
    app.router.add_post('/tr', transmit)
    server = yield from loop.create_server(app.make_handler(), '0.0.0.0', 7812)
    return server


if __name__ == '__main__':
    frames_count = {}
    queue_of_run_tasks = []
    run_tasks = []

    new_loop = asyncio.new_event_loop()


    policy = asyncio.get_event_loop_policy()
    policy.set_event_loop(policy.new_event_loop())
    loop = asyncio.get_event_loop()
    srv = loop.run_until_complete(main_loop(loop))

#    p = mp.Process(target=loop_in_thread, args = (new_loop,))
#    p.start()
   

    t = threading.Thread(target=loop_in_thread, args=(new_loop,))
    t.start()


    
    try:
        logging.info( '{} SRV: {} '.format(
            datetime.now().strftime('%c'),
            srv.sockets[0].getsockname()))
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info( '{} SRV: closing  {} '.format(
            datetime.now().strftime('%c'),
            srv.sockets[0].getsockname()))
        asyncio.gather(*asyncio.Task.all_tasks()).cancel()
        loop.stop()
        loop.close()
