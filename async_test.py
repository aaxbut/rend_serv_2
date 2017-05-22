import asyncio
from datetime import datetime
import logging
import json
from aiohttp import web
import threading
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
               # yield from executor.map(rend_full_movie, parts_tasks)
#                yield from executor.map(screens_maker, parts_tasks[0])
                logging.info('{} ####EXECUTOR RUN ###'.format('1'))

            elif rend_type == 4:
                f_count = 50
                parts = return_list_of_parts(f_count, 2)
                p = parts
                parts_tasks = [(x, task) for x in parts]
                yield from executor.map(rend_task, parts_tasks)
                yield q.task_done()
                #yield from executor.map(screens_maker, parts_tasks[0])
                logging.info('{} ####EXECUTOR RUN PRIVIEW ###'.format('1'))

            elif rend_type == 2:
                logging.info('{} ####EXECUTOR RUN SCREEN ###'.format('2'))
                parts = return_list_of_parts(5, 1)
                parts_tasks = [(x, task) for x in parts]
                logging.info('{} ####EXECUTOR RUN SCREEN ###'.format('res'))
        except Exception as e:
            logging.info('POOL err  {}'.format(str(e)))

    logging.info('{} #####################{} $#'.format(task['render_type'], ''))
    # here nee add function for split of projects and set rights
    time_end = time.time() - time_start
    logging.info('# {} complete TIME: {}'.format(task['result_dir'], time_end))




#@asyncio.coroutine
def rend_task(task):
    logging.info( '# {} REND'.format( task ) )
    frame_set, task_set = task
    f_s, f_e =frame_set

    gggg = r'/var/www/cmex.ru/data/uploads/rollers/Rauf/Rauf.blend'

    bpy.ops.wm.open_mainfile(filepath=gggg)
    #bpy.data.filepath = gggg

    bpy.context.scene.frame_start = f_s
    bpy.context.scene.frame_end = f_e
    bpy.context.scene.render.filepath = '{}{}.mp4'.format( str( task_set['result_dir'] ) + '/' + str( 'roller_video' ),
                                                           random.randint( 1, 2000 ) )
    bpy.context.scene.render.engine = 'CYCLES'
    bpy.context.scene.cycles.device = 'CPU'
    bpy.context.scene.render.ffmpeg.format = 'MPEG4'
    bpy.context.scene.render.ffmpeg.video_bitrate = 750
    bpy.context.scene.render.ffmpeg.audio_bitrate = 124

    bpy.ops.render.render( animation=True, scene=bpy.context.scene.name )

   # return [1,2]

#    os.chown( bpy.context.scene.render.filepath, int(500), int(500) )
#    os.chmod( bpy.context.scene.render.filepath, 0o777 )



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
    q.put(req_json)
    return web.json_response(req_json)



@asyncio.coroutine
def boo(q):
    while True:
        id_th = threading.get_ident()
        cur_th = threading.currentThread()
        yield from asyncio.sleep(1)
        logging.info('{1}: From BOO  ::res : {0}'.format(id_th, cur_th))
        if not q.empty():
            task = q.get()
            logging.info('{1}: From BOO ####### {2}::res : {0}'.format(id_th, q.qsize(), task))
            try:
                yield from starter_works(task)
                logging.info('From BOO: {0}'.format(q.qsize()))
            except Exception as e:
                logging.info('From BOO ERR: {0}'.format(e))


        #if len(queue_of_run_tasks) != 0:
        #    yield from loop.run_in_executor(None, rend_task(queue_of_run_tasks.pop()))
        #logging.info('{1}: From BOO  ::res : {0}'.format(id_th, cur_th))

 #       return '1'
#    while True:
#        id_th = threading.get_ident()
#        cur_th = threading.currentThread()

        #res = yield from queue_of_run_tasks.pop()
        #yield from asyncio.sleep(1)
 #       if len(queue_of_run_tasks) != 0:
 #           res = queue_of_run_tasks.pop()
 #           logging.info('{1}: {2} Froom BOO  ::res : {0}'.format(res,id_th, cur_th))



  #      logging.info('Transmit2e2e2e2e  :: {}'.format('dd'))
        #data = yield from asyncio.wait_for(bo(), timeout=2.0, loop= )




def loop_in_thread(loop, q):
    asyncio.set_event_loop(loop)
    #yield from loop.run_in_executor(None, boo())
    loop.run_until_complete(boo(q))



    #asyncio.set_event_loop(loop)
    #loop.run_forever()
    #new_loop = asyncio.new_loop()



def main_loop(loop):
    # set logging
    # logging.basicConfig(level=logging.DEBUG)
    FORMAT = '%(asctime)-15s %(processName)s %(message)s'
    logging.basicConfig(
        filename='sds',
        format=FORMAT,
        level=logging.INFO
    )
    app = web.Application( loop=loop )

    # loop_check_queue = loop.create_task(check_queue())
    app.router.add_post( '/tr', transmit )
    server = yield from loop.create_server( app.make_handler(), '0.0.0.0', 7812 )
    return server


if __name__ == '__main__':
    frames_count = {}
    queue_of_run_tasks = []
    run_tasks = []
    loop_new = asyncio.new_event_loop()



    policy = asyncio.get_event_loop_policy()
    policy.set_event_loop( policy.new_event_loop() )
    loop = asyncio.get_event_loop()
    #executor = ProcessPoolExecutor(2)
   # asyncio.ensure_future( loop.run_in_executor( executor, loop_in_thread ) )
    q = mp.JoinableQueue()
    srv = loop.run_until_complete(main_loop(loop))
    #pool = mp.Pool(4)
    #pool.map(loop_in_thread,([q,]))
    #pool.close()
    #pool.join()
    procs = [mp.Process(target=loop_in_thread, args=(loop, q)) for _ in range(4)]
    for proc in procs:
        #proc.daemon = True
        proc.start()

#    t = threading.Thread( target=loop_in_thread, args=(loop_new,) )
#    t.start()


    try:
        logging.info( '{} SRV: {} '.format(
            datetime.now().strftime( '%c' ),
            srv.sockets[0].getsockname() ) )
        loop.run_forever()
    except KeyboardInterrupt:
        for proc in procs:
            proc.join()
        logging.info( '{} SRV: closing  {} '.format(
            datetime.now().strftime( '%c' ),
            srv.sockets[0].getsockname() ) )
        asyncio.gather( *asyncio.Task.all_tasks() ).cancel()
        loop.stop()
        loop.close()