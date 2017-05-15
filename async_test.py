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

@asyncio.coroutine
def rend_task(task):

    bpy.ops.wm.open_mainfile( filepath=task['project_name'] )
    bpy.context.scene.frame_start = 0
    bpy.context.scene.frame_end = 10
    bpy.context.scene.render.filepath = '{}{}.mp4'.format( str( task['result_dir'] ) + '/' + str( 'roller_video' ),
                                                           random.randint( 1, 2000 ) )
    bpy.context.scene.render.engine = 'CYCLES'
    bpy.context.scene.cycles.device = 'CPU'
    bpy.context.scene.render.ffmpeg.format = 'MPEG4'
    bpy.context.scene.render.ffmpeg.video_bitrate = 750
    bpy.context.scene.render.ffmpeg.audio_bitrate = 124

    bpy.ops.render.render( animation=True, scene=bpy.context.scene.name )

    os.chown( bpy.context.scene.render.filepath, int(500), int(500) )
    os.chmod( bpy.context.scene.render.filepath, 0o777 )



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




def boo():
    while True:
        #res = yield from queue_of_run_tasks.pop()
        yield from asyncio.sleep(1)
        if len(queue_of_run_tasks) != 0:
            res = queue_of_run_tasks.pop()
            logging.info( 'Transmit2e2e2e2e  :: {}'.format(res) )

        logging.info( 'Transmit2e2e2e2e  :: {}'.format( 'dd' ) )
        #data = yield from asyncio.wait_for(bo(), timeout=2.0, loop= )




def loop_in_thread(loop):
    asyncio.set_event_loop(loop)

    loop.run_until_complete(boo())



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
    t = threading.Thread( target=loop_in_thread, args=(loop_new,) )
    t.start()
    srv = loop.run_until_complete( main_loop( loop ) )

    try:
        logging.info( '{} SRV: {} '.format(
            datetime.now().strftime( '%c' ),
            srv.sockets[0].getsockname() ) )
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info( '{} SRV: closing  {} '.format(
            datetime.now().strftime( '%c' ),
            srv.sockets[0].getsockname() ) )
        asyncio.gather( *asyncio.Task.all_tasks() ).cancel()
        loop.stop()
        loop.close()