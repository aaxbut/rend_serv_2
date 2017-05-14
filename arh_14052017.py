# -*- coding: utf-8 -*-
import asyncio
from datetime import datetime
import logging
import json
from aiohttp import web
import itertools
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import asyncio.subprocess
import random

from queue import Empty
from collections import deque
import subprocess as sp
import bpy
import os
import time
import MySQLdb as mysql
import math
import configparser

conf = configparser.RawConfigParser()
conf.read( '/etc/wb4.conf' )
FFMPEG_BIN = 'ffmpeg'
BLEND_DIR = conf.get( 'bl_path', 'BLEND_DIR' )
USERS_DIR = conf.get( 'bl_path', 'USERS_DIR' )
dbconnectionhost = conf.get( 'base', 'dbconnectionhost' )
dbname = conf.get( 'base', 'dbname' )
dbusername = conf.get( 'base', 'dbusername' )
dbpassword = conf.get( 'base', 'dbpassword' )
u_ugid = conf.get( 'usr_permission', 'uid' )
u_gguid = conf.get( 'usr_permission', 'gid' )
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


# base connect


class DecoWithArgs( object ):
    def __init__(self, args1, args2, args3):
        # additional agrs in decorator
        self.args1 = str( args1 )
        self.args2 = str( args2 )
        self.args3 = str( args3 )

    def __call__(self, func):
        def warap(*args, **kwagrs):
            func1 = yield from func( *args )
            # run task update in list,
            loop.create_task( corobas_barabas( '23', func1.text ) )
            return func1

        return warap


class DecoWithArgsMysqlUpd( object ):
    def __init__(self, args1, args2, args3):

        # $ print('inside class __init__')
        # additional agrs in decorator
        self.args1 = str( args1 )
        self.args2 = str( args2 )
        self.args3 = str( args3 )

    def __call__(self, func):
        # print('inside __call__  %s' % str(func))

        def warap(*args):

            logging.info( 'WRAP KWARGS BEFORE: {}'.format( args[0] ) )
            render_type = int( args[0]['render_type'] )
            user_roller_id = args[0]['user_roller_id']
            if render_type == 1:
                with mysql.connect( host=dbconnectionhost, user=dbusername, passwd=dbpassword, db=dbname ) as db:
                    try:
                        db.execute( 'update users_rollers set is_render=1,filename_video=%s where id=%s',
                                    ('video/roller_video.mp4', user_roller_id) )
                        db.execute( 'update users_rollers set is_render=1,filename_screen=%s where id=%s',
                                    ('video/roller_video.jpg', user_roller_id) )
                    except Exception as e:
                        logging.info( 'Base err : {}'.format( e ) )
                    finally:
                        db.close()

            if render_type == 4:
                with mysql.connect( host=dbconnectionhost, user=dbusername, passwd=dbpassword, db=dbname ) as db:
                    try:
                        db.execute( 'update users_rollers set is_render_demo=1,filename_video_demo=%s where id=%s',
                                    ('video/roller_video_demo.mp4', user_roller_id) )
                        db.execute( 'update users_rollers set is_render_demo=1,filename_screen_demo=%s where id=%s',
                                    ('video/roller_video_demo.jpg', user_roller_id) )
                    except Exception as e:
                        logging.info( 'Base err : {}'.format( e ) )
                    finally:
                        db.close()

            if render_type == 2:
                with mysql.connect( host=dbconnectionhost, user=dbusername, passwd=dbpassword, db=dbname ) as db:
                    try:
                        db.execute( 'update users_rollers set is_render=1,filename_screen=%s where id=%s',
                                    ('video/roller_video.jpg', user_roller_id) )
                    except Exception as e:
                        logging.info( 'Base err : {}'.format( e ) )
                    finally:
                        db.close()
            logging.info( 'WRAP KWARGS BEFORE: {}'.format( args[0]['render_type'] ) )
            func1 = func( *args )
            logging.info(
                'WRAP KWARGS BEFORE ########################################: {}'.format( args[0]['render_type'] ) )

            if render_type is 1:
                with mysql.connect( host=dbconnectionhost, user=dbusername, passwd=dbpassword, db=dbname ) as db:
                    try:
                        db.execute(
                            'update users_rollers set is_ready=1,filename_video="%s", filename_screen="%s" where id=%s' %
                            (
                                'video/roller_video.mp4',
                                'video/roller_video.jpg',
                                user_roller_id
                            ) )
                        # db.execute('update users_rollers set is_ready=1,filename_screen=%s where id=%s',('video/roller_video.jpg',user_roller_id))



                    except Exception as e:
                        logging.info( 'Base err 111111 : {}'.format( e ) )
                    finally:
                        db.close()

            if render_type == 4:
                with mysql.connect( host=dbconnectionhost, user=dbusername, passwd=dbpassword, db=dbname ) as db:
                    try:
                        db.execute( 'update users_rollers set is_ready_demo=1,filename_video_demo=%s where id=%s',
                                    ('video/roller_video_demo.mp4', user_roller_id) )
                        db.execute( 'update users_rollers set is_ready_demo=1,filename_screen_demo=%s where id=%s',
                                    ('video/roller_video_demo.jpg', user_roller_id) )
                    except Exception as e:
                        logging.info( 'Base err : {}'.format( e ) )
                    finally:
                        db.close()

            if render_type == 2:
                with mysql.connect( host=dbconnectionhost, user=dbusername, passwd=dbpassword, db=dbname ) as db:
                    try:
                        db.execute( 'update users_rollers set is_ready=1,filename_screen=%s where id=%s',
                                    ('video/roller_video.jpg', user_roller_id) )
                    except Exception as e:
                        logging.info( 'Base err : {}'.format( e ) )
                    finally:
                        db.close()
            logging.info( 'WRAP KWARGS BEFORE: {}'.format( args[0]['render_type'] ) )
            return func1

        return warap


def data_update(**kwargs):
    render_type = int( kwargs['render_type'] )
    cond = kwargs['cond']
    user_rollerid = kwargs['user_roller_id']

    if cond is False:
        try:
            with mysql.connect( host=dbconnectionhost, user=dbusername, passwd=dbpassword, db=dbname ) as db:
                if render_type != 2:
                    db.execute( 'update users_rollers set {}=1,{}="{}", {}="{}" where id={}'.format(
                        p_rend_type[render_type]['status_start'],

                        p_rend_type[render_type]['render_type_video'],
                        p_rend_type[render_type]['file_video'],
                        p_rend_type[render_type]['render_type_screen'],
                        p_rend_type[render_type]['file_screen'],
                        user_rollerid
                    ) )
                else:
                    db.execute( 'update users_rollers set {}=1, {}="{}" where id={}'.format(
                        p_rend_type[render_type]['status_start'],
                        p_rend_type[render_type]['render_type_screen'],
                        p_rend_type[render_type]['file_screen'],

                        user_rollerid
                    ) )
        except mysql.Error as e:
            logging.error( '{}'.format( str( e ) ) )
        finally:
            db.close()
    elif cond is True:
        with mysql.connect( host=dbconnectionhost, user=dbusername, passwd=dbpassword, db=dbname ) as db:
            try:
                if kwargs['render_type'] == 1:
                    db.execute( 'update users_rollers set {}=1,{}="{}", {}="{}" where id={}'.format(
                        p_rend_type[render_type]['status_end'],
                        p_rend_type[render_type]['render_type_video'],
                        p_rend_type[render_type]['file_video'],
                        p_rend_type[render_type]['render_type_screen'],
                        p_rend_type[render_type]['file_screen'],
                        user_rollerid
                    ) )
                elif kwargs['render_type'] == 4:
                    db.execute( 'update users_rollers set {}=1,{}="{}", {}="{}" where id={}'.format(
                        p_rend_type[render_type]['status_end'],
                        p_rend_type[render_type]['render_type_video'],
                        p_rend_type[render_type]['file_video'],
                        p_rend_type[render_type]['render_type_screen'],
                        p_rend_type[render_type]['file_screen'],
                        user_rollerid
                    ) )
                elif kwargs['render_type'] == 2:
                    db.execute( 'update users_rollers set {}=1, {}="{}" where id={}'.format(
                        p_rend_type[render_type]['status_end'],
                        p_rend_type[render_type]['file_screen'],
                        p_rend_type[render_type]['render_type_screen'],
                        user_rollerid
                    ) )
            except mysql.Error as e:
                logging.error( '{}'.format( str( e ) ) )
            finally:
                db.close()


def return_list_of_parts(len_frames, parts):
    """  function make parts from size 
    :rtype: list
    """
    chunk = len_frames / parts
    floor_chunk = math.floor( chunk )
    chunk_all = floor_chunk * parts
    i = 0
    parts_list = []
    for x in range( parts ):
        i += floor_chunk
        parts_list.append( [i - floor_chunk, i - 1] )
    if parts_list[-1][1] != len_frames:
        i += len_frames - chunk_all
        x1, y1 = parts_list.pop()
        parts_list.append( [x1, i] )
    return parts_list


def timit(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logging.info( 'before run' )
        func( *args, **kwargs )
        end_time = time.time() - start_time
        logging.info( 'after run {}      TIME:{}'.format( func.__name__, end_time ) )

    return wrapper


def timit2(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logging.info( 'before run' )
        func( *args )
        end_time = time.time() - start_time
        logging.info( 'after ARGS KWARGS {}      TIME:'.format( args ) )
        logging.info( 'after run {}      TIME:{}'.format( func.__name__, end_time ) )

    return wrapper


@asyncio.coroutine
def great_split(task, parts):
    #    logging.info('IN GreatSplit {} : {}'.format(task, parts))
    rend_type = int( task['render_type'] )
    rend_result_dir = task['result_dir']
    file_name = p_rend_type[rend_type]['file_video'].split( '/' )[1].split( '.' )[0]

    file_txt_for_concat = '{}/tst.txt'.format( rend_result_dir )
    # str(rend_result_dir) + '/' + 'tst.txt'

    logging.info(
        'IN GreatSplit {} : {} : {} :{}'.format( rend_type, rend_result_dir, file_name, file_txt_for_concat ) )

    with open( file_txt_for_concat, 'w' ) as f:
        for x in parts:
            f.write( 'file {}_{}.mp4 \n'.format( file_name, x[0] ) )

    out_file = '{}/{}.mp4'.format( rend_result_dir, file_name )
    logging.info( 'IN GreatSplit :{}'.format( out_file ) )
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
    except Exception as e:
        logging.info( '{}'.format( str( e ) ) )
    stdout, _ = yield from process.communicate()
    logging.info( '{}'.format( stdout ) )

    os.chown( out_file, int( u_ugid ), int( u_gguid ) )
    os.chmod( out_file, 0o777 )


def rend_picture(task):
    frame_set, task_set = task
    #    frame_start, frame_end = frame_set
    try:
        bpy.ops.wm.open_mainfile( filepath=task_set['project_name'] )
    except Exception as e:
        logging.info( '{}'.format( str( e ) ) )
    try:
        scn = bpy.context.scene
        bpy.data.scenes[scn.name].render.image_settings.file_format = 'JPEG'
        scn.render.filepath = '{}.mp40000.jpg'.format(
            str( task_set['result_dir'] ) + '/' + str( 'roller_video' ) )
        bpy.ops.render.render( write_still=True )
        os.chown( scn.render.filepath, int( u_ugid ), int( u_gguid ) )
        os.chmod( scn.render.filepath, 0o777 )
    except Exception as e:
        logging.info( 'ERR PIC {}'.format( str( e ) ) )

    return 1


def rend_preview(task):
    frame_set, task_set = task
    frame_start, frame_end = frame_set
    # logging.info('REND_PRIVIEW {} ::: {} in queue {} '.format(frames_start, frames_end, len(queue_of_run_tasks)))
    #    logging.info('REND_PRIVIEW def {} :: '.format(kwargs))
    try:
        bpy.ops.wm.open_mainfile( filepath=task_set['project_name'] )
    except Exception as e:
        logging.info( '{}'.format( str( e ) ) )

    try:
        scn = bpy.context.scene
        scn.frame_start = frame_start
        scn.frame_end = frame_end
        scn.render.filepath = '{}_{}.mp4'.format(
            str( task_set['result_dir'] ) + '/' + str( 'roller_video_demo' ),
            str( frame_start ) )
        scn.render.engine = 'CYCLES'
        scn.cycles.device = 'CPU'
        scn.render.ffmpeg.format = 'MPEG4'
        scn.render.ffmpeg.codec = 'MPEG4'
        scn.render.ffmpeg.video_bitrate = 6000
        scn.render.ffmpeg.maxrate = 9000
        scn.render.ffmpeg.packetsize = 4096
        scn.render.resolution_percentage = 60
        scn.render.ffmpeg.audio_bitrate = 384
        bpy.ops.render.render( animation=True, scene=scn.name )
        os.chown( scn.render.filepath, int( u_ugid ), int( u_gguid ) )
        os.chmod( scn.render.filepath, 0o777 )

        # make screen from roller
        #  make screen from video roller
    #        bpy.context.scene.frame_start = 100
    #        bpy.context.scene.frame_end = 101
    #        bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
    #        scn.render.filepath = '{}.jpg'.format(str(task_set['result_dir']) + '/' + str('roller_video_demo'))
    #        bpy.ops.render.render(write_still=True)
    #        try:
    #            os.chown(scn.render.filepath, int(u_ugid), int(u_gguid))
    #            os.chmod(scn.render.filepath, 0o777)
    #        except Exception as e:
    #            logging.info('Render TASK  moview_priview {}'.format(str(e)))

    except Exception as e:
        logging.info( 'REND_PRIVIEW def {} :::  '.format( str( e ) ) )

    return '1'


def rend_full_movie(task):
    frame_set, task_set = task
    frame_start, frame_end = frame_set
    logging.info( 'REND_full movie {} ::: {} '.format( frame_start, frame_end ) )
    #  logging.info('REND_full movie {} :: '.format(kwargs))
    try:
        bpy.ops.wm.open_mainfile( filepath=task_set['project_name'] )
    except Exception as e:
        logging.info( '{}'.format( str( e ) ) )

    try:
        scn = bpy.context.scene
        scn.frame_start = frame_start
        scn.frame_end = frame_end
        scn.render.filepath = '{}_{}.mp4'.format(
            str( task_set['result_dir'] ) + '/' + str( 'roller_video' ),
            str( frame_start ) )
        scn.render.engine = 'CYCLES'
        scn.cycles.device = 'CPU'
        scn.render.ffmpeg.format = 'MPEG4'
        scn.render.ffmpeg.codec = 'MPEG4'
        scn.render.ffmpeg.video_bitrate = 6000
        scn.render.ffmpeg.maxrate = 9000
        scn.render.ffmpeg.packetsize = 4096
        scn.render.ffmpeg.audio_bitrate = 384
        bpy.ops.render.render( animation=True, scene=scn.name )
        os.chown( scn.render.filepath, int( u_ugid ), int( u_gguid ) )
        os.chmod( scn.render.filepath, 0o777 )

        # make screen from video roller
    #        bpy.context.scene.frame_start = 100
    #        bpy.context.scene.frame_end = 101
    #        bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
    #        scn.render.filepath = '{}.jpg'.format(str(task_set['result_dir']) + '/' + str('roller_video'))
    #        bpy.ops.render.render(write_still=True)
    #        try:
    #            os.chown(scn.render.filepath, int(u_ugid), int(u_gguid))
    #            os.chmod(scn.render.filepath, 0o777)
    #        except Exception as e:
    #            logging.info('Render TASK  moview_full {}'.format(str(e)))
    except Exception as e:
        logging.info( '{}'.format( str( e ) ) )
    return '1'


@DecoWithArgsMysqlUpd( 'aaxbut', '@', 'gmail.com' )
def rend_task(task):
    try:
        bpy.ops.wm.open_mainfile( filepath=task['project_name'] )
        context_frame_start = bpy.context.scene.frame_start
        context_frame_end = bpy.context.scene.frame_end
    except Exception as e:
        logging.info(
            '{} Render TASK{} ########## {} ##########'.format( datetime.now().strftime( '%D:: %H:%M:%S' ), str( e ),
                                                                'KY KY KY BACKGROUND ' ) )
        pass

    if int( task['render_type'] ) is 2:  # render picture
        start_time = time.time()
        logging.info(
            '{} Render TASK{} render picture  {} ##########'.format( datetime.now().strftime( '%D:: %H:%M:%S' ), '',
                                                                     'KY KY KY BACKGROUND ' ) )
        try:
            bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'
            bpy.context.scene.render.filepath = '{}.mp40000.jpg'.format(
                str( task['result_dir'] ) + '/' + str( 'roller_video' ) )
            bpy.ops.render.render( write_still=True )
            os.chown( bpy.context.scene.render.filepath, int( u_ugid ), int( u_gguid ) )
            os.chmod( bpy.context.scene.render.filepath, 0o777 )
            end_time = time.time() - start_time
            logging.info( '{} TASK description {} moview_picture  {} ### TIME :{}'.format(
                datetime.now().strftime( '%D:: %HH:%MM:%SS' ), task['result_dir'], '', end_time ) )
        except Empty:
            pass

    if int( task['render_type'] ) is 4:
        start_time = time.time()
        try:
            start_time = time.time()
            logging.info(
                '{} Render TASK{} ##   priview   ### {} ####'.format( datetime.now().strftime( '%D:: %H:%M:%S' ),
                                                                      task['render_type'], ' ' ) )
            l_1 = return_list_of_parts( 5, 5 )
            logging.info( '{} TASK des'.format( l_1 ) )
            procs = [mp.Process( target=rend_preview, args=(x,), kwargs=task ) for x in l_1]
            for p in procs:
                run_tasks.append( p )
                # logging.info('{} TASK des !!!! in proc start'.format(run_tasks))
                p.daemon = True
                p.start()
            for p in procs:
                p.join()


        except Exception as e:
            logging.info(
                '{} Render TASK  moview_priview{} ###### {} ######'.format( datetime.now().strftime( '%D:: %H:%M:%S' ),
                                                                            str( e ), start_time ) )

        file_txt_for_concat = str( task['result_dir'] ) + '/' + 'tst.txt'
        with open( file_txt_for_concat, 'w' ) as f:
            for x in l_1:
                f.write( 'file roller_video_demo_{}.mp4\n'.format( x[0] ) )
        out_file = str( task['result_dir'] ) + '/' + 'roller_video_demo.mp4'
        command = [
            FFMPEG_BIN,
            '-y',
            '-f', 'concat',
            '-safe', str( 0 ),
            '-i', file_txt_for_concat,
            '-c', 'copy',
            out_file,
        ]
        pipe = sp.Popen( command )

        bpy.context.scene.frame_start = 100
        bpy.context.scene.frame_end = 101
        bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'

        bpy.context.scene.render.filepath = '{}.jpg'.format( str( task['result_dir'] ) + '/' + str( 'roller_video' ) )
        bpy.ops.render.render( write_still=True )
        os.chown( bpy.context.scene.render.filepath, int( u_ugid ), int( u_gguid ) )
        os.chmod( bpy.context.scene.render.filepath, 0o777 )
        try:
            os.chown( out_file, int( u_ugid ), int( u_gguid ) )
            os.chmod( out_file, 0o777 )
        except Exception as e:
            logging.info( 'Render TASK  moview_priview {}'.format( str( e ) ) )

        end_time = time.time() - start_time

        # oi = run_tasks.pop()
        logging.info( '{} TASK description  @@! {} render priview {} ### TIME :{}'.format(
            datetime.now().strftime( '%D:: %H:%M:%S' ), task['result_dir'], '', end_time ) )

    if int( task['render_type'] ) is 1:
        start_time = time.time()
        try:
            start_time = time.time()
            logging.info(
                '{} Render TASK{} ##   fullmoview   ### {} ####'.format( datetime.now().strftime( '%D:: %H:%M:%S' ),
                                                                         task['render_type'], ' ' ) )
            l_1 = return_list_of_parts( 50, 5 )  # context_frame_end
            logging.info( '{} TASK des'.format( l_1 ) )
            procs = [mp.Process( target=rend_full_movie, args=(x,), kwargs=task ) for x in l_1]
            for p in procs:
                run_tasks.append( p )
                # logging.info('{} TASK des !!!! in proc start'.format(run_tasks))
                p.daemon = True
                p.start()
            for p in procs:
                p.join()


        except Exception as e:
            logging.info(
                '{} Render TASK full {} ###### {} ######'.format( datetime.now().strftime( '%D:: %H:%M:%S' ), str( e ),
                                                                  start_time ) )

        file_txt_for_concat = str( task['result_dir'] ) + '/' + 'tst.txt'
        with open( file_txt_for_concat, 'w' ) as f:
            for x in l_1:
                f.write( 'file roller_video_{}.mp4\n'.format( x[0] ) )
        out_file = str( task['result_dir'] ) + '/' + 'roller_video.mp4'
        command = [
            FFMPEG_BIN,
            '-y',
            '-f', 'concat',
            '-safe', str( 0 ),
            '-i', file_txt_for_concat,
            '-c', 'copy',
            out_file,
        ]
        pipe = sp.Popen( command )

        bpy.context.scene.frame_start = 100
        bpy.context.scene.frame_end = 101
        bpy.data.scenes[bpy.context.scene.name].render.image_settings.file_format = 'JPEG'

        bpy.context.scene.render.filepath = '{}.jpg'.format( str( task['result_dir'] ) + '/' + str( 'roller_video' ) )
        bpy.ops.render.render( write_still=True )
        os.chown( bpy.context.scene.render.filepath, int( u_ugid ), int( u_gguid ) )
        os.chmod( bpy.context.scene.render.filepath, 0o777 )
        try:
            os.chown( out_file, int( u_ugid ), int( u_gguid ) )
            os.chmod( out_file, 0o777 )
        except Exception as e:
            logging.info( 'TASK full {} ###### {} ######'.format( 'IIIIII', str( e ) ) )

        end_time = time.time() - start_time

        # oi = run_tasks.pop()
        logging.info( '{} TASK description  @@! {} render full moview  {} ### TIME :{}'.format(
            datetime.now().strftime( '%D:: %H:%M:%S' ), task['result_dir'], '', end_time ) )


@asyncio.coroutine
def check_queue():
    """ check queue mby need another variant to make waiter """
    while True:
        logging.info( 'Awaiting task ' )
        yield from asyncio.sleep( 5 )
        loop.create_task( (start_background_tasks()) )


def bframes_count(**kwargs) -> int:
    """
        look frames count in dict frames_count if None open
        file and take it from blend file

    :rtype: int
    """
    path_project = kwargs['project_name']
    project_name = path_project.split( '/' )[-1].strip( '.' )
    if project_name in frames_count:
        return frames_count[project_name]['count']
    else:
        bpy.ops.wm.open_mainfile( filepath=path_project )
        count_frames = bpy.context.scene.frame_end
        frames_count[project_name] = {'project_name': project_name, 'count': count_frames}
        return count_frames


def screens_maker(task):
    """
    screens_maker make screens for full and preview movie

    :param task - kwargs of task  
    :return: int 
    """
    rend_type = int( task['render_type'] )
    rend_project = task['project_name']
    rend_result_dir = task['result_dir']
    file_name = p_rend_type[rend_type]['file_screen'].split( '/' )[1]
    logging.info( 'IN SCREEN Maker {}'.format( task ) )
    try:
        bpy.ops.wm.open_mainfile( filepath=rend_project )
        scn = bpy.context.scene
        scn.frame_start = 100
        scn.frame_end = 101
        bpy.data.scenes[scn.name].render.image_settings.file_format = 'JPEG'
        scn.render.filepath = '{}'.format( str( rend_result_dir ) + '/' + str( file_name ) )
        bpy.ops.render.render( write_still=True )
        try:
            os.chown( scn.render.filepath, int( u_ugid ), int( u_gguid ) )
            os.chmod( scn.render.filepath, 0o777 )
        except Exception as e:
            logging.info( 'err SCREEN MAKER rights{}'.format( str( e ) ) )
    except Exception as e:
        logging.info( 'ERR IN SCREEN Maker {}'.format( str( e ) ) )

    return 1


def tester1(task):
    frame_set, task_set = task
    frame_start, frame_end = frame_set
    asyncio.sleep( 0.001 )
    logging.info( 'BEFORE tester run #: {}, #{},{}:'.format( task, frame_start, frame_end ) )

    try:
        bpy.ops.wm.open_mainfile( filepath=task_set['project_name'] )
    except Exception as e:
        logging.info( '{}'.format( str( e ) ) )
    try:
        scn = bpy.context.scene
        scn.frame_start = frame_start
        scn.frame_end = frame_end
        scn.render.filepath = '{}_{}.mp4'.format(
            str( task_set['result_dir'] ) + '/' + str( 'roller_video' ),
            str( frame_start ),
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
        bpy.ops.render.render( animation=True, scene=bpy.context.scene.name )
        os.chown( scn.render.filepath, int( u_ugid ), int( u_gguid ) )
        os.chmod( scn.render.filepath, 0o777 )
    except Exception as e:
        logging.info( 'EXCEPTION : {} #'.format( str( e ) ) )
    logging.info( 'After tester run' )


@asyncio.coroutine
def starter_works(task):
    time_start = time.time()
    rend_type = int( task['render_type'] )
    user_roller_id = task['user_roller_id']
    p = []

    logging.info( '{} ######################'.format( task['render_type'] ) )
    # before run need update base for task started
    data_update(
        render_type=rend_type,
        user_roller_id=user_roller_id,
        cond=False
    )

    with ProcessPoolExecutor( max_workers=5 ) as executor:

        #  before run need split dict with task settings and list with parts
        #  function look in dict have part if full movie, if movie pri run 500 // 5 parts
        try:
            if rend_type == 1:
                # f_count = bframes_count(project_name=task['project_name'])
                parts = return_list_of_parts( 30, 3 )
                p = parts
                parts_tasks = [(x, task) for x in parts]
                executor.map( rend_full_movie, parts_tasks )
                executor.map( screens_maker, parts_tasks[0] )
                logging.info( '{} ####EXECUTOR RUN ###'.format( '1' ) )

            elif rend_type == 4:
                f_count = 15
                parts = return_list_of_parts( f_count, 3 )
                p = parts
                parts_tasks = [(x, task) for x in parts]
                executor.map( rend_preview, parts_tasks )
                executor.map( screens_maker, parts_tasks[0] )
                logging.info( '{} ####EXECUTOR RUN PRIVIEW ###'.format( '1' ) )

            elif rend_type == 2:
                parts = return_list_of_parts( 5, 1 )
                parts_tasks = [(x, task) for x in parts]
                executor.map( rend_picture, parts_tasks )
        except Exception as e:
            logging.info( 'POOL err{}'.format( str( e ) ) )

    logging.info( '{} #####################{} $#'.format( task['render_type'], '' ) )
    # here nee add function for split of projects and set rights
    if rend_type == 1 or rend_type == 4:
        logging.info( '{} #########{}#########{} $# {} '.format( rend_type, 'BEFORE SPLIT', p, task ) )
        yield from great_split( task, p )

    try:
        data_update(
            render_type=rend_type,
            user_roller_id=user_roller_id,
            cond=True )
    except mysql.Error as e:
        logging.info( 'POOL err{}'.format( str( e ) ) )

    # function to update in base complited task
    time_end = time.time() - time_start
    logging.info( '# {} complete TIME: {}'.format( task['project_name'], time_end ) )


@asyncio.coroutine
def start_background_tasks():
    #  len of queue in lists
    len_queue = len( queue_of_run_tasks )
    sub_tasks = []
    logging.info( '{} ##  Object len: {}'.format( queue_of_run_tasks, len( queue_of_run_tasks ) ) )
    #  2 tasks in 1 click !!! not more or slow down rend
    if len_queue != 0 and len_queue > 2:
        for x in range( 2 ):
            logging.info( '{}'.format( str( x ) ) )
            sub_tasks.append( starter_works( queue_of_run_tasks.pop() ) )
        asyncio.gather( *sub_tasks )
    # logging.info('{} len >2 {}'.format(asyncio.gather.__name__, len_queue))
    else:
        for x in range( len_queue ):
            logging.info( '{}'.format( str( x ) ) )
            sub_tasks.append( starter_works( queue_of_run_tasks.pop() ) )
        asyncio.gather( *sub_tasks )


# logging.info('GATHER  !!!! else {}'.format(len_queue))


def transmit(request):
    data = yield from request.text()
    req_json = json.loads( data )
    # logging.info('{} :::'.format(datetime.now().strftime('%c')))
    if request.content_type == 'application/json':
        logging.info( 'Transmit description time : {} ## Data : {} ###'.format(
            datetime.now().strftime( '%c' ),
            req_json.__class__.__name__
        ) )
    # some short work mby here
    queue_of_run_tasks.append( req_json )
    return web.json_response( req_json )


@asyncio.coroutine
def corobas_barabas(*args, **kwargs):
    logging.info( '{} ##  Object name: {} ##########'.format( datetime.now().strftime( '%D:: %HH:%MM:%SS' ),
                                                              corobas_barabas.__name__ ) )
    queue_of_run_tasks.insert( 0, args )
    # run_tasks.insert(0, args)
    yield from asyncio.sleep( 1 )


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

    #    hello_corobas = loop.create_task( corobas_1() )
    loop_check_queue = loop.create_task( check_queue() )

    app.router.add_post( '/tr', transmit )
    server = yield from loop.create_server( app.make_handler(), '0.0.0.0', 7812 )
    return server


if __name__ == '__main__':
    # executor = concurrent.futures.ThreadPoolExecutor( max_workers=2 )
    frames_count = {}
    queue_of_run_tasks = []
    run_tasks = []

    policy = asyncio.get_event_loop_policy()
    policy.set_event_loop( policy.new_event_loop() )
    loop = asyncio.get_event_loop()
    srv = loop.run_until_complete( main_loop( loop ) )
    # executor = ProcessPoolExecutor(2)
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