# -*- coding: utf-8 -*-
import asyncio
from datetime import datetime
import logging 
import json
from aiohttp import web



def start_background_tasks():
    print('***'*80)
    logging.info('Job description {} ########## {} ##########'.format(' $$ ',''))



@asyncio.coroutine
def transmit(request):
    
    data = yield from request.text()
    

    #mp.freeze_support()

   # pool = mp.Pool(1)

    req_json = json.loads(data)
    logging.info('{} :::'.format(datetime.now().strftime('%c')))
    

    if request.content_type == 'application/json':

#        joq_data = Job
 #       joq_data = joq_data(3, request.method, data)
        #TaskWait.__queue.put(joq_data)
        #data = TaskWait._set_in_q(1,joq_data,joq_data)


      #  q_priority_job.put(joq_data)


        logging.info('Job description {} ########## {} ##########'.format(' $$ ',req_json.__class__.__name__))
        #logging.info('user :{} ::'.format(req_json['user']))
        #l = [joq_data]
        #logging.info('!!!!!$#$####!!!!!!!! {} ******d ***********'.format(l))
        
        #return web.Response(body=json.dumps({'ok': req_json}).encode('utf-8'), content_type='application/json')
        #return web.json_response({'ok':'ok_transmit'}) 
    return web.json_response({'ok':'ok_transmit'})   


def init(loop):
    logging.basicConfig(level=logging.INFO)
   # logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
 
    #bpy.app.handlers.render_complete.append(render_complete)
     
    app = web.Application(loop=loop)
    #app = web.Application(loop=loop)
   
    app.router.add_post('/tr', transmit)
   

    #app.on_startup.append(start_bk_task)


    server = yield from loop.create_server(app.make_handler(),'0.0.0.0',7811)
    return server


if __name__ == '__main__':
    #pool = ThreadPoolExecutor(4)
    
    policy = asyncio.get_event_loop_policy()
    policy.set_event_loop(policy.new_event_loop())
    
    loop = asyncio.get_event_loop()
    loop.call_soon_threadsafe(start_background_tasks)
   
    #queue = asyncio.Queue(maxsize=12, loop=loop)


    
    #loop.set_debug(True)
   # queue = asyncio.Queue(maxsize=12)
   
    #task_wait_test = TaskWait(loop, pool, logging, queue)
    #loop.run_in_executor(task_wait_test, start_bk_task)

    #loop.add_signal_handler(signal.SIGTERM, handler_signal, loop)

    #loop.call_soon_threadsafe(print, 'test')

    #loop.run_in_executor(pool,)

    srv = loop.run_until_complete(init(loop))

    
    try:
        logging.info('{} SRV: {} '.format(datetime.now().strftime('%c'), srv.sockets[0].getsockname())) 
        loop.run_forever()
    
    except KeyboardInterrupt:
        logging.info('{} SRV: closing  {} '.format(datetime.now().strftime('%c'), srv.sockets[0].getsockname())) 
        asyncio.gather(*asyncio.Task.all_tasks()).cancel()
        loop.stop()
        loop.close()  