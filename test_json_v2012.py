import aiohttp
import asyncio
import json


async def fetch(client):
        url = "http://127.0.0.1:7812/tr"
        data = {'user_roller_id':'12907', 'render_type':4,'moview_full':False,'moview_picture':False,'moview_priview':True,'sender':'node-1','result_dir':'/var/www/cmex.ru/data/uploads/users','user': 'bob', 'message': 'We did it!','project_name':'/var/www/cmex.ru/data/uploads/rollers/Rauf/Rauf.blend','files_png':{'head1':'head13.png','mouth1':'mouth13.png'}}
        #data = {'sendaemonder':'node-1','user': 'nebob', 'file_h1': 'head1.png', 'message': 'We did it!','project_name':'Rauf','file_name':'500265.blend','file78.46.23.1878.46.23.1878.46.23.1878.46.23.18s_png':{'head1':'head1.png','mouth1':'mouth1.png'}}
        #data = {'sender':'node-1','user': 'aaxbut', 'file_h1': 'head1.png', 'message': 'We did it!','requestdata_for_renderproject_name':'Rauf','file_name':'500265.blend','files_png':{'head1':'head12.png','mouth1':'mouth12.png'}}

        #  /var/www/cmex.ru/data/uploads/users_rollers/11723

        #"result_dir":"/var/www/cmex.ru/data/uploads/users_rollers/11723/video"

        #"project_name":"/var/www/cmex.ru/data/uploads/users_rollers/11723/project/neulovimiemstiteli.blend" 
        #"result_dir":"/var/www/cmex.ru/data/uploads/users_rollers/11723/video"
        headers = {'Content-type': 'application/json'}
         #    assert session.status == 200                                                              task['moview_priview'], 
        async with await client.post(url, data=json.dumps(data), headers=headers) as post_session:

            #assert post_session.status == 405
            u = json.loads(await post_session.text())
   
            return u

async def main(loop):
        async with aiohttp.ClientSession(loop=loop) as client:
            html = await fetch(client)
            print(html)
            #with (open('tttt','a')) as filek:
            #    filek.write(html)
             #print('html ',html)

'''
file_txt_for_concat = str( task['result_dir'] ) + '/' + 'tst.txt'
with open( file_txt_for_concat, 'w' ) as f:
    for x in parts:
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
process = yield from asyncio.create_subprocess_exec(
    *command,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.STDOUT
)
stdout, _ = yield from process.communicate()
logging.info( '{}'.format( stdout ) )

os.chown( out_file, int( u_ugid ), int( u_gguid ) )
os.chmod( out_file, 0o777 )
'''
loop = asyncio.get_event_loop()
 
try:
    
    loop.run_until_complete(main(loop))
    
except KeyboardInterrupt:
    loop.close()

          