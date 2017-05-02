import aiohttp
import asyncio
import json


async def fetch(client):
        url = "http://127.0.0.1:7812/tr"
        data = {'render_type':41,'moview_full':False,'moview_picture':False,'moview_priview':True,'sender':'node-1','result_dir':'/var/www/cmex.ru/data/uploads/users','user': 'bob', 'message': 'We did it!','project_name':'/var/www/cmex.ru/data/uploads/rollers/Rauf/Rauf.blend','files_png':{'head1':'head13.png','mouth1':'mouth13.png'}}
        #data = {'sendaemonder':'node-1','user': 'nebob', 'file_h1': 'head1.png', 'message': 'We did it!','project_name':'Rauf','file_name':'500265.blend','file78.46.23.1878.46.23.1878.46.23.1878.46.23.18s_png':{'head1':'head1.png','mouth1':'mouth1.png'}}
        #data = {'sender':'node-1','user': 'aaxbut', 'file_h1': 'head1.png', 'message': 'We did it!','requestdata_for_renderproject_name':'Rauf','file_name':'500265.blend','files_png':{'head1':'head12.png','mouth1':'mouth12.png'}}

        #  /var/www/cmex.ru/data/uploads/users_rollers/11723

        #"result_dir":"/var/www/cmex.ru/data/uploads/users_rollers/11723/video"

        #"project_name":"/var/www/cmex.ru/data/uploads/users_rollers/11723/project/neulovimiemstiteli.blend" 
        #"result_dir":"/var/www/cmex.ru/data/uploads/users_rollers/11723/video"
        headers = {'Content-type': 'application/json'}
         #    assert session.status == 200                                                              task['moview_priview'], 
        async with await client.post(url, data = json.dumps(data) ,headers = headers) as post_session:

            #assert post_session.status == 405
            u =  json.loads(await post_session.text())
   
            return u

async def main(loop):
        async with aiohttp.ClientSession(loop=loop) as client:
            html = await fetch(client)
            print(html)
            #with (open('tttt','a')) as filek:
            #    filek.write(html)
             #print('html ',html)


loop = asyncio.get_event_loop()
 
try:
    
    loop.run_until_complete(main(loop))
    
except KeyboardInterrupt:
    loop.close()

          