#Scientific Cloud Computing Architecture  
#Xiyang Dai    
#N19871532
#2012.8.15
#Instructor: Prof. J.C.Franchitti

#GAE script 

import bottle
from bottle import route, run, get, post, request, template, static_file, response, error
import urllib
import S3
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import urlfetch


def main():
    #debug(True)
    run_wsgi_app(bottle.default_app())

@route('/')
def homepage():
    return template('./templates/index.tpl')

@route('/files/<filename>')
def server_static(filename):
    return static_file(filename, root='./files/')

@error(404)
def error404(error):
    return template('./templates/home.tpl', Data='Nothing here, sorry')

@error(405)
def error405(error):
    return template('./templates/home.tpl', Data='Nothing here, sorry')

@error(500)
def error500(error):
    return template('./templates/home.tpl', Data='Sorry! Something is wrong')


@route('/login', method='POST')
def login_submit():
    name     = request.forms.get('name')
    password = request.forms.get('password')
    if check_login(name, password):
        response.set_cookie("account", name, secret='secret-key')
        return template('./templates/user.tpl', Context='Welcome,'+name+'!')
    else:
        return template('./templates/home.tpl', Data='Login Fail!')

def check_login(name, password):
    info = name + '|' +password
    reply = ping('checkname',info)
    if reply == 'Error':
        return False
    else:
        id = reply.split('|')
        global AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
        AWS_ACCESS_KEY_ID = id[0]
        AWS_SECRET_ACCESS_KEY = id[1]
        return True

@route('/newtask')
def newtask():
    username = request.get_cookie("account", secret='secret-key')
    if username:
        data = get_s3_data()
        return template('./templates/newtask.tpl', datas=data['name'], Context='You can create a new task now!')
    else:
        return template('./templates/home.tpl', Data='Login Fail!')    

@route('/newtask', method='POST')
def new_submit():
    username = request.get_cookie("account", secret='secret-key')
    if username:
        tname = request.forms.get('tname')
        tno = request.forms.get('tno')
        ttype = request.forms.get('ttype')
        tdata = request.forms.get('tdata')
        info = tname + '|' + tno + '|' + ttype +'|' + tdata
        reply = ping('newtask',info)
        return template('./templates/user.tpl', Context='Creat Status: '+reply)
    else:
        return template('./templates/home.tpl', Data='Login Fail!')  

@route('/existtask')
def existingtask():
    username = request.get_cookie("account", secret='secret-key')
    if username:
        message ='getall'
        reply = ping(message,'')
        data = reply.split(';')
        for i in range(len(data)):
            data[i] = data[i].split('|')
        return template('./templates/task.tpl', Context='You have following tasks:', Rows = data)
    else:
        return template('./templates/home.tpl', Data='Login Fail!')  

@route('/taskchange',method = 'POST')
def changetask():
    username = request.get_cookie("account", secret='secret-key')
    if username:
        message ='changetask'
        taction =''
        reply = ping('getall','')
        data = reply.split(';')
        for i in range(len(data)):
            if request.forms.get('taction' + str(i+1)) != 'Null':
                taction += request.forms.get('taction' + str(i+1)) 
                taction += ';'
        taction = taction[:-1]
        reply = ping(message,taction)
        data = reply.split(';')
        for i in range(len(data)):
            data[i] = data[i].split('|')
        return template('./templates/task.tpl', Context='Change apply successfully!', Rows = data)
    else:
        return template('./templates/home.tpl', Data='Login Fail!') 

@route('/storage')
def result():
    username = request.get_cookie("account", secret='secret-key')
    if username:
        data = get_s3_data()
        return template('./templates/storage.tpl', Context='You have following data in cloud!', datas_time = data['time'], datas_name = data['name'])
    else:
        return template('./templates/home.tpl', Data='Login Fail!') 

@route('/upload', method='POST')
def do_upload():
    if request.forms.get('tdata') == 'New_directory':
        dname = request.forms.get('dname')
        isnew = True
    else:
        dname = request.forms.get('tdata')
        isnew = False          
    data = request.files.data
    if dname and data.file:
        upload_s3_data(data,dname,isnew)
        data = get_s3_data()
        return template('./templates/storage.tpl', Context='Upload Successfully!', datas_time = data['time'], datas_name = data['name'])
    return template('./templates/user.tpl', Context='Something is wrong!')

@route('/logout')
def logout():
    return static_file("index.html", root='./')

def upload_s3_data(data, bucket, isnew):
    conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    print bucket
    if isnew:
        conn.create_bucket(bucket)
    print conn.put(bucket, data.filename, data.file.read())

def get_s3_data():
    conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    reply = conn.list_all_my_buckets().entries
    datas_name = {}
    datas_time = {}
    for i in range(len(reply)):
        datas_name[i] = reply[i].name
        datas_time[i] = reply[i].creation_date
    return {'name':datas_name, 'time':datas_time}

def ping(message,info):    
    url = 'http://ec2_ip:1234'
    parameters = {'message':message, 'info':info}
    info = submitInformation(url,parameters);
    print(info);
    return info

def submitInformation(url,parameters) :
    encodedParams =  urllib.urlencode(parameters);
    net = urlfetch.fetch(url=url,
                        payload=encodedParams,
                        method=urlfetch.POST,
                        headers={'Content-Type': 'application/x-www-form-urlencoded'})
    return net.content;

if __name__=="__main__":
    main()
