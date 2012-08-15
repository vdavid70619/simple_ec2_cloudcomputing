#Scientific Cloud Computing Architecture  
#Xiyang Dai    
#N19871532
#2012.8.15
#Instructor: Prof. J.C.Franchitti

#Front Node Script 


from boto.ec2.connection import EC2Connection
from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
import time
import boto

AWS_ACCESS_KEY_ID='AKIAIXQ2HHDWHV6BCANA'
AWS_SECRET_ACCESS_KEY='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'

def run_newtask(info):
    task = info.split('|')
    task_name = task[0]
    task_ino = task[1]
    task_itype = task[2]
    task_data = task[3]
    
    add_newtask(task_name,task_data,task_ino,task_itype)
    start_instances(task_ino)
    send_task_sns(task_name,task_data,task_ino,task_itype)
    return 'OK'

def start_instances(num):
    Instnace_Type=['t1.micro','m1.small','c1.medium','m1.large','cc1.4xlarge']

    conn = EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    reservations = conn.get_all_instances()
    if num > 4 : 
        num = 4
    for i in range(num):
        reservations[0].instances[i+1].start()

def send_task_sns(taskname,taskdata,taskino,taskitype):
    SQS_Id=['NewTaskForNode1','NewTaskForNode2','NewTaskForNode3','NewTaskForNode4']
    time = get_time_now()
    message = time + '|' + taskname + '|' + taskdata + '|' + taskino + '|' + taskitype
    conn = SQSConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    for i in range(int(taskino)):
        my_queue = conn.get_queue(SQS_Id[i])
        m = Message()
        m.set_body(message) 
        my_queue.write(m)

def end_all_instances():
    num = 4
    conn = EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    reservations = conn.get_all_instances()
    if num > 4 : 
        num = 4
    for i in range(num):
        reservations[0].instances[i+1].stop()

def add_newtask(taskname,taskdata,taskino,taskitype):
    conn = boto.connect_dynamodb(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
    table = conn.get_table('Tasks')
    task_data = {
        'Data': taskdata,
        'Start Time': get_time_now(),
        'End Time': '--',
        'Cost': '--',
        'Status': 'Running',
        'Node No.' : taskino,
        'Node Type' : taskitype
    }
    item = table.new_item(hash_key=taskname, attrs=task_data)
    item.put()

def apply_task_change(info):
    datas = info.split(';')
    for i in range(len(datas)):
        datas[i] = datas[i].split('_')

    print datas
    conn = boto.connect_dynamodb(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
    table = conn.get_table('Tasks')
    
    for data in datas:
        item = table.get_item(hash_key=data[0])
        if item['Status'] != 'Completed':
            if data[1] == 'stop':
                item['Status'] = 'Stop'
                item.put()
            elif data[1] == 'run':
                item['Status'] = 'Run'
                item.put()
            elif data[1] == 'del':
                item.delete()
        else:
            if data[1] == 'del':
                item.delete()
                
    return get_all_tasks()    
    
def get_all_tasks():
    conn = boto.connect_dynamodb(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
    table = conn.get_table('Tasks')
    results = table.scan()
    all =''
    for item in results:
        all += item['Task Name'] + '|'\
             + item['Data'] + '|'\
             + item['Start Time'] + '|'\
             + item['End Time'] + '|'\
             + item['Status'] + '|'\
             + item['Cost']\
             +';'
    all = all[:-1]
    return all

def check_name(info):
    user = info.split('|')
    print user
    conn = boto.connect_dynamodb(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
    table = conn.get_table('Users')
    item = table.get_item(user[0])
    print item
    if item and item['Password'] != user[1]:
        result = 'Error'
    else:
        result = item['Key'] + '|' + item['SKey']
    print result
    return result

def get_time_now():
    return time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime())
