#Scientific Cloud Computing Architecture  
#Xiyang Dai    
#N19871532
#2012.8.15
#Instructor: Prof. J.C.Franchitti

#Compute Node Script 

from boto.s3.connection import S3Connection
from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
import time
import string

AWS_ACCESS_KEY_ID='AKIAIXQ2HHDWHV6BCANA'
AWS_SECRET_ACCESS_KEY='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'

def start(task, node_id):
    
    info = task.split('|')
    total_nodes = info[3]
    task_name = info[1]
    task_data = info[2]
      
    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        
    bucket = conn.get_bucket(task_data)
    keys = bucket.get_all_keys()
    
    for i in range(len(keys)):
        if i%int(total_nodes) == node_id -1:     
            print keys[i].key
            ComputePi(100)
    
    finshi_task_sns(task_name,node_id)
    return 'Finished'
        
def do_computation():
    time.sleep(10)

def ComputePi(numdigits):
    pi = ""
    a = [2] * (10*numdigits / 3)
    nines = 0
    predigit = 0
    for j in xrange(0, numdigits):
        q = 0
        p = 2 * len(a) - 1
        for i in xrange(len(a)-1, -1, -1):
            x = 10*a[i] + q*(i+1)
            q, a[i] = divmod(x, p)
            p -= 2


        a[0] = q % 10
        q /= 10
        if q == 9:
            nines += 1
        elif q == 10:
            pi += chr(predigit + 1 + ord("0"))
            pi += "0" * nines
            predigit = 0
            nines = 0
        else:
            pi += chr(predigit + ord("0"))
            predigit = q;
            pi += "9" * nines
            nines = 0

    pi += chr(predigit + ord('0'))
    return pi
   
def finshi_task_sns(taskname,node_id):
    SQS_Id='TaskFinished'
    time = get_time_now()
    message = time + '|' + taskname + '|' + str(node_id)
    conn = SQSConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    my_queue = conn.get_queue(SQS_Id)
    m = Message()
    m.set_body(message) 
    my_queue.write(m)

def get_time_now():
    return time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime())