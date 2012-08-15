#!/usr/bin/python

#Scientific Cloud Computing Architecture  
#Xiyang Dai    
#N19871532
#2012.8.15
#Instructor: Prof. J.C.Franchitti

#End Node Script 

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
import time
import boto

SQS_Id = 'TaskFinished'
Pull_period = 10

AWS_ACCESS_KEY_ID='AWS_ACCESS_KEY_ID'
AWS_SECRET_ACCESS_KEY='AWS_SECRET_ACCESS_KEY'

def main():
    conn = SQSConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    my_queue = conn.get_queue(SQS_Id)
    
    while True:
        print my_queue.count()
        if my_queue.count()>0 :
            task = my_queue.read()
            process(task.get_body())
            my_queue.delete_message(task)
        else:
            time.sleep(Pull_period)
        
        
def process(info):
    print info
    data = info.split('|')
    task_name = data[1]
    node_id = data[2]

    conn = boto.connect_dynamodb(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
    table = conn.get_table('Tasks')
    item = table.get_item(hash_key=task_name)
    if item:
        start_time = time.strptime(item['Start Time'], "%Y/%m/%d-%H:%M:%S")
        end_time = time.localtime()
        cost = time.mktime(end_time) - time.mktime(start_time)
        node_id = int(item['Node No.']) - 1
        item['Node No.'] = str(node_id)
        if node_id == 0:
            item['Status'] = 'Completed'
            item['End Time'] = time.strftime("%Y/%m/%d-%H:%M:%S", end_time)
            item['Cost'] = str(cost)
        item.put()
    return 'OK'   


if __name__=="__main__":
    main()