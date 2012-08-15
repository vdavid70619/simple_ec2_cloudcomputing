#!/usr/bin/python

#Scientific Cloud Computing Architecture  
#Xiyang Dai    
#N19871532
#2012.8.15
#Instructor: Prof. J.C.Franchitti

#Compute Node Script 

from boto.sqs.connection import SQSConnection
import compute
import time

Node_Id = 2
SQS_Id = 'NewTaskForNode' + str(Node_Id)
Pull_period = 10

AWS_ACCESS_KEY_ID='AKIAIXQ2HHDWHV6BCANA'
AWS_SECRET_ACCESS_KEY='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'

conn = SQSConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
my_queue = conn.get_queue(SQS_Id)

while True:
    print my_queue.count()
    if my_queue.count()>0 :
        task = my_queue.read()
        my_queue.delete_message(task)
        compute.start(task.get_body(), Node_Id)
    else:
        time.sleep(Pull_period)