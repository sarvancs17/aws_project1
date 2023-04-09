from __future__ import print_function

import json
import boto3
import time
import urllib

print ("Loading functioin")
s3=boto3.client("s3")
tests3=boto3.resource(u's3')
dynamodb=boto3.resource('dynamodb')

def insert_data(recList):
    table=dynamodb.Table('student')     #Tablename
    for i in range(len(recList)):
        record=recList[i]
        table.put_item(
            item={
                'username' : record['rollno']   ,
                'lastname' : record['name']
            }
        )

def lamda_handler(event,context):
    source_bucket=event['Records'][0]['s3']['bucket']['name']
    key=urllib.unquote_plus(event['Records'][0]['s3']['object']['key'])
    copy_source={'Bucket':source_bucket,'key':key}
    print(event)

    print("log stream name : ", context.log_stream_name)
    print("Log group name :", context.log_group_name)
    print("Request Id:", context.memory_limit_in_mb)

    try:
        print("Using waiter to waiting for object to persist thru s3 service")
        waiter=s3.get_waiter("Object Exists")
        waiter.wait(Bucket=source_bucket,key=key)
        print("Accessing the received file and reading the same")
        bucket=tests3.Bucket(u'mylamdafunctionguvi')      #lamda function name
        obj=bucket.Object(key='student.csv')        #csv file name
        response=obj.get()
        print("Response from file object")
        print(response)
        lines=response['body'].read().split()
        print(response['Body'].read())


        recList=list()
        i=0
        while i< len(lines):
            record={}
            record['username']=lines[i]
            record['lastname']=lines[i+1]
            print(record)
            recList.append(record)
            i=i=2
        print(recList)
        insert_data(recList)

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket{}.Make sure they exist and your bucket is in the same region')
        raise e
