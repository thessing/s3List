#
# CVS Lambda - lambda_function.py
#
# Description:- When Executed will provide get a list of files stored in S3 and provide that list
#
# Author: Tim Hessing
# Created: 12-20-2022
# Updated: 12-21-2022
#
import json
import boto3
import boto3.session
from datetime import datetime 
import time

#
# Create DynamoDB & S3 Client
#
dynamo_cli = boto3.client('dynamodb')
session    = boto3.session.Session(region_name='us-east-2')
s3_cli     = boto3.client('s3')

def lambda_handler(event, context):
    #
    # Print event to log for debugging purposes
    #print("Received event: ", json.dumps(event, indent=2))
    #print("Recieved context: ", context)
    #
    # Load event
    jsevt = json.loads(json.dumps(event))
    #
    # Get File Key from message body
    try:
        userid = json.loads(jsevt["body"])["uid"]
    except:
        Message = '<h1><b style="color:red">Improper Call</b></h1>'
        response = {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'text/html',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST'
            },
            'body': Message
        }
        return response
    #
    # Good File Key, now get bucket from DynamoDB
    # Make sure Main Table Exists
    try:
        descrTable = dynamo_cli.describe_table(TableName='BDDMainTable')
        #print("Table Exists ", descrTable)
    except:
        print("ERROR: No BDDMainTable")
        Message = '<h1><b style="color:red">Missing Configuration</b></h1>'
        response = {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'text/html',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST'
            },
            'body': Message
        }
        return response
    #
    # Now see if anything in it or too many things in it (if not return)
    try:
        scanResponse = dynamo_cli.scan(TableName='BDDMainTable', Select='ALL_ATTRIBUTES')
        #print("Scan succeeded ", scanResponse)
    except:
        print("ERROR: BDDMainTable Scan Failed!!!")
        Message = '<h1><b style="color:red">Not properly configured - missing item.</b></h1>'
        response = {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'text/html',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST'
            },
            'body': Message
        }
        return response
    #
    # If count zero then return bad
    #print("Count: ", scanResponse['Count'] )
    if scanResponse['Count'] == 0:
        print("ERROR: No items!!! Count=", scanResponse['Count'])
        Message = '<h1><b style="color:red">Not properly configured - empty item.</b></h1>'
        response = {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'text/html',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST'
            },
            'body': Message
        }
        return response
    #
    # Verify 1 item only
    if scanResponse['Count'] != 1:
        print("ERROR: Too many items!!! Count=", scanResponse['Count'])
        Message = '<h1><b style="color:red">Application not properly configured - too many items.</b></h1>'
        response = {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'text/html',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST'
            },
            'body': Message
        }
        return response
    #
    # Extract Defaults
    bucket = scanResponse['Items'][0]['data-bucket']['S']
    #
    prefix = 'downloads/{}'.format(userid) 
    #
    print("Bucket: ", bucket)
    print("UserID: ", userid)
    print("Prefix: ", prefix)
    #
    # Get list of objects for this User ID
    resp = s3_cli.list_objects(Bucket=bucket,Prefix=prefix)
    #
    # Loop over Object
    filelist = []
    try:
        objects = resp['Contents']
        for iobj in objects:
            fkey = iobj['Key']
            osze = iobj['Size']
            bsze = int(iobj['Size'] / 1024)
            #'2022-12-19 14:42:44', 'End': datetime.datetime(2022, 12, 20, 16, 45, 39, tzinfo=tzlocal())}]
            etme = iobj['LastModified'].strftime('%Y-%m-%d %H:%M:%S') 
            fsplit = fkey.split('/')
            #
            # If fkey goes beyond downloas/uid/epoch then check for size and keep
            if len(fsplit) > 3:
                #
                # Get S3 object info
                if osze > 0:
                    epoch = int(fsplit[2])
                    ltime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch / 1000))
                    fname = fsplit[3]
                    item = {"FileName": fname, "FileKey": fkey, "KBytes": bsze, "Bytes": osze, "Epoch": epoch, "Start": ltime, "End": etme}
                    filelist.append(item)
    except:
        print('No Objects Found in Bucket')
    #
    # Create Response
    print("  Files: ", filelist)
    #
    response = {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/html',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST'
            },
        'body': json.dumps(filelist)
    }
    return response