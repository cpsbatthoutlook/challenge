import json, urllib.parse, boto3, pickle, bz2

json='application/json'
csv='text/plain'

def moveToError(bucket, key)
    try:
        print('Move to archive with done suffix')    
    except Exception as e:
        raise('Failed moving file to S3 archive ' + e)

def parseCSV(bucket, key):
    try:
        import pandas as pd
        print("Parse CSV File")
        df = pd.read_csv(key)
    except Exception as e:
        moveToError(bucket,key)
        raise('Exception parsing file ' + e)
    try:
        print('copy Pickle to s3-pickle-Fname.csv')
    except Exception as e:
        moveToError(bucket,key)
        raise('Exception copying file to S3 picket ' + e)
    try:
        print('Move to archive with done suffix')    
    except Exception as e:
        print("move to error S3 bucket")
        raise('Exception moving file to S3 archive ' + e)


parseCSV('bucket', 'workarea\test.csv')

