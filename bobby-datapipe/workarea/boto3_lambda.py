import json, urllib.parse, boto3, pickle, bz2, pandas as pd

json='application/json'
csv='text/plain'
archive_bucket='cbatth-pragra-lambda-trigger-input'
pickle_bucket='cbatth-pragra-lambda-trigger-input'
error_bucket='cbatth-pragra-lambda-trigger-input'


def moveToError(bucket, key):
    # print('Hi')
    try:
        print('Move to archive with done suffix')
        s3_resource.Object(error_bucket, key).copy_from(CopySource=key)

    except Exception as e:
        print('Error')
        raise(e)
        
def parseCSV(bucket, key):
    okey='pickle_csv_'+key  ##Should suffix with date stamp
    print('In Parsing CSV module')
    try:
        print("Parse CSV File")
        df = pd.read_csv(key)
    except Exception as e:
        moveToError(bucket, key)
        raise('Exception ' + e)

    print('Create CSV to pickle object')
    # https://inneka.com/programming/aws/writing-a-pickle-file-to-an-s3-bucket-in-aws-2/
    try:
        df.to_pickle(okey)
        s3_resource.Object(bucket,'pickle_'+okey).put(Body=open(okey, 'rb'))
    except Exception as e:
        moveToError(bucket, key)
        raise('Exception ' + e)

    print('Move input file to Archive')
    # https://medium.com/plusteam/move-and-rename-objects-within-an-s3-bucket-using-boto-3-58b164790b78
    try:
        #  Copy object A as object B
        s3_resource.Object(archive_bucket, key+'.archive').copy_from(CopySource=key)
        # Delete Source file
        s3_resource.Object(bucket, key).delete()
    except Exception as e:
        moveToError(bucket, key)
        raise('Exception ' + e)


# parseCSV('bucket', 'test.csv')
# parseCSV(bucket, key)


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    # print(str(event))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print('Bucket: ' + bucket + 'Key: ' + key)
        print("CONTENT TYPE: " + response['ContentType'])
        contentType=response['ContentType']
        if ( contentType == csv):
            parseCSV(bucket, key)
        elif ( x == 'y'):
            print('Call Json Parser')
            # parseJson(bucket,key)
        else:
            print('what are you sending me??' + str(contentType))
    except Exception as e:
        moveToError(error_bucket,key)
        print('Issue in the master module')
        raise(e)

