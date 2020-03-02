import  urllib.parse, boto3
import json as JSON

json='application/json'
json='binary/octet-stream'
csv='text/plain'
archive_bucket='cbatth-pragra-lambda-trigger-archive'
pickle_bucket='cbatth-pragra-lambda-trigger-pickle'
error_bucket='cbatth-pragra-lambda-trigger-error'

s3 = boto3.client('s3')
s3res = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')

def moveToError(bucket, key):
    try:
        print('Fn: Move problematic to Error with err suffix')
        copy_source={'Bucket':bucket, 'Key':key}
        s3res.meta.client.copy(copy_source, error_bucket , key + '.err')
        s3res.Object(bucket, key).delete()
    except Exception as e:
        print('Error in moveToError Exception')
        raise(e)
        
def parseJson(bucket, key):
    print('In Parsing JSON module')
    try:
        json_object = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
        jsonDict = JSON.loads(json_object)
    except Exception as e:
        print('Issue parsing Json file')
        moveToError(bucket, key)
        raise(e)

    # print('Create CSV to pickle object')
    # okey='pickle_json_'+key  ##Should suffix with date stamp
    # # https://inneka.com/programming/aws/writing-a-pickle-file-to-an-s3-bucket-in-aws-2/
    # try:
    #     pass
    # except Exception as e:
    #     moveToError(bucket, key)
    #     raise(e)

    print('Adding step to Write to DynamoDB.. Need fname key')
    # https://inneka.com/programming/aws/writing-a-pickle-file-to-an-s3-bucket-in-aws-2/
    try:
        dbTable = dynamodb.Table('pragra-json')
        dbTable.put_item(Item=jsonDict)        
    except Exception as e:
        print('Issue putting in DynamoDB')
        moveToError(bucket, key)
        raise(e)

    print('Move input file to Archive')
    # https://medium.com/plusteam/move-and-rename-objects-within-an-s3-bucket-using-boto-3-58b164790b78
    try:
        copy_source={'Bucket':bucket, 'Key':key}
        s3res.meta.client.copy(copy_source,archive_bucket, key + '.archive')
        # Delete Source file
        s3res.Object(bucket, key).delete()
    except Exception as e:
        moveToError(bucket, key)
        raise(e)


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print('We got ' + str(bucket)+ " :  key " + str(key))
    response = s3.get_object(Bucket=bucket, Key=key)
    contentType=response['ContentType']
    print("CONTENT TYPE: " + contentType)
    try:
        print('Trying to find type of file')
        if ( contentType == csv):
            print('Call CSV Module')
        elif ( contentType == json):
            print('Call Json Parser')
            parseJson(bucket,key)
        else:
            print('what are you sending me??' + str(contentType))
    except Exception as e:
        print('Issue in the master module')
        moveToError(bucket,key)
        raise(e)
