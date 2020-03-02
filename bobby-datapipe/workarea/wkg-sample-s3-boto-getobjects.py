import json, urllib.parse, boto3

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
# http://jsonviewer.stack.hu/

print('Loading function')
s3 = boto3.client('s3')
s3r = boto3.resource('s3')


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    print(str(event))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print('Bucket: ' + bucket + 'Key: ' + key)
        print("CONTENT TYPE: " + response['ContentType'])
        getFile = s3.get_object(Bucket=bucket, Key=key)
        # return response['ContentType']
        
        # https://medium.com/plusteam/move-and-rename-objects-within-an-s3-bucket-using-boto-3-58b164790b78
        #Copy file  & delete
        #s3r.Object(bucket, key+'.orig').copy_from(bucket, key) 
        # s3r.Object('pragra-data-ingest', "object_B.txt").copy_from(CopySource="sample.json")
        copy_source={'Bucket':bucket, 'Key':key}
        s3r.meta.client.copy(copy_source, bucket , key + '.error')

   
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e



# application/json
# text/plain
# {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-2', 'eventTime': '2020-03-01T03:15:32.493Z', 'eventName': 'ObjectCreated:Put', 'userIdentity': {'principalId': 'ANPT8VKVUIMQA'}, 'requestParameters': {'sourceIPAddress': '216.165.209.171'}, 'responseElements': {'x-amz-request-id': '5568B08FF822F95C', 'x-amz-id-2': 'k156iqJIq6J2+4BBleIWXPz2W0lizY7ftHD3C6rQk3CwbxSqATAD1OzPVJ4VOHePoSafRNFaYXTj44F7S3HM9v41ginAsm+t'}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': '7c38e491-917d-4233-a6e8-9692291ab7be', 'bucket': {'name': 'cbatth-pragra-lambda-trigger-input', 'ownerIdentity': {'principalId': 'ANPT8VKVUIMQA'}, 'arn': 'arn:aws:s3:::cbatth-pragra-lambda-trigger-input'}, 'object': {'key': '1.txt', 'size': 5, 'eTag': '3dae760aa7403a56a5f9bfc044a4ac76', 'sequencer': '005E5B28D5493C4328'}}}]}
