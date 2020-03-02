      1 Create the S3 buckets
		2020-03-01 15:41:50 cbatth-pragra-lambda-trigger-archive
		2020-03-01 15:05:20 cbatth-pragra-lambda-trigger-error
		2020-03-01 17:20:51 cbatth-pragra-lambda-trigger-input
		2020-03-01 17:34:37 cbatth-pragra-lambda-trigger-pickle
		2020-03-01 15:32:09 pragra-data-ingest
      2 Create the DynamoDB with 'fname" as the key
      3 Create the IAM role with unlimited access from S3, Dynamodb
      4 Create Lambda
      5 Create table "pragra-json" in the DynamoDB

Lessons learnt:  import json as JSON, boto3.client vx boto3.resource
	pandas implementation is not very easy


