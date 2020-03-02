set S3=s3://cbatth-pragra-lambda-trigger-pickle
for /L %d in (1,1,9) do aws s3 cp test%d.json %S3%
