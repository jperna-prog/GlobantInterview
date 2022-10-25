import json
import boto3
import csv


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='pocbucketglobant', Key='DataSets/departments.csv')
    data = obj['Body'].read().decode('utf-8').splitlines()
    lines = csv.reader(data)
    headers = next(lines)
    print('headers: %s' %(headers))
    for line in lines:
        print(line)