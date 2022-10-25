import json
import pymysql
import pandas as pd
import csv
import boto3
import json
import base64
from botocore.exceptions import ClientError
import collections

def connect():
    
    #Secret manager, get MysqlConnection. It daes not reveal the enpoint, user, password, database's name.
    client=boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId='MysqlConection')
    
    #Create a dictionary to load connection's string database.
    secretDict=json.loads(response['SecretString'])
    rdsEndpoint=secretDict['host']
    rdsUserName=secretDict['username']
    rdsPassword=secretDict['password']
    rdsDbname=secretDict['dbInstanceIdentifier']
    
    try:
        # Try to connect to database, if not an error is raise.
        rds_connection=pymysql.connect(host=rdsEndpoint, user=rdsUserName,password=rdsPassword,database=rdsDbname, autocommit=True )
        mycursor = rds_connection.cursor()
        rds_connection.commit();
    except Exception as e:
        raise e
    return mycursor


def lambda_handler(event, context):
    
    #To load query results.
    
    result = []
    
    #Create a cursor
    my_cursor=connect()
    
    #Employees query
    sql="select id, name, hired, department_id, job_id from hired_employees limit 1000"
    my_cursor.execute(sql)
    #Self descriptive 
    rows = my_cursor.fetchall()

    objects_list=[]
    #For each row of data query, load in a dictionary 
    for row in rows:
        #An ordered collection dictionary 
        d=collections.OrderedDict()
        d['id'] = row[0]
        d['name']= row[1]
        d['hired'] = row[2]
        d['department_id'] = row[3]
        d['job_id'] = row[4]
        objects_list.append(d)
    
    result=json.dumps(objects_list)
    result=json.loads(result)

    return {'statusCode': 200,
            'body': result
    }