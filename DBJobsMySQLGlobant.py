import json
import pymysql
import pandas as pd
import csv
import boto3
import json
import base64
from botocore.exceptions import ClientError

#Secreat manager, connection string under aws root account.

def connect():
    
    client=boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId='MysqlConection')
    secretDict=json.loads(response['SecretString'])
    rdsEndpoint=secretDict['host']
    rdsUserName=secretDict['username']
    rdsPassword=secretDict['password']
    rdsDbname=secretDict['dbInstanceIdentifier']
    
    try:
        # String connection defined
        rds_connection=pymysql.connect(host=rdsEndpoint, user=rdsUserName,password=rdsPassword,database=rdsDbname, autocommit=True )
        mycursor = rds_connection.cursor()
        rds_connection.commit();
    except Exception as e:
        raise e
    return mycursor
    
    #Drop tables if exist. It's likely that It is not a one time procedure and going to last for undefined time. This is why this portion's code must be commented in  qa/prod envs. 
    
def drop_tables(cursor):
    try:
        sql = "DROP TABLE IF EXISTS hired_employees"
        cursor.execute(sql)
        sql = "DROP TABLE IF EXISTS jobs"
        cursor.execute(sql)
        sql = "DROP TABLE IF EXISTS departments"
        cursor.execute(sql)
        return True
    except Exception as e:
        return False

    #Recreate tables

def create_jobs_table(cursor):
    try:
        # TODO: write code...
        cursor.execute('create table jobs (id integer not null primary key, job varchar(200) not null)')
        return True
    except Exception as e:
        print("Oops! One error has occurred table Jobs!!!.")
    
def create_departments_table(cursor):
    try:
        # TODO: write code...
        cursor.execute('create table departments (id integer not null primary key, department varchar(200) not null)')
        return True
    except Exception as e:
        print("Oops! One error has occurred table Departments!!!.")

def create_hired_employees_table(cursor):
    str =  "create table hired_employees(id integer  not null primary key," 
    str+=  "name varchar(400) not null,hired varchar(200) not null,"
    str+=  "department_id integer not null, index idx(department_id)," 
    str+=  "FOREIGN KEY (department_id) references departments (id),"
    str+=  "job_id integer not null, index idx2(job_id)," 
    str+=  "FOREIGN KEY (job_id) references jobs(id))"

    print (str)

    try:
        # TODO: write code...
        cursor.execute(str)
        return True
    except Exception as e:
        print("Oops! One error has occurred table hired_employees!!!.")

def departments_reader_insert(file, cursor):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='pocbucketglobant', Key='DataSets/' + file)
    data = obj['Body'].read().decode('utf-8').splitlines()
    csv_data= csv.reader(data)
    check=True
    
    for row in csv_data:
        id=row[0].replace("'","")
        department=row[1].replace("'","")
        try:
            sql=f"INSERT into departments (id, department) VALUES('{id}','{department}')"
            cursor.execute(sql)
        except Exception as e:
            print(e)
    return check    
    
        
def jobs_reader_insert(file, cursor):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='pocbucketglobant', Key='DataSets/' + file)
    data = obj['Body'].read().decode('utf-8').splitlines()
    csv_data= csv.reader(data)
    check=True
    
    for row in csv_data:
        id=row[0].replace("'","")
        job=row[1].replace("'","")
        
        try:
            sql=f"INSERT into jobs (id, job) VALUES('{id}','{job}')"
            cursor.execute(sql)
            check+=True
        except Exception as e:
            check+=False
            print(e)
    return check   
    
    
def employees_hired_reader_insert(file, cursor):
    rowInsertedOK=0
    rowInsertedNOOK=0
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='pocbucketglobant', Key='DataSets/' + file)
    data = obj['Body'].read().decode('utf-8').splitlines()
    csv_data= csv.reader(data)
    check=True
    
    
    for row in csv_data:
        
        id=row[0].replace("'","")
        name=row[1].replace("'","")
        hired=row[2].replace("'","")
        department_id=row[3].replace("'","")
        job_id=row[4].replace("'","")
        try:
            sql=f"INSERT into hired_employees (id, name, hired, department_id, job_id) VALUES('{id}','{name}','{hired}','{department_id}','{job_id}')"
            cursor.execute(sql)
            rowInsertedOK+=1
            
        except Exception as e:
            print(sql)
            rowInsertedNOOK+=1

    print('')
    print('Summary:')
    print('Records inserted in tables: ' + str(rowInsertedOK))
    print('Records not inserted in tables: ' + str(rowInsertedNOOK))
    return check 


def lambda_handler(event, context):
    if(drop_tables(connect())):
        if (create_jobs_table(connect())):
            if(create_departments_table(connect())):
                if create_hired_employees_table(connect()):
                    if departments_reader_insert('departments.csv', connect()):
                        if jobs_reader_insert('jobs.csv', connect()):
                            if employees_hired_reader_insert('hired_employees.csv', connect()):
                                print("Operation OK")
                            else:
                                print("Operation NO OK emploees_hired")
                        else:
                            print("Operation NO OK Jobs")
                    else:
                        print("Operation NO departments")
                else:
                    print("Operation NO OK create employees created")
            else:
                print("Operation NO OK departments")
        else:
            print("Operation NO OK Jobs")
    else:
        print("Drops Tables procedure has failed!")
                
    return 
    {
        'statusCode': 200
    }