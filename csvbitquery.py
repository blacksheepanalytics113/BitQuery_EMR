import s3fs
import botocore
from boto3.s3.transfer import S3Transfer
import boto3
import datetime
from airflow.models import Variable
from test_bitqueryy import oAuth_example


aws_access_key_id = Variable.get("aws_access_key")
aws_secret_access_key = Variable.get("aws_secret_access_key")
aws_region = "eu-north-1"
aws_bucket_name = Variable.get("aws_bucket_name")
file_path = Variable.get("file_path")
# s3_file_name = Variable.get("s3_file_name")
file_name = Variable.get("file_name")
def connect_to_s3():
    """
    Post All Data saved to s3 Bucket
    """
    try:
        data_file = oAuth_example()
        s3 = s3fs.S3FileSystem(anon=False,
                               key= aws_access_key_id,
                               secret=aws_secret_access_key,
                               client_kwargs={
                                'region_name': 'eu-north-1'
    })
        # return s3
        if not s3.exists(aws_bucket_name):
                s3.mkdir(aws_bucket_name)
                print("Bucket created")
        else :
            print("Bucket already exists")
        s3_objects = s3.ls(aws_bucket_name)
        print(s3_objects)
        current_time= datetime.datetime.now()
        Bucket_file_name =   str(current_time)
        Bucket_file_name = Bucket_file_name + file_name 
        print(Bucket_file_name)
        if s3.upload(file_path,f"{aws_bucket_name}/Transformed_Data/"+ file_name):
        # print(upload_data)
            print(f"File uploaded successfully to s3://{aws_bucket_name}/Transformed_Data/" + Bucket_file_name)
        else:
            # FileNotFoundError:
            print('The file was not found')   
        response = s3.put_file(file_path,f'{aws_bucket_name}/{data_file}')
        if response == None:
            print(f"File Uploaded Nothing")
        else:
            print(f"print found success!")
        
        
    except Exception as e:
        print(e)
connect_to_s3()