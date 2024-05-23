import csv
import codecs
import boto3

def csv_to_s3():
        
        with open('objects.csv', 'rb') as file:
         
         
         reader = file.read().decode('utf-8')
         
       
        s3_client = boto3.client("s3",
                  endpoint_url="https://hb.bizmrg.com",
                  aws_access_key_id="r5RPwVNHK3znEFW8f1b43D",
                  aws_secret_access_key="dWYYtHFmzuQnGzJehi41iwsQkWfQbFLFQUMYTbRXUWkF"
        )
          
        bucket_name = "workspace"
        
        s3_client.put_object(
        Body=reader, 
        Bucket=bucket_name, 
        Key='objects.csv' 
        )
        
        file.close()
        print("CSV file has been sent to s3!")

csv_to_s3()