import csv
import codecs
import boto3

def csv_to_s3():
        
        with open('objects.csv', 'rb') as file:
         
         # Читаем данные из csv файла
         reader = file.read().decode('utf-8')
         
        # Создаем клиент для работы с s3 хранилищем
        s3_client = boto3.client("s3",
                  endpoint_url="https://hb.bizmrg.com",
                  aws_access_key_id="r5RPwVNHK3znEFW8f1b43D",
                  aws_secret_access_key="dWYYtHFmzuQnGzJehi41iwsQkWfQbFLFQUMYTbRXUWkF"
        )
          
        bucket_name = "workspace"
        # Передаем данные в s3 хранилище
        s3_client.put_object(
        Body=reader, # Передаем данные из csv файла
        Bucket=bucket_name, # Указываем название бакета в s3 хранилище
        Key='objects.csv' # Указываем название файла, под которым будут храниться данные в s3 хранилище
        )
        # Закрываем файл
        file.close()
        print("CSV file has been sent to s3!")

csv_to_s3()