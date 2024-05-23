import datetime
import pandas as pd
import json
from airflow.models import Variable
from airflow.exceptions import AirflowException

def get_gata_from_json():
    
    with open(r'dags/otvet.json') as json_file:
        data = json.load(json_file)
    """""
    data = Variable.get(key="regular_var_json", deserialize_json=True)
    """
    list_of_data = []
    
    for page in data:
        temp_dict = {}
        temp_dict['total'] = page.get('total')
        temp_dict['totalPages'] = page.get('totalPages')
        temp_dict['currentPage'] = page.get('currentPage')
        list_of_data.append(temp_dict)
        for ksrs in page["ksrs"]:
           temp_ksrs_dict = {}
           temp_ksrs_dict['total'] = None
           temp_ksrs_dict['totalPages'] = None
           temp_ksrs_dict['currentPage'] = None
           temp_ksrs_dict['partnerId'] = ksrs.get('partnerId')
           temp_ksrs_dict['dateVersion'] = ksrs.get('dateVersion')
           temp_ksrs_dict['objectId'] = ksrs.get('objectId')
           temp_ksrs_dict['objectType'] = ksrs.get('objectType')
           temp_ksrs_dict['selectKSRTypes'] = ksrs.get('selectKSRTypes')
           temp_ksrs_dict['selectCategory'] = None
           temp_ksrs_dict['selectGastroType'] = None
           temp_ksrs_dict['selectConferenceHallNumber'] = ksrs.get('selectConferenceHallNumber')
           temp_ksrs_dict['inputConferenceHallHumans'] = ksrs.get('inputConferenceHallHumans')
           temp_ksrs_dict['inputConferenceHallSquare'] = ksrs.get('inputConferenceHallSquare')
           temp_ksrs_dict['inputRoomsNumber'] = ksrs.get('inputRoomsNumber')
           temp_ksrs_dict['inputRussiansTourists'] = ksrs.get('inputRussiansTourists')
           temp_ksrs_dict['inputForeignerTourists'] = ksrs.get('inputForeignerTourists')
           temp_ksrs_dict['inputEventsNumber'] = None
           temp_ksrs_dict['inputSquare'] = None
           temp_ksrs_dict['inputSeatsNumber'] = None
           temp_ksrs_dict['selectBanquetHallsNumber'] = None
           temp_ksrs_dict['inputVisitors'] = None
           temp_ksrs_dict['status'] = ksrs.get('status')
           temp_ksrs_dict['selectKSRCategory'] = ksrs.get('selectKSRCategory')
           list_of_data.append(temp_ksrs_dict)

        for display_object in page["displayObjects"]:
           temp_display_object_dict = {}
           temp_display_object_dict['total'] = None
           temp_display_object_dict['totalPages'] = None
           temp_display_object_dict['currentPage'] = None
           temp_display_object_dict['partnerId']= display_object.get('partnerId')
           temp_display_object_dict['dateVersion'] = display_object.get('dateVersion')
           temp_display_object_dict['objectId'] = display_object.get('objectId')
           temp_display_object_dict['objectType'] = display_object.get('objectType')
           temp_display_object_dict['selectKSRTypes'] = None
           temp_display_object_dict['selectCategory'] = display_object.get('selectCategory')
           temp_display_object_dict['selectGastroType'] = None
           temp_display_object_dict['selectConferenceHallNumber'] = None
           temp_display_object_dict['inputConferenceHallHumans'] = None
           temp_display_object_dict['inputConferenceHallSquare'] = None
           temp_display_object_dict['inputRoomsNumber'] = None
           temp_display_object_dict['inputRussiansTourists'] = display_object.get('inputRussiansTourists')
           temp_display_object_dict['inputForeignerTourists'] = display_object.get('inputForeignerTourists')
           temp_display_object_dict['inputEventsNumber'] = display_object.get('inputEventsNumber')
           temp_display_object_dict['inputSquare'] = display_object.get('inputSquare')
           temp_display_object_dict['inputSeatsNumber'] = None
           temp_display_object_dict['selectBanquetHallsNumber'] = None
           temp_display_object_dict['inputVisitors'] = display_object.get('inputVisitors')
           temp_display_object_dict['status'] = display_object.get('status')
           list_of_data.append(temp_display_object_dict)

        for gasstro in page["gasstros"]:
           temp_gasstro_dict = {}
           temp_gasstro_dict['total'] = None
           temp_gasstro_dict['totalPages'] = None
           temp_gasstro_dict['currentPage'] = None
           temp_gasstro_dict['partnerId'] = gasstro.get('partnerId')
           temp_gasstro_dict['dateVersion'] = gasstro.get('dateVersion')
           temp_gasstro_dict['objectId'] = gasstro.get('objectId')
           temp_gasstro_dict['objectType'] = gasstro.get('objectType')
           temp_gasstro_dict['selectKSRTypes'] = None
           temp_gasstro_dict['selectCategory'] = None
           temp_gasstro_dict['selectGastroType'] = gasstro.get('selectGastroType')
           temp_gasstro_dict['selectConferenceHallNumber'] = None
           temp_gasstro_dict['inputConferenceHallHumans'] = None
           temp_gasstro_dict['inputConferenceHallSquare'] = None
           temp_gasstro_dict['inputRoomsNumber'] = None
           temp_gasstro_dict['inputRussiansTourists'] = None
           temp_gasstro_dict['inputForeignerTourists'] = None
           temp_gasstro_dict['inputEventsNumber'] = None
           temp_gasstro_dict['inputSquare'] = None
           temp_gasstro_dict['inputSeatsNumber'] = gasstro.get('inputSeatsNumber')
           temp_gasstro_dict['selectBanquetHallsNumber'] = gasstro.get('selectBanquetHallsNumber')
           temp_gasstro_dict['inputVisitors'] = gasstro.get('inputVisitors')
           temp_gasstro_dict['status'] = gasstro.get('status')
           list_of_data.append(temp_gasstro_dict)

    json_file.close()
    print("Data recived!")
    return list_of_data

def data_to_csv(data_to_df):
    columns = ['total','totalPages', 'currentPage', 'partnerId', 'dateVersion', 'objectId', 'objectType',
                     'selectKSRTypes','selectCategory',
                     'selectGastroType', 'selectConferenceHallNumber', 'inputConferenceHallHumans',
                     'inputConferenceHallSquare','inputRoomsNumber', 'inputRussiansTourists', 
                     'inputForeignerTourists', 'inputEventsNumber', 'inputSquare',  'inputSeatsNumber',
                     'selectBanquetHallsNumber','inputVisitors', 'status', 'selectKSRCategory', ]

    df = pd.DataFrame(data_to_df,columns=columns)
    df.to_csv(r'dags/objects.csv')
    print("CSV file created!")
""""    
def csv_to_s3(csv):
        file = open(csv, 'r')

        # Читаем данные из csv файла
        reader = csv.reader(file)

        # Создаем клиент для работы с s3 хранилищем
        s3 = boto3.client('s3')

        # Передаем данные в s3 хранилище
        s3.put_object(
        Body=reader, # Передаем данные из csv файла
        Bucket='workspace', # Указываем название бакета в s3 хранилище
        Key='objets.csv' # Указываем название файла, под которым будут храниться данные в s3 хранилище
        )
        # Закрываем файл
        file.close()
        print("CSV file has been sent to s3!")
"""
def file_step_g2b_object():
    try: 
        list_of_data = get_gata_from_json()
        data_to_csv(list_of_data)
        

        
    except Exception as e:
        """""
        ti = kwargs["ti"]
        ti.xcom_push(f"{ti.task_id}_error", {"step": "postgres", "error": str(e)})
        """
        raise e
    print(0)
file_step_g2b_object()