import kaggle
import os
import requests


def extract_data(**kwargs):
    ti = kwargs['t1']
    response = requests.get(
        kaggle.api.authenticate()

        kaggle.api.dataset_download_files('Marketing: Electronic Products and Pricing Data', path='/datasets/arashnic/e-product-pricing', unzip=True)
    )
    if response.status_code==200:
        dataset = response.json()

        ti.xcom_push(key='dataset_json', velue=dataset)