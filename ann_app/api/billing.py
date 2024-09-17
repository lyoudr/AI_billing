from ann_app.utils.api_code import ApiCode
from ann_app.utils.api_response import gen_api_response
from ann_app.services.billing_service import handle_csv_billing_data
from ann_app.utils.db_session import session_scope
from ann_app.db_models import GCPBill
from . import api 

from flask import request
from flasgger import swag_from
import pandas as pd 


@api.route(
    "/count_billing", 
    methods=["POST"]
)
@swag_from({
    "tags": ["billing"],
    "responses": {
        "200": {
            "description": "handle billing data"
        }
    }
})
def count_billing():
    """
    Handle billing data.
    """
    handle_csv_billing_data()
    return gen_api_response(ApiCode.SUCCESS, 'process billing data successfully')


@api.route('/import_gcp_bill', methods=['POST'])
@swag_from({
    "summary": "Handle import gcp bill",
    "description": "Import GCP billing data from a CSV file to the database.",
    "consumes": ["multipart/form-data"],
    "tags": ["data"],
    "parameters": [
        {
            "name": "file",
            "in": "formData",
            "required": True,
            "type": "file",
            "description": "The CSV file containing GCP billing data."
        }
    ],
    "responses": {
        "200": {
            "description": "Import GCP bill to database successfully."
        }
    }
})
def import_gcp_bill():
    """
    Handle import gcp bill
    """
    file = request.files['file']
    df = pd.read_csv(file)
    df = df.replace(',', '', regex=True)
    df = df.where(pd.notna(df), None)
    df = df[:30]
    # Convert specific columns from string to decimal
    columns_to_observe = ['帳單帳戶名稱', '帳單帳戶 ID', '專案名稱', '專案 ID', '專案編號', '服務說明', '服務 ID', 'SKU 說明', 'SKU ID']
    columns_to_convert = ['用量', '詳細費用金額', '定價費用', '未捨入費用 ($)', '費用 ($)']
    for column in columns_to_observe:
        df[column] = df[column].where(pd.notna(df[column]), '-')
    for column in columns_to_convert:
        df[column] = df[column].where(pd.notna(df[column]), 0)
    df[columns_to_convert] = df[columns_to_convert].astype(float)
    if df.isna().any().any():
        print("DataFrame contains NaN values")
    with session_scope() as session:
        for index, row in df.iterrows():
            try:
                gcp_bill = GCPBill(
                    account_name=row['帳單帳戶名稱'],
                    account_id=row['帳單帳戶 ID'],
                    project_name=row['專案名稱'],
                    project_id=row['專案 ID'],
                    project_number=row['專案編號'],
                    service_name=row['服務說明'],
                    service_id=row['服務 ID'],
                    service_sku=row['SKU 說明'],
                    sku_id=row['SKU ID'],
                    start_date=row['用量開始日期'],
                    end_date=row['用量結束日期'],
                    amount=row['用量'],
                    amount_unit=row['用量單位'],
                    detail_fee=row['詳細費用金額'],
                    price=row['定價費用'],
                    unrounded_fee=row['未捨入費用 ($)'],
                    rounded_fee=row['費用 ($)']
                ) 
                session.add(gcp_bill)
            except Exception as e:
                raise Exception(str(e))
    return gen_api_response(ApiCode.SUCCESS, 'File uploaded and data imported to database successfully')


@api.route('/count_billing', methods = ['POST'])
@swag_from({
    "summary": "Handle GCS data",
    "description": "Process gcs data",
    "parameters": [
        {
            "name": "gcs_path",
            "in": "body",
            "required": True,
            "schema": {
                "type": "object",
                "properties": {
                    "gcs_path": {
                        "type": "string",
                        "example": "gs://bucket_name/filename"
                    }
                }
            }
        }
    ],
    "responses": {
        "200": {
            "description": "Process data successfully."
        }
    }
})
def process_data():
    handle_csv_billing_data()
    return gen_api_response(ApiCode.SUCCESS, 'Process data in pipeline and store to database.')


