from ann_app.utils.api_code import ApiCode
from ann_app.utils.api_response import gen_api_response
from ann_app.services.bigquery_service import BigQuery
from . import api

from flask import current_app
from flasgger import swag_from

@api.route(
    "/gcs_to_bigquery",
    methods=["POST"]
)
@swag_from({
    "tags": ["data"],
    "responses": {
        "200": {
            "description": "Import data from GCS to BigQuery",
        }
    }
})
def gcs_to_bigquery():
    """
    Import data from GCS to BigQuery
    """
    project_id = current_app.config['PROJECT_ID']
    bq = BigQuery(project_id=project_id)
    bq.load_data_from_file(
        table_id=f'{project_id}.billing.report',
        gs_path='gs://ann-billing/staging/billing_report.csv'
    )
    return gen_api_response(
        ApiCode.SUCCESS,
        'import data to bigquery successfully.'
    )
