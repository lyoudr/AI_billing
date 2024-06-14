from google.cloud import bigquery 


class BigQuery:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)
    
    def execute_query(self, query):
        query_job = self.client.query(query)
        results = query_job.result() 
        return results 

    def load_data_from_file(
        self,
        table_id: str,
        gs_path: str,
        job_config=None
    ):
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("service", "STRING"),
                bigquery.SchemaField("cost", "NUMERIC"),
                bigquery.SchemaField("company", "STRING"),
            ],
            skip_leading_rows=1,  # Skip the header row
            source_format=bigquery.SourceFormat.CSV,
        )
        job_config.source_format = bigquery.SourceFormat.CSV 

        # Load data from Cloud Storage into BigQuery 
        uri = gs_path
        load_job = self.client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

        # Wait for the job to complete
        load_job.result()