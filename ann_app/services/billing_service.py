from ann_app.utils.db_session import get_db 
from ann_app.db_models.billing import BillingReport
from ann_app.models.billing import BillingReportModel
from ann_app.utils.errors import CustomException
from ann_app.__init__ import create_app 

from apache_beam.options.pipeline_options import PipelineOptions

from http import HTTPStatus
from decimal import Decimal
import apache_beam as beam
import argparse  

"""
    1. Transforms:
        - Transforms are the operations in your pipeline, and provide a generic processing framework.
        - Apply Transforms:
            [Final Output PCollection] = [Initial Input PCollection] | ([First Transform])
                                                                     | ([Second Transform])
                                                                     | ([Third Transform])
        - Core Beam Transfroms:
            (1) ParDo:
                ParDo is a Beam trnasform for generic parallel processing. 
                The ParDo processing paradigm is similar to the "Map" phase of a Map/Shuffle/Reduce-style algorithm
    
    2. Window
        - Window subdivides a "PCollection" into windows according to the timestamps of its individual elements. 
        - Windows enable grouping operations over unbounded collections by dividing the collection into windows of finite collections 
    3. Watermark 
        - is a guess as to when all data in a certain window is expected to have arrived in the pipeline
        - Data sources are responsible for producing a watermark, and every PCollection must have a watermark that estimates how complete the PCollection is.
        - A windowing function tells the runner how to assign elements to one or more initial windows, and how to merge windows of grouped elements. Each element in a PCollection can only be in one window
"""



def save_billing_data_to_db(data: BillingReportModel) -> None:
    app = create_app()
    with app.app_context():
        try:
            db = next(get_db())
            print(BillingReport(**data.dict()))
            billing_report = BillingReport(**data.dict())
            db.add(billing_report)
            db.commit()
        except Exception as e:
            db.rollback()
            raise CustomException(
                code=HTTPStatus.BAD_REQUEST,
                error_code=400,
                error_msg=f'create billing data error {str(e)}'
            )


def count_fee_and_save_to_db(element) -> None:
    from decimal import Decimal
    company, total_cost = element
    tech_fee = Decimal(total_cost) * Decimal('0.05')  # Applying 5% Serving Fee
    discounted_cost = Decimal(total_cost) * Decimal('0.98')  # Appying 98% Discount
    print(
        f"company:{company}"
        f"total_cost:{total_cost}",
        f"discounted_cost:{discounted_cost}",
        f"tech_fee:{tech_fee}"
    )
    data = BillingReportModel(
        company=company,
        total_cost=total_cost,
        discounted_cost=discounted_cost,
        tech_fee=tech_fee,
    )
    print("data is ->", data)
    save_billing_data_to_db(data)


def transform_and_save_data(billing_data) -> None:
    # Composite Transformation -> Group the services by company and sum the total cost
    (
        billing_data
        # Map Phase `Map(k1, k2) → list(k2,v2)`
        | 'Group by Company' >> beam.Map(
            lambda billing: (billing.company, billing.cost)
        )
        # Map Phase >> beam.ParDo(print) # This is not worked, as it is a
        # whole ParDo, and afterward Map is not worked
        # Reduce Phase `Reduce(k2, list(v2)) → list((k3, v3))`
        | 'Combine' >> beam.CombinePerKey(sum)
        # If your ParDo performs a one-to-one mapping of input elements to
        # output elements–that is,
        # for each input element, it applies a function that produces exactly
        # one output element,
        # you can use the higher-level Map transform.
        | 'Count Several Fees and Save to DB' >> beam.Map(
            count_fee_and_save_to_db
        )
    )

# handle billing data
def handle_csv_billing_data(gcs_path: str, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://ann-billing/staging/billing_report.csv',
        help='Input file to process.'
    )
    beam_options = PipelineOptions(
        runner='DataflowRunner',
        project='ann-project-390401',
        job_name='billing-service',
        temp_location='gs://ann-billing/temp/',
        region='asia-east1',
        service_account_email="742937875290-compute@developer.gserviceaccount.com",
        extra_packages=["gs://ann-5432/ann_app-0.1.tar.gz"],
        requirements_file="/workspace/requirements.txt"
    )
    
    with beam.Pipeline(options=beam_options) as pipeline:
        # Read data from the CSV file
        lines = pipeline | beam.io.ReadFromText(
            gcs_path,
            skip_header_lines=1
        )

        class Bill:
            def __init__(self, service: str, cost: float, company: str):
                self.service = service
                self.cost = cost
                self.company = company

        # Parse csv file for apache beam
        class ParseCSV(beam.DoFn):
            
            def process(self, element):
                import csv
                reader = csv.reader([element])
                row = next(reader)
                service, cost, company = row[:3]
                return [Bill(
                    service=service, 
                    cost=float(cost), 
                    company=company
                )]
        
        # Apply the ParseCSV ParDo function to parse the CSV data
        billing_data = lines | beam.ParDo(ParseCSV())

        # Map/Reduce data, and save data
        transform_and_save_data(billing_data)


# -------------------------- Read From BigQuery ----------------------

# read from bigquery 
def handle_bq_billing_data(argv=None, save_main_session=True):
    # The SQL query to run inside BigQuery.
    query_string = """
        SELECT * FROM [ann-project-390401.billing.report]
        WHERE service IS NOT NULL
        AND cost IS NOT NULL
        AND company IS NOT NULL
    """
    beam_options = PipelineOptions(
        runner='DataflowRunner',
        project='ann-project-390401',
        job_name='billing-service',
        temp_location='gs://ann-billing/temp/',
        region='asia-east1',
        service_account_email="742937875290-compute@developer.gserviceaccount.com"
    )
    local_options = PipelineOptions(
        runner = 'DirectRunner',
        project='ann-project-390401',
        temp_location='gs://ann-billing/temp/',
        region='asia-east1',
        service_account_email="742937875290-compute@developer.gserviceaccount.com"
    )

    with beam.Pipeline(options=local_options) as pipeline:
        lines = (pipeline
            # Read the query results into TableRow objects.
            | 'Read from BigQuery' >> beam.io.Read(
                beam.io.BigQuerySource(query=query_string)
            )
            # Map each row to a key-value pair where key is the company and value is cost.
            | 'Map Company-Cost' >> beam.Map(lambda row: (row['company'], row['cost']))
            # Group by key (company) and sum up the costs for each company.
            | 'Sum Costs by Company' >> beam.CombinePerKey(sum)
            | 'Count Several Fees and Save to DB' >> beam.Map(
                count_fee_and_save_to_db
            )
        )

        def print_ele(element):
            print('element is ->', element)

        lines | 'Print Elements' >> beam.Map(print_ele)


if __name__ == '__main__':
   handle_csv_billing_data()
