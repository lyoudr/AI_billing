from apache_beam.options.pipeline_options import PipelineOptions
from sqlalchemy import create_engine, text
from google.cloud import secretmanager
import apache_beam as beam
import argparse


# Function to access secret from Google Secret Manager
def access_secret_version(secret_id, version_id="latest"):
    """
    Access the payload for the given secret version from Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/ann-project-390401/secrets/{secret_id}/versions/{version_id}"
    
    response = client.access_secret_version(name=secret_name)
    
    # Return the decoded secret payload
    return response.payload.data.decode('UTF-8')


SERVICE_ACCOUNT = access_secret_version("SERVICE_ACCOUNT")
GCS_BUCKET = access_secret_version("GCS_BUCKET")
REGION = access_secret_version("REGION")
DB_CONNECTION_URL = access_secret_version("DB_CONNECTION_URL")
db = create_engine(DB_CONNECTION_URL)

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
    try:
        # Connect to PostgreSQL using the engine
        with db.connect() as connection:
            # Define raw SQL insert query
            insert_query = text("""
            INSERT INTO billing_report (company, total_cost, discounted_cost, tech_fee)
            VALUES (:company, :total_cost, :discounted_cost, :tech_fee);
            """)

            # Execute SQL query with parameters as a dictionary
            connection.execute(insert_query, {
                "company": company,
                "total_cost": total_cost,
                "discounted_cost": discounted_cost,
                "tech_fee": tech_fee
            })

            # Commit the transaction
            connection.commit()

            print(f"Successfully saved data for {company}")
    
    except Exception as e:
        print(f"Failed to save data for {company}: {e}")


def transform_and_save_data(billing_data) -> None:
    # Composite Transformation -> Group the services by company and sum the total cost
    (
        billing_data
        # Map Phase `Map(k1, k2) → list(k2,v2)`
        | 'Group by Company' >> beam.Map(
            lambda billing: (billing[2], billing[1])
        )
        # Reduce Phase `Reduce(k2, list(v2)) → list((k3, v3))`
        | 'Combine' >> beam.CombinePerKey(sum)
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
        default=f'{GCS_BUCKET}/staging/*.csv',
        help='Input file to process.'
    )
    beam_options = PipelineOptions(
        runner='DataflowRunner',
        project='ann-project-390401',
        job_name='billing-service',
        temp_location=f'{GCS_BUCKET}/temp/',
        region=REGION,
        service_account_email=SERVICE_ACCOUNT
    )
    
    with beam.Pipeline(options=beam_options) as pipeline:
        # Read data from the CSV file
        lines = pipeline | beam.io.ReadFromText(
            gcs_path,
            skip_header_lines=1
        )

        # Parse csv file for apache beam
        class ParseCSV(beam.DoFn):
            
            def process(self, element):
                import csv
                reader = csv.reader([element])
                row = next(reader)
                service, cost, company = row[:3]
                return [(
                    service, 
                    float(cost), 
                    company
                )]
        
        # Apply the ParseCSV ParDo function to parse the CSV data
        billing_data = lines | beam.ParDo(ParseCSV())

        # Map/Reduce data, and save data
        transform_and_save_data(billing_data)

if __name__ == '__main__':
    handle_csv_billing_data(
        f'{GCS_BUCKET}/staging/*.csv'
    )
