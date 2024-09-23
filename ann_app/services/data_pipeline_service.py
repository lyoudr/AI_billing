from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import argparse

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
    # data = BillingReportModel(
    #     company=company,
    #     total_cost=total_cost,
    #     discounted_cost=discounted_cost,
    #     tech_fee=tech_fee,
    # )
    # print("data is ->", data)
    # save_billing_data_to_db(data)


def transform_and_save_data(billing_data) -> None:
    # Composite Transformation -> Group the services by company and sum the total cost
    (
        billing_data
        # Map Phase `Map(k1, k2) → list(k2,v2)`
        | 'Group by Company' >> beam.Map(
            lambda billing: (billing[2], billing[1])
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
        'gs://ann-billing/staging/billing_report.csv'
    )
