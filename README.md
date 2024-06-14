# Billing Count using Map/Reduce and AI Model to Predict

I am using Google's Map/Reduce and Apache Beam, a practical tool, to streamline my billing data processing. This includes calculating the total amount each customer has spent on Google Cloud Platform (GCP) services. Additionally, I am utilizing Apache Beam to continuously receive streaming data published by the Pub/Sub service. I employ windowing techniques to segment the unbounded data into smaller windows.

## Map Reduce Implement

- Input reader 
    - Read streaming data from Pub/Sub service publications in GCP and partition it into windows:
    ```
        beam.WindowInto(
            FixedWindows(1*5) , 
            trigger=AfterAny(AfterWatermark( 
                early=AfterProcessingTime(10),
                late=AfterProcessingTime(30)
            )),
            accumulation_mode=AccumulationMode.DISCARDING
        )
    ```
- Map Phase
    - `Map(k1,v1) → list(k2,v2)`
    ```
      beam.Map(lambda billing: (billing.company, billing.cost))  
    ```
- Shuffle and Sort Phase 
    - The intermediate key-value pairs generated by the mappers are shuffled and sorted based on the keys
- Reduce Phase
    - `Reduce(k2, list (v2)) → list((k3, v3))`
    ```
        beam.CombinePerKey(sum)
    ```

## Running Platform 
- DataFlow

## Tools Used
- Apache beam
- GCP Pub/Sub 

## AI Model Prediction
> calculation process is in **prediction.ipynb**
- Linear Regression Model
    - To predict the future cost of each GCP service, I input gcp services and every sum cost to `Linear Regression` model to train the model, And use this trained model to predict future service cost.
    - r is the relationship between service and sum cost. The r value ranges from -1 to 1, where 0 means no relationship, and 1 (and -1) means 100% related.
    ```
    r = 0.2276403489710411
    Compute Engine future cost = 0.064
    ```
- Polynomial Regression
    - To predict the future cost of each GCP service, I input gcp services and every sum cost to `Polynomial Regression` model to train the model, And use this trained model to predict future service cost.
    - The r-squared value ranges from 0 to 1, where 0 means no relationship, and 1 means 100% related.
    ```
    r2 = 1.0
    Compute Engine future cost = 0.04
    ```

## DataBase
- Cloud SQL (MySQL)
- SQLAlchemy : for migration and database version

## Impact
By applying Map/Reduce to our billing counting system, we have significantly reduced the processing time for handling large volumes of data. The parallel processing capabilities of Map/Reduce allow for concurrent processing of billing data from different companies, resulting in faster and more efficient calculations.

--- 
## Start Service
1. Create and Activate virtual environment
```
python3 -m venv ma_venv
source ma_venv
```
2. Install required packages
```
pip install requirements.txt
```
3. Set google environment variable
```
export GOOGLE_APPLICATION_CREDENTIALS=/Users/apple/Code/Ann/master/service-account-key.json
```
4. Run the app
```
flask --app ann_app run --debug
```

---
## Word Count Exampmle
- ann_app.services.billing_service.BeamProcessor.handle_word_count
```
def handle_word_count(self, argv=None, save_main_session=True):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--input',
            dest='input',
            default='/Users/apple/Code/Ann/master/static/sample.txt',
            help='Input file to process.'
        )
        parser.add_argument(
            '--output',
            dest='output',
            # CHANGE 1/6: (OPTIONAL) The Google Cloud Storage path is required
            # for outputting the results.
            default='/Users/apple/Code/Ann/master/static/output.txt',
            help='Output file to write results to.'
        )
        known_args, pipeline_args = parser.parse_known_args(argv)
        pipeline_options = PipelineOptions(pipeline_args)
        with beam.Pipeline(options=pipeline_options) as p:
            
            # Read the text file[pattern] into a PCollection.
            lines = p | beam.io.ReadFromText(known_args.input)

            # Count the occurrences of each word.
            counts = (
                lines
                | 'Split' >> (
                    beam.FlatMap(
                        lambda x: re.findall(r'[A-Za-z\']+', x)
                    ).with_output_types(str)
                )
                | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
                | 'GroupAndSum' >> beam.CombinePerKey(sum)
            )
            # Format the counts into a PCollection of strings
            def format_result(word_count):
                (word, count) = word_count
                return '%s: %s' % (word, count)

            output = counts | 'Format' >> beam.Map(format_result)

            # Write the output using a "Write" transform taht has side effects.
            output | WriteToText(known_args.output)
```

- execute word count on local

`
python -m ann_app.services.billing_service
`