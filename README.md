# Clinical Trial Data Analysis

A toolkit for converting clinical trial data exports from clinicaltrials.gov into json and parquet formats for subsequent processing or analytics.

Clinical trial data can be exported from [clinicaltrials.gov](https://clinicaltrials.gov/ct2/resources/download) in the XML format. Embedded in each clinical trial/study record is core metadata, contact information, and sometimes links to study protocols or other documents. 

This script converts the XML format into JSON for individual processing or PARQUET to be imported into a data warehouse or query engine like AWS Athena.

## Usage

1. Install dependencies with `pipenv install`
2. Download data from [clinicaltrials.gov](https://clinicaltrials.gov/ct2/resources/download) 
3. Run the script `ct-xml-to-json.py` to convert the XML data into JSON
4. Run the script `ct-json-to-parquet.py` to convert the JSON data into PARQUET

```
python3 ct-xml-to-json.py 
    --schema ./schemas/public.xsd 
    --src ./data/AllPublicXML 
    --dst ./data/OutputJSON

python3 ct-json-to-parquet.py 
    --src ./data/OutputJSON
    --dst ./data/OutputPARQUET
```

### Configuring AWS Athena for Querying

1. Push the PARQUET data to an S3 bucket with `aws s3 sync ./data/OutputPARQUET/ s3://your-bucket-here/ct-data/`
2. Configure a table in AWS Athena/Glue that points to your S3 location and infers the schema from the data.
3. You should get a new table with a schema that looks like the one saved [here](./schemas/aws-glue-inferred.json).
4. Query! 
   - Since the data is with structs/arrays instead of columns, there is some nuance to querying. See the [AWS docs](https://docs.aws.amazon.com/athena/latest/ug/rows-and-structs.html) for more info.
   - Optional fields within structs/arrays can be wrapped with the `try` function (`try(X[1].Y)`) to nullify errors. 

```
SELECT 
    clinical_study.id_info.nct_id, 
    clinical_study.brief_title, 
    clinical_study.overall_status
FROM <your-table-name> 
LIMIT 10;
```
