# Clinical Trial Data Analysis

A toolkit for converting clinical trial data exports from clinicaltrials.gov into json and parquet formats for subsequent processing or analytics.

Clinical trial data can be exported from [clinicaltrials.gov](https://clinicaltrials.gov/ct2/resources/download) in the XML format. Embedded in each clinical trial/study record is core metadata, contact information, and sometimes links to study protocols or other documents. 

This script converts the XML format into JSON for individual processing or PARQUET to be imported into a data warehouse or query engine like AWS Athena.

## Usage

1. Install dependencies with `pipenv install`
2. Download data from [clinicaltrials.gov](https://clinicaltrials.gov/ct2/resources/download) 
3. Run the script `ct-xml-to-json.py`

```
python3 ct-xml-to-json.py 
    --schema ./schemas/public.xsd 
    --src ./data/AllPublicXML 
    --dst ./data/OutputJSON
```
