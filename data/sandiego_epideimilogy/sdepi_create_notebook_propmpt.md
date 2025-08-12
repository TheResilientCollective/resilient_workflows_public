
this is for the San Diego Epidemiology & Immunization Services Branch.
You want to extract the data from a tableau workbook using python code.
You want to download the workbook from the Tableau public site and extract the data.

Downloading the workbook is done by using url request.
It looks like the link is: https://public.tableau.com/workbooks/DraftRespDash.twb

The data should be extracted from the workbook and saved in a format that can be used for analysis, such as CSV or JSON.
You want to create a Jupyter notebook that demonstrates how to download the workbook, extract the data
and save it in a usable format.

The workbook downloaded is actually a Tableau workbook file (.twb) which is a zip file containing XML and other resources.
You will need to extract the data from the XML files within the zip archive.
You will also need to use the Tableau Hyper API to read the data from the workbook.


Lets start with a notebook to test and see what data is found in the download
put the notebook in worflows/public/notebooks, with the name "sandiego_epideimilogy.ipynb"

For the notebook,
* you will need to import the necessary libraries such as `requests`, `zipfile`, `xml.etree.ElementTree`, and `tableauhyperapi`.
* put the downloaded workbook and extracted data in  data/sandiego_epideimilogy





# Start Hyper API process
```
with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint, database=hyper_path) as connection:
        # List all tables
        tables = connection.catalog.get_table_names("Extract")
        for table in tables:
            print(f"Reading from table: {table}")
            # Query all rows
            result = connection.execute_list_query(f"SELECT * FROM {table}")
            for row in result:
                print(row)

```
