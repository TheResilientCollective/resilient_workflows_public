You want to convert concepts from the notebook workflows/public/notebooks/sandiego_epidemiology_extraction.ipynb 
to a dagster asset.

you want to encapsulate the tableau workbook processing into a utility class
where the unzipping, extraction happens, and a dataframe is returned.

The asset file should be named sandiego_epidemiology.py.
Ignore the previous sandiego_epidemiology assets and start from scratch.

Asset one: download the tableau workbook from the url and store in s3. 
Asset two: extract the hyper file from the workbook and store in s3. 
Asset three: convert the hyper file to a dataframe and process the dataframe and store using the utility functions. 

