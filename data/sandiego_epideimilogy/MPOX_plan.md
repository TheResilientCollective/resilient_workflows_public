# Mpox workflow
The San Diego Epidemilogy has public data in a tableau notebook at: https://public.tableau.com/workbooks/MPX.twb
The metadata is at: https://public.tableau.com/profile/api/single_workbook/MPX
## step 1: Download the workbook and store in s3 using the utils/store_assets class

## Step 2: extract the hyperfiles
There are three workbooks, and we want to use These: 'MPXV Disease Summary2', 'Demographics3 (MPXV Disease Summary)'
Extract the .hyper and store in s3 using the utils/store_assets class

## Step 3: transform the hyper files to data files
for each workbook read the hyper file, store the output as CSV and Json and should use the utils/store_assets class
'MPXV Disease Summary' transform the output to the specfied columns.

### MPXV Disease Summary
for the workbook 'MPXV Disease Summary2'
extract the data from the hyper file, and store
Then 
There are two columns that need to be used in the dataset:
* Date
* Count

The output should be:
- Jurisdiction: SanDiego
- date_week_start: Date from dataset
- date_week_end: Date + Seven Days
- Week_Number: Week Number of Date
- Year: Year of Date
- Week_Year: f'{Week_Number}-{Year}'
- Cases: Case from dataset



### 'Demographics3 (MPXV Disease Summary)'
This should just be downloaded, and transformed to csv and json

# Workflow implementation 
There is an already existing Tableau file to asset workflow in workflows/public/public/assets/sandiego_epidemiology.py
I would like to create a similar workflow, and perhaps update the workflows/public/public/assets/sandiego_epidemiology.py to be generic, if it already is no

I would like to create a workflow with the name: sandiego_epidemiology_mox.py in the workflows/public/public/ dagster
