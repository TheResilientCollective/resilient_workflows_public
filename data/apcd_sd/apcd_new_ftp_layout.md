#Task:
collect the historic APCD data from an  website: http://jtimmer.digitalspacemail17.net/data/
The files of interest have the naming convention: yesterday_{YYYYmmdd}.CSV eg yesterday_20250502.CSV

# Layout:
The top level will have recent data with the names yesterday_{YYYYmmdd}.CSV
under this, there are yearly/monthly directories: data/2025/Apr/ is 2025 April

The code will need to check to see what data is available, the latest data may be split 
* the latest data is at the top level, starting with yesterday_20250801.CSV
* the this year (eg 2025) is partially 

I would like a code to generate a of files for each year for that date pattern from 2020 


Ouput:
Year,Month,filename,url

I would then like to upload the files into yearly directories in s3 buckets at a path tijuana/sd_apcd_air/raw



