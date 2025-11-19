in the workflows/public/public/utils/resilient_epi_schemas.py I would lke to create a detailed_epidelilogy_format for the WAHIS
so I can reformat the wahis_excel parquet file
the parquet file is data/woah/infur_test.parquet

each 'outbreak' has a Report_id that links the reports together. Sort by Report_id then Outbreak_start_date
* Outbreak_start_date == date_period_start
* the output field date_period_end is the next reports Outbreak_start_date, if an additional report exists, or Outbreak_end_date if the report is the last one
* Location_name == jurisdiction
* ('susceptible	cases' , 'dead',	'killed_disposed',	'slaughtered',	'vaccinated',	'morbidity'	,'mortality') are additional columns
* 'country'	and 'region'
* Location_aprox is a boolean field indicating if a location is approximate
* disease_eng is a string in english containing the pathogen
* 'reason of notification' is a categorical field
* 'Species' and 	'quantitative_unit' are categorical fields
* 'Longitude'	'Latitude' columns are  decimal degrees, and to be included, and used to create a geometry column containing a POINT
* add Week_Number, Year, and Week_Year columns based on the Outbreak_start_date
