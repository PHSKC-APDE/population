# King County Population Estimate Data Processing
This README describes the folder and file layout of the population estimates received from the Office of Financial Management (OFM) and how to use an R package to store the data in our SQL database.

## The Data
The data from OFM is posted on the https://sft.wa.gov/ file share site. An analyst will send an email with login information to download the files twice a year. In the network folder \\phdata01\DROF_DATA\DOH DATA\POP\data\raw, create a new folder for the new batch of data with the date the data is received and the data's status (preliminary or revised). 
- Use the following folder naming layout: YYYMMDDstatus
- Example: 20190722revised

Place all of the files in the new folder. Then, create a "files_to_load" folder and move all of the zipped (compressed) csv files to that folder. These files should look like "csv_2016.zip" and be filled with many .csv files. Overwrite the preliminary data with revised data. 

## The Database Tables
The data is stored in the PH_APDEStore database in the ref.pop and ref.pop77 tables. ref.pop77 includes data with an r_type of 77 (race classifications from before 1997) while ref.pop includes data with an r_type of 97 (race classifications from 1997 to present). Up to two previous versions of each geo_type and year's data is stored in archive.pop and archive.pop77. There are raw tables that are created and deleted during data processing (raw.pop and raw.pop97). 

The these tables have data listed by geography type (Block, Zip Code, School District, etc), scope (all of WA or King, Pierce and Snohomish county, KPS), year and census year. It is further split out by age, gender, race groups and geo id (Example: a Zip Code).

For further explination of the column headers and values, look at the ref.pop_crosswalk and ref.pop_labels tables for reference.

To keep track of all of the files loaded into the database, review the metadata.pop_etl_log table. This lists the etl id, batch name, batch data, file name, geography type, scope, year, census year and r_type. It also keeps track of the following:
- The number of rows in the file
- The number of rows loaded into the database 
- The number of rows kept in the database (data is removed if it is outside the designated geo scope)
- The total population of the rows loaded into the database
- The total population of the rows kept in the database
- The time and date of when the file was loaded and deleted from the raw, ref and archive tables
- The etl notes (this lists the entire data processing log for the file)
- The time and date the last time the data for this file was updated

The pop_etl_log.sql file has a number of queries to help review the data processing and quickly see which files are in which tables.

## R Package Instructions
1. Review the config/common.pop.yaml file. Set the path_tmp to a local or network temporary folder (local is typically faster). This folder will be used to unzip the files and temporarily store the .csv files.
2. Open the main_etl.R file. Set the in_geo_types list to determine which geo_types the script will load. Set the min_year variable to determine the earliest year's data that will be loaded.
3. Run all (CTRL+ALT+R)

## R Package Process
1. A list of zipped files is created
2. The contents of the first zipped file is reviewed. This determines what files have not already been loaded into the database

