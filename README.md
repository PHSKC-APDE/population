# King County Population Estimate Data Processing
This README describes the folder and file layout of the population estimates received from the Office of Financial Management (OFM) and how to use an R package to store the data in our SQL database.

## The Data
The data from OFM is posted on the https://sft.wa.gov/ file share site. An analyst will send an email with login information to download the files twice a year. In the network folder \\phdata01\DROF_DATA\DOH DATA\POP\data\raw, create a new folder for the new batch of data with the date the data is received and the data's status (preliminary or revised). 
- Use the following folder naming layout: YYYMMDDstatus
- Example: 20190722revised

Place all of the files in the new folder. Then, create a "files_to_load" folder and move all of the zipped (compressed) csv files to that folder. These files should look like "csv_2016.zip" and be filled with many .csv files. Overwrite the preliminary data with revised data. 

## The R Package Process
1. Review the config/common.pop.yaml file. Set the path_tmp to a local or network temporary folder (local is typically faster). This folder will be used to unzip the files and temporarily store the .csv files.
2. Open the main_etl.R file. Set the in_geo_types list to determine which geo_types the script will load. Set the min_year variable to determine the earliest year's data that will be loaded.
3. Run all (CTRL+ALT+R)



