# Codac Assignment
## Description 
The aim of the assignment is to prepare data file for the company KommatiPara, which deals with bitcoin tradings. 
Using 2 datasets, which contains client details and their financial details. The company needs to have one dataset including details from the two datasets without personal information and for specific countries.

--------------------------------------------------------
## Input 
For the assignment a user should upload two .csv files.

The first file with client details, consisting of the following columns:

| id | first_name | last_name | email | country |
|----|------------|-----------|-------|---------|

The second file with financial details consisting of the followig columns:

| id | btc_a | cc_t | cc_n | 
|----|-------|------|------|

--------------------------------------------------------
## Modifications
For both datasets applied following modifications:
* Filtered the first dataset by the country column to obtain only records for United Kingdom and Netherlands
* Dropped unneccesary columns which consisted personal informations like first_name and last_name
* Joined both datasets on id column
* Renamed columns, as follows:
    - btc_a -> bitcoin_address
    - cc_t -> credit_card_type
    - id -> client_identifier

--------------------------------------------------------
## Output
The output file contains only id, email, btc_a, cc_t and cc_n columns which are also renamed. The file should look like as follows:

| client identifier | email | country | bitcoin_address | credit card type |
|-------------------|-------|---------|-----------------|------------------|
--------------------------------------------------------

## Example of use
After instalation all of neccessary packages, run the following comand:
```
python3 main.py --file_one ./datasets/dataset_one.csv --file_two ./datasets/dataset_two.csv --countries Netherlands '''United Kingdom'''
```
Explanation:
* _python3_ is a language used to run this script
* _main.py_ is the main file of the application
* _--file_one_ is the argument name for the first dataset file
* _./datasets/dataset_one.csv_ is the path for the fist dataset file
* _--file_two_ is the argument name for the second dataset file
* _./datasets/dataset_two.csv_ is the path for the second dataset file
* _--countries_ is the argument for the list containing country names by which the first dataset will be filtered
* _Netherlands_ is the country by which the first dataset will be filtered
* _'''United Kingdom'''_ is also the country by which the first dataset will be filtered, this value contains three double quotes because of the appearing space.

