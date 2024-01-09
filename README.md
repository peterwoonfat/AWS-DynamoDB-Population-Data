# AWS-DynamoDB-Population-Data
The program connects to and interacts with AWS Dynamodb using the Boto3 SDK.

## Description
The program allows the user to create tables and load them with data provided from csv files (In this example, the csv files are hard-coded). Additionally, the user has the option to provide additional information to update existing tables with. Lastly, the user can generate a country-level report that details non-economic information, population information, and economic information about the specified country.
Program waits until table creation and deletion operations finish before continuing.

## Usage
command to run the shell:<br>
    python app.py<br>
app.py imports functions from tableOperations.py and recordOperations.py, it also pulls AWS login credentials from S5-S3.conf; these files must be in the same directory for the program to function properly.<br>
The AWS region has been specified in S5-S3.conf as the location, the default is the Canada server (ca-central-1).<br>
The console displays a series of actions available to the user, each action corresponding to a number that the user can input to carry out said action:<br>
    1: Initializes 3 tables - pwoonfat_Country_Details_Table, pwoonfat_Country_Population_Table, and pwoonfat_Country_Economic_Table (economic and non-economic data are kept in separate tables even if they originated from the same csv file, for example in shortlist_curpop the population fields are loaded into pwoonfat_Country_Population_Table and the Currency field is loaded into pwoonfat_Country_Economic_Table).<br>
    2: Reads the data in short_area.csv, shortlist_capitals.csv, shortlist_curpop.csv, shortlist_gdppc.csv, shortlist_languages.csv, and un_shortlist.csv files. The data is then loaded into the appropiate tables data with the same primary key, "Country Name" or "Country", are joined into a single record per table.<br>
    3: Displays the data in a specified table or all tables. Each table is displayed as a list of dictionaries in the console.<br>
    4: Deletes the specified table or all tables.<br>
    5: Prompts the user for any missing information to update existing tables with. Missing information is entered via the console.<br>
    6: Generates a country level report for a specified country. Report is displayed in the console and is formatted as shown in the Assginment2_Description.pdf.<br>
    7: Exits the program.<br>

## Assumptions and Limitations
The program changes the "Country" field in shortlist_curpop and shortlist_gdppc to "Country Name". The same is also done to "Common Name" field in un_shortlist. This change was made so that there could be a uniform key across all tables, making joins possible. The rest of the field names are left as specified in the original files.
Functionality to generate global reports was not added.
The program has an error when generating country level reports for countries that don't have population data for each year.
