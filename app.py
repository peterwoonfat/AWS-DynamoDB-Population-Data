import boto3
import configparser
import pandas as pd
import csv
import tableOperations as tableFuncs
import recordOperations as recordFuncs


# function reads from .csv files and stores data in data frames
def getData():
    # store data in csv files as a dictionary of lists
    data = {}

    # read data from shortlist_area.csv
    area_data = []
    with open('shortlist_area.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            area_data.append(row)
    data["area_data"] = area_data

    # read data from shortlist_capitals.csv
    capitals_data = []
    with open('shortlist_capitals.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            capitals_data.append(row)
    data["capitals_data"] = capitals_data

    # read data from shortlist_curpop.csv
    curpop_data = []
    with open('shortlist_curpop.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            row_filtered = {}
            for key in row.keys():
                if 'Country' in key:
                    row_filtered['Country Name'] = row[key]
                elif 'Population' in key:
                    row_filtered[key.split(" ")[1]] = row[key]
                else:
                    row_filtered[key] = row[key]
            curpop_data.append(row_filtered)
    data["curpop_data"] = curpop_data

    # read data from shortlist_gdppc.csv
    gdppc_data = []
    with open('shortlist_gdppc.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            row_filtered = {}
            for key in row.keys():
                if 'Country' in key:
                    row_filtered['Country Name'] = row[key]
                else:
                    row_filtered[key] = row[key]
            gdppc_data.append(row_filtered)
    data["gdppc_data"] = gdppc_data

    # read data from shortlist_languages.csv
    # if number of fields > number of field names then store additional field data in the Languages field
    languages_data = []
    with open('shortlist_languages.csv', 'r') as file:
        reader = csv.DictReader(file, restkey='Additional Languages')
        for row in reader:
            if row['Languages'] != 'Languages':
                languages_list = []
                languages_list.append(row['Languages'])
                if 'Additional Languages' in row:
                    for lang in row['Additional Languages']:
                        languages_list.append(lang)
                    row.pop('Additional Languages')
                row['Languages'] = languages_list
            languages_data.append(row)
    data["languages_data"] = languages_data

    # read data from un_shortlist.csv
    un_data = []
    un_data_headers = ['ISO3', 'Country Name', 'Official Name', 'ISO2']
    with open('un_shortlist.csv', 'r') as file:
        reader = csv.DictReader(file, fieldnames=un_data_headers)
        for row in reader:
            un_data.append(row)
    data["un_data"] = un_data

    return data


# function loads intial data from csv files into DynamoDB tables
def loadData(db_res, data):
    tables = tableFuncs.getTables(db_res)
    # call functions to load data into a table
    for d in data:
        if d == 'area_data':
            for table in tables:
                if table.name == 'pwoonfat_Country_Details_Table':
                    recordFuncs.loadArea(table, data[d])
        elif d == 'capitals_data':
            for table in tables:
                if table.name == 'pwoonfat_Country_Details_Table':
                    recordFuncs.loadCapitals(table, data[d])
        elif d == 'languages_data':
            for table in tables:
                if table.name == 'pwoonfat_Country_Details_Table':
                    recordFuncs.loadLanguages(table, data[d])
        elif d == 'un_data':
            for table in tables:
                if table.name == 'pwoonfat_Country_Details_Table':
                    recordFuncs.loadUn(table, data[d])
        elif d == 'curpop_data':
            for table in tables:
                if table.name == 'pwoonfat_Country_Population_Table':
                    pop_table = table
                if table.name == 'pwoonfat_Country_Economic_Table':
                    eco_table = table
            recordFuncs.loadCurpop(pop_table, eco_table, data[d])
        elif d == 'gdppc_data':
            for table in tables:
                if table.name == 'pwoonfat_Country_Economic_Table':
                    recordFuncs.loadGDPPC(table, data[d])


# function adds records into existing tables
def addInfo(db_res):
    num_records = int(input('Enter the number of records you want to add/update: '))
    for i in range(num_records):
        table_name = input('Enter the table to update: ')
        for table in tableFuncs.getTables(db_res):
            if table.name == table_name:
                table_to_update = table
                break
        primary_key = input('Enter the primary key value: ')
        fields = int(input('Enter the number of fields to add/update: '))
        item = {'primary_key': primary_key}
        for j in range(fields):
            field_name = input('Enter the field name: ')
            value = input('Enter the value: ')
            item[field_name] = value
        recordFuncs.addRecord(table_to_update, item)


# function generates a country level report including general details, population details, and economic details for the specified country
def generateCountryLevelReport(db_res):
    country = input('Enter the name of the country to generate the report for: ')
    # get data from existing tables
    pop_table = None
    eco_table = None
    noneco_table = None
    pop_data = []
    noneco_data = []
    eco_data = []
    for table in tableFuncs.getTables(db_res):
        if table.name == 'pwoonfat_Country_Population_Table':
            pop_data = tableFuncs.getTableData(table)
            pop_table = table
        elif table.name == 'pwoonfat_Country_Economic_Table':
            eco_data = tableFuncs.getTableData(table)
            eco_table = table
        elif table.name == 'pwoonfat_Country_Details_Table':
            noneco_data = tableFuncs.getTableData(table)
            noneco_table = table
    # check if valid country
    for record in noneco_data:
        if country == record['Country Name']:
            # calculate rankings
            displayNoneconomicData(noneco_table, country, noneco_data)
            displayPopulationData(pop_table, noneco_table, country, pop_data, noneco_data)
            displayGDPData(eco_table, country, eco_data)
            return
    print('Invalid country.')


# function generates the area ranking of the country and displays non-economic data for the country
def displayNoneconomicData(table, country, data):
    # find the data for only the specified country
    country_data = {}
    for record in data:
        if record['Country Name'] == country:
            country_data = record
            break
    # find the area ranking for the country
    country_data_keys = country_data.keys()
    area_ranking = None
    for key in country_data_keys:
        if key == 'Area':
            area_ranking = recordFuncs.queryRanking(table, key, country_data[key])
            break
    # display country's noneconomic data
    country_noneco_data = recordFuncs.queryTable(table, 'Country Name', country)
    country_noneco_data = country_noneco_data[0]
    # print country's non-economic details
    print(country)
    print('[Official Name: ' + country_noneco_data['Official Name'] + ']')
    print('Area: ' + country_noneco_data['Area'] + ' sq km (rank ' + str(area_ranking) + ')')
    languages_str = 'Official/National Languages: '
    for language in country_noneco_data['Languages']:
        languages_str += language + ', '
    print(languages_str[:-2])
    print('Capital City: ' + country_noneco_data['Capital'] + '\n')
    

def displayPopulationData(pop_table, noneco_table, country, pop_data, noneco_data):
    # find the data for only the specified country
    country_data = {}
    for record in pop_data:
        if record['Country Name'] == country:
            country_data = record
            break
    # get country's area
    country_area = None
    for record in noneco_data:
        if record['Country Name'] == country:
            country_area = record['Area']
    # loop through keys for population at each year and query table for all values in the column
    country_data_keys = country_data.keys()
    print('Population\nTable of Population, Population Density, and their respective world ranking for that year, ordered by year:')
    print('Year\tPopulation\tRank\tPopulation Density (people/sq km)\tRank')
    for key in country_data_keys:
        if key.isnumeric():
            pop_ranking = recordFuncs.queryRanking(pop_table, key, country_data[key])
            # calculate population density ranking
            pop_density = int(country_data[key]) / int(country_area)
            popdensity_ranking = 1
            for other_country in noneco_data:
                if other_country['Country Name'] != country and key in other_country.keys():
                    for other_country_pop in pop_data:
                        if other_country_pop['Country Name'] == other_country['Country Name']:
                            other_popdensity = int(other_country_pop[key]) / int(other_country['Area'])
                            if other_popdensity > pop_density:
                                popdensity_ranking += 1
            print(key + '\t' + country_data[key] + '  \t' + str(pop_ranking) + '\t' + str(round(pop_density, 2)) + '\t\t\t\t\t' + str(popdensity_ranking))


# function generates the GDP rankings for the specified country each year and displays the country's economic data
def displayGDPData(table, country, data):
    # find the data for only the specified country
    country_data = {}
    for record in data:
        if record['Country Name'] == country:
            country_data = record
            break
    # loop through keys for GDP at each year and query table for all values in the column
    country_data_keys = country_data.keys()
    # find earliest and latest years recorded for the specified country
    earliest_year = None
    latest_year = None
    for key in country_data_keys:
        if key.isnumeric():
            if earliest_year == None:
                earliest_year = key
            elif int(key) < int(earliest_year):
                earliest_year = key
            if latest_year == None:
                latest_year = key
            elif int(key) > int(latest_year):
                latest_year = key
    print('Economics\nCurrency: ' + country_data['Currency'] + '\nTable of GDP per capita (GDPCC) from ' + earliest_year + ' to ' + latest_year + ' and rank within the world for that year')
    print('Year\t\tGDPPC\t\tRank')
    for key in country_data_keys:
        if key.isnumeric():
            gdp_ranking = recordFuncs.queryRanking(table, key, country_data[key])
            print(key + '\t\t' + str(country_data[key]) + '\t\t' + str(gdp_ranking))

 

if __name__ == "__main__":
    # authenticate user using credentials in S5-S3.conf
    config = configparser.ConfigParser()
    config.read("S5-S3.conf")
    aws_access_key_id = config['default']['aws_access_key_id']
    aws_secret_access_key = config['default']['aws_secret_access_key']
    region_name = config['default']['region_name']

    try:
        # establish an AWS session
        session = boto3.Session(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            region_name=region_name
        )
        # set up client and resources
        db = session.client("dynamodb")
        db_res = session.resource("dynamodb")
        print ("Welcome to the AWS DynamoDB Shell\nYou are now connected to your database")
    except:
        print ("Welcome to the AWS DynamoDB Shell\nYou could not connected to your database")

    # loop to get user input
    while(True):
        # prompt user for desired action
        print("-----------------------------------------\n1) Initialize tables\n2) Read and load data from CSV files\n3) Display data in table(s)\n4) Delete table(s)\n5) Add missing information to tables\n6) Generate a country level report\n7) Exit program\n-----------------------------------------")
        action = input('Enter a number: ')
        if action == '1':
            # initialize tables with schemas
            tableFuncs.initializeTables(db_res)
            print('--FINISHED INITIALIZING TABLES--')
        elif action == '2':
            # read csv files to get data
            data = getData()
            # load data from csv files into the initialized DynamoDB tables
            loadData(db_res, data)
            print('--FINISHED LOADING DATA--')
        elif action == '3':
            # prompt user for table to display
            tables = tableFuncs.getTables(db_res)
            tables_readable = ''
            tables_list = []
            for table in tables:
                tables_readable += table.name + ', '
                tables_list.append(table.name)
            print('Current tables: ' + tables_readable[:-2])
            table_to_display = input('Enter the name of the table to display (enter "all" to display all tables): ')
            if table_to_display.casefold() == 'all'.casefold():
                # display data in all tables
                for table in tables:
                    tableFuncs.displayTable(table)
            elif table_to_display in tables_list:
                # display data in specified table
                for table in tables:
                    if table.name == table_to_display:
                        tableFuncs.displayTable(table)
            else:
                # invalid input for table
                print('Invalid table.')
                continue
        elif action == '4':
            # prompt user for table to delete
            tables = tableFuncs.getTables(db_res)
            tables_readable = ''
            tables_list = []
            for table in tables:
                tables_readable += table.name + ', '
                tables_list.append(table.name)
            print('Current tables: ' + tables_readable[:-2])
            table_to_delete = input('Enter the name of the table to delete (enter "all" to delete all tables): ')
            if table_to_delete.casefold() == 'all'.casefold():
                # delete all created tables when finishing session
                for table in tableFuncs.getTables(db_res):
                    tableFuncs.deleteTable(table)
            elif table_to_delete in tables_list:
                # delete specified table
                for table in tables:
                    if table.name == table_to_delete:
                        tableFuncs.deleteTable(table)
            else:
                # invalid input for table
                print('Invalid table.')
                continue
            print('--FINISHED DELETING TABLE--')
        elif action == '5':
            # manually add new record or update an existing record
            if len(tableFuncs.getTables(db_res)) == 0:
                print('There are currently no tables to update.')
                continue
            addInfo(db_res)
        elif action == '6':
            # generate a country level report
            generateCountryLevelReport(db_res)
        elif action == '7':
            # exit the program
            print('Closing program...')
            break
        else:
            # user entered invalid number
            print('Invalid input - please enter a number between 1 and 5 based on your desired action.')