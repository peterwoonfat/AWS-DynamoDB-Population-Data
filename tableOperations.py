from __future__ import print_function


# function initializes all tables with their schemas
def initializeTables(db_res):
    # can only specify one hash key (IS03) and one range key max (Country_Name)
    # create a table for general country details data from shortlist_area.csv, shortlist_capitals.csv, shortlist_languages.csv, and un_shortlist.csv
    country_details_params = {
        'TableName': 'pwoonfat_Country_Details_Table',
        'KeySchema': [
            {'AttributeName': 'ISO3', 'KeyType': 'HASH'},
            {'AttributeName': 'Country Name', 'KeyType': 'RANGE'}
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'ISO3', 'AttributeType': 'S'},
            {'AttributeName': 'Country Name', 'AttributeType': 'S'}
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    }
    createTable(db_res, country_details_params)

    # create a table for population data from shortlist_curpop.csv
    country_population_params = {
        'TableName': 'pwoonfat_Country_Population_Table',
        'KeySchema': [
            {'AttributeName': 'Country Name', 'KeyType': 'HASH'},
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'Country Name', 'AttributeType': 'S'},
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    }
    createTable(db_res, country_population_params)
    
    # create a table for economic data from shortlist_gdppc.csv
    # includes country name, currency, and gdp for each year
    country_economic_params = {
        'TableName': 'pwoonfat_Country_Economic_Table',
        'KeySchema': [
            {'AttributeName': 'Country Name', 'KeyType': 'HASH'},
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'Country Name', 'AttributeType': 'S'},
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    }
    createTable(db_res, country_economic_params)
    


# function creates a table with the specified parameters
def createTable(db_res, table_params):
    try:
        table = db_res.create_table(**table_params)
        print("Table status: ", table.table_status, table.table_name)
        table.wait_until_exists()
    except:
        print("Error: couldn't create table.")


# function deletes the specified table
def deleteTable(table):
    table.delete()
    print("Table status: ", table.table_status, table.table_name)
    table.wait_until_not_exists()


# function returns a list of all existing tables
def getTables(db_res):
    return list(db_res.tables.all())


# function displays all records in the specified table
def displayTable(table):
    data = getTableData(table)
    print(table.name + ":\n" + str(data))


# function returns all records in the specified table
def getTableData(table):
    response = table.scan()
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return data