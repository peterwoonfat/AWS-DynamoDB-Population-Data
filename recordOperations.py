from boto3.dynamodb.conditions import Key, Attr


# function adds a new record to the specified table
# item is a set containg attribute-value pairs
def addRecord(table, item):
    #table.put_item(Item=item)
    # get primary key
    keys = list(item.keys())
    primary_key = ''
    if table.name == 'pwoonfat_Country_Details_Table':
        primary_key = 'ISO3'
    elif table.name == 'pwoonfat_Country_Population_Table' or table.name == 'pwoonfat_Country_Economic_Table':
        primary_key = 'Country Name'
    # generate ExpressionAttributeNames, UpdateExpression and ExpressionAttributeValues parameters
    expr_attr_name = {}
    update_expr = 'set '
    expr_attr_vals = {}
    for i in range(1, len(keys)):
        expr_attr_name['#' + str(keys[i])] = str(keys[i])
        update_expr += '#' + str(keys[i]) + '=:val' + str(i) + ', '
        expr_attr_vals[':val' + str(i)] = item[keys[i]]
    update_expr = update_expr[:-2]
    # add item to table
    table.update_item(
        Key={primary_key: item['primary_key']},
        ExpressionAttributeNames=expr_attr_name,
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_attr_vals
    )


# loads the initial country area data into the specified table
def loadArea(table, data):
    for item in data:
        table.update_item(
            Key={'ISO3': item['ISO3'], 'Country Name': item['Country Name']},
            UpdateExpression="set Area=:a",
            ExpressionAttributeValues={
                ':a': item['Area']
            }
        )


# loads the initial country capitals data into the specified table
def loadCapitals(table, data):
    for item in data:
        table.update_item(
            Key={'ISO3': item['ISO3'], 'Country Name': item['Country Name']},
            UpdateExpression="set Capital=:c",
            ExpressionAttributeValues={
                ':c': item['Capital']
            }
        )


# loads the initial country languages data into the specified table
def loadLanguages(table, data):
    for item in data:
        table.update_item(
            Key={'ISO3': item['ISO3'], 'Country Name': item['Country Name']},
            UpdateExpression="set Languages=:l",
            ExpressionAttributeValues={
                ':l': item['Languages']
            }
        )


# loads the initial country un data into the specified table
def loadUn(table, data):
    for item in data:
        table.update_item(
            Key={'ISO3': item['ISO3'], 'Country Name': item['Country Name']},
            ExpressionAttributeNames={'#OfficialName': 'Official Name'},
            UpdateExpression="set #OfficialName=:on, ISO2=:i2",
            ExpressionAttributeValues={
                ':on': item['Official Name'],
                ':i2': item['ISO2']
            }
        )


# loads the initial country population data into the specified table
def loadCurpop(pop_table, eco_table, data):
    for item in data:
        eco_table.update_item(
            Key={'Country Name': item['Country Name']},
            UpdateExpression='set Currency=:c',
            ExpressionAttributeValues={
                ':c': item['Currency']
            }
        )
        # need to choose only specific columns (exclude currency)
        item.pop('Currency')
        pop_table.put_item(Item=item)


# loads the initial country GDP data into the specified table
def loadGDPPC(table, data):
    for item in data:
        keys = list(item.keys())
        expr_attr_name = {}
        update_expr = 'set '
        expr_attr_vals = {}
        # generate UpdateExpression and ExpressionAttributeValues parameters
        for i in range(1, len(keys)):
            expr_attr_name['#' + str(keys[i])] = str(keys[i])
            update_expr += '#' + str(keys[i]) + '=:val' + str(i) + ', '
            expr_attr_vals[':val' + str(i)] = item[keys[i]]
        update_expr = update_expr[:-2]
        table.update_item(
            Key={'Country Name': item['Country Name']},
            ExpressionAttributeNames=expr_attr_name,
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_attr_vals
        )


# function deletes an individual record from the specified table
# key contains a set of attribute-value pairs
def deleteRecord(table, key):
    table.delete_item(key)


# function queries specified table for a Key equal to Value
def queryTable(table, key, value):
    response = table.scan(
        FilterExpression=Attr(key).eq(value)
    )
    return response['Items']


# function queries ranking by counting the number of records with a greater value for the specified key
def queryRanking(table, key, value):
    response = table.scan(
        FilterExpression=Attr(key).gt(value)
    )
    return len(response['Items']) + 1