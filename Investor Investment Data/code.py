import pandas as pd
import psycopg2

def getColumnDtypes(dataTypes):
    '''
    Function to create list of data types
    required for database table
    '''
    dataList = []
    for x in dataTypes:
        if(x == 'int64'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar')
    return dataList

# Reaf csv files into dataframe
investorDF = pd.read_csv('investor.csv')
investmentDF = pd.read_csv('investment.csv')

# Merge operation on dataframes
resultDF = investorDF.merge(investmentDF)

# Collect column names into a list
columnName = list(resultDF.columns.values)

# Collect column data types into a list
columnDataType = getColumnDtypes(resultDF.dtypes)

# Code for create table statement
createTableStatement = 'CREATE TABLE IF NOT EXISTS investordetails ('
for i in range(len(columnDataType)):
    createTableStatement = createTableStatement + '\n' + columnName[i] + ' ' + columnDataType[i] + ','
createTableStatement = createTableStatement[:-1] + ' );'

# Connect to database server to run create table statement
conn = psycopg2.connect(dbname='testdb', host='abc.xyz.com', port='xxxx', user='mydbuser', password='xxxxxxxx')
cur = conn.cursor()
cur.execute(createTableStatement)
conn.commit()