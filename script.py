import pandas as pd
import requests
import psycopg2
import json
import csv
import datetime as dt

# extracting bof data

# url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"

# querystring = {"limit":"2000"}

# headers = {
# 	"x-rapidapi-key": "ce3693686amshd8a81ba62ce435ep1113afjsn137d2b9aa172",
# 	"x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
# }

# response = requests.get(url, headers=headers, params=querystring)

# data = response.json()

# saving the data as a file

# filename = 'propertyRecords.json'

# # to write the file into the filename
# with open (filename, 'w') as file:
#     json.dump(data,file, indent = 4)

# to read into a dataframe
propertyrecords_df = pd.read_json('/Users/apple/Desktop/Amdari/Postgresql_etl/propertyRecords.json')
print('file extracted')
#Transforming of the dataset
# approach of cleaning
propertyrecords_df['features'] = propertyrecords_df['features'].apply(json.dumps)

# second approach to find and replace the nulls
propertyrecords_df.fillna({
        'bedrooms': 0,
        'addressLine2': 'Not available', 
        'squareFootage': 0,
        'yearBuilt': 0,
        'features': 'None',
        'assessorID': 'Unknown',
        'legalDescription': 'Not available',
        'subdivision': 'Not available', 
         'zoning': 'Unknown', 
         'bathrooms': 0, 
         'lotSize': 0,
         'propertyType': 'Unknown', 
         'taxAssessment': 'Not available',
        'propertyTaxes':  'Not available', 
         'lastSalePrice': 0,
        'lastSaleDate': 'Not available',
        'owner': 'Unknown',
        'ownerOccupied': 0,
        'addressLine2': 'Unknown',
        'county': 'Not available'}, inplace =True)

# to extract the id from the address
propertyrecords_df['id'] = propertyrecords_df['id'].apply(lambda x: id(x))

# transforming the lastSaleDate to datetime
propertyrecords_df['lastSaleDate'] = propertyrecords_df['lastSaleDate'].replace("Not available", pd.NaT)
#df['date_column'] = pd.to_datetime(df['date_column'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')

propertyrecords_df['lastSaleDate'] = pd.to_datetime(propertyrecords_df['lastSaleDate'],format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')

propertyrecords_df['year'] = propertyrecords_df['lastSaleDate'].dt.year
propertyrecords_df['month'] = propertyrecords_df['lastSaleDate'].dt.month
propertyrecords_df['monthName'] = propertyrecords_df['lastSaleDate'].dt.month_name()
propertyrecords_df['quarter'] = propertyrecords_df['lastSaleDate'].dt.quarter
    
#Transforming location dataset
location_dim = propertyrecords_df[['county','zipCode','formattedAddress','state','city']].drop_duplicates().reset_index(drop=True)
location_dim['location_id'] = location_dim.index +1

propertyrecords_df = propertyrecords_df.merge(
    location_dim[['location_id', 'county','zipCode','formattedAddress','state','city']],  # Bring sales_id into propertyrecords_df
    on=['county','zipCode','formattedAddress','state','city'],  # Match on shared columns
    how='left'
)

#Transforming features dataset and creating an id for the table
features_dim = propertyrecords_df[['features', 'propertyType', 'zoning']].drop_duplicates().reset_index(drop=True)
features_dim['feature_id'] = features_dim.index +1  # Ensure consistent naming

# Merge features_dim into propertyrecords_df
propertyrecords_df = propertyrecords_df.merge(
    features_dim[['feature_id', 'features', 'propertyType', 'zoning']],  # Use 'feature_id' consistently
    on=['features', 'propertyType', 'zoning'],  # Match on shared columns
    how='left'
)


date_dim = propertyrecords_df[['lastSaleDate', 'year', 'month',	'monthName', 'quarter']].drop_duplicates().reset_index(drop=True)
date_dim['date_id'] = date_dim.index +1

propertyrecords_df = propertyrecords_df.merge(
    date_dim[['date_id','lastSaleDate', 'year', 'month',	'monthName', 'quarter']],  # Bring sales_id into propertyrecords_df
    on=['lastSaleDate', 'year', 'month',	'monthName', 'quarter'],  # Match on shared columns
    how='right'
)

#Extracting Facts columns
fact_columns = ['id', 'date_id','feature_id','location_id','bedrooms', 'squareFootage','bathrooms', 'lotSize','lastSalePrice','lastSaleDate', 'longitude', 'latitude']
fact_table = propertyrecords_df[fact_columns]

# saving dataset as csv
features_csv_path = '/Users/apple/Desktop/Amdari/Postgresql_etl/data/features_dimension.csv'
location_csv_path = '/Users/apple/Desktop/Amdari/Postgresql_etl/data/location_dimension.csv'
date_csv_path = '/Users/apple/Desktop/Amdari/Postgresql_etl/data/date_dimension.csv'
fact_csv_path = '/Users/apple/Desktop/Amdari/Postgresql_etl/data/property_fact.csv'




# connecting to Postgrest database
def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database 'Zapco_db' on localhost:5432.

    Returns:
        connection: A connection object to the database.

    Raises:
        Exception: If there's an error connecting to the database.
    """
    try:
        connection = psycopg2.connect(
        host = 'localhost',
        port = 5432,
        user = 'postgres',
        password = 'chichi',
        database = 'Zapco_db')
        
        return connection
        
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        print('database connected succesfully')
        
    

conn = get_db_connection()

# creating the schema and tables

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Corrected SQL statements
    create_table_query = '''
        CREATE SCHEMA IF NOT EXISTS zapco_schema;

        DROP TABLE IF EXISTS zapco_schema.fact_table;
        DROP TABLE IF EXISTS zapco_schema.location_dim;
        DROP TABLE IF EXISTS zapco_schema.date_dim;
        DROP TABLE IF EXISTS zapco_schema.features_dim;

        CREATE TABLE zapco_schema.location_dim (
            county VARCHAR(300),
            zipCode INTEGER,
            formattedAddress VARCHAR(300),
            state VARCHAR(200),
            city VARCHAR(200),
            location_id INTEGER PRIMARY KEY
        );

        CREATE TABLE zapco_schema.features_dim (
            features TEXT, 
            propertyType TEXT,
            zoning  TEXT,
            feature_id INT PRIMARY KEY
        );

        CREATE TABLE zapco_schema.date_dim (
            date_id INT PRIMARY KEY,
            lastSaleDate DATE, 
            year INTEGER, 
            month INTEGER,	
            monthName VARCHAR(100), 
            quarter INTEGER 
        );

        CREATE TABLE zapco_schema.fact_table (
            id NUMERIC PRIMARY KEY, 
            date_id INT REFERENCES zapco_schema.date_dim(date_id) ON DELETE CASCADE,
            feature_id INT REFERENCES zapco_schema.features_dim(feature_id) ON DELETE CASCADE,
            location_id INT REFERENCES zapco_schema.location_dim(location_id) ON DELETE CASCADE,
            bedrooms FLOAT, 
            squareFootage FLOAT,
            bathrooms FLOAT, 
            lotSize FLOAT,
            lastSalePrice FLOAT,
            lastSaleDate DATE, 
            longitude FLOAT, 
            latitude FLOAT
        );
    '''
    
    # Execute query and commit changes
    try:
        cursor.execute(create_table_query)
        conn.commit()
        print("Tables created successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        cursor.close()
        conn.close()
        
create_tables()
    

# uploading dataset into the tables created
def load_data(csv_path, table_name, fact_columns):
    """
    Load the data from a given CSV file into a table in the database.

    Args:
        csv_path (str): The path to the CSV file containing the data.
        table_name (str): The name of the table to load the data into.
        fact_columns (list[str]): The names of the columns in the table, in the order they appear in the CSV file.

    Returns:
        None
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            # Replace 'Not available' or empty string with None for lastSaleDate column
            row = [None if (cell == ' ' or cell == 'Not available') and col_name == 'lastSaleDate' else cell for cell, col_name in zip(row, fact_columns)]
            
            # Prepare placeholders for the insert statement
            placeholders = ', '.join(['%s'] * len(row))
            query = f'INSERT INTO {table_name} ({", ".join(fact_columns)}) VALUES ({placeholders});'
            
            # Execute the query with the row data
            cursor.execute(query, row)

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection properly
    cursor.close()
    conn.close()

# Load data for the features table
features_csv_path = '/Users/apple/Desktop/Postgresql_etl/data/features_dimension.csv'
load_data(features_csv_path, 'zapco_schema.features_dim', ['features', 'propertyType', 'zoning', 'feature_id'])

# Load data for the location table
location_csv_path = '/Users/apple/Desktop/Postgresql_etl/data/location_dimension.csv'
load_data(location_csv_path, 'zapco_schema.location_dim', ['county', 'zipCode', 'formattedAddress', 'state', 'city', 'location_id'])

date_csv_path = '/Users/apple/Desktop/Postgresql_etl/data/date_dimension.csv'
load_data(date_csv_path, 'zapco_schema.sales_facts',['lastSalePrice','lastSaleDate','sales_id'])
# Define the columns for the fact table (assuming these match your table schema)
fact_columns = ['id', 'date_id', 'feature_id', 'location_id', 'bedrooms', 'squareFootage', 'bathrooms', 'lotSize', 'lastSalePrice', 'lastSaleDate', 'longitude', 'latitude']

# Load data for the fact table
fact_csv_path = '/Users/apple/Desktop/Postgresql_etl/data/property_fact.csv'
load_data(fact_csv_path, 'zapco_schema.fact_table', fact_columns)

print('congratulations Zapco api successfully extracted and loaded')