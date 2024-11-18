import pandas as pd
import requests
import psycopg2
import json
import csv

# extracting bof data

url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"

querystring = {"limit":"2000"}

headers = {
	"x-rapidapi-key": "your_apikey",
	"x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

data = response.json()

# saving the data as a file

filename = 'propertyRecords.json'

# to write the file into the filename
with open (filename, 'w') as file:
    json.dump(data,file, indent = 4)

# to read into a dataframe
propertyrecords_df = pd.read_json('propertyRecords.json')
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


    
#Transforming sale fact dataset and creating an id for the table
sales_fact = propertyrecords_df[['lastSalePrice', 'lastSaleDate']].drop_duplicates().reset_index(drop=True)
sales_fact['sales_id'] = sales_fact.index + 1 # Assign sales_id directly from the index

# Map sales_id back to propertyrecords_df
propertyrecords_df = propertyrecords_df.merge(
    sales_fact[['sales_id', 'lastSalePrice', 'lastSaleDate']],  # Bring sales_id into propertyrecords_df
    on=['lastSalePrice', 'lastSaleDate'],  # Match on shared columns
    how='left'
)

#Extracting Facts columns
fact_columns = ['id', 'sales_id','feature_id','location_id','bedrooms', 'squareFootage','bathrooms', 'lotSize','lastSalePrice','lastSaleDate', 'longitude', 'latitude']
fact_table = propertyrecords_df[fact_columns]

# saving dataset as csv
fact_table.to_csv('property_fact.csv', index=False)
location_dim.to_csv('location_dimension.csv', index = False)
sales_fact.to_csv('sales_facts.csv', index = False)
features_dim.to_csv('features_dimension.csv', index = False)


# connecting to Postgrest database
def get_db_connection():
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
        DROP TABLE IF EXISTS zapco_schema.sales_facts;
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

        CREATE TABLE zapco_schema.sales_facts (
            lastSalePrice NUMERIC, 
            lastSaleDate DATE,
            sales_id INT PRIMARY KEY
        );

        CREATE TABLE zapco_schema.fact_table (
            id NUMERIC PRIMARY KEY, 
            sales_id INT REFERENCES zapco_schema.sales_facts(sales_id) ON DELETE CASCADE,
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

sales_csv_path = '/Users/apple/Desktop/Postgresql_etl/data/sales_facts.csv'
load_data(sales_csv_path, 'zapco_schema.sales_facts',['lastSalePrice','lastSaleDate','sales_id'])
# Define the columns for the fact table (assuming these match your table schema)
fact_columns = ['id', 'sales_id', 'feature_id', 'location_id', 'bedrooms', 'squareFootage', 'bathrooms', 'lotSize', 'lastSalePrice', 'lastSaleDate', 'longitude', 'latitude']

# Load data for the fact table
fact_csv_path = '/Users/apple/Desktop/Postgresql_etl/data/property_fact.csv'
load_data(fact_csv_path, 'zapco_schema.fact_table', fact_columns)

print('congratulations Zapco api successfully extracted and loaded')