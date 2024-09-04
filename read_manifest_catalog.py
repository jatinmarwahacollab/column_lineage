import json
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# Step 1: Load JSON Files
def load_manifest(file_path):
    with open(file_path, 'r') as file:
        manifest = json.load(file)
    return manifest.get('nodes', {})

def load_catalog(file_path):
    with open(file_path, 'r') as file:
        catalog = json.load(file)
    return catalog.get('nodes', {})

def build_dataframe_from_manifest(nodes, catalog_nodes):
    data = []

    # Build initial table and column list from catalog.json
    for node_key, node_info in catalog_nodes.items():
        table_name = node_key
        columns = node_info.get('columns', {})
        for column_name in columns: 
            # Unique key creation
            database = node_info.get('metadata', {}).get('database', '')
            schema = node_info.get('metadata', {}).get('schema', '')
            unique_key = f"{database}.{schema}.{table_name}.{column_name}"

            # Append data to list with new columns
            data.append({
                'unique_key': unique_key,
                'database': database,
                'schema': schema,
                'table_name': table_name,
                'column_name': column_name,
                'resource_type': '',  # Initialize as empty
                'name': '',  # Initialize as empty
                'sql': '',  # Initialize as empty
                'reference': '',  # Initialize as empty
                'column_description': '' # Initialize as empty
            })

    # Enrich data with information from manifest.json
    for node_key, node_info in nodes.items():
        table_name = node_key  # Use the full node_key from manifest.json
        resource_type = node_info.get('resource_type', '')
        name = node_info.get('name', '')
        sql = node_info.get('raw_code', '')
        refs = node_info.get('refs', [])
        columns = node_info.get('columns', {}) # Get columns from manifest

        table_name_lower = table_name.lower()  # Convert once for comparison

        for item in data:
            # Perform comparison without modifying original data
            if item['table_name'].lower() == table_name_lower:
                item['resource_type'] = resource_type
                item['name'] = name
                item['sql'] = sql

                # Find column description in manifest
                for column_key, column_info in columns.items():
                    if item['column_name'].lower() == column_key.lower():
                        item['column_description'] = column_info.get('description', '')

                # Prepare reference information
                reference_info = []
                for ref in refs:
                    ref_name = ref.get('name', '')
                    if ref_name:
                        ref_columns = catalog_nodes.get(f"model.jaffle_shop.{ref_name}", {}).get('columns', {})
                        for ref_column_name, ref_column_info in ref_columns.items():
                            ref_column_description = ref_column_info.get('description', '')
                            reference_info.append(f"{ref_name}.{ref_column_name}: {ref_column_description}")

                item['reference'] = ', '.join(reference_info)

    df = pd.DataFrame(data)
    return df


# Step 3: Connect to Snowflake and Load Data
def connect_to_snowflake():
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('user'),
        password=os.getenv('password'),
        account=os.getenv('account'),
        warehouse=os.getenv('warehouse'),
        database=os.getenv('database'),
        schema=os.getenv('schema'),
        role=os.getenv('role')
    )
    return conn

def insert_data_to_snowflake(conn, df):
    # Truncate the existing table
    truncate_table_query = "TRUNCATE TABLE LINEAGE_DATA.COLUMN_LINEAGE;"

    # Insert data into Snowflake table
    insert_query = """
    INSERT INTO JAFFLE_LINEAGE.lineage_data.column_lineage (unique_key, database, schema, table_name, column_name, column_description, resource_type, name, sql, reference)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Get environment variables
    warehouse = os.getenv('warehouse')
    database = os.getenv('database')
    schema = os.getenv('schema')  
    
    cursor = conn.cursor()
    # Use environment variables in SQL commands
    cursor.execute(f"USE WAREHOUSE {warehouse};")  # Explicitly set the warehouse
    cursor.execute(f"USE DATABASE {database};")
    cursor.execute(f"USE SCHEMA {schema};")

    # Truncate the table before inserting
    cursor.execute(truncate_table_query)

    # Convert DataFrame to list of tuples for insertion
    data_to_insert = df[['unique_key', 'database', 'schema', 'table_name', 'column_name',
                         'column_description', 'resource_type', 'name', 'sql', 'reference']].values.tolist()
    cursor.executemany(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

# Main Function to Execute the Process
def main():
    # Load the manifest and catalog files
    manifest_path = 'manifest.json'  # Replace with your manifest.json path
    catalog_path = 'catalog.json'    # Replace with your catalog.json path

    nodes = load_manifest(manifest_path)
    catalog_nodes = load_catalog(catalog_path)

    # Build the DataFrame
    df = build_dataframe_from_manifest(nodes, catalog_nodes)

    # Connect to Snowflake
    conn = connect_to_snowflake()

    # Insert data into Snowflake
    insert_data_to_snowflake(conn, df)

    # Close Snowflake connection
    conn.close()

    print("Data has been successfully inserted into Snowflake.")

if __name__ == "__main__":
    main()
