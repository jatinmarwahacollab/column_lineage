import hashlib
import pandas as pd
import snowflake.connector
import openai
import os
from dotenv import load_dotenv


load_dotenv()

# Set up your OpenAI API key
openai.api_key = os.getenv('openai_api_key')  # Replace with your OpenAI API key

# Step 1: Connect to Snowflake
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

# Step 2: Load Data from Snowflake
def load_data_from_snowflake(conn):
    query_lineage = "SELECT * FROM COLUMN_LINEAGE"
    query_lineage_genai = "SELECT * FROM COLUMN_LINEAGE_GENAI"

    df_lineage = pd.read_sql(query_lineage, conn)
    df_lineage_genai = pd.read_sql(query_lineage_genai, conn)

    # Filter out rows where reference column is null
    df_lineage = df_lineage[df_lineage['REFERENCE'].notna()]

    return df_lineage, df_lineage_genai

# Step 3: Send Prompt to OpenAI for Lineage Information
def get_column_lineage_from_openai(table_name, column_name, reference, sql):
    # Construct the prompt for OpenAI
    prompt = (
        f"Analyze the following column and determine its most likely upstream column(s) based on the provided information. "
        f"You must parse the SQL code and use your reasoning to find the primary source table for the column of interest.\n\n"
        f"Model: {table_name}\n\n"
        f"Column Name: {column_name}\n\n"
        f"Upstream Models and their columns: {reference}\n\n"
        f"SQL Code used to build the table:\n{sql}\n\n"
        f"Important Instructions:\n"
        f"- Carefully parse the SQL code to determine which table the column of interest is sourced from.\n"
        f"- Focus on identifying the primary source table by analyzing the joins, where clauses, and select statements.\n"
        f"- If the same column name appears in multiple upstream tables, determine the true source by examining which table it was originally selected from or joined on.\n"
        f"- If the column is not calculated, it should have a single source table. Use this knowledge to deduce the correct lineage.\n"
        f"- If a column is present in multiple upstream tables, consider typical naming conventions, data transformations, and table relationships to identify the most likely source.\n\n"
        f"Your response should adhere to the following format:\n"
        f"Response Format should be like this: Upstream Column(s): [list the most likely upstream Column(s)], Upstream Table(s): [Table related to Upstream column], Reasoning: [Short one-liner transformation rule which is applied on this column, if no transformation then its a one to one mapping.]"
    )

    # Send the prompt to OpenAI
    response = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ],
        max_tokens=1000,
        temperature=0.5
    )

    response_text = response.choices[0].message.content.strip()
    print(f"Response received from LLM:\n{response_text}\n")  # Print the response from the LLM

    return response_text

# Step 4: Process and Insert/Update Records
def process_and_update_records(conn, df_lineage, df_lineage_genai):

    # Get environment variables
    warehouse = os.getenv('warehouse')
    database = os.getenv('database')
    schema = os.getenv('schema')

    cursor = conn.cursor()
    # Use environment variables in SQL commands
    cursor.execute(f"USE WAREHOUSE {warehouse};")  # Explicitly set the warehouse
    cursor.execute(f"USE DATABASE {database};")
    cursor.execute(f"USE SCHEMA {schema};")



    # Set index for df_lineage_genai
    df_lineage_genai = df_lineage_genai.set_index('UNIQUE_KEY', drop=False)

    df_lineage = df_lineage.set_index('UNIQUE_KEY', drop=False)

    # Identify records to insert (UNIQUE_KEY not present in df_lineage_genai)
    new_records = df_lineage[~df_lineage['UNIQUE_KEY'].isin(df_lineage_genai.index)]

    # Compute MD5 hash of the SQL column for df_lineage before using it
    df_lineage['SQL_MD5'] = df_lineage['SQL'].apply(
      lambda x: hashlib.md5(x.encode()).hexdigest() if pd.notnull(x) else ''
    )

    df_lineage_genai['SQL_MD5'] = df_lineage_genai['SQL'].apply(
      lambda x: hashlib.md5(x.encode()).hexdigest() if pd.notnull(x) else ''
    )

    # Ensure both UNIQUE_KEY columns are strings for consistent comparison
    df_lineage['UNIQUE_KEY'] = df_lineage['UNIQUE_KEY'].astype(str)
    df_lineage_genai['UNIQUE_KEY'] = df_lineage_genai['UNIQUE_KEY'].astype(str)

    # Now you can proceed with identifying records to update
    common_keys = df_lineage['UNIQUE_KEY'][
      df_lineage['UNIQUE_KEY'].isin(df_lineage_genai.index)
    ]
    changed_records = df_lineage.loc[
        common_keys[
            df_lineage.loc[common_keys, 'SQL_MD5']
            != df_lineage_genai.loc[common_keys, 'SQL_MD5']
        ]
    ]

    # Process new records
    for index, row in new_records.iterrows():
        response = get_column_lineage_from_openai(row['TABLE_NAME'], row['COLUMN_NAME'], row['REFERENCE'], row['SQL'])
        # Parse response and prepare data for insertion
        upstream_tables, upstream_columns, reasoning = parse_openai_response(response)

        # Insert new record with additional OpenAI information
        insert_query = """
        INSERT INTO COLUMN_LINEAGE_GENAI (UNIQUE_KEY, DATABASE, SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_DESCRIPTION,
                                          RESOURCE_TYPE, NAME, SQL, REFERENCE, UPSTREAM_TABLE, UPSTREAM_COLUMN, REASONING)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        data_to_insert = (row['UNIQUE_KEY'], row['DATABASE'], row['SCHEMA'], row['TABLE_NAME'], row['COLUMN_NAME'],
                          row['COLUMN_DESCRIPTION'], row['RESOURCE_TYPE'], row['NAME'], row['SQL'], row['REFERENCE'],
                          upstream_tables, upstream_columns, reasoning)
        cursor.execute(insert_query, data_to_insert)
        conn.commit()



    # Process changed records
    for index, row in changed_records.iterrows():
        response = get_column_lineage_from_openai(row['TABLE_NAME'], row['COLUMN_NAME'], row['REFERENCE'], row['SQL'])
        # Parse response and prepare data for update
        upstream_tables, upstream_columns, reasoning = parse_openai_response(response)

        # Update the existing record with new OpenAI information
        update_query = """
        UPDATE COLUMN_LINEAGE_GENAI
        SET COLUMN_DESCRIPTION = %s, RESOURCE_TYPE = %s, NAME = %s, SQL = %s, REFERENCE = %s,
            UPSTREAM_TABLE = %s, UPSTREAM_COLUMN = %s, REASONING = %s
        WHERE UNIQUE_KEY = %s
        """
        data_to_update = (row['COLUMN_DESCRIPTION'], row['RESOURCE_TYPE'], row['NAME'], row['SQL'], row['REFERENCE'],
                          upstream_tables, upstream_columns, reasoning, row['UNIQUE_KEY'])
        cursor.execute(update_query, data_to_update)
        conn.commit()

    # Query to fetch data from the Snowflake table
    query = "SELECT * FROM column_lineage_genai;"

    # Read the data into a pandas DataFrame
    df = pd.read_sql(query, conn)

    # Save DataFrame to CSV
    output_file_path = 'dbt_manifest_extracted_data_with_lineage.csv'
    df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

    cursor.close()

import re

def parse_openai_response(response):
    # Initialize variables to store the extracted values
    upstream_tables = ''
    upstream_columns = ''
    reasoning = ''

    try:
        # Normalize spaces and remove leading/trailing whitespace
        response = ' '.join(response.split()).strip()

        # Extract upstream tables, columns, and reasoning using regular expressions
        upstream_tables_match = re.search(r"Upstream Table\(s\):\s*\[([^\]]*)\]", response)
        upstream_columns_match = re.search(r"Upstream Column\(s\):\s*\[([^\]]*)\]", response)
        reasoning_match = re.search(r"Reasoning:\s*(.*)", response)

        # Extract values if matches are found, otherwise default to empty strings
        upstream_tables = upstream_tables_match.group(1).strip() if upstream_tables_match else ''
        upstream_columns = upstream_columns_match.group(1).strip() if upstream_columns_match else ''
        reasoning = reasoning_match.group(1).strip() if reasoning_match else ''

        # Remove any surrounding brackets or extra whitespace
        upstream_tables = upstream_tables.replace('[', '').replace(']', '').strip()
        upstream_columns = upstream_columns.replace('[', '').replace(']', '').strip()

    except Exception as e:
        print(f"Error parsing response: {e}")

    return upstream_tables, upstream_columns, reasoning


# Main Function to Execute the Process
def main():
    # Connect to Snowflake
    conn = connect_to_snowflake()

    # Load data from Snowflake
    df_lineage, df_lineage_genai = load_data_from_snowflake(conn)

    # Filter for specific tables (for testing purposes)
    #selected_tables = ['model.jaffle_shop.customers']  # Replace with actual table names for testing
    #df_lineage = df_lineage[df_lineage['TABLE_NAME'].isin(selected_tables)]

    # Process and update records
    process_and_update_records(conn, df_lineage, df_lineage_genai)

    # Close Snowflake connection
    conn.close()

    print("Transactions has been successfully processed and updated in Snowflake.")



if __name__ == "__main__":
    main()
