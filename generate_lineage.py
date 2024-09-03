import json
import pandas as pd
import openai
import os
from dotenv import load_dotenv

load_dotenv()
# Set up your OpenAI API key
openai.api_key = os.getenv('openai_api_key')  # Replace with your actual OpenAI API key

# Step 1: Load JSON Files
def load_manifest(file_path):
    with open(file_path, 'r') as file:
        manifest = json.load(file)
    return manifest.get('nodes', {})

def load_catalog(file_path):
    with open(file_path, 'r') as file:
        catalog = json.load(file)
    return catalog.get('nodes', {})

# Step 2: Extract Data and Populate DataFrame
def build_dataframe_from_manifest(nodes, catalog_nodes):
    # Initialize an empty list to store rows for the DataFrame
    data = []

    for node_key, node_info in nodes.items():
        table_name = node_key  # Key in the JSON that represents the table (e.g., "model.jaffle_shop.customers")
        resource_type = node_info.get('resource_type', '')
        name = node_info.get('name', '')
        sql = node_info.get('raw_code', '')
        refs = node_info.get('refs', [])
        columns = node_info.get('columns', {})

        # Extract columns and their descriptions
        for column_name, column_info in columns.items():
            column_description = column_info.get('description', '')

            # Gather reference information for dependent tables and columns
            reference_info = []
            for ref in refs:
                ref_name = ref.get('name', '')
                if ref_name:
                    # Find corresponding columns for the reference table using catalog.json
                    ref_columns = catalog_nodes.get(f"model.jaffle_shop.{ref_name}", {}).get('columns', {})
                    ref_column_names = list(ref_columns.keys())
                    reference_info.append(f"{ref_name}: {ref_column_names}")

            # Append row to the data list
            data.append({
                'table': table_name,
                'name': name,
                'resource_type': resource_type,
                'column_name': column_name,
                'column_description': column_description,
                'sql': sql,
                'reference': ', '.join(reference_info)
            })

    # Create a DataFrame from the data list
    df = pd.DataFrame(data)
    return df

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
    f"Response Format: Upstream Column(s): [list the most likely upstream Column(s)], Upstream Table(s): [Table related to Upstream column], Reasoning: [Short one-liner reasoning on why you chose these column(s) and table(s), focusing on the primary source as indicated by the SQL context]"
)

    # Send the prompt to OpenAI
    response = openai.chat.completions.create(
        model="gpt-4o-mini",  # Modify as needed for Google Gemini
        messages=[
            {"role": "user", "content": prompt}
        ],
        max_tokens=1000,
        temperature=0.5
    )

    response_text = response.choices[0].message.content.strip()
    #print(f"Response received from LLM:\n{response_text}\n")  # Print the response from the LLM

    return response_text

# Step 4: Process DataFrame and Fetch Lineage Information
def process_dataframe_for_lineage(df):
    # Create new columns in the DataFrame for Upstream Table(s), Upstream Column(s), and Reasoning
    df['Upstream Table(s)'] = ''
    df['Upstream Column(s)'] = ''
    df['Reasoning'] = ''

    # Fetch lineage information and parse it into the DataFrame
    for index, row in df.iterrows():
        response = get_column_lineage_from_openai(row['table'], row['column_name'], row['reference'], row['sql'])
        
        # Extract upstream tables, columns, and reasoning from the response
        upstream_tables = ''
        upstream_columns = ''
        reasoning = ''
        
        # Use try-except to handle any errors in response parsing
        try:
            if "Upstream Table(s):" in response and "Upstream Column(s):" in response and "Reasoning:" in response:
                # Extract using more robust string splitting based on the response format
                upstream_tables = response.split("Upstream Table(s): [")[1].split("],")[0].strip()
                upstream_columns = response.split("Upstream Column(s): [")[1].split("],")[0].strip()
                reasoning = response.split("Reasoning:")[1].strip()
        except Exception as e:
            print(f"Error parsing response: {e}")
        
        # Assign extracted values to the DataFrame
        df.at[index, 'Upstream Table(s)'] = upstream_tables
        df.at[index, 'Upstream Column(s)'] = upstream_columns
        df.at[index, 'Reasoning'] = reasoning

    return df

# Step 5: Save DataFrame to CSV
def save_dataframe_to_csv(df, output_file_path):
    # Save the DataFrame to a CSV file with formatting options
    df.to_csv(output_file_path, index=False, encoding='utf-8-sig', lineterminator='\n')

# Main Function to Execute the Process
def main():
    # Load the manifest and catalog files
    manifest_path = 'manifest.json'  # Replace with your manifest.json path
    catalog_path = 'catalog.json'    # Replace with your catalog.json path

    nodes = load_manifest(manifest_path)
    catalog_nodes = load_catalog(catalog_path)
    
    # Build the DataFrame
    df = build_dataframe_from_manifest(nodes, catalog_nodes)
    
    # Print the DataFrame to verify its contents
    print(df)

    # Process DataFrame to get lineage information from OpenAI
    df_with_lineage = process_dataframe_for_lineage(df)

    # Save the DataFrame with lineage to a CSV file
    output_file_path = 'dbt_manifest_extracted_data_with_lineage.csv'  # Specify your desired output file path
    save_dataframe_to_csv(df_with_lineage, output_file_path)

    print(f"DataFrame with lineage information saved to CSV file at: {output_file_path}")

# Run the main function
if __name__ == "__main__":
    main()
