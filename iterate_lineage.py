import pandas as pd

# Function to read data from the CSV file
def read_csv_data(file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    # Convert relevant columns to lowercase for case-insensitive matching
    df['name'] = df['name'].str.lower()
    df['column_name'] = df['column_name'].str.lower()
    df['Upstream Table(s)'] = df['Upstream Table(s)'].str.lower()
    df['Upstream Column(s)'] = df['Upstream Column(s)'].str.lower()
    return df

# Recursive function to build JSON hierarchy for a given table and column
def build_hierarchy(df, table_name, column_name):
    # Convert input table and column names to lowercase for consistency
    table_name = table_name.lower()
    column_name = column_name.lower()

    # Initialize the base structure for the current node
    base_structure = {
        "model": table_name,
        "column": column_name,
        "column Description": "",
        "upstream_models": []
    }

    # Safely extract the description for the current table and column
    try:
        base_structure["column Description"] = df.loc[(df['name'] == table_name) & (df['column_name'] == column_name), 'column_description'].values[0]
    except IndexError:
        # If there is no description, we leave it as an empty string
        base_structure["column Description"] = "Description not available"

    # Get the row corresponding to the current table and column
    current_row = df[(df['name'] == table_name) & (df['column_name'] == column_name)]

    if current_row.empty:
        # If no data is found, return the base structure without upstream models
        return base_structure

    # Extract the upstream tables and columns
    upstream_tables = str(current_row['Upstream Table(s)'].values[0]).split(',')
    upstream_columns = str(current_row['Upstream Column(s)'].values[0]).split(',')

    # Iterate over each upstream table and column pair
    for upstream_table, upstream_column in zip(upstream_tables, upstream_columns):
        upstream_table = upstream_table.strip()
        upstream_column = upstream_column.strip()

        # Build hierarchy for upstream
        upstream_hierarchy = build_hierarchy(df, upstream_table, upstream_column)
        if upstream_hierarchy:
            base_structure["upstream_models"].append(upstream_hierarchy)

    return base_structure

# Function to build the entire JSON hierarchy for all columns in the DataFrame
def build_full_hierarchy(df):
    # Initialize an empty dictionary to store the full hierarchy
    full_hierarchy = []

    # Iterate through all unique tables and columns in the DataFrame
    for index, row in df.iterrows():
        table_name = row['name']
        column_name = row['column_name']

        # Build the hierarchy for each table and column
        hierarchy = build_hierarchy(df, table_name, column_name)

        # Append the hierarchy to the full list
        full_hierarchy.append(hierarchy)

    return full_hierarchy

# Main Function to Execute the Process
def main():
    # Load data from the CSV file
    file_path = 'dbt_manifest_extracted_data_with_lineage.csv'  # Replace with your file path
    df = read_csv_data(file_path)

    # Build the full JSON hierarchy for all columns
    full_hierarchy = build_full_hierarchy(df)

    # Save the JSON hierarchy to a file
    import json
    with open('lineage.json', 'w') as f:
        json.dump(full_hierarchy, f, indent=4)

    print('JSON file created: lineage.json')

# Run the main function
if __name__ == "__main__":
    main()