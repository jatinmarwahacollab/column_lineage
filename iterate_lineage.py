import pandas as pd

# Function to read data from the CSV file
def read_csv_data(file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    # Convert relevant columns to lowercase for case-insensitive matching
    df['NAME'] = df['NAME'].str.lower().str.strip()
    df['COLUMN_NAME'] = df['COLUMN_NAME'].str.lower().str.strip()
    df['UPSTREAM_TABLE'] = df['UPSTREAM_TABLE'].str.lower().str.strip()
    df['UPSTREAM_COLUMN'] = df['UPSTREAM_COLUMN'].str.lower().str.strip()
    return df

# Function to extract column name after the dot
def extract_column_name(column_name):
    # Extract the part after the dot, if it exists
    return column_name.split('.')[-1].strip()

# Recursive function to build JSON hierarchy for a given table and column
def build_hierarchy(df, model_name, column_name):
    # Convert input model name and column name to lowercase for consistency
    model_name = model_name.lower().strip()
    column_name = column_name.lower().strip()

    # Initialize the base structure for the current node
    base_structure = {
        "model": model_name,
        "column": column_name,
        "column Description": "",
        "reasoning": "",  # New field for reasoning
        "upstream_models": []
    }

    # Safely extract the description and reasoning for the current model and column
    try:
        base_structure["column Description"] = df.loc[
            (df['NAME'] == model_name) & (df['COLUMN_NAME'] == column_name), 'COLUMN_DESCRIPTION'
        ].values[0]
    except IndexError:
        base_structure["column Description"] = "Description not available"

    try:
        base_structure["reasoning"] = df.loc[
            (df['NAME'] == model_name) & (df['COLUMN_NAME'] == column_name), 'REASONING'
        ].values[0]
    except IndexError:
        base_structure["reasoning"] = "Reasoning not available"

    # Get the row corresponding to the current model and column
    current_row = df[(df['NAME'] == model_name) & (df['COLUMN_NAME'] == column_name)]

    if current_row.empty:
        # If no data is found, return the base structure without upstream models
        return base_structure

    # Extract the upstream tables and columns
    upstream_tables = str(current_row['UPSTREAM_TABLE'].values[0]).split(',')
    upstream_columns = str(current_row['UPSTREAM_COLUMN'].values[0]).split(',')

    # Iterate over each upstream table and column pair
    for upstream_table, upstream_column in zip(upstream_tables, upstream_columns):
        upstream_table = upstream_table.strip()
        # Extract only the column name after the dot
        upstream_column_name = extract_column_name(upstream_column.strip())

        # Build hierarchy for upstream
        upstream_hierarchy = build_hierarchy(df, upstream_table, upstream_column_name)
        if upstream_hierarchy:
            base_structure["upstream_models"].append(upstream_hierarchy)

    return base_structure

# Function to build the entire JSON hierarchy for all columns in the DataFrame
def build_full_hierarchy(df):
    # Initialize an empty dictionary to store the full hierarchy
    full_hierarchy = []

    # Iterate through all unique tables and columns in the DataFrame
    for index, row in df.iterrows():
        model_name = row['NAME']
        column_name = extract_column_name(row['COLUMN_NAME'])

        # Build the hierarchy for each model and column
        hierarchy = build_hierarchy(df, model_name, column_name)

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
