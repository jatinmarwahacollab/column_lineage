import json
import pandas as pd

# Load the JSON files
with open('tableau_lineage.json', 'r') as tableau_file:
    tableau_data = json.load(tableau_file)

with open('lineage.json', 'r') as db_file:
    db_data = json.load(db_file)

def find_database_lineage(model, column, db_data):
    """
    Recursively find the lineage for a given model and column from the database lineage.
    """
    for entry in db_data:
        if entry["model"].lower() == model.lower() and entry["column"].lower() == column.lower():
            return entry
    return None

def stitch_lineages(tableau_data, db_data):
    """
    Stitch together the tableau and database lineages into a combined nested lineage.
    """
    combined_lineage = {"workbooks": []}

    for workbook in tableau_data["workbooks"]:
        workbook_entry = {"name": workbook["name"], "dashboards": []}

        for dashboard in workbook["dashboards"]:
            dashboard_entry = {"name": dashboard["name"], "sheets": []}

            for sheet in dashboard["upstreamDatasources"][0]["sheets"]:  # Access downstreamSheets in upstreamDatasources
                sheet_entry = {"name": sheet["name"], "sheetFieldInstances": []}

                # Iterate over the sheetFieldInstances in each sheet
                field_registry = {}
                for field in sheet["sheetFieldInstances"]:
                    # Initialize the field entry and include the formula if present
                    field_key = f"{field['name']}_{sheet['name']}"
                    if field_key not in field_registry:
                        field_registry[field_key] = {
                            "name": field["name"],
                            "formula": field.get("formula", ""),
                            "upstreamColumns": []
                        }

                    # Add direct upstream details for each column in the field
                    for column in field["upstreamColumns"]:
                        # Check if 'upstreamTables' exists in the column
                        upstream_tables = column.get("upstreamTables", [])
                        
                        column_entry = {
                            "name": column["name"],
                            "formula": column.get("formula", ""),
                            "upstreamTables": upstream_tables,
                            "dblineage": []  # Start with an empty lineage list
                        }

                        # Populate the lineage information based on upstream tables
                        for table in upstream_tables:
                            db_lineage = find_database_lineage(table["name"], column["name"], db_data)
                            if db_lineage:
                                column_entry["dblineage"].append(process_lineage(db_lineage))

                        field_registry[field_key]["upstreamColumns"].append(column_entry)

                # Append all unique fields to the sheetFieldInstances
                for field_key, field_details in field_registry.items():
                    sheet_entry["sheetFieldInstances"].append(field_details)

                dashboard_entry["sheets"].append(sheet_entry)

            workbook_entry["dashboards"].append(dashboard_entry)
        combined_lineage["workbooks"].append(workbook_entry)

    return combined_lineage

def process_lineage(lineage_item):
    """
    Recursively process lineage data to construct a properly nested structure.
    """
    lineage_entry = {
        "model": lineage_item["model"],
        "column": lineage_item["column"],
        "column Description": lineage_item.get("column Description", ""),
        "reasoning": lineage_item.get("reasoning", ""),
        "upstream_models": []
    }

    # Process nested upstream models recursively
    if "upstream_models" in lineage_item and lineage_item["upstream_models"]:
        for upstream_model in lineage_item["upstream_models"]:
            processed_model = process_lineage(upstream_model)
            lineage_entry["upstream_models"].append(processed_model)

    return lineage_entry

# Generate the combined lineage
combined_lineage = stitch_lineages(tableau_data, db_data)

# Save the combined lineage to a file
with open('combined_lineage.json', 'w') as output_file:
    json.dump(combined_lineage, output_file, indent=4)

print("Combined lineage file generated successfully.")

# Load the JSON file
with open('combined_lineage.json', 'r') as file:
    data = json.load(file)

# Helper function to recursively flatten the JSON hierarchy
def flatten_lineage(dashboard_name, column_name, data_source, table_name, lineage_data, level=1):
    flattened_rows = []
    for lineage_item in lineage_data:
        row = {
            "Dashboard Name": dashboard_name,
            "Column Name on Dashboard": column_name,
            "Data Source Name": data_source,
            "Table Name": table_name,
            "Column Name in Table": lineage_item["column"],
            "Upstream Table Name": lineage_item.get("model", ""),
            "Upstream Column Name": lineage_item.get("column", ""),
            "Level": level,
            "Formula": lineage_item.get("formula", ""),
            "Description": lineage_item.get("column Description", ""),
            "Reasoning": lineage_item.get("reasoning", "")
        }
        flattened_rows.append(row)

        # Recursively handle nested upstream models
        if "upstream_models" in lineage_item and lineage_item["upstream_models"]:
            flattened_rows.extend(flatten_lineage(
                dashboard_name,
                column_name,
                data_source,
                lineage_item.get("model", ""),
                lineage_item["upstream_models"],
                level + 1
            ))
    return flattened_rows

# Convert JSON to DataFrame
flattened_data = []

for workbook in data["workbooks"]:
    for dashboard in workbook["dashboards"]:
        dashboard_name = dashboard["name"]
        for sheet in dashboard["sheets"]:
            for field_instance in sheet["sheetFieldInstances"]:
                for upstream_column in field_instance.get("upstreamColumns", []):
                    table_name = upstream_column["upstreamTables"][0]["name"] if upstream_column.get("upstreamTables") else ""
                    lineage = upstream_column.get("dblineage", [])
                    flattened_data.extend(flatten_lineage(
                        dashboard_name,
                        field_instance["name"],
                        sheet["name"],
                        table_name,
                        lineage
                    ))

# Convert the flattened data to a DataFrame
df = pd.DataFrame(flattened_data)

# Save DataFrame to CSV
df.to_csv('flattened_lineage.csv', index=False)

print("Flattened data has been successfully written to 'flattened_lineage.csv'.")
