import json
import requests

# Replace these with your actual Tableau Online details
instance = "prod-apnortheast-a"
api_version = "3.14"
auth_url = f"https://{instance}.online.tableau.com/api/{api_version}/auth/signin"

token_name = "demo_lineage"
token_value = "OduNru8eTcevWyUj75fFHQ==:NwB6cBGwWeOjrhSbVoUkIIFLdxy67ACh"
site_id = ""

auth_payload = {
    "credentials": {
        "personalAccessTokenName": token_name,
        "personalAccessTokenSecret": token_value,
        "site": {"contentUrl": site_id}
    }
}

auth_headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

# Authenticate
try:
    response = requests.post(auth_url, json=auth_payload, headers=auth_headers)
    response.raise_for_status()
    data = response.json()
    auth_token = data['credentials']['token']
    print(f"Authenticated with token: {auth_token}")
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
    exit()

# Define the GraphQL endpoint
metadata_api_url = f"https://{instance}.online.tableau.com/api/metadata/graphql"
headers = {
    "Content-Type": "application/json",
    "X-Tableau-Auth": auth_token
}

# Step 1: Fetch the list of IDs for the published datasource
fetch_datasource_query = """
{
  publishedDatasources {
    id
    name
  }
}
"""

response = requests.post(metadata_api_url, json={'query': fetch_datasource_query}, headers=headers)
response.raise_for_status()

# Parse the JSON response to get the datasource IDs
datasource_data = response.json()

# Extract multiple IDs from the response
published_datasource_ids = [ds['id'] for ds in datasource_data['data']['publishedDatasources']]

print(f"Published Datasource IDs: {published_datasource_ids}")

# Prepare the list of IDs for the `idWithin` filter
idWithin_str = '", "'.join(published_datasource_ids)
idWithin_filter = f'["{idWithin_str}"]'

# Step 2: Construct the main GraphQL query using multiple IDs in `idWithin`
graphql_query = f"""
{{
  workbooks(filter: {{name: "Jaffle Shop "}}) {{
    name
    dashboard: dashboards(filter: {{name: "Dashboard 1"}}) {{
      name
      id
      upstreamDatasources(filter: {{idWithin: {idWithin_filter}}}) {{
        name
        downstreamSheets {{
          name
          worksheetFields {{
            name
          }}
          sheetFieldInstances(orderBy: {{field: NAME, direction: ASC}}) {{
            name
            upstreamDatabases {{
              name
            }}
            upstreamTables {{
              name
            }}
            upstreamColumns {{
              name
            }}
            referencedByCalculations {{
              name
              formula
              upstreamDatabases {{
                name
              }}
              upstreamTables {{
                name
              }}
              upstreamColumns {{
                name
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# Make the request to the GraphQL endpoint (use updated headers with auth token)
response = requests.post(metadata_api_url, json={'query': graphql_query}, headers=headers)
response.raise_for_status()

# Parse the JSON response
data = response.json()
print(json.dumps(data, indent=2))

def build_lineage(data):
    output = {"workbooks": []}

    # Iterate over the workbooks
    for workbook in data['data']['workbooks']:
        workbook_output = {
            "name": workbook["name"],
            "dashboards": []
        }

        # Iterate over the dashboards in the workbook
        for dashboard in workbook["dashboard"]:
            dashboard_output = {
                "name": dashboard["name"],
                "upstreamDatasources": []
            }

            # Iterate over the upstreamDatasources in the dashboard
            for datasource in dashboard["upstreamDatasources"]:
                datasource_output = {
                    "name": datasource["name"],
                    "sheets": []
                }

                # Iterate over the downstreamSheets under each datasource
                for sheet in datasource["downstreamSheets"]:
                    sheet_output = {
                        "name": sheet["name"],
                        "worksheetFields": [],
                        "sheetFieldInstances": []
                    }

                    # Add worksheetFields to sheet output
                    for field in sheet["worksheetFields"]:
                        worksheet_field_output = {
                            "name": field["name"]
                        }
                        sheet_output["worksheetFields"].append(worksheet_field_output)

                    # Add sheetFieldInstances and treat them like upstream columns
                    for field_instance in sheet["sheetFieldInstances"]:
                        field_output = {
                            "name": field_instance["name"],
                            "upstreamColumns": [],
                            "formula": ""
                        }

                        # Add direct upstream details for each column in the field
                        for column in field_instance.get("upstreamColumns", []):
                            column_entry = {
                                "name": column["name"],
                                "upstreamDatabases": field_instance.get("upstreamDatabases", []),
                                "upstreamTables": field_instance.get("upstreamTables", [])
                            }
                            field_output["upstreamColumns"].append(column_entry)

                        # Handle referencedByCalculations
                        process_calculations(field_instance, field_output)

                        # Add the field instance to the sheet output
                        sheet_output["sheetFieldInstances"].append(field_output)

                    # Add sheet output to the datasource
                    datasource_output["sheets"].append(sheet_output)

                # Add datasource output to the dashboard
                dashboard_output["upstreamDatasources"].append(datasource_output)

            # Add dashboard output to the workbook
            workbook_output["dashboards"].append(dashboard_output)

        # Add the workbook output to the final result
        output["workbooks"].append(workbook_output)

    return output

# Function to handle recursive referencedByCalculations
def process_calculations(field, field_output):
    if field.get("referencedByCalculations"):
        for calc in field["referencedByCalculations"]:
            calc_entry = {
                "name": calc["name"],
                "formula": calc.get("formula", ""),
                "upstreamColumns": []
            }

            # Add upstream columns for this calculation
            for upstream_col in calc.get("upstreamColumns", []):
                upstream_column_entry = {
                    "name": upstream_col["name"],
                    "upstreamDatabases": calc.get("upstreamDatabases", []),
                    "upstreamTables": calc.get("upstreamTables", [])
                }
                calc_entry["upstreamColumns"].append(upstream_column_entry)

            field_output["upstreamColumns"].append(calc_entry)

# Generate the output
lineage_output = build_lineage(data)

# Write the output to a file to review
with open('tableau_lineage.json', 'w') as f:
    json.dump(lineage_output, f, indent=4)

print("Lineage file generated successfully.")
