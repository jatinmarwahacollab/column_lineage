import requests

# Replace these with your actual Tableau Online details
instance = "prod-apnortheast-a"  # Replace with your Tableau Online instance (e.g., "10ax", "10az", etc.)
api_version = "3.14"  # Use the appropriate API version
auth_url = f"https://{instance}.online.tableau.com/api/{api_version}/auth/signin"

#auth_url = f"https://prod-apnortheast-a.online.tableau.com/api/VERSION/auth/signin"

token_name = "demo_lineage"  # Your personal access token name
token_value = ""  # Your personal access token secret
site_id = ""  # Content URL for your Tableau Online site

headers = {
    "Content-Type": "application/json"
}

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

try:
    response = requests.post(auth_url, json=auth_payload, headers=auth_headers)
    response.raise_for_status()

    if response.content:
        data = response.json()
        auth_token = data['credentials']['token']
        print(f"Authenticated with token: {auth_token}")
    else:
        print("Authentication successful, but empty response received.")

except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
    if response is not None:
        print(f"Response content: {response.content}")

# Define the GraphQL endpoint
metadata_api_url = f"https://prod-apnortheast-a.online.tableau.com/api/metadata/graphql"

# Step 1: Fetch the ID of the published datasource
fetch_datasource_query = """
{
  publishedDatasources(filter: {name: "JAFFLE_SHOP"}) {
    id
    name
  }
}
"""

# Include the 'X-Tableau-Auth' header with the authentication token
headers = {
    "Content-Type": "application/json",
    "X-Tableau-Auth": auth_token  # Add the authentication token here
}

# Make the request to get published datasource ID
response = requests.post(metadata_api_url, json={'query': fetch_datasource_query}, headers=headers)
response.raise_for_status()

# Parse the JSON response to get the datasource ID
datasource_data = response.json()
published_datasource_id = datasource_data['data']['publishedDatasources'][0]['id']

print(f"Published Datasource ID: {published_datasource_id}")

# Step 2: Construct the main GraphQL query using the fetched ID
graphql_query = f"""
{{
  dashboard: dashboards(filter: {{name: "Dashboard 1"}}) {{
    name
    id
    upstreamDatasources(filter: {{name: "JAFFLE_SHOP", id: "{published_datasource_id}"}}) {{
      name
      fields(orderBy: {{field: NAME, direction: ASC}}) {{
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
"""

# Make the request to the GraphQL endpoint (use updated headers with auth token)
response = requests.post(metadata_api_url, json={'query': graphql_query}, headers=headers)
response.raise_for_status()

# Parse the JSON response
data = response.json()
print(data)

def build_lineage(data):
    output = {"dashboard": []}

    # Iterate over the dashboards
    for dashboard in data['data']['dashboard']:
        dashboard_output = {
            "name": dashboard["name"],
            "upstreamDatasources": []
        }

        # Iterate over the upstreamDatasources in the dashboard
        for datasource in dashboard["upstreamDatasources"]:
            datasource_output = {
                "name": datasource["name"],
                "fields": []
            }

            # Step 1: Create a registry of fields and calculations to ensure unique entries
            field_registry = {}

            # Iterate over the fields in each datasource
            for field in datasource["fields"]:
                field_name = field["name"]
                datasource_name = datasource["name"]

                # Create a unique key using both field name and datasource name
                field_key = f"{field_name}_{datasource_name}"

                # Create or update entry for the field
                if field_key not in field_registry:
                    # Initialize field output
                    field_output = {
                        "name": field_name,
                        "upstreamColumns": [],
                        "formula": field.get("formula", "")
                    }
                    field_registry[field_key] = field_output
                else:
                    # If field exists, update formula if available
                    if "formula" in field and field["formula"]:
                        field_registry[field_key]["formula"] = field["formula"]

                # Add direct upstream details for each column in the field
                for column in field.get("upstreamColumns", []):
                    column_name = column["name"]

                    # Avoid duplicate columns in the same field
                    if column_name not in [col["name"] for col in field_registry[field_key]["upstreamColumns"]]:
                        column_entry = {
                            "name": column_name,
                            "upstreamDatabases": field.get("upstreamDatabases", []),
                            "upstreamTables": field.get("upstreamTables", []),
                        }
                        field_registry[field_key]["upstreamColumns"].append(column_entry)

                # Check if there are referenced calculations for the field
                if field.get("referencedByCalculations"):
                    for calc in field["referencedByCalculations"]:
                        calc_name = calc["name"]

                        # Create or update entry for the calculation
                        calc_key = f"{calc_name}_{datasource_name}"
                        if calc_key not in field_registry:
                            calc_entry = {
                                "name": calc_name,
                                "formula": calc.get("formula", ""),
                                "upstreamDatabases": calc.get("upstreamDatabases", []),
                                "upstreamTables": calc.get("upstreamTables", []),
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

                            field_registry[calc_key] = calc_entry

            # Append all unique fields and calculations to the datasource
            for field_key, field_details in field_registry.items():
                datasource_output["fields"].append(field_details)

            # Add datasource output to the dashboard
            dashboard_output["upstreamDatasources"].append(datasource_output)

        # Add the completed dashboard output to the final result
        output["dashboard"].append(dashboard_output)

    return output

# Generate the output
lineage_output = build_lineage(data)

# Write the output to a file to review
with open('tableau_lineage.json', 'w') as f:
    json.dump(lineage_output, f, indent=4)

print("Lineage file generated successfully.")


