# Databricks notebook source
# This notebook automates the process of adding new retention field configurations
# to the lake loader error JSON files based on configuration from a GitHub repository.

# MAGIC %pip install pygithub

# COMMAND ----------

# Required imports for JSON processing and GitHub API interaction
import json
from github import Github

# COMMAND ----------
# MAGIC %run ./dependencies/shared_config

# COMMAND ----------

# ============================================================================
# GitHub Enterprise Configuration
# ============================================================================
# Configure connection to the GitHub Enterprise server to fetch retention field configs
github_enterprise_host='github.example.com'
organization_name='acme-data-platform'
repo_name='retention-fields-config'
file = 'lake_retention_fields_config.json'  # Configuration file containing retention field definitions
path = './rawjson/lake_loader_error_{source}_retention.json'

# Retrieve GitHub token from Databricks secrets for authentication
try:
    github_token = dbutils.secrets.get(scope="retention_github_token", key="github_token")
except Exception as e:
    raise

# Initialize GitHub API client and connect to the repository
try:
    github_api = Github(base_url=f"https://{github_enterprise_host}/api/v3", login_or_token=github_token)
    repo = github_api.get_repo(f"{organization_name}/{repo_name}")
except Exception as e:
    print(f"Failed to initialize GitHub client or get repo: {str(e)}")

# COMMAND ----------

# Fetch and parse the retention fields configuration file from the master branch
contents = repo.get_contents(file, ref='master')
content_data = contents.decoded_content.decode('utf-8')
config_json = json.loads(content_data)

# COMMAND ----------

# ============================================================================
# Mapping Dictionaries for Retention Field Processing
# ============================================================================

# Maps source URI patterns to their simplified source type names
# These types determine which JSON configuration file to update
source_map = {
    'acme.retail.crm.contact.source.rev.1': 'crm',
    'acme.retail.crm.lead.source.rev.1': 'crm',
    'acme.retail.billing.invoice.source.rev.1': 'billing',
    'acme.retail.payments.refund.source.rev.1': 'billing',
    'acme.retail.support.ticket.source.rev.1': 'support',
    'acme.retail.webshop.session.source.rev.1': 'webshop',
    'acme.retail.logistics.shipment.source.rev.1': 'logistics',
    'acme.platform.iot.sensor.source.rev.1': 'iot',
    'acme.retail.iot.gateway.source.rev.1': 'iot'}

# Note: rule_column_to_table_mapping, paired_fields, ambiguous_fields, and
# rule_to_bronze_path_mapping are imported from dependencies/shared_config.py

# COMMAND ----------

# List of all known/valid bronze field paths that are recognized for retention processing
# Used to validate incoming fields from the config file
old_bronze_fields = list(set([v for values in rule_to_bronze_path_mapping.values() for v in values]))

# COMMAND ----------

# ============================================================================
# Core Function: Add New Retention Field Configuration
# ============================================================================
def is_new_rule_field(path, rule_table, rule_field, bronze_field,
                      rule_field_2=None, bronze_field_2=None):
    """
    Checks if a retention field configuration already exists in the JSON file,
    and adds it to all environments (dev, itg, prod) if it doesn't exist.

    Args:
        path: Path to the source-specific retention JSON config file
        rule_table: Name of the rule table (e.g., 'ret_device_keys_enrich')
        rule_field: Primary retention field name (e.g., 'device_id')
        bronze_field: JSON path to extract the field from rawJson
        rule_field_2: Optional secondary retention field (for paired fields like device_model)
        bronze_field_2: Optional JSON path for the secondary field

    Returns:
        String indicating whether a new rule id was added or already exists
    """
    # Load the existing JSON configuration file
    with open(path, 'r') as f:
        json_file = json.loads(f.read())

    # Format the bronze field as a Spark SQL get_json_object expression
    bronze_field = f"""get_json_object(rawJson, '$.{bronze_field}')"""

    # Build the rule source configuration object
    rule_id ={
      "name": rule_table,
      "table_key_aliases": {
        rule_field: bronze_field
      }
    }

    # Handle paired-field scenarios (e.g., device_id + device_model pairs)
    if bronze_field_2:
        bronze_field_2 = f"""get_json_object(rawJson, '$.{bronze_field_2}')"""
        # Order fields based on whether primary field is device_model
        if rule_field == 'device_model':
            rule_id ={
              "name": rule_table,
              "table_key_aliases": {
                rule_field: bronze_field,
                rule_field_2: bronze_field_2
              }
            }
        else:
            rule_id ={
              "name": rule_table,
              "table_key_aliases": {
                rule_field_2: bronze_field_2,
                rule_field: bronze_field
              }
      }
    print(rule_id)

    # Check if this rule source configuration already exists in the dev environment
    rule_sources = json_file['dev']['retention_configuration']['data_tables'][0]['rule_sources']
    if rule_id not in rule_sources:
        # Add the new rule source to all three environments
        json_file['dev']['retention_configuration']['data_tables'][0]['rule_sources'].append(rule_id)
        json_file['itg']['retention_configuration']['data_tables'][0]['rule_sources'].append(rule_id)
        json_file['prod']['retention_configuration']['data_tables'][0]['rule_sources'].append(rule_id)

        # Write the updated configuration back to the file
        with open(path, 'w') as f:
             f.write(json.dumps(json_file, indent=2))
        return "New rule_id added"
    else:
        return "Rule_id already exists"

# COMMAND ----------

# ============================================================================
# Main Processing Loop: Process Retention Fields from GitHub Config
# ============================================================================
# Iterate through each event type configuration from the GitHub config file
for k, v in config_json.items():
    # Extract and normalize the source URI (remove trailing slash)
    uri = v['source_uri'].strip('/')

    # Only process if the source type is recognized
    if uri in source_map:
        source = source_map[uri]
        # Construct path to the corresponding error source JSON file
        uri_path = path.format(source=source)

        # Extract retention field definitions and their bronze field mappings
        retention_fields = v['retention_fields'][0]
        bronze_fields = [b for a, b in retention_fields.items()]

        # Process each bronze field in the configuration
        for bronze_field in bronze_fields:
            single = True  # Flag to track if this is a single-field or paired-field scenario

            # Validate that the bronze field is recognized
            if bronze_field in old_bronze_fields:
                # Handle ambiguous fields (deviceId can map to different rule tables)
                if bronze_field not in ambiguous_fields:
                    # Look up the retention field name from the mapping
                    for k, v in rule_to_bronze_path_mapping.items():
                        if bronze_field in v:
                            rule_field = k
                            break
                    rule_table = rule_column_to_table_mapping[rule_field]

                else:
                    # If device_model fields exist, use ret_device_keys_enrich table
                    if len(set(bronze_fields).intersection(rule_to_bronze_path_mapping['device_model'])) > 0:
                        rule_field = 'device_id'
                        rule_table = 'ret_device_keys_enrich'
                    else:
                        # Otherwise use the simpler ret_device_only_keys table
                        rule_field = 'deviceid'
                        rule_table = 'ret_device_only_keys'

                # Handle paired-field scenario for ret_device_keys_enrich table
                # This table requires both device_id and device_model fields
                if rule_table == 'ret_device_keys_enrich':
                    bronze_field_2 = paired_fields[bronze_field]
                    # If multiple paired fields are possible, find the one present in config
                    if type(bronze_field_2) == list:
                        for bronze in bronze_field_2:
                            if bronze in bronze_fields:
                                bronze_fields.remove(bronze)  # Avoid duplicate processing
                                # Find the retention field name for the secondary bronze field
                                for k, v in rule_to_bronze_path_mapping.items():
                                    if bronze in v:
                                        rule_field_2 = k
                                        break
                                print(is_new_rule_field(uri_path, rule_table, rule_field, bronze_field, rule_field_2, bronze))
                                single = False

                    else:
                        # Single paired field defined
                        rule_field_2 = 'device_id' if rule_field == 'device_model' else 'device_model'
                        print(is_new_rule_field(uri_path, rule_table, rule_field, bronze_field, rule_field_2, bronze_field_2))
                        single = False

                # Process as single-field if not handled as paired-field above
                if single:
                    print(is_new_rule_field(uri_path, rule_table, rule_field, bronze_field))
            else:
                # Log warning for unrecognized bronze fields
                print(f'Column {v} not found in rule mapping')
