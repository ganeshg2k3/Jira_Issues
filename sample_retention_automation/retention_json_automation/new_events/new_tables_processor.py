# Databricks notebook source
# MAGIC %pip install Pygithub

# COMMAND ----------
# MAGIC %run "../dependencies/shared_config"

# COMMAND ----------
# MAGIC %run "../dependencies/Pre_Requisites"

# COMMAND ----------
# MAGIC %run "../dependencies/common_functions"

# COMMAND ----------

dbutils.widgets.text("env", "")
env = dbutils.widgets.get("env")
print(f"Finding new tables for {env} environment")

# COMMAND ----------

from pyspark.sql.functions import *
import requests
import base64
import json
from pyspark.dbutils import DBUtils
import re
from collections import defaultdict
from github import Github

github_enterprise_host='github.example.com'
organization_name='acme-data-platform'
repo_name='retention-config'
pr_repo_name='retention-gitops'
path_to_file=['retention_json_automation/source_json/lake_loader_crm_retention.json', 'retention_json_automation/source_json/lake_loader_billing_retention.json', 'retention_json_automation/source_json/lake_loader_support_retention.json', 'retention_json_automation/source_json/lake_loader_webshop_retention.json', 'retention_json_automation/source_json/lake_loader_logistics_retention.json', 'retention_json_automation/source_json/lake_loader_iot_retention.json']

# COMMAND ----------

bold_black = "\033[1;30m"
reset = "\033[0m"

def process_content(path, content_data):
    """Process JSON content and extract table names, also store original JSON."""
    json_data = json.loads(content_data)
    tables_names = [table["name"] for table in json_data[env]["retention_configuration"]["data_tables"]]
    print(f"{bold_black}File: {path} - Total tables in JSON: {len(tables_names)}{reset}")
    return {
        "path": path,
        "tables": tables_names,
        "original_json": json_data,
        "length": len(tables_names)
    }

# COMMAND ----------

# Configuration Constants
fields_config_repo_name = "retention-fields-config"
fields_config_path = "lake_retention_fields_config.json"

# Note: rule_column_to_table_mapping, paired_fields, ambiguous_fields, and
# rule_to_bronze_path_mapping are imported from dependencies/shared_config.py

# COMMAND ----------

source_branch = 'rc/dev'
try:
    github_token = dbutils.secrets.get(scope="retention_github_token", key="github_token")
except Exception as e:
    print("Failed to fetch GitHub token from both scopes. Error:", str(e))
    raise

# Initialize GitHub client
try:
    github_api = Github(base_url=f"https://{github_enterprise_host}/api/v3", login_or_token=github_token)
    repo = github_api.get_repo(f"{organization_name}/{repo_name}")
except Exception as e:
    print(f"Failed to initialize GitHub client or get repo: {str(e)}")

# Load Fields Config Repository
fields_config_data = []
try:
    fields_repo = github_api.get_repo(f"{organization_name}/{fields_config_repo_name}")
    cfg_contents = fields_repo.get_contents(fields_config_path, ref="master")
    fields_config_raw = cfg_contents.decoded_content.decode("utf-8")
    raw_parsed = json.loads(fields_config_raw)

    # Extract config objects from dictionary values
    if isinstance(raw_parsed, dict):
        fields_config_data = [v for v in raw_parsed.values() if isinstance(v, dict)]

    print(f"Loaded {len(fields_config_data)} fields config entries")
except Exception as e:
    print(f"Warning: unable to load fields config from {fields_config_repo_name}/{fields_config_path}: {e}")

data_storage = {
    "crm": [],
    "billing": [],
    "support": [],
    "webshop": [],
    "logistics": [],
    "iot": []
}

# Index JSON table entries by source and env for quick lookup
json_table_entries_by_source = defaultdict(lambda: defaultdict(dict))

try:
    for path in path_to_file:
        contents = repo.get_contents(path, ref=source_branch)
        content_data = contents.decoded_content.decode('utf-8')

        for keyword in data_storage.keys():
            if f"loader_{keyword}_retention" in path:
                data_storage[keyword].append(process_content(path, content_data))

                try:
                    json_payload = json.loads(content_data)
                    for env_key, env_payload in json_payload.items():
                        if not isinstance(env_payload, dict):
                            continue
                        tables = env_payload.get("retention_configuration", {}).get("data_tables", [])
                        json_table_entries_by_source[keyword][env_key] = {tbl["name"]: tbl for tbl in tables}
                except Exception as json_err:
                    print(f"Warning: failed to index json tables from {path}: {json_err}")
                break

except Exception as e:
    print(f"Failed to fetch or process contents of repo: {repo_name}, Error: {str(e)}")
# COMMAND ----------

# Extract tables from all schemas using shared function
all_events, schema_table_mapping = build_schema_table_mapping(env, spark)

# Filter for ratified events
if env=='prod':
  all_ratified_events = [table for table in all_events]
  # Include alpha/beta tables in prod
  #[if 'ctx_v1_pol_v1' in table or '_v1_pol_v1' in table  or '_v1_ctx_' in table or re.search(r'_v1_ctx_v\d+beta\d+_pol_v1', table)]

else:
  non_alpha_beta_events = [table for table in all_events if 'alpha' not in table and 'beta' not in table]
  all_ratified_events = [table for table in non_alpha_beta_events if 'ctx_v1_pol_v1' in table or '_v1_pol_v1' in table]

print(f"Total ratified events found: {len(all_ratified_events)}")

# Log distribution of ratified events by schema
schema_distribution = {}
for table in all_ratified_events:
    schema = schema_table_mapping.get(table, "Unknown")
    schema_distribution[schema] = schema_distribution.get(schema, 0) + 1

print("Ratified events distribution by schema:")
for schema, count in schema_distribution.items():
    print(f"  {schema}: {count} tables")

# COMMAND ----------

all_lists = []

def add_list_if_defined(list_name):
    try:
        all_lists.extend(globals()[list_name])
    except KeyError:
        pass

list_names = [
    'suppression_list',
    'purge_list',
    'No_Rules_list',
    'No_Context_list'
]

for list_name in list_names:
    add_list_if_defined(list_name)

combined_exception_list = set(all_lists)


ratified_events=[tables for tables in all_ratified_events if tables not in combined_exception_list ]

crm_source=[s for s in ratified_events if '_src_crm_' in s.lower()]
billing_source = [s for s in ratified_events if '_src_billing_' in s.lower() or '_src_payments_' in s.lower()]
support_source = [s for s in ratified_events if '_src_support_' in s.lower() ]
webshop_source= [s for s in ratified_events if '_src_webshop_' in s.lower() ]
logistics_source = [s for s in ratified_events if '_src_logistics_' in s.lower() ]
iot_source = [s for s in ratified_events if '_platform_iot_' in s.lower() or '_src_iot_' in s.lower()]


print(f"{bold_black}Count of crm_source across all schemas: {len(crm_source)}{reset}")
print(f"{bold_black}Count of billing_source across all schemas: {len(billing_source)}{reset}")
print(f"{bold_black}Count of support_source across all schemas: {len(support_source)}{reset}")
print(f"{bold_black}Count of webshop_source across all schemas: {len(webshop_source)}{reset}")
print(f"{bold_black}Count of logistics_source across all schemas: {len(logistics_source)}{reset}")
print(f"{bold_black}Count of iot_source across all schemas: {len(iot_source)}{reset}")


# COMMAND ----------

missing_tables_dict = {}

for source, data in data_storage.items():
    print(f"-------------------------------{source}-----------------------------")
    for source_data in data:
        source_count = len(source_data["tables"])
        external_source_count = len(globals()[f"{source}_source"])

        print(f"{source}: {f'Counts are the same between json and {source}' if source_count == external_source_count else f'Counts are different. Json count: {source_count}, {source}_source count: {external_source_count}'}")

        missing_tables = [table for table in source_data["tables"] if table not in globals()[f"{source}_source"]]

        if missing_tables:
            print(f"Tables in json but not in {source}_source:")
            for table in missing_tables:
                print(f"  - {table}")
            # Add missing tables to the dictionary
            missing_tables_dict[source] = missing_tables
        else:
            print(f"No tables are missing from json compared to {source}_source.")

        print()

# COMMAND ----------

# Check for differences in counts and process only those with discrepancies
def find_new_events(source_events, data_storage_key):
    """Find new events not listed in data_storage for a given source type."""
    return [table for table in source_events if table not in data_storage[data_storage_key][0]["tables"]]

source_mapping = {
    "crm": crm_source,
    "billing": billing_source,
    "support": support_source,
    "webshop": webshop_source,
    "logistics": logistics_source,
    "iot": iot_source
}

# Initialize a dictionary to store new events only for source types with discrepancies
new_events_to_process = {}

for source_type, source_events in source_mapping.items():
    new_events = find_new_events(source_events, source_type)
    if new_events:
        new_events_to_process[source_type] = new_events

if not new_events_to_process and len(missing_tables_dict) == 0:
    dbutils.notebook.exit("No new tables for any source, so exiting")
else:
    if new_events_to_process:
        for source_type, events in new_events_to_process.items():
            print(f"New tables which will be added for {source_type} source in Json:", events)
            print("\n")
    if len(missing_tables_dict) > 0:
        print("The Tables which needs to be removed from json are:", missing_tables_dict)
        # Include all events from the corresponding source list if there are entries in missing_tables_dict
        # for source_type in missing_tables_dict.keys():
        #     if source_type in source_mapping:
        #         new_events_to_process[source_type] = source_mapping[source_type]

# COMMAND ----------

new_events_to_process

# COMMAND ----------

bold = "\033[1m"
reset = "\033[0m"

for source, tables in new_events_to_process.items():
    if source == "webshop" and not any("_v1_ctx_" in table for table in tables):
        print(f"⚠️ {bold}WARNING: 'webshop' has tables without '_v1_ctx_' — to be put in No_Context_list:{reset} {tables}")

# COMMAND ----------

# ========== INTELLIGENT RULE MAPPING ==========

# Main Mapping Logic
new_existing_events = defaultdict(list)  # Events with reused rule_sources
new_fields_config_events = defaultdict(list)  # Events mapped via fields config
exception_events = defaultdict(list)  # Events with no mapping
warnings_log = []

for source_type, new_tables in new_events_to_process.items():
    for table_name in new_tables:

        # === case 1: Find similar table and reuse rule_sources ===
        similar_entry = find_similar_table_entry(source_type, table_name, json_table_entries_by_source)

        if similar_entry:
            env_key, matched_table_name, matched_payload = similar_entry
            rebuilt_sources = []
            rebuild_warnings = []

            # Rebuild rule_sources by deriving retention fields from rule table names
            for source in matched_payload.get("rule_sources", []):
                rule_table_name = source.get("name")

                # Use unified function to rebuild from rule table name
                rule_source, build_warnings = build_rule_sources_from_rule_table(
                    table_name, rule_table_name, env
                )
                rebuild_warnings.extend(build_warnings)

                if rule_source:
                    rebuilt_sources.append(rule_source)

            if rebuilt_sources:
                new_existing_events[source_type].append({
                    "name": table_name,
                    "derived_from": matched_table_name,
                    "env": env_key,
                    "rule_sources": rebuilt_sources
                })
                if rebuild_warnings:
                    warnings_log.extend(rebuild_warnings)
                continue
            else:
                warnings_log.append(
                    f"Similar table '{matched_table_name}' found for {table_name} but could not rebuild "
                    f"rule_sources (warnings: {rebuild_warnings})"
                )

        # === case 2: Match via fields config using URI tags ===
        table_tags = get_table_tags(table_name, env)
        source_uri = table_tags.get("source_uri")
        event_uri = table_tags.get("event_uri")

        matched_config_entry = None
        if source_uri and event_uri and fields_config_data:
            for entry in fields_config_data:
                if not isinstance(entry, dict):
                    continue
                if uris_match(source_uri, entry.get("source_uri")) and \
                   uris_match(event_uri, entry.get("event_uri")):
                    matched_config_entry = entry
                    break
        else:
            warnings_log.append(
                f"No tags or fields config available for {table_name} "
                f"(source_uri={source_uri}, event_uri={event_uri})"
            )

        if matched_config_entry:
            candidate_sources, cfg_warnings = build_rule_sources_from_fields_config(
                table_name, matched_config_entry, source_type, env
            )
            warnings_log.extend(cfg_warnings)

            if candidate_sources:
                new_fields_config_events[source_type].append({
                    "name": table_name,
                    "rule_sources": candidate_sources,
                    "config_source_uri": matched_config_entry.get("source_uri"),
                    "config_event_uri": matched_config_entry.get("event_uri")
                })
                continue

        # === case 3: No mapping found - add to exceptions ===
        exception_events[source_type].append(table_name)

# COMMAND ----------

#MAGIC %md
# MAGIC ## Results Summary
# COMMAND ----------
print(f"--- case 1: Reused rule_sources from similar tables ---")
if any(new_existing_events.values()):
    for source, entries in new_existing_events.items():
        if entries:
            for entry in entries:
                print(f"\n[{source}] {entry['name']}")
                print(f"  Derived from: {entry['derived_from']} ({entry['env']})")
                print(f"  rule_sources: {json.dumps(entry['rule_sources'], indent=4)}")
else:
    print("  (None)")

# COMMAND ----------
print(f"\n--- case 2: Mapped via fields config ---")
if any(new_fields_config_events.values()):
    for source, entries in new_fields_config_events.items():
        if entries:
            for entry in entries:
                print(f"\n[{source}] {entry['name']}")
                print(f"  Matched config: source_uri={entry['config_source_uri']}, "
                      f"event_uri={entry['config_event_uri']}")
                print(f"  rule_sources: {json.dumps(entry['rule_sources'], indent=4)}")
else:
    print("  (None)")
# COMMAND ----------
print(f"\n--- case 3: Exception events (no mapping found) ---")
if any(exception_events.values()):
    for source, tables in exception_events.items():
        if tables:
            print(f"[{source}] {tables}")
else:
    print("  (None)")
# COMMAND ----------
if warnings_log:
    print(f"\n--- Warnings ---")
    for warning in warnings_log:
        print(f"WARNING: {warning}")
