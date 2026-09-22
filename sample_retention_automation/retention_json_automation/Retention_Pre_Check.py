# MAGIC %run "./dependencies/shared_config"

# COMMAND ----------

# Databricks notebook source
import json
from github import Github

# GitHub authentication
try:
    github_token = dbutils.secrets.get(scope="retention_github_token", key="github_token")
except Exception as e:
    print("Failed to fetch GitHub token from both scopes. Error:", str(e))
    raise

# Initialize GitHub client
github_api = Github(base_url=f"https://{github_enterprise_host}/api/v3", login_or_token=github_token)
cfg_repo = github_api.get_repo(f"{organization_name}/{cfg_repo_name}")

print("Git authorization succeeded")


# Data storage structure for retention-config repository
data_storage = {
    "crm": [],
    "billing": [],
    "support": [],
    "webshop": [],
    "logistics": [],
    "iot": []
}

def extract_data_from_repo(repo, paths, repo_name, source_branch):
    """Extract data from a specific repository."""
    print(f"Using branch '{source_branch}' for {repo_name}")
    for path in paths:
        try:
            contents = repo.get_contents(path, ref=source_branch)
            content_data = contents.decoded_content.decode('utf-8')

            # Match path to keyword and store data
            for keyword in data_storage.keys():
                if f"loader_{keyword}_retention" in path:
                    processed_data = process_content(path, content_data, repo_name)
                    data_storage[keyword].append(processed_data)
                    break
        except Exception as e:
            print(f"Failed to fetch or process file {path} from {repo_name}: {str(e)}")

# Extract data from retention-config repository
print("Extracting data from retention-config repository...")
extract_data_from_repo(cfg_repo, cfg_paths, cfg_repo_name, cfg_source_branch)

# COMMAND ----------

# DBTITLE 1,Ratified Events with/without adhocTables
# Extract tables from all schemas using shared function
all_events, schema_table_mapping = build_schema_table_mapping(env, spark)

# Filter for ratified events
if env=='prod':
  all_ratified_events = [table for table in all_events]
#   if 'ctx_v1_pol_v1' in table or '_v1_pol_v1' in table  or '_v1_ctx_' in table]

else:
  non_alpha_beta_events = [table for table in all_events if 'alpha' not in table and 'beta' not in table]
  all_ratified_events = [table for table in non_alpha_beta_events if 'ctx_v1_pol_v1' in table or '_v1_pol_v1' in table]

print(f"Ratified events found: {len(all_ratified_events)}")

# Log distribution of ratified events by schema
schema_distribution = {}
for table in all_ratified_events:
    schema = schema_table_mapping.get(table, "Unknown")
    schema_distribution[schema] = schema_distribution.get(schema, 0) + 1

print("Ratified events distribution by schema:")
for schema, count in schema_distribution.items():
    print(f"  {schema}: {count} tables")
#Excluding exception list here
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

# Create source mapping using shared function
source_mapping = create_source_mapping(ratified_events)

# Extract individual source lists for backward compatibility
crm_source = source_mapping["crm"]
billing_source = source_mapping["billing"]
support_source = source_mapping["support"]
webshop_source = source_mapping["webshop"]
logistics_source = source_mapping["logistics"]
iot_source = source_mapping["iot"]

bold_black = "\033[1;30m"
reset = "\033[0m"

print(f"{bold_black}Count of crm_source: {len(crm_source)}{reset}")
print(f"{bold_black}Count of billing_source: {len(billing_source)}{reset}")
print(f"{bold_black}Count of support_source: {len(support_source)}{reset}")
print(f"{bold_black}Count of webshop_source: {len(webshop_source)}{reset}")
print(f"{bold_black}Count of logistics_source: {len(logistics_source)}{reset}")
print(f"{bold_black}Count of iot_source: {len(iot_source)}{reset}")
# COMMAND ----------

# DBTITLE 1,Mismatch between json and sources in lake bronze
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

# DBTITLE 1,Ratified and Json Table Comparsion
def find_new_events(source_events, data_storage_key):
    """Find new events not listed in data_storage for a given source type."""
    return [table for table in source_events if table not in data_storage[data_storage_key][0]["tables"]]

# Use shared source mapping instead of hardcoded dictionary
#change1
# Initialize a dictionary to store new events only for source types with discrepancies
# source_events stores catalog tables for each source and data_storage stores json tables from github for each source.
#It will run only for those sources where new table has be created or any table needs to be removed from json.
new_events_to_process = {}

for source_type, source_events in source_mapping.items():
    if len(source_events) != len(data_storage[source_type][0]["tables"]):
        new_events = find_new_events(source_events, source_type)
        if new_events:
            new_events_to_process[source_type] = new_events

# Debug summary: how many input tables will be sent to intelligent mapping

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

new_jsonevents_to_process = [key + "_source" for key in new_events_to_process.keys()]
new_jsonevents_to_process_set = set(new_jsonevents_to_process)

if len(missing_tables_dict) > 0:
    for key, value in missing_tables_dict.items():
        new_jsonevents_to_process_set.add(key + "_source")
    new_jsonevents_to_process = list(new_jsonevents_to_process_set)
else:
    new_jsonevents_to_process = list(new_jsonevents_to_process_set)

print(new_jsonevents_to_process)

# COMMAND ----------

# DBTITLE 1,Extracting sources using regex
import re

sources_found = set()

for text in ratified_events:
    match_platform = re.search(r'_v1_platform_(.*?)_v1_pol_v1', text)
    match_src = re.search(r'_v1_src_(.*?)_v1_pol_v1', text)

    if match_platform:
        sources_found.add(match_platform.group(1))

    if match_src:
        sources_found.add(match_src.group(1))

sources_found.update(sorted(set(word for event in ratified_events for word in re.findall(r"src_([a-zA-Z0-9]+)_v1_ctx", event))))

bold_black = "\033[1;30m"
reset = "\033[0m"

print(f"{bold_black}Sources: {sources_found}{reset}")

# COMMAND ----------

# DBTITLE 1,Exception Columns
# exception_list_columns=['event.detail.deviceId', 'event.detail.customer.id', 'event.detail.email']

if len(ExceptionColumnsList)>0:
  exception_list_columns.extend(ExceptionColumnsList)
  print("Newly added Exception Columns :", exception_list_columns)
else:
  exception_list_columns
  print("Exception Columns:", exception_list_columns)

# COMMAND ----------

# process_source_events took the catalog tables for each source and matched them against column schema data to figure out
# which retention-relevant columns (customer id, email, device id, etc.) each table contained.
# for source in sources_required:
#     process_source_events(source_events_mapping[source], source_type_mapping, schema_cache)

# COMMAND ----------

#crm table specific changes
#contactupdated_v1 and leadmerged_v1 were not identified for customerId, but they have the customerId column, so removing them as not to changes the structure
if 'crm_list' in globals():
    search_conditions = [
        ('contactupdated_v1_src_crm_v1_ctx_v1_pol_v1', lambda col: col != 'event.detail.email'),
        ('leadmerged_v1_src_crm_v1_ctx_v1_pol_v1', lambda col: col != 'event.detail.customer.id'),
        ('optinchanged_v1_src_crm_v1_ctx_v1_pol_v1', lambda col: col == 'event.detail.email')
    ]

    for search_name, condition in search_conditions:
        for item in crm_list:
            if item['name'] == search_name:
                item['columns'] = [col for col in item['columns'] if condition(col)]
                break

    # Remove items with no columns left
    crm_list = [item for item in crm_list if item['columns']]

    # Remodify source type mapping for crm_list
    source_type_mapping["crm"]["list"] = crm_list

# COMMAND ----------

#iot table specific changes
if 'iot_list' in globals():
    search_conditions = [
        ('sensorreading_v1_src_iot_v1_ctx_v1_pol_v1', 'originator.detail.deviceId')
    ]

    for search_name, new_column in search_conditions:
        for item in iot_list:
            if item['name'] == search_name:
                item['columns'] = [new_column]

    iot_list = [item for item in iot_list if item['columns'] != ['No matching columns']]
    source_type_mapping["iot"]["list"] = iot_list
