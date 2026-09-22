# MAGIC %run "./shared_config"

# COMMAND ----------

# Databricks notebook source
# DBTITLE 1,lake_bronze_identified columns
# crm_list_columns=["originator.detail.customerId", "event.detail.email"]

# billing_list_columns=["event.detail.customer.id", "event.detail.accountId"]

# support_list_columns=["event.detail.contact.email", "context.detail.associatedCustomerId"]

# webshop_list_columns=["context.detail.associatedCustomerId"]

# logistics_list_columns=["event.detail.phone", "event.detail.orderRef"]

# # IoT shares its retention account space with logistics
# # Uses ret_device_keys_enrich on device_id/device_model
# iot_list_columns=["originator.detail.deviceId", "originator.detail.deviceModel", "originator.detail.hwModel"]

# COMMAND ----------
# DBTITLE 1,Events need to be removed from Json
suppression_list = ['legacyprofile_v1_src_crm_v1_ctx_v1_pol_v1', 'oldinvoice_v1_src_billing_v1_ctx_v1_pol_v1', 'retiredcampaign_v2_src_crm_v1_ctx_v1_pol_v1']

purge_list = ['contactupdatd_v1_src_crm_v1_ctx_v1_pol_v1', 'tickte_v1_src_support_v1_ctx_v1_pol_v1', 'shipmnt_v1_src_logistics_v1_ctx_v1_pol_v1', 'sensorreadng_v1_src_iot_v1_ctx_v1_pol_v1']

No_Rules_list = ['heartbeat_v1_src_iot_v1_ctx_v1_pol_v1', 'pageview_v1_src_webshop_v1_ctx_v1_pol_v1']

No_Context_list = ['bannerclick_v1_src_webshop_v1_pol_v1', 'sessionstart_v1_src_webshop_v1_pol_v1']

# COMMAND ----------
# DBTITLE 1,Exception List
exception_list_columns = ['event.detail.deviceId', 'event.detail.customer.id', 'event.detail.email', 'event.detail.phone', 'event.detail.accountId']

# COMMAND ----------
# DBTITLE 1,Github data

bold_black = "\033[1;30m"
reset = "\033[0m"

def process_content(path, content_data, repo_name):
    """Process JSON content and extract table names, also store original JSON."""
    json_data = json.loads(content_data)
    tables_names = [table["name"] for table in json_data[env]["retention_configuration"]["data_tables"]]
    print(f"{bold_black}Repo: {repo_name} - File: {path} - Total tables in JSON: {len(tables_names)}{reset}")
    return {
        "repo": repo_name,
        "path": path,
        "tables": tables_names,
        "original_json": json_data,
        "length": len(tables_names)
    }

# COMMAND ----------
# DBTITLE 1,Schema Extraction
from pyspark.sql.functions import *
from pyspark.sql.types import *

def extract_fields_from_schema(schema, prefix=""):
    fields = []
    for field in schema.fields:
        if isinstance(field.dataType, StructType):
            fields += extract_fields_from_schema(field.dataType, prefix + field.name + ".")
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            fields += extract_fields_from_schema(field.dataType.elementType, prefix + field.name + ".")
        else:
            fields.append((prefix + field.name, str(field.dataType)))
    return fields

# COMMAND ----------
# DBTITLE 1,Helper function to determine table schema

def get_table_schema_path(table_name, env):
    """Determine the correct schema path for a table"""
    # First check if we have schema mapping from Retention_Pre_Check
    if 'schema_table_mapping' in globals() and table_name in schema_table_mapping:
        return f"{schema_table_mapping[table_name]}.{table_name}"

    # Fallback: check each schema manually using shared function
    schemas_to_check = get_schemas_to_check(env)

    for schema in schemas_to_check:
        try:
            tables = spark.catalog.listTables(schema)
            if any(table.name == table_name for table in tables):
                return f"{schema}.{table_name}"
        except Exception as e:
            continue

    # Default fallback to original schema
    return f"team_retail_{env}.lake_bronze.{table_name}"

# COMMAND ----------
# DBTITLE 1,Generate rule table list
# LEGACY: Not used in current intelligent mapping flow (Case1/Case2/Case3).
# Kept for reference. Part of old pre-check column scan pipeline.
# def generate_rule_table_list(table_names, valid_columns):
#     rule_table_list = []
#     for table in table_names:
#         column_query = f"DESCRIBE team_retail_{env}.ref_enrich.{table}"
#         columns = spark.sql(column_query).collect()
#         for row in columns:
#             column = row.col_name
#             for alias, potential_columns in alias_to_column_mapping.items():
#                 if column in potential_columns and alias in valid_columns["columns"]:
#                     rule_table_list.append({
#                         "name": table,
#                         "column": column,
#                         "alias": alias
#                     })
#     return rule_table_list

# COMMAND ----------
# DBTITLE 1,Json Skeleton

def create_json_skeleton(source_name):
    """Create an empty config skeleton for one source across all environments"""
    skeleton = {}
    for env_key in ["dev", "itg", "prod"]:
        skeleton[env_key] = {
            "retention_configuration": {
                "source": source_name,
                "data_tables": []
            }
        }
    return skeleton

key_to_category = {
    "crm": "crm",
    "billing": "billing",
    "support": "support",
    "webshop": "webshop",
    "logistics": "logistics",
    "iot": "iot"
}

# COMMAND ----------
# DBTITLE 1,Post json check

def check_json_skeleton(skeleton_dict, ratifiedevent_list):
    """Compare tables present in the generated json against the ratified list"""
    tables_in_json = [t["name"] for t in skeleton_dict[env]["retention_configuration"]["data_tables"]]
    missing_in_json = [t for t in ratifiedevent_list if t not in tables_in_json]
    extra_in_json = [t for t in tables_in_json if t not in ratifiedevent_list]

    if not missing_in_json and not extra_in_json:
        return "Json check passed: json tables match ratified tables"

    message = "Json check found differences:"
    if missing_in_json:
        message += f" missing in json={missing_in_json};"
    if extra_in_json:
        message += f" not ratified={extra_in_json};"
    return message
