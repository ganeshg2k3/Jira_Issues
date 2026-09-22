# Databricks notebook source

# DBTITLE 1,Shared Configuration Constants

# Schema configuration used across multiple notebooks
def get_schemas_to_check(env):
    """Get list of schemas to check for tables"""
    return [
        f"team_retail_{env}.lake_bronze",
        f"lake_{env}.lake_bronze_sensitive_orders",
        f"lake_{env}.lake_bronze_sensitive_profiles",
        f"lake_{env}.lake_bronze_sensitive_telemetry"
    ]

# COMMAND ----------

# DBTITLE 1,Source Constants

# List of all supported sources
# SOURCES = [
#     "crm",
#     "billing",
#     "support",
#     "webshop",
#     "logistics",
#     "iot"
# ]

# Mapping for source names to their corresponding list variable names
# SOURCE_LIST_MAPPINGS = {
#     "crm": ("crm_list", "crm_rule_table_list"),
#     "billing": ("billing_list", "billing_rule_table_list"),
#     "support": ("support_list", "support_rule_table_list"),
#     "webshop": ("webshop_list", "webshop_rule_table_list"),
#     "logistics": ("logistics_list", "logistics_rule_table_list"),
#     "iot": ("iot_list", "iot_rule_table_list")
# }

# COMMAND ----------

# DBTITLE 1,Shared Rule Generation Functions
# LEGACY: Not used in current intelligent mapping flow (Case1/Case2/Case3).
# These helpers were wrappers around generate_rule_table_list from the old pre-check pipeline.

# def generate_rule_list_for_source(source, generate_rule_table_list_func):
#     table_names_var = f"table_names_{source}"
#     source_list_var = f"{source}_list"
#
#     if (table_names_var in globals() and source_list_var in globals() and
#         globals()[source_list_var]):
#         table_names = globals()[table_names_var]
#         source_list = globals()[source_list_var]
#         unique_columns = list({
#             column
#             for table_dict in source_list
#             for column in table_dict['columns']
#         })
#         return generate_rule_table_list_func(table_names, {"columns": unique_columns})
#     return []

# COMMAND ----------

# DBTITLE 1,Schema Table Mapping Helper

def build_schema_table_mapping(env, spark):
    """
    Build mapping of table names to their schemas

    Returns:
        Tuple: (all_events, schema_table_mapping)
    """
    schemas_to_check = get_schemas_to_check(env)
    all_events = []
    schema_table_mapping = {}

    for schema in schemas_to_check:
        try:
            lake_tables = spark.catalog.listTables(schema)
            schema_events = [i.name for i in lake_tables]
            all_events.extend(schema_events)

            # Store schema mapping for each table
            for table in schema_events:
                schema_table_mapping[table] = schema

            print(f"Found {len(schema_events)} tables in schema: {schema}")
        except Exception as e:
            print(f"Warning: Could not access schema {schema}: {str(e)}")

    print(f"Total tables found across all schemas: {len(all_events)}")
    return all_events, schema_table_mapping

# COMMAND ----------

# DBTITLE 1,Common Source Mapping Utility

def create_source_mapping(all_ratified_events):
    """
    Create source mapping for all sources based on ratified events

    Returns:
        Dictionary mapping source type to list of tables
    """
    source_mapping = {
        "crm": [s for s in all_ratified_events if '_src_crm_' in s.lower()],
        "billing": [s for s in all_ratified_events if '_src_billing_' in s.lower() or '_src_payments_' in s.lower()],
        "support": [s for s in all_ratified_events if '_src_support_' in s.lower()],
        "webshop": [s for s in all_ratified_events if '_src_webshop_' in s.lower()],
        "logistics": [s for s in all_ratified_events if '_src_logistics_' in s.lower()],
        "iot": [s for s in all_ratified_events if '_platform_iot_' in s.lower() or '_src_iot_' in s.lower()]
    }

    return source_mapping

# COMMAND ----------

# DBTITLE 1,Rule Table Mappings

# Maps retention field identifiers to their corresponding rule tables
rule_column_to_table_mapping = {
    'device_id': 'ret_device_keys_enrich',
    'device_model': 'ret_device_keys_enrich',
    'deviceid': 'ret_device_only_keys',
    'customer_id': 'ret_customer_keys',
    'email': 'ret_email_keys',
    'phone': 'ret_phone_keys'
}

# Paired Fields Configuration (a field that must always travel with its partner)
paired_fields = {
    'originator.detail.deviceModel': 'originator.detail.deviceId',
    'originator.detail.deviceId': ['originator.detail.deviceModel', 'originator.detail.hwModel'],
    'originator.detail.hwModel': 'originator.detail.deviceId',
    'event.detail.deviceModel': 'event.detail.deviceId',
    'event.detail.deviceId': ['event.detail.deviceModel', 'event.detail.hwModel'],
    'event.detail.hwModel': 'event.detail.deviceId'
}

# Fields that can appear in multiple contexts and need special handling to avoid duplicates
ambiguous_fields = ['event.detail.deviceId', 'originator.detail.deviceId']

# Retention Field to Bronze Field Mapping
# Key: retention field name (used in rule tables)
# Value: List of JSON paths in the bronze layer rawJson column
rule_to_bronze_path_mapping = {
    'customer_id': ['originator.detail.customerId',
                    'event.detail.customer.id',
                    'event.detail.accountId',
                    'context.detail.associatedCustomerId'],
    'email': ['event.detail.email',
              'event.detail.contact.email'],
    'phone': ['event.detail.phone'],
    'deviceid': ['originator.detail.deviceId',
                 'event.detail.deviceId'],
    'device_model': ['originator.detail.deviceModel',
                     'event.detail.deviceModel',
                     'originator.detail.hwModel',
                     'event.detail.hwModel'],
    'device_id': ['event.detail.deviceId',
                  'originator.detail.deviceId']
}
