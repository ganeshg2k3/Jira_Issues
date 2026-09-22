# Databricks notebook source
# DBTITLE 1,Common Retention Functions
"""
Common functions shared between new_tables_processor.py and Retention_Processing.py
These functions handle rule mapping logic, table schema resolution, and fields configuration.
"""

# COMMAND ----------

from collections import defaultdict

# COMMAND ----------
# DBTITLE 1,Schema and Table Resolution Functions

def get_table_schema_path(table_name, environment):
    """Get the full schema path for a table using shared config function"""
    try:
        if 'schema_table_mapping' in globals() and table_name in schema_table_mapping:
            return f"{schema_table_mapping[table_name]}.{table_name}"

        # Fallback: check each schema manually
        schemas_to_check = get_schemas_to_check(environment)
        for schema in schemas_to_check:
            try:
                tables = spark.catalog.listTables(schema)
                if any(t.name == table_name for t in tables):
                    return f"{schema}.{table_name}"
            except Exception:
                continue
        return None
    except Exception as e:
        print(f"Warning: Error resolving schema for {table_name}: {e}")
        return None


def table_has_data_at_path(table_name, field_path, environment):
    """Check if table has data at specified field path"""
    try:
        table_path = get_table_schema_path(table_name, environment)
        if not table_path:
            return False
        df = spark.read.table(table_path).select(col(field_path)).where(col(field_path).isNotNull()).limit(1)
        return df.count() > 0
    except Exception as e:
        print(f"Warning: unable to check data for {table_name}.{field_path}: {e}")
        return False


def table_has_column_at_path(table_name, field_path, environment):
    """Check if table has a column at the specified field path (regardless of whether it has data)"""
    try:
        table_path = get_table_schema_path(table_name, environment)
        if not table_path:
            return False
        df = spark.read.table(table_path)
        df.select(col(field_path))  # Will throw if column doesn't exist in schema
        return True
    except Exception:
        return False


# COMMAND ----------
# DBTITLE 1,JSON Table Index Functions

def build_json_table_index(storage):
    """Index tables from existing source JSON by source and env for quick lookup"""
    table_index = defaultdict(lambda: defaultdict(dict))
    for source, entries in storage.items():
        for entry in entries:
            try:
                payload = entry.get("original_json", {})
                for env_key, env_payload in payload.items():
                    if not isinstance(env_payload, dict):
                        continue
                    tables = env_payload.get("retention_configuration", {}).get("data_tables", [])
                    table_index[source][env_key] = {tbl["name"]: tbl for tbl in tables}
            except Exception as exc:
                print(f"Warning: failed to index json tables for {source}: {exc}")
    return table_index


def find_similar_table_entry(source_type, table_name, table_index):
    """Find an existing table with the same prefix (before _v) inside current JSON"""
    base_prefix = table_name.split("_v", 1)[0]
    for env_key, table_map in table_index.get(source_type, {}).items():
        for existing_name, entry in table_map.items():
            if existing_name == table_name or existing_name.split("_v", 1)[0] == base_prefix:
                return env_key, existing_name, entry
    return None

# COMMAND ----------
# DBTITLE 1,Rule Sources Building Functions

def build_rule_sources_from_rule_table(table_name, rule_table_name, environment):
    """
    Build rule_sources by reverse-engineering from rule table name.
    Used for both case 1 (similar tables) and case 2 (fields config).
    Returns (rule_source_dict, warnings_list).
    """
    warnings = []

    # Step 1: Reverse lookup - find retention field key(s) that map to this rule table
    matching_fields = [
        field for field, mapped_table in rule_column_to_table_mapping.items()
        if mapped_table == rule_table_name
    ]

    if not matching_fields:
        warnings.append(
            f"Rule table '{rule_table_name}' not found in rule_column_to_table_mapping"
        )
        return None, warnings

    # Step 2: Build table_key_aliases using paths from rule_to_bronze_path_mapping
    new_aliases = {}
    for field in matching_fields:
        possible_paths = rule_to_bronze_path_mapping.get(field, [])

        # Find first path with data in the new table
        for field_path in possible_paths:
            if table_has_data_at_path(table_name, field_path, environment):
                new_aliases[field] = field_path
                break
        else:
            # Fallback: if no path has data, use first path that exists in schema
            for field_path in possible_paths:
                if table_has_column_at_path(table_name, field_path, environment):
                    new_aliases[field] = field_path
                    break

    # Step 3: Apply paired_fields validation for ret_device_keys_enrich
    if rule_table_name == "ret_device_keys_enrich":
        if not {"device_id", "device_model"}.issubset(set(new_aliases.keys())):
            warnings.append(
                f"Skipping {rule_table_name} for {table_name} - missing required pair (device_id + device_model)"
            )
            return None, warnings

    if not new_aliases:
        warnings.append(f"No valid data paths found for rule table '{rule_table_name}' in table {table_name}")
        return None, warnings

    return {
        "name": rule_table_name,
        "table_key_aliases": new_aliases
    }, warnings


def rebuild_rule_sources_from_similar(table_name, similar_payload, source_type, environment):
    """Rebuild rule_sources by reverse-engineering rule table names from similar table"""
    rebuilt_sources = []
    rebuild_warnings = []

    for source in similar_payload.get("rule_sources", []):
        rule_table_name = source.get("name")

        # Use unified function to rebuild from rule table name
        rule_source, build_warnings = build_rule_sources_from_rule_table(
            table_name, rule_table_name, environment
        )
        rebuild_warnings.extend(build_warnings)

        if rule_source:
            rebuilt_sources.append(rule_source)

    return rebuilt_sources, rebuild_warnings

# COMMAND ----------
# DBTITLE 1,URI Matching Functions

def normalize_uri(uri_value):
    """Normalize URI value by removing revision suffix"""
    if not uri_value:
        return None
    return uri_value.split('.rev', 1)[0].lower()


def uris_match(table_uri, config_uri):
    """Check if two URI values match after normalization"""
    return normalize_uri(table_uri) and normalize_uri(table_uri) == normalize_uri(config_uri)


def get_table_tags(table_name, environment):
    """Fetch Unity Catalog tags for a table"""
    try:
        table_path = get_table_schema_path(table_name, environment)
        if not table_path:
            return {}
        schema = table_path.rsplit('.', 1)[0]
        catalog_schema = schema.rsplit('.', 1)[0] if '.' in schema else schema

        tags_df = spark.sql(
            f"SELECT tag_name, tag_value FROM {catalog_schema}.information_schema.table_tags "
            f"WHERE table_name = '{table_name}'"
        )
        return {row.tag_name: row.tag_value for row in tags_df.collect()}
    except Exception as exc:
        print(f"Warning: unable to get table tags for {table_name}: {exc}")
        return {}

# COMMAND ----------
# DBTITLE 1,Fields Config Processing Functions

def build_rule_sources_from_fields_config(table_name, config_entry, source_type, environment):
    """
    Build rule_sources from fields config entry.
    Returns (rule_sources_list, warnings_list).
    """
    rule_sources = []
    warnings = []
    retention_fields = config_entry.get("retention_fields", []) or []

    # Extract all field paths from config
    all_field_paths = []
    for field_map in retention_fields:
        for _, field_path in field_map.items():
            all_field_paths.append(field_path)

    device_id_paths = []
    device_model_paths = []

    for field_path in all_field_paths:
        # Ambiguous device id paths are collected and resolved later
        if field_path in ambiguous_fields:
            device_id_paths.append(field_path)
            continue

        # Device model paths are only valid as part of a pair
        if field_path in rule_to_bronze_path_mapping.get('device_model', []):
            device_model_paths.append(field_path)
            continue

        field_name = None
        for candidate_name, known_paths in rule_to_bronze_path_mapping.items():
            if field_path in known_paths:
                field_name = candidate_name
                break

        if not field_name:
            warnings.append(f"Field path '{field_path}' not found in rule_to_bronze_path_mapping")
            continue

        # Get the rule table for this retention field
        rule_table = rule_column_to_table_mapping.get(field_name)
        if not rule_table:
            warnings.append(f"No rule table found for retention field '{field_name}'")
            continue

        if not table_has_data_at_path(table_name, field_path, environment):
            # Fallback: check if column exists in schema even without data
            if not table_has_column_at_path(table_name, field_path, environment):
                warnings.append(f"Column path '{field_path}' does not exist in table {table_name} schema")
                continue
            warnings.append(f"No data found at path '{field_path}' in table {table_name}, proceeding with schema match")

        rule_sources.append({
            "name": rule_table,
            "table_key_aliases": {
                field_name: field_path
            }
        })

    if device_id_paths and device_model_paths:
        matched = False
        for id_path in device_id_paths:
            for model_path in device_model_paths:
                if table_has_data_at_path(table_name, id_path, environment) and \
                   table_has_data_at_path(table_name, model_path, environment):
                    rule_sources.append({
                        "name": "ret_device_keys_enrich",
                        "table_key_aliases": {
                            "device_id": id_path,
                            "device_model": model_path
                        }
                    })
                    matched = True
                    break
            if matched:
                break
    elif device_id_paths:
        for id_path in device_id_paths:
            if table_has_data_at_path(table_name, id_path, environment):
                rule_sources.append({
                    "name": "ret_device_only_keys",
                    "table_key_aliases": {
                        "deviceid": id_path
                    }
                })

    return rule_sources, warnings
