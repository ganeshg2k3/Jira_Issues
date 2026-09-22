# MAGIC %run "./dependencies/shared_config"

# COMMAND ----------
# MAGIC %run ./dependencies/common_functions

# COMMAND ----------

# Databricks notebook source

# DBTITLE 1,mapping rule column and alias from lake bronze

# # Use direct variable access approach - this works correctly
# crm_rule_table_list = generate_rule_table_list(table_names_crm, {"columns": list({column for table_dict in crm_list for column in table_dict['columns']})}) if 'crm_list' in locals() or 'crm_list' in globals() else []

# billing_rule_table_list = generate_rule_table_list(table_names_billing, {"columns": list({column for table_dict in billing_list for column in table_dict['columns']})}) if 'billing_list' in locals() or 'billing_list' in globals() else []

# iot_rule_table_list = generate_rule_table_list(table_names_iot, {"columns": list({column for table_dict in iot_list for column in table_dict['columns']})}) if 'iot_list' in locals() or 'iot_list' in globals() else []

# COMMAND ----------

# DBTITLE 1,Json Creation
originator_json_results = {}
warning_tables_aggregate = []

# # Use shared source mappings
# for source_name, (source_list_name, rule_table_list_name) in SOURCE_LIST_MAPPINGS.items():
#     if source_list_name in locals() and rule_table_list_name in locals():
#         source_list = locals()[source_list_name]
#         rule_table_list = locals()[rule_table_list_name]
#         skeleton_dict = create_json_skeleton(source_name)
#         try:
#             warning_tables = append_source_data(skeleton_dict, source_list, rule_table_list, env)
#             warning_tables_aggregate.extend(warning_tables)
#             modified_json = json.dumps(skeleton_dict, indent=2)
#             originator_json_results[source_name] = modified_json
#         except Exception as e:
#             print(f"Error processing {source_name}: {e}")

# originator_json_results = {key: value for key, value in originator_json_results.items() if value}

# COMMAND ----------

# DBTITLE 1, Rule Field Mappings
# Note: rule_column_to_table_mapping, paired_fields, ambiguous_fields, and
# rule_to_bronze_path_mapping are imported from dependencies/shared_config.py

# COMMAND ----------

# DBTITLE 1, Rule Mapping Functions
# Note: Common functions (get_table_schema_path, table_has_data_at_path, build_json_table_index,
# find_similar_table_entry, build_rule_sources_from_rule_table, rebuild_rule_sources_from_similar,
# normalize_uri, uris_match, get_table_tags, build_rule_sources_from_fields_config) are imported
# from dependencies/common_functions.py

import builtins
from collections import defaultdict
import json

def load_fields_config_data():
    """Load fields config from GitHub repository"""
    fields_config_repo_name = "retention-fields-config"
    fields_config_path = "lake_retention_fields_config.json"

    try:
        if 'github_api' not in globals():
            print("Warning: github_api not available; skipping fields config load")
            return []

        fields_repo = github_api.get_repo(f"{organization_name}/{fields_config_repo_name}")
        cfg_contents = fields_repo.get_contents(fields_config_path, ref="master")
        fields_config_raw = cfg_contents.decoded_content.decode("utf-8")
        raw_parsed = json.loads(fields_config_raw)

        # Extract config objects from dictionary values
        if isinstance(raw_parsed, dict):
            return [v for v in raw_parsed.values() if isinstance(v, dict)]
        return []
    except Exception as e:
        print(f"Warning: unable to load fields config: {e}")
        return []


def process_new_events_intelligent_mapping(new_events_by_source, table_index, fields_config_data, environment):
    """
    Process new events using intelligent rule mapping (ONLY 2 cases):
    Case 1: Similar/same table exists in JSON files
    Case 2: Table URI tags match fields_config URIs (source_uri + event_uri)

    Note: Tables without matches are NOT added (handled by new_tables_processor exception list)
    """
    new_existing_events = defaultdict(list)
    new_fields_config_events = defaultdict(list)
    warnings_log = []
    skipped_tables = defaultdict(list)  # For logging only

    for source_type, new_tables in (new_events_by_source or {}).items():
        for table_name in new_tables:
            # Case 1: Find similar/same table in existing JSON files
            similar_entry = find_similar_table_entry(source_type, table_name, table_index)

            if similar_entry:
                env_key, matched_table_name, matched_payload = similar_entry
                rebuilt_sources, rebuild_warnings = rebuild_rule_sources_from_similar(
                    table_name, matched_payload, source_type, environment
                )

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

            # Case 2: Match via fields config using URI tags (source_uri + event_uri)
            table_tags = get_table_tags(table_name, environment)
            source_uri = table_tags.get("source_uri")
            event_uri = table_tags.get("event_uri")

            # ONLY proceed if BOTH URI tags exist
            if not source_uri or not event_uri:
                skipped_tables[source_type].append(f"{table_name} (missing URI tags)")
                continue

            # Find matching entry in fields_config
            matched_config_entry = None
            if fields_config_data:
                for entry in fields_config_data:
                    if not isinstance(entry, dict):
                        continue
                    # Both source_uri AND event_uri must match
                    if uris_match(source_uri, entry.get("source_uri")) and \
                       uris_match(event_uri, entry.get("event_uri")):
                        matched_config_entry = entry
                        break

            # ONLY add table if URI match found in fields_config
            if matched_config_entry:
                candidate_sources, cfg_warnings = build_rule_sources_from_fields_config(
                    table_name, matched_config_entry, source_type, environment
                )
                warnings_log.extend(cfg_warnings)

                if candidate_sources:
                    new_fields_config_events[source_type].append({
                        "name": table_name,
                        "rule_sources": candidate_sources,
                        "config_source_uri": matched_config_entry.get("source_uri"),
                        "config_event_uri": matched_config_entry.get("event_uri"),
                        "table_source_uri": source_uri,
                        "table_event_uri": event_uri
                    })
                else:
                    skipped_tables[source_type].append(f"{table_name} (no valid rule_sources from fields_config)")
            else:
                # No URI match in fields_config - skip (handled by new_tables_processor)
                skipped_tables[source_type].append(
                    f"{table_name} (no fields_config match for uris: {source_uri}, {event_uri})"
                )

    return new_existing_events, new_fields_config_events, skipped_tables, warnings_log


def apply_intelligent_mapping_to_json(originator_json_results, new_existing_events, new_fields_config_events, environment):
    """Apply intelligently mapped rule sources to source JSON results"""
    intelligently_mapped_tables = set()

    def ensure_json_dict(source_key):
        if source_key in originator_json_results:
            current_val = originator_json_results[source_key]
            return json.loads(current_val) if isinstance(current_val, str) else current_val
        return create_json_skeleton(source_key)

    def upsert_table(json_dict, table_name, rule_sources):
        env_section = json_dict.setdefault(environment, {})
        config = env_section.setdefault("retention_configuration", {})
        data_tables = config.setdefault("data_tables", [])

        # Check if table already exists
        for entry in data_tables:
            if isinstance(entry, dict) and entry.get("name") == table_name:
                if not entry.get("rule_sources"):
                    entry["rule_sources"] = rule_sources
                return

        # Add new table
        data_tables.append({"name": table_name, "rule_sources": rule_sources})

    # Process both case 1 and case 2 results
    for source_dict in (new_existing_events, new_fields_config_events):
        for source_name, entries in source_dict.items():
            if not entries:
                continue
            json_dict = ensure_json_dict(source_name)
            for entry in entries:
                upsert_table(json_dict, entry["name"], entry["rule_sources"])
                intelligently_mapped_tables.add(entry["name"])
            originator_json_results[source_name] = json.dumps(json_dict, indent=2)

    return originator_json_results, intelligently_mapped_tables


# Execute intelligent mapping if new_events_to_process exists
intelligently_mapped_tables = set()

try:
    if 'new_events_to_process' in globals() and new_events_to_process:
        fields_config_data = load_fields_config_data()
        table_index = build_json_table_index(data_storage)

        new_existing_events, new_fields_config_events, skipped_tables, warnings_log = process_new_events_intelligent_mapping(
            new_events_to_process, table_index, fields_config_data, env
        )

        if warnings_log:
            print(f"\n{len(warnings_log)} warnings during intelligent mapping:")
            for w in warnings_log[:10]:
                print(f"  - {w}")
            if len(warnings_log) > 10:
                print(f"  ... and {len(warnings_log) - 10} more warnings")

        if new_existing_events:
            case1_count = builtins.sum(len(v) for v in new_existing_events.values())
            print(f"\n✓ Case 1 (Similar/Same Tables): Mapped {case1_count} tables")
            for source_type, entries in new_existing_events.items():
                for entry in entries:
                    print(f"  - [{source_type}] {entry['name']} (from {entry['derived_from']})")

        if new_fields_config_events:
            case2_count = builtins.sum(len(v) for v in new_fields_config_events.values())
            print(f"\n✓ Case 2 (Fields Config URI Match): Mapped {case2_count} tables")
            for source_type, entries in new_fields_config_events.items():
                for entry in entries:
                    print(f"  - [{source_type}] {entry['name']}")
                    print(f"      Table URIs: {entry['table_source_uri']}, {entry['table_event_uri']}")
                    print(f"      Config URIs: {entry['config_source_uri']}, {entry['config_event_uri']}")

        if skipped_tables:
            skipped_count = builtins.sum(len(v) for v in skipped_tables.values())
            print(f"\nℹ️ Skipped Tables (handled by new_tables_processor): {skipped_count} tables")
            for source_type, tables in skipped_tables.items():
                if tables:
                    print(f"  [{source_type}]:")
                    for table in tables[:5]:  # Show first 5 per source type
                        print(f"    - {table}")
                    if len(tables) > 5:
                        print(f"    ... and {len(tables) - 5} more")

        # Apply intelligent mappings to JSON results
        originator_json_results, intelligently_mapped_tables = apply_intelligent_mapping_to_json(
            originator_json_results, new_existing_events, new_fields_config_events, env
        )

        print(f"\n📊 Summary: {len(intelligently_mapped_tables)} tables successfully mapped and protected")
    else:
        print("No new_events_to_process detected; skipping intelligent rule mapping")
except Exception as exc:
    print(f"❌ Error: rule mapping failed: {exc}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# DBTITLE 1,Warning Tables
if len(warning_tables_aggregate)>0:
  alert_message = f"Number of Tables which are mapped to ret_device_keys_enrich and are not having either device id or model are: {warning_tables_aggregate} "
  displayHTML(f"<div style='color: red; font-size: 16px; border: 2px solid red; padding: 10px;'><strong>Warning:</strong> {alert_message}</div>")
else:
  alert_message = f"All Tables have required retention fields "
  displayHTML(f"<div style='color: green; font-size: 16px; border: 2px solid red; padding: 10px;'><strong>Success:</strong> {alert_message}</div>")



# COMMAND ----------

# DBTITLE 1,Changes for specific crm tables
import json

# Apply special rule_sources ONLY for specific tables that need custom configuration
for key, value in originator_json_results.items():
    if "crm" in key:
        value_dict = json.loads(value) if isinstance(value, str) else value
        if isinstance(value_dict, dict) and env in value_dict:
            data_tables = value_dict[env]["retention_configuration"]['data_tables']
            for table in data_tables:
                table_name = table.get('name', '')

                # Skip custom config if table was intelligently mapped (Case 1 or Case 2)
                if table_name in intelligently_mapped_tables:
                    print(f"ℹ️ Skipping custom config for '{table_name}' - already intelligently mapped")
                    continue

                # Only apply custom handling to these specific tables (if not intelligently mapped)
                if table_name == 'leadmerged_v1_src_crm_v1_ctx_v1_pol_v1':
                    table['rule_sources'] = [
                        {'name': 'ret_customer_keys', 'table_key_aliases': {'customer_id': 'event.detail.customer.id'}}
                    ]
                    print(f"✓ Applied custom config for: {table_name}")

                elif table_name == 'leadmerged_v1_src_crm_v1_pol_v1':
                    table['rule_sources'] = [
                        {'name': 'ret_customer_keys', 'table_key_aliases': {'customer_id': 'event.detail.customer.id'}}
                    ]
                    print(f"✓ Applied custom config for: {table_name}")

                elif table_name == 'contactphone_v1_src_crm_v1_ctx_v1_pol_v1':
                    table['rule_sources'] = [
                        {'name': 'ret_phone_keys', 'table_key_aliases': {'phone': 'event.detail.phone'}}
                    ]
                    print(f"✓ Applied custom config for: {table_name}")

                elif table_name == 'contactphone_v1_src_crm_v1_pol_v1':
                    table['rule_sources'] = [
                        {'name': 'ret_phone_keys', 'table_key_aliases': {'phone': 'event.detail.phone'}}
                    ]
                    print(f"✓ Applied custom config for: {table_name}")

                # All other tables: preserve their existing rule_sources unchanged

            originator_json_results[key] = json.dumps(value_dict, indent=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Json update
# MAGIC ## 2.Final json generated with updated value

# COMMAND ----------

Original_json={}

for i,j in data_storage.items():
  Original_json[i]=j[0]['original_json']



# COMMAND ----------

import json

final_result = {}

key_to_category = {
    "crm": "crm",
    "billing": "billing",
    "support": "support",
    "webshop": "webshop",
    "logistics": "logistics",
    "iot": "iot"
}

for key, value in originator_json_results.items():
    category = key_to_category.get(key)

    if category and category in Original_json:
        # Handle both string and dict types for value
        if isinstance(value, str):
            try:
                decoded_value = json.loads(value)
            except json.JSONDecodeError:
                print(f"Value associated with {key} is not valid JSON. Skipping.")
                continue
        elif isinstance(value, dict):
            decoded_value = value
        else:
            print(f"Unexpected type for value associated with {key}: {type(value)}. Skipping.")
            continue

        if isinstance(decoded_value, dict):
            required_config = decoded_value.get(env, {}).get('retention_configuration', None)
            if required_config:
                if env not in Original_json[category]:
                    Original_json[category][env] = {}
                if 'retention_configuration' not in Original_json[category][env]:
                    Original_json[category][env]['retention_configuration'] = {}

                # INTELLIGENT MERGE: Preserve existing tables with their rule_sources
                # Only add NEW tables from required_config
                original_tables = Original_json[category][env]['retention_configuration'].get('data_tables', [])
                new_tables = required_config.get('data_tables', [])

                # Create a mapping of existing table names to their full configurations
                original_table_map = {
                    table['name']: table
                    for table in original_tables
                    if 'name' in table
                }

                new_table_map = {
                    table['name']: table
                    for table in new_tables
                    if 'name' in table
                }

                # Build merged list
                merged_tables = []
                new_tables_added = []
                intelligently_updated = []

                # First, preserve ALL existing tables exactly as they are
                for original_table in original_tables:
                    table_name = original_table.get('name')
                    if not table_name:
                        merged_tables.append(original_table)
                        continue

                    # If table was intelligently mapped, use the INTELLIGENT version (Case 1 or Case 2)
                    if table_name in intelligently_mapped_tables and table_name in new_table_map:
                        merged_tables.append(new_table_map[table_name])
                        intelligently_updated.append(table_name)
                        print(f"Updated via intelligent mapping: {table_name}")
                    else:
                        # Preserve legacy rule_sources for non-intelligently-mapped tables
                        merged_tables.append(original_table)

                # Then, add only truly NEW tables (not in original)
                for new_table in new_tables:
                    table_name = new_table.get('name')
                    if table_name and table_name not in original_table_map:
                        merged_tables.append(new_table)
                        new_tables_added.append(table_name)
                        print(f"  ✓ Added new table: {table_name}")

                # Update the configuration with merged list
                Original_json[category][env]['retention_configuration']['data_tables'] = merged_tables
                final_result[category] = Original_json[category]
            else:
                print(f"Decoded value for {key} does not contain expected {env}->'retention_configuration' structure.")
        else:
            print(f"Decoded value for {key} is not a dictionary.")



# COMMAND ----------
# DBTITLE 1,Remove tables with empty rule_sources (except intelligently mapped tables)
# Remove the entry of the table where rule_sources is empty
# BUT preserve tables that were intelligently mapped (they might have valid empty sources in some cases)
for source_type, tables in final_result.items():
    entries_to_remove = []
    env_section = tables.get(env, {})
    data_tables = env_section.get('retention_configuration', {}).get('data_tables', [])

    if data_tables:
        for entry in data_tables:
            if isinstance(entry, dict) and not entry.get("rule_sources"):
                table_name = entry.get('name', '')

                # Don't remove intelligently mapped tables even if rule_sources is empty
                # (they were validated by the intelligent mapping logic)
                if table_name in intelligently_mapped_tables:
                    print(f"ℹ️ Info: Table '{table_name}' in '{source_type}' was intelligently mapped - preserving even with empty rule_sources.")
                else:
                    print(f"⚠️ Warning: Table '{table_name}' in '{source_type}' has an empty 'rule_sources'. It will be removed.")
                    entries_to_remove.append(entry)

        for entry in entries_to_remove:
            data_tables.remove(entry)

        # Update the final_result dictionary
        final_result[source_type][env]['retention_configuration']['data_tables'] = data_tables
