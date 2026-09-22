# Databricks notebook source
# DBTITLE 1,Post Json checks
import json
from pyspark.dbutils import DBUtils

dbutils = DBUtils(spark)

for key, json_str in originator_json_results.items():
    json_data = json.loads(json_str)
    key_to_check = key + "_list"
    ratified_list = globals().get(key_to_check)

    # If the *_list variable doesn't exist (intelligent mapping case),
    # use source_mapping instead
    if ratified_list is None and 'source_mapping' in globals():
        ratified_list = source_mapping.get(key, [])
        if ratified_list:
            print(f"Using source_mapping for {key} validation ({len(ratified_list)} tables)")

    # Only run validation if we have a ratified list to check against
    if ratified_list:
        result = check_json_skeleton(json_data, ratified_list)
        print(result)
    else:
        print(f"Skipping validation for {key} (no ratified list available - intelligent mapping may have filtered tables)")

# COMMAND ----------

# DBTITLE 1,PR Raise
from github import Github
import json
from datetime import datetime


new_branch_name = branch


def create_branch(repo, base_branch, new_branch_name):
    source_branch = repo.get_branch(base_branch)
    try:
        repo.get_branch(new_branch_name)
        print(f"Branch '{new_branch_name}' already exists.")
        dbutils.notebook.exit(f"Branch {new_branch_name} already exist, delete branch and run again")
    except:
        repo.create_git_ref(ref=f"refs/heads/{new_branch_name}", sha=source_branch.commit.sha)
        print(f"Branch '{new_branch_name}' created.")

create_branch(cfg_repo, cfg_base_branch, new_branch_name)


# COMMAND ----------
keys_of_interest = [
    "crm", "billing", "support",
    "webshop", "logistics", "iot"
]

data_to_commit = {
    f"lake_loader_{k}_retention": final_result.get(k, '')
    for k in keys_of_interest
}


def convert_to_hashable(item):
    if isinstance(item, dict):
        return tuple(sorted((k, convert_to_hashable(v)) for k, v in item.items()))
    elif isinstance(item, list):
        return tuple(convert_to_hashable(i) for i in item)
    return item


def extract_names(tables_set):
    return [table[0][1] for table in tables_set if '_src_' in table[0][1] or '_platform_' in table[0][1]]


def update_file(repo, path_to_file, updated_content, new_branch_name, key):
    try:
        contents = repo.get_contents(path_to_file, ref=new_branch_name)
        sha = contents.sha
        file_content = contents.decoded_content.decode('utf-8')
        file_content_dict = json.loads(file_content)

        new_content_dict = json.loads(updated_content)
        original_table = file_content_dict[env]["retention_configuration"]['data_tables']
        new_table = new_content_dict[env]["retention_configuration"]['data_tables']

        original_set = {convert_to_hashable(d) for d in original_table}
        new_set = {convert_to_hashable(d) for d in new_table}

        tables_in_new_not_in_original = new_set - original_set
        tables_removed_from_json = original_set - new_set

        new_tables = extract_names(tables_in_new_not_in_original)
        deleted_tables = extract_names(tables_removed_from_json)
        source = key.split('_')[2]

        if not new_tables and not deleted_tables:
            print(f"No table changes for {path_to_file} - skipping update")
            return []

        changes = [
            f"Updated {path_to_file}",
            f"updated tables under source: {source}"
        ]
        if new_tables:
            changes.append(f"New Tables added for {key}:\n  - " + "\n  - ".join(new_tables))
        if deleted_tables:
            changes.append(f"Tables removed from JSON for {key}:\n  - " + "\n  - ".join(deleted_tables))

        repo.update_file(path_to_file, "Update retention JSON data", updated_content, sha, branch=new_branch_name)
        print(f"✅ Updated {path_to_file} in branch '{new_branch_name}'")

        return changes

    except Exception as e:
        print(f"❌ Error updating {path_to_file} in '{new_branch_name}': {e}")
        return [f"Error processing {path_to_file}: {e}"]


# === Process and update repo ===
all_changes = []

# Part 1: Update source_json files (ratified tables)
print("UPDATING SOURCE_JSON FILES")
source_updated_count = 0

for key, value in data_to_commit.items():
    if not value:
        continue

    try:
        value_dict = json.loads(value) if isinstance(value, str) else value
        updated_content = json.dumps(value_dict, indent=2)

        # Update retention config repo
        cfg_path = f"retention_json_automation/source_json/{key}.json"
        changes_cfg = update_file(cfg_repo, cfg_path, updated_content, new_branch_name, key)
        all_changes.extend(changes_cfg)
        source_updated_count += 1

    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error for {key}: {e}")

print(f"\n✅ Completed source_json updates: {source_updated_count} files processed\n")

# COMMAND ----------

# Part 2: Update rawjson error files from local filesystem
print("=" * 60)
print("PROCESSING RAWJSON ERROR FILES")
print("=" * 60)

# Define the rawjson files to check and potentially update
rawjson_files = [
    'lake_loader_error_crm_retention.json',
    'lake_loader_error_billing_retention.json',
    'lake_loader_error_support_retention.json',
    'lake_loader_error_webshop_retention.json',
    'lake_loader_error_logistics_retention.json',
    'lake_loader_error_iot_retention.json',
    'lake_loader_error_crm_bridge_retention.json'
]

rawjson_updated_count = 0

for filename in rawjson_files:
    try:
        # Read the local file (updated by rawConfig_new_rules.py)
        local_path = f'/Workspace/Repos/{organization_name}/retention-config/retention_json_automation/rawjson/{filename}'

        try:
            with open(local_path, 'r') as f:
                local_content = f.read()
                local_json = json.loads(local_content)
        except FileNotFoundError:
            print(f"Local file not found: {filename} - skipping")
            continue

        # Get the corresponding file from GitHub to compare
        github_path = f'retention_json_automation/rawjson/{filename}'
        try:
            github_file = cfg_repo.get_contents(github_path, ref=new_branch_name)
            github_content = github_file.decoded_content.decode('utf-8')
            github_json = json.loads(github_content)

            # Compare the JSON structures
            if local_json != github_json:
                # Files are different, update in GitHub
                cfg_repo.update_file(
                    github_path,
                    f"Update retention fields for error table: {filename}",
                    local_content,
                    github_file.sha,
                    branch=new_branch_name
                )
                print(f"Updated {filename}")
                all_changes.append(f"Updated rawjson error config: {filename}")
                rawjson_updated_count += 1
            else:
                print(f"No changes needed for {filename}")

        except Exception as e:
            print(f"Could not process {filename} in GitHub: {str(e)}")

    except json.JSONDecodeError as e:
        print(f"JSON decode error for {filename}: {e}")
    except Exception as e:
        print(f"Error processing {filename}: {str(e)}")

print(f"\nCompleted rawjson updates: {rawjson_updated_count} files updated\n")
print("=" * 60)
print(f"SUMMARY: Source files: {source_updated_count} | RawJson files: {rawjson_updated_count}")
print("=" * 60)

# COMMAND ----------

# Create PR Description
if all_changes:
    pr_description_cfg = """#Update Retention JSON Configurations

## Changes Included:

### Source JSON Files (Ratified Tables)
Updates to `retention_json_automation/source_json/` files for production-ready tables.

### RawJson Error Files (Error Tables)
Updates to `retention_json_automation/rawjson/` files for error handling configurations.

## Detailed Changes:
"""
    pr_description_cfg += "\n".join(f"- {change}" for change in all_changes)
    pr_description_cfg += f"\n\n---\n**Statistics:**\n- Source files updated: {source_updated_count}\n- RawJson files updated: {rawjson_updated_count}\n- Total changes: {len(all_changes)}\n\n*Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}*"
    print("\n" + pr_description_cfg)
else:
    print("Warning: No changes to commit!")
    dbutils.notebook.exit("No changes detected - skipping PR creation")

# COMMAND ----------

reviewers = ['sample-reviewer']

# Create PR for retention config repository
try:
    # Determine what types of changes are included
    has_source_changes = source_updated_count > 0
    has_rawjson_changes = rawjson_updated_count > 0

    # Create descriptive PR title
    if has_source_changes and has_rawjson_changes:
        pr_title = f"Update Retention Configs (Source + RawJson) - {new_branch_name}"
    elif has_rawjson_changes:
        pr_title = f"Update Retention RawJson Error Configs - {new_branch_name}"
    else:
        pr_title = f"Update Retention Source Configs - {new_branch_name}"

    cfg_pr = cfg_repo.create_pull(
        title=pr_title,
        body=pr_description_cfg,
        head=new_branch_name,
        base=cfg_source_branch
    )
    displayHTML(f"<a href='{cfg_pr.html_url}' target='_blank'>Created PR for retention config repo: {cfg_pr.html_url}</a>")

    # Add reviewers to PR
    try:
        cfg_pr.create_review_request(reviewers=reviewers)
        print(f"Reviewers added to PR: {reviewers}")
    except Exception as reviewer_error:
        print(f"Warning: Failed to add reviewers to PR: {reviewer_error}")

except Exception as e:
    print(f"Error creating PR for retention config repo: {e}")

print("\nPR creation completed for retention-config repository")
