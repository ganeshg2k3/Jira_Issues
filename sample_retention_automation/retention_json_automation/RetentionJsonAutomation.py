# Databricks notebook source
# Retention JSON Automation System
# This notebook orchestrates the complete retention JSON automation process
#
# Supported Schemas:
# - team_retail_{env}.lake_bronze (original schema)
# - lake_{env}.lake_bronze_sensitive_orders (new sensitive order tables)
# - lake_{env}.lake_bronze_sensitive_profiles (new sensitive profile tables)
# - lake_{env}.lake_bronze_sensitive_telemetry (new sensitive telemetry tables)
#
# The system identifies new tables across all schemas and processes them with the same
# filtering and rule column mapping logic as the original implementation.

dbutils.widgets.text("github_token", "")
dbutils.widgets.text("branch", "", "Branch Name")
dbutils.widgets.text("env", "")
dbutils.widgets.text("ad_hoc_table", "")
dbutils.widgets.text("exception_columns", "")

# COMMAND ----------

def get_widget_values(widget_name):
    widget_value = dbutils.widgets.get(widget_name)
    return [] if widget_value is None or widget_value == '' else widget_value.split(",")

github_token = dbutils.widgets.get("github_token")
branch = dbutils.widgets.get("branch")
env=dbutils.widgets.get("env")
AdHocTableList = get_widget_values("ad_hoc_table")
ExceptionColumnsList = get_widget_values("exception_columns")

# COMMAND ----------

from pyspark.sql.functions import *
import requests
import base64
import json
from pyspark.dbutils import DBUtils
import re
from github import Github

# COMMAND ----------

# MAGIC %run ./dependencies/Pre_Requisites

# COMMAND ----------

# DBTITLE 1,Git Variables
print(branch)
github_enterprise_host='github.example.com'
organization_name='acme-data-platform'
cfg_repo_name='retention-config'

# Source branches for repository
cfg_source_branch = 'rc/dev'


# Paths for retention config repo
cfg_paths = [
    'retention_json_automation/source_json/lake_loader_crm_retention.json',
    'retention_json_automation/source_json/lake_loader_billing_retention.json',
    'retention_json_automation/source_json/lake_loader_support_retention.json',
    'retention_json_automation/source_json/lake_loader_webshop_retention.json',
    'retention_json_automation/source_json/lake_loader_logistics_retention.json',
    'retention_json_automation/source_json/lake_loader_iot_retention.json'
]

# COMMAND ----------
# Initialize GitHub client

# GitHub authentication
try:
    github_token = dbutils.secrets.get(scope="retention_github_token", key="github_token")
except Exception as e:
    print("Failed to fetch GitHub token from both scopes. Error:", str(e))
    raise

# Initialize GitHub client
g = Github(base_url=f"https://{github_enterprise_host}/api/v3", login_or_token=github_token)
print("Git authorization succeeded")

# Access repositories
cfg_repo = g.get_repo(f"{organization_name}/{cfg_repo_name}")

# Branch name
cfg_base_branch = "rc/dev"

# Call the function to compare table counts in both repos for all sources.
#If any mismatch found check for the mismatch table
# COMMAND ----------

# MAGIC %md
# MAGIC ## Json Pre Check

# COMMAND ----------

# MAGIC %run ./Retention_Pre_Check

# COMMAND ----------

# MAGIC %md
# MAGIC ## Json Formation
# MAGIC

# COMMAND ----------

# MAGIC %run ./Retention_Processing

# COMMAND ----------

# MAGIC %md
# MAGIC ## RawJson Error Files Processing
# MAGIC

# COMMAND ----------

# MAGIC %run ./rawConfig_new_rules

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post Json checks and PR creation

# COMMAND ----------

# MAGIC %run ./Post_Checks_PR

# COMMAND ----------

# displayHTML(html_content)


# COMMAND ----------

dbutils.notebook.exit("Success")
