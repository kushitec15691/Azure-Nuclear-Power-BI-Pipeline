{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "0d0690b9-5b43-409f-ac27-d086f16ebd81",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "/mnt/frenchnuclear has been unmounted.\nMounted successfully!\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>path</th><th>name</th><th>size</th><th>modificationTime</th></tr></thead><tbody><tr><td>dbfs:/mnt/frenchnuclear/french_nuclear_reactors_availability.csv</td><td>french_nuclear_reactors_availability.csv</td><td>28801352</td><td>1739720097000</td></tr><tr><td>dbfs:/mnt/frenchnuclear/french_nuclear_reactors_availability_full.csv</td><td>french_nuclear_reactors_availability_full.csv</td><td>781130554</td><td>1739720161000</td></tr><tr><td>dbfs:/mnt/frenchnuclear/french_nuclear_reactors_list.csv</td><td>french_nuclear_reactors_list.csv</td><td>4158</td><td>1739727743000</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "dbfs:/mnt/frenchnuclear/french_nuclear_reactors_availability.csv",
         "french_nuclear_reactors_availability.csv",
         28801352,
         1739720097000
        ],
        [
         "dbfs:/mnt/frenchnuclear/french_nuclear_reactors_availability_full.csv",
         "french_nuclear_reactors_availability_full.csv",
         781130554,
         1739720161000
        ],
        [
         "dbfs:/mnt/frenchnuclear/french_nuclear_reactors_list.csv",
         "french_nuclear_reactors_list.csv",
         4158,
         1739727743000
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "path",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "size",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "modificationTime",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "storage_account_name = \"kushmi123\"  # Azure Storage Account Name\n",
    "sas_token = \"sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-03-21T12:33:33Z&st=2025-03-21T04:33:33Z&spr=https&sig=vh3kxJcwgecqGdr6p0%2Bf6FLpDpj6Cfw1xpVF7SYJwJo%3D\"  # SAS Token\n",
    "container_name = \"frenchnuclear\"              # Name of the Azure Blob Storage container\n",
    "mount_point = f\"/mnt/{container_name}\"                # Mount point path in Databricks\n",
    "\n",
    "# Unmount if already mounted\n",
    "try:\n",
    "    dbutils.fs.unmount(mount_point)\n",
    "except Exception as e:\n",
    "    print(f\"Unmount Warning: {e}\")\n",
    "\n",
    "# Mount the Blob Storage using SAS Token\n",
    "configs = {f\"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net\": sas_token}\n",
    "dbutils.fs.mount(\n",
    "    source=f\"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/\",\n",
    "    mount_point=mount_point,\n",
    "    extra_configs=configs\n",
    ")\n",
    "\n",
    "# Verify the mount\n",
    "print(\"Mounted successfully!\")\n",
    "if dbutils.fs.ls(mount_point): display(dbutils.fs.ls(mount_point))\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "fc6ee6b7-a357-4b0a-a778-eef17efc0527",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "  Bronze Table: french_nuclear_reactors_list Created!\n  Bronze Table: french_nuclear_reactors_availability Created!\n  Bronze Table: french_nuclear_reactors_availability_full Created!\n\n Validating Bronze Table Encoding:\n+--------+\n|count(1)|\n+--------+\n|       0|\n+--------+\n\n  Encoding Fix Applied Successfully!\n"
     ]
    }
   ],
   "source": [
    "from pyspark.sql.functions import col, to_timestamp, udf\n",
    "import unicodedata\n",
    "\n",
    "# Define the file paths in mounted storage\n",
    "french_nuclear_list_path = \"/mnt/frenchnuclear/french_nuclear_reactors_list.csv\"\n",
    "availability_path = \"/mnt/frenchnuclear/french_nuclear_reactors_availability.csv\"\n",
    "availability_full_path = \"/mnt/frenchnuclear/french_nuclear_reactors_availability_full.csv\"\n",
    "\n",
    "# Define UDF to fix encoding\n",
    "def fix_encoding(text):\n",
    "    if text is None:\n",
    "        return None\n",
    "    return (\n",
    "        unicodedata.normalize(\"NFKC\", text)\n",
    "        .replace(\"Ã©\", \"é\")\n",
    "        .replace(\"Ã¨\", \"è\")\n",
    "        .replace(\"Ã´\", \"ô\")\n",
    "        .replace(\"Ã¢\", \"â\")\n",
    "        .replace(\"Ãª\", \"ê\")\n",
    "        .replace(\"Ã«\", \"ë\")\n",
    "        .replace(\"Ã§\", \"ç\")\n",
    "        .replace(\"Ã\", \"à\")\n",
    "        .replace(\"â€™\", \"'\")\n",
    "        .replace(\"â€\", '\"')\n",
    "    )\n",
    "\n",
    "fix_encoding_udf = udf(fix_encoding)\n",
    "\n",
    "# -------------------------------------\n",
    "# **1️ Load 'french_nuclear_reactors_list'**\n",
    "# -------------------------------------\n",
    "df_list = (\n",
    "    spark.read.format(\"csv\")\n",
    "    .option(\"header\", \"true\")\n",
    "    .option(\"inferSchema\", \"true\")\n",
    "    .option(\"encoding\", \"UTF-8\")  # Ensure correct encoding\n",
    "    .option(\"ignoreLeadingWhiteSpace\", True)\n",
    "    .option(\"ignoreTrailingWhiteSpace\", True)\n",
    "    .load(french_nuclear_list_path)\n",
    ")\n",
    "\n",
    "# Apply text fixes\n",
    "string_cols = [c for c in df_list.columns if df_list.schema[c].dataType.simpleString() == \"string\"]\n",
    "for col_name in string_cols:\n",
    "    df_list = df_list.withColumn(col_name, fix_encoding_udf(col(col_name)))\n",
    "\n",
    "df_list.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"bronze.french_nuclear_reactors_list\")\n",
    "print(\"  Bronze Table: french_nuclear_reactors_list Created!\")\n",
    "\n",
    "# -------------------------------------\n",
    "# **2️ Load 'french_nuclear_reactors_availability'**\n",
    "# -------------------------------------\n",
    "df_availability = (\n",
    "    spark.read.format(\"csv\")\n",
    "    .option(\"header\", \"true\")\n",
    "    .option(\"inferSchema\", \"true\")\n",
    "    .option(\"encoding\", \"UTF-8\")\n",
    "    .option(\"ignoreLeadingWhiteSpace\", True)\n",
    "    .option(\"ignoreTrailingWhiteSpace\", True)\n",
    "    .load(availability_path)\n",
    "    .withColumn(\"date_time\", to_timestamp(col(\"date_time\"), \"M/d/yyyy H:mm\"))\n",
    ")\n",
    "\n",
    "string_cols = [c for c in df_availability.columns if df_availability.schema[c].dataType.simpleString() == \"string\"]\n",
    "for col_name in string_cols:\n",
    "    df_availability = df_availability.withColumn(col_name, fix_encoding_udf(col(col_name)))\n",
    "\n",
    "df_availability.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"bronze.french_nuclear_reactors_availability\")\n",
    "print(\"  Bronze Table: french_nuclear_reactors_availability Created!\")\n",
    "\n",
    "# -------------------------------------\n",
    "# **3️ Load 'french_nuclear_reactors_availability_full'**\n",
    "# -------------------------------------\n",
    "df_availability_full = (\n",
    "    spark.read.format(\"csv\")\n",
    "    .option(\"header\", \"true\")\n",
    "    .option(\"inferSchema\", \"true\")\n",
    "    .option(\"encoding\", \"UTF-8\")\n",
    "    .option(\"ignoreLeadingWhiteSpace\", True)\n",
    "    .option(\"ignoreTrailingWhiteSpace\", True)\n",
    "    .load(availability_full_path)\n",
    "    .withColumn(\"date_time\", to_timestamp(col(\"date_time\"), \"M/d/yyyy H:mm\"))\n",
    ")\n",
    "\n",
    "string_cols = [c for c in df_availability_full.columns if df_availability_full.schema[c].dataType.simpleString() == \"string\"]\n",
    "for col_name in string_cols:\n",
    "    df_availability_full = df_availability_full.withColumn(col_name, fix_encoding_udf(col(col_name)))\n",
    "\n",
    "df_availability_full.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"bronze.french_nuclear_reactors_availability_full\")\n",
    "print(\"  Bronze Table: french_nuclear_reactors_availability_full Created!\")\n",
    "\n",
    "# -------------------------------------\n",
    "# **4️ Validate Encoding Fix**\n",
    "# -------------------------------------\n",
    "print(\"\\n Validating Bronze Table Encoding:\")\n",
    "\n",
    "# Identify all string columns in df_availability_full\n",
    "df_availability_full_tmp = spark.read.table(\"bronze.french_nuclear_reactors_availability_full\")\n",
    "string_cols = [c for c in df_availability_full_tmp.columns if df_availability_full_tmp.schema[c].dataType.simpleString() == \"string\"]\n",
    "\n",
    "# Build dynamic SQL query to check encoding issues\n",
    "if string_cols:\n",
    "    where_clause = \" OR \".join([f\"`{col_name}` LIKE '%Ã%' OR `{col_name}` LIKE '%â%' OR `{col_name}` LIKE '%�%'\" for col_name in string_cols])\n",
    "    invalid_characters = spark.sql(f\"\"\"\n",
    "        SELECT COUNT(*) FROM bronze.french_nuclear_reactors_availability_full\n",
    "        WHERE {where_clause}\n",
    "    \"\"\")\n",
    "    invalid_characters.show()\n",
    "else:\n",
    "    print(\" No encoding issues detected!\")\n",
    "\n",
    "print(\"  Encoding Fix Applied Successfully!\")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "2"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "mostRecentlyExecutedCommandWithImplicitDF": {
     "commandId": 1452583256882325,
     "dataframes": [
      "_sqldf"
     ]
    },
    "pythonIndentUnit": 4
   },
   "notebookName": "Bronze_Layer",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}