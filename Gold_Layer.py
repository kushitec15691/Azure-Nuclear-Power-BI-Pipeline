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
     "nuid": "43dace1d-ad72-440e-b721-1c38d4f69aa0",
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
      " Reactor Count After Fix: 58\n Gold Layer ETL Complete! \n"
     ]
    }
   ],
   "source": [
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql.window import Window\n",
    "\n",
    "#  Step 1: Set the Gold Database\n",
    "spark.sql(\"CREATE DATABASE IF NOT EXISTS gold\")\n",
    "spark.sql(\"USE gold\")\n",
    "\n",
    "#  Step 2: Drop Existing Gold Tables (if they exist)\n",
    "tables_to_drop = [\n",
    "    \"gold.dim_plant\", \"gold.dim_reactor\", \"gold.dim_model\",\n",
    "    \"gold.dim_cooling\", \"gold.dim_datetime\",\n",
    "    \"gold.bridge_reactor_model_cooling\", \"gold.fact_availability\"\n",
    "]\n",
    "for table in tables_to_drop:\n",
    "    spark.sql(f\"DROP TABLE IF EXISTS {table}\")\n",
    "\n",
    "#  Step 3: Read Silver Layer Source Tables\n",
    "df_reactors_list = spark.read.table(\"silver.french_nuclear_reactors_list\")\n",
    "df_availability_full = spark.read.table(\"silver.french_nuclear_reactors_availability_full\")\n",
    "\n",
    "#  Step 4: Create Dimension - Plant (Unique Plant ID)\n",
    "df_dim_plant = (\n",
    "    df_reactors_list.select(\"plant\", \"lat\", \"lon\").dropDuplicates()\n",
    "    .withColumn(\"plant_id\", F.row_number().over(Window.orderBy(\"plant\")).cast(\"int\"))\n",
    ")\n",
    "df_dim_plant.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"gold.dim_plant\")\n",
    "\n",
    "#  Step 5: Create Dimension - Reactor (Ensure 58 Reactors, Fix Duplicates)\n",
    "df_dim_reactor = (\n",
    "    df_reactors_list\n",
    "    .select(\"reactor\", \"plant\", \"grid_connection\")\n",
    "    .dropDuplicates([\"reactor\"])  # 🚀 Fix Duplicates: Keep Only 1 Row Per Reactor\n",
    "    .join(df_dim_plant.select(\"plant\", \"plant_id\"), on=\"plant\", how=\"left\")  #  Get Plant ID\n",
    "    .withColumn(\"reactor_id\", F.row_number().over(Window.orderBy(\"reactor\")).cast(\"int\"))  #  Generate Unique ID\n",
    "    .select(\"reactor_id\", \"reactor\", \"plant_id\", \"grid_connection\")  #  Keep Only Needed Columns\n",
    ")\n",
    "\n",
    "#  REMOVE EXTRA DUPLICATE REACTORS (Ensure exactly 58)\n",
    "df_dim_reactor = df_dim_reactor.dropDuplicates([\"reactor\"])\n",
    "reactor_count = df_dim_reactor.count()\n",
    "print(f\" Reactor Count After Fix: {reactor_count}\")\n",
    "\n",
    "#  STOP Execution if Count ≠ 58\n",
    "if reactor_count != 58:\n",
    "    print(\" Duplicate reactors still exist! Checking duplicate details...\")\n",
    "\n",
    "    #  Show Remaining Duplicates (If Any)\n",
    "    df_dim_reactor.groupBy(\"reactor\").count().filter(\"count > 1\").show()\n",
    "\n",
    "    raise ValueError(f\" Incorrect reactor count: {reactor_count}, expected 58. Investigate duplicates!\")\n",
    "\n",
    "df_dim_reactor.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"gold.dim_reactor\")\n",
    "\n",
    "#  Step 6: Create Dimension - Model\n",
    "df_dim_model = (\n",
    "    df_reactors_list.select(\"model\", \"rated_power\").dropDuplicates()\n",
    "    .withColumn(\"model_id\", F.row_number().over(Window.orderBy(\"model\")).cast(\"int\"))\n",
    "    .select(\"model_id\", \"model\", \"rated_power\")\n",
    ")\n",
    "df_dim_model.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"gold.dim_model\")\n",
    "\n",
    "#  Step 7: Create Dimension - Cooling\n",
    "df_dim_cooling = (\n",
    "    df_reactors_list.select(\"cooling\", \"cooling_source\").dropDuplicates()\n",
    "    .withColumn(\"cooling_id\", F.row_number().over(Window.orderBy(\"cooling\")).cast(\"int\"))\n",
    "    .select(\"cooling_id\", \"cooling\", \"cooling_source\")\n",
    ")\n",
    "df_dim_cooling.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"gold.dim_cooling\")\n",
    "\n",
    "#  Step 8: Create Dimension - DateTime\n",
    "df_dim_datetime = (\n",
    "    df_availability_full.select(\"date_time\").distinct()\n",
    "    .withColumn(\"date_key\", F.date_format(\"date_time\", \"yyyyMMddHHmm\").cast(\"bigint\"))\n",
    "    .withColumn(\"year\",   F.year(\"date_time\").cast(\"int\"))\n",
    "    .withColumn(\"month\",  F.month(\"date_time\").cast(\"int\"))\n",
    "    .withColumn(\"day\",    F.dayofmonth(\"date_time\").cast(\"int\"))\n",
    "    .withColumn(\"hour\",   F.hour(\"date_time\").cast(\"int\"))\n",
    ")\n",
    "df_dim_datetime.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"gold.dim_datetime\")\n",
    "\n",
    "#  Step 9: Create Bridge Table (Reactor-Model-Cooling)\n",
    "df_bridge = (\n",
    "    df_reactors_list\n",
    "    .select(\"reactor\", \"model\", \"cooling\").dropDuplicates([\"reactor\"])\n",
    "    .join(df_dim_reactor.select(\"reactor_id\", \"reactor\"), on=\"reactor\", how=\"left\")\n",
    "    .join(df_dim_model.select(\"model_id\", \"model\"), on=\"model\", how=\"left\")\n",
    "    .join(df_dim_cooling.select(\"cooling_id\", \"cooling\"), on=\"cooling\", how=\"left\")\n",
    "    .select(\"reactor_id\", \"model_id\", \"cooling_id\")\n",
    ")\n",
    "df_bridge.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"gold.bridge_reactor_model_cooling\")\n",
    "\n",
    "#  Step 10: Create Fact Table (Availability)\n",
    "df_power = df_availability_full.filter(F.col(\"parameter\") == \"power_available\") \\\n",
    "    .select(\"date_time\", \"reactor_name\", F.col(\"value\").cast(\"int\").alias(\"power_available\"))\n",
    "df_derating = df_availability_full.filter(F.col(\"parameter\") == \"derating\") \\\n",
    "    .select(\"date_time\", \"reactor_name\", F.col(\"value\").cast(\"int\").alias(\"derating\"))\n",
    "df_type = df_availability_full.filter(F.col(\"parameter\") == \"unavailability_type\") \\\n",
    "    .select(\"date_time\", \"reactor_name\", F.col(\"value\").alias(\"unavailability_type\"))\n",
    "df_cause = df_availability_full.filter(F.col(\"parameter\") == \"unavailability_cause\") \\\n",
    "    .select(\"date_time\", \"reactor_name\", F.col(\"value\").alias(\"unavailability_cause\"))\n",
    "df_info = df_availability_full.filter(F.col(\"parameter\") == \"additional_info\") \\\n",
    "    .select(\"date_time\", \"reactor_name\", F.col(\"value\").alias(\"additional_info\"))\n",
    "\n",
    "df_fact = df_power \\\n",
    "    .join(df_derating, [\"date_time\", \"reactor_name\"], \"left\") \\\n",
    "    .join(df_type, [\"date_time\", \"reactor_name\"], \"left\") \\\n",
    "    .join(df_cause, [\"date_time\", \"reactor_name\"], \"left\") \\\n",
    "    .join(df_info, [\"date_time\", \"reactor_name\"], \"left\")\n",
    "\n",
    "df_fact = df_fact.fillna({\"derating\": 0})\n",
    "\n",
    "df_fact = df_fact.join(df_dim_datetime.select(\"date_time\", \"date_key\", \"year\"), on=\"date_time\", how=\"left\") \\\n",
    "    .join(df_dim_reactor.select(\"reactor_id\", \"reactor\").withColumnRenamed(\"reactor\", \"reactor_name\"),\n",
    "          on=\"reactor_name\", how=\"left\")\n",
    "\n",
    "df_fact = df_fact.withColumn(\n",
    "    \"is_unavailable\",\n",
    "    F.when((F.col(\"power_available\") == 0) & F.col(\"unavailability_type\").isNotNull(), True).otherwise(False)\n",
    ").withColumn(\n",
    "    \"is_derated\",\n",
    "    F.when((F.col(\"power_available\") == 0) & F.col(\"unavailability_type\").isNull(), True)\n",
    "     .when((F.col(\"power_available\") > 0) & (F.col(\"derating\") > 0), True)\n",
    "     .otherwise(False)\n",
    ")\n",
    "\n",
    "df_fact.write.format(\"delta\").partitionBy(\"year\", \"reactor_id\").mode(\"overwrite\").saveAsTable(\"gold.fact_availability\")\n",
    "\n",
    "print(\" Gold Layer ETL Complete! \")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "c4525102-6dbd-4849-a63f-388af87bc066",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>reactor_id</th><th>reactor</th><th>plant_id</th><th>grid_connection</th></tr></thead><tbody><tr><td>1</td><td>belleville_1</td><td>1</td><td>1987-10-14</td></tr><tr><td>2</td><td>belleville_2</td><td>1</td><td>1988-07-06</td></tr><tr><td>3</td><td>blayais_1</td><td>2</td><td>1981-06-12</td></tr><tr><td>4</td><td>blayais_2</td><td>2</td><td>1982-07-17</td></tr><tr><td>5</td><td>blayais_3</td><td>2</td><td>1983-08-17</td></tr><tr><td>6</td><td>blayais_4</td><td>2</td><td>1983-05-16</td></tr><tr><td>7</td><td>bugey_2</td><td>3</td><td>1978-05-10</td></tr><tr><td>8</td><td>bugey_3</td><td>3</td><td>1978-09-21</td></tr><tr><td>9</td><td>bugey_4</td><td>3</td><td>1979-03-08</td></tr><tr><td>10</td><td>bugey_5</td><td>3</td><td>1979-07-31</td></tr><tr><td>11</td><td>cattenom_1</td><td>4</td><td>1986-11-13</td></tr><tr><td>12</td><td>cattenom_2</td><td>4</td><td>1987-09-17</td></tr><tr><td>13</td><td>cattenom_3</td><td>4</td><td>1990-07-06</td></tr><tr><td>14</td><td>cattenom_4</td><td>4</td><td>1991-05-27</td></tr><tr><td>15</td><td>chinon_1</td><td>5</td><td>1982-11-30</td></tr><tr><td>16</td><td>chinon_2</td><td>5</td><td>1983-11-29</td></tr><tr><td>17</td><td>chinon_3</td><td>5</td><td>1986-10-20</td></tr><tr><td>18</td><td>chinon_4</td><td>5</td><td>1987-11-14</td></tr><tr><td>19</td><td>chooz_1</td><td>6</td><td>1996-08-30</td></tr><tr><td>20</td><td>chooz_2</td><td>6</td><td>1997-04-10</td></tr><tr><td>21</td><td>civaux_1</td><td>7</td><td>1997-12-24</td></tr><tr><td>22</td><td>civaux_2</td><td>7</td><td>1999-12-24</td></tr><tr><td>23</td><td>cruas_1</td><td>8</td><td>1983-04-29</td></tr><tr><td>24</td><td>cruas_2</td><td>8</td><td>1984-09-06</td></tr><tr><td>25</td><td>cruas_3</td><td>8</td><td>1984-05-14</td></tr><tr><td>26</td><td>cruas_4</td><td>8</td><td>1984-10-27</td></tr><tr><td>27</td><td>dampierre_1</td><td>9</td><td>1980-03-23</td></tr><tr><td>28</td><td>dampierre_2</td><td>9</td><td>1980-12-10</td></tr><tr><td>29</td><td>dampierre_3</td><td>9</td><td>1981-01-30</td></tr><tr><td>30</td><td>dampierre_4</td><td>9</td><td>1981-08-18</td></tr><tr><td>31</td><td>fessenheim_1</td><td>11</td><td>1977-04-06</td></tr><tr><td>33</td><td>fessenheim_2</td><td>11</td><td>1977-10-07</td></tr><tr><td>35</td><td>flamanville_1</td><td>13</td><td>1985-12-04</td></tr><tr><td>37</td><td>flamanville_2</td><td>13</td><td>1986-07-18</td></tr><tr><td>39</td><td>golfech_1</td><td>14</td><td>1990-06-07</td></tr><tr><td>40</td><td>golfech_2</td><td>14</td><td>1993-06-18</td></tr><tr><td>41</td><td>gravelines_1</td><td>15</td><td>1980-03-13</td></tr><tr><td>42</td><td>gravelines_2</td><td>15</td><td>1980-08-26</td></tr><tr><td>43</td><td>gravelines_3</td><td>15</td><td>1980-12-12</td></tr><tr><td>44</td><td>gravelines_4</td><td>15</td><td>1981-06-14</td></tr><tr><td>45</td><td>gravelines_5</td><td>15</td><td>1984-08-28</td></tr><tr><td>46</td><td>gravelines_6</td><td>15</td><td>1985-08-01</td></tr><tr><td>47</td><td>nogent_1</td><td>16</td><td>1987-10-21</td></tr><tr><td>48</td><td>nogent_2</td><td>16</td><td>1988-12-14</td></tr><tr><td>49</td><td>paluel_1</td><td>17</td><td>1984-06-22</td></tr><tr><td>50</td><td>paluel_2</td><td>17</td><td>1984-09-14</td></tr><tr><td>51</td><td>paluel_3</td><td>17</td><td>1985-09-30</td></tr><tr><td>52</td><td>paluel_4</td><td>17</td><td>1986-04-11</td></tr><tr><td>53</td><td>penly_1</td><td>18</td><td>1990-05-04</td></tr><tr><td>54</td><td>penly_2</td><td>18</td><td>1992-02-04</td></tr><tr><td>55</td><td>st_alban_1</td><td>19</td><td>1985-08-30</td></tr><tr><td>56</td><td>st_alban_2</td><td>19</td><td>1986-07-03</td></tr><tr><td>57</td><td>st_laurent_1</td><td>20</td><td>1981-01-21</td></tr><tr><td>58</td><td>st_laurent_2</td><td>20</td><td>1981-06-01</td></tr><tr><td>59</td><td>tricastin_1</td><td>21</td><td>1980-05-31</td></tr><tr><td>60</td><td>tricastin_2</td><td>21</td><td>1980-08-07</td></tr><tr><td>61</td><td>tricastin_3</td><td>21</td><td>1981-02-10</td></tr><tr><td>62</td><td>tricastin_4</td><td>21</td><td>1981-06-12</td></tr></tbody></table></div>"
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
         1,
         "belleville_1",
         1,
         "1987-10-14"
        ],
        [
         2,
         "belleville_2",
         1,
         "1988-07-06"
        ],
        [
         3,
         "blayais_1",
         2,
         "1981-06-12"
        ],
        [
         4,
         "blayais_2",
         2,
         "1982-07-17"
        ],
        [
         5,
         "blayais_3",
         2,
         "1983-08-17"
        ],
        [
         6,
         "blayais_4",
         2,
         "1983-05-16"
        ],
        [
         7,
         "bugey_2",
         3,
         "1978-05-10"
        ],
        [
         8,
         "bugey_3",
         3,
         "1978-09-21"
        ],
        [
         9,
         "bugey_4",
         3,
         "1979-03-08"
        ],
        [
         10,
         "bugey_5",
         3,
         "1979-07-31"
        ],
        [
         11,
         "cattenom_1",
         4,
         "1986-11-13"
        ],
        [
         12,
         "cattenom_2",
         4,
         "1987-09-17"
        ],
        [
         13,
         "cattenom_3",
         4,
         "1990-07-06"
        ],
        [
         14,
         "cattenom_4",
         4,
         "1991-05-27"
        ],
        [
         15,
         "chinon_1",
         5,
         "1982-11-30"
        ],
        [
         16,
         "chinon_2",
         5,
         "1983-11-29"
        ],
        [
         17,
         "chinon_3",
         5,
         "1986-10-20"
        ],
        [
         18,
         "chinon_4",
         5,
         "1987-11-14"
        ],
        [
         19,
         "chooz_1",
         6,
         "1996-08-30"
        ],
        [
         20,
         "chooz_2",
         6,
         "1997-04-10"
        ],
        [
         21,
         "civaux_1",
         7,
         "1997-12-24"
        ],
        [
         22,
         "civaux_2",
         7,
         "1999-12-24"
        ],
        [
         23,
         "cruas_1",
         8,
         "1983-04-29"
        ],
        [
         24,
         "cruas_2",
         8,
         "1984-09-06"
        ],
        [
         25,
         "cruas_3",
         8,
         "1984-05-14"
        ],
        [
         26,
         "cruas_4",
         8,
         "1984-10-27"
        ],
        [
         27,
         "dampierre_1",
         9,
         "1980-03-23"
        ],
        [
         28,
         "dampierre_2",
         9,
         "1980-12-10"
        ],
        [
         29,
         "dampierre_3",
         9,
         "1981-01-30"
        ],
        [
         30,
         "dampierre_4",
         9,
         "1981-08-18"
        ],
        [
         31,
         "fessenheim_1",
         11,
         "1977-04-06"
        ],
        [
         33,
         "fessenheim_2",
         11,
         "1977-10-07"
        ],
        [
         35,
         "flamanville_1",
         13,
         "1985-12-04"
        ],
        [
         37,
         "flamanville_2",
         13,
         "1986-07-18"
        ],
        [
         39,
         "golfech_1",
         14,
         "1990-06-07"
        ],
        [
         40,
         "golfech_2",
         14,
         "1993-06-18"
        ],
        [
         41,
         "gravelines_1",
         15,
         "1980-03-13"
        ],
        [
         42,
         "gravelines_2",
         15,
         "1980-08-26"
        ],
        [
         43,
         "gravelines_3",
         15,
         "1980-12-12"
        ],
        [
         44,
         "gravelines_4",
         15,
         "1981-06-14"
        ],
        [
         45,
         "gravelines_5",
         15,
         "1984-08-28"
        ],
        [
         46,
         "gravelines_6",
         15,
         "1985-08-01"
        ],
        [
         47,
         "nogent_1",
         16,
         "1987-10-21"
        ],
        [
         48,
         "nogent_2",
         16,
         "1988-12-14"
        ],
        [
         49,
         "paluel_1",
         17,
         "1984-06-22"
        ],
        [
         50,
         "paluel_2",
         17,
         "1984-09-14"
        ],
        [
         51,
         "paluel_3",
         17,
         "1985-09-30"
        ],
        [
         52,
         "paluel_4",
         17,
         "1986-04-11"
        ],
        [
         53,
         "penly_1",
         18,
         "1990-05-04"
        ],
        [
         54,
         "penly_2",
         18,
         "1992-02-04"
        ],
        [
         55,
         "st_alban_1",
         19,
         "1985-08-30"
        ],
        [
         56,
         "st_alban_2",
         19,
         "1986-07-03"
        ],
        [
         57,
         "st_laurent_1",
         20,
         "1981-01-21"
        ],
        [
         58,
         "st_laurent_2",
         20,
         "1981-06-01"
        ],
        [
         59,
         "tricastin_1",
         21,
         "1980-05-31"
        ],
        [
         60,
         "tricastin_2",
         21,
         "1980-08-07"
        ],
        [
         61,
         "tricastin_3",
         21,
         "1981-02-10"
        ],
        [
         62,
         "tricastin_4",
         21,
         "1981-06-12"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "createTempViewForImplicitDf": true,
        "dataframeName": "_sqldf",
        "executionCount": 4
       },
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
         "name": "reactor_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "reactor",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "plant_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{\"__detected_date_formats\":\"M/d/yyyy\"}",
         "name": "grid_connection",
         "type": "\"date\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "Select * from dim_reactor"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "159f6c47-75b3-44ce-b15b-a48b7f61ea51",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr></tr></thead><tbody></tbody></table></div>"
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
       "data": [],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {
        "dataframeName": null
       },
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
       "schema": [],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "%sql\n",
    "DROP TABLE IF EXISTS gold.fact_availability_enhanced;\n",
    "DROP TABLE IF EXISTS gold.fact_availability_partitioned;\n",
    "DROP TABLE IF EXISTS gold.reactor_summary;\n"
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
     "commandId": 8887299799452625,
     "dataframes": [
      "_sqldf"
     ]
    },
    "pythonIndentUnit": 4
   },
   "notebookName": "GOLD Layer",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}