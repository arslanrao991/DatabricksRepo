# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType

catalog_schema_path = 'databricks_practice.user_data'

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("action_ts", TimestampType(), True),
    StructField("item_id", StringType(), True),
    StructField("device", StringType(), True),
    StructField("b2c", BooleanType(), True)
])

# File path
file_path0 = "/Volumes/databricks_practice/user_data/mvol1/0000_part_00"
file_path1 = "/Volumes/databricks_practice/user_data/mvol1/0001_part_00"
file_path2 = "/Volumes/databricks_practice/user_data/mvol1/0002_part_00"
file_path3 = "/Volumes/databricks_practice/user_data/mvol1/0003_part_00"
file_path4 = "/Volumes/databricks_practice/user_data/mvol1/0004_part_00"


df0 = spark.read.format('csv').option('delimiter', '|').option('header', 'false').schema(schema).load(file_path0)
df1 = spark.read.format('csv').option('delimiter', '|').option('header', 'false').schema(schema).load(file_path1)
df2 = spark.read.format('csv').option('delimiter', '|').option('header', 'false').schema(schema).load(file_path2)
df3 = spark.read.format('csv').option('delimiter', '|').option('header', 'false').schema(schema).load(file_path3)
df4 = spark.read.format('csv').option('delimiter', '|').option('header', 'false').schema(schema).load(file_path4)


# COMMAND ----------

print(df0.count())
print(df1.count())
print(df2.count())
print(df3.count())
print(df4.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS databricks_practice.user_data.actions;
# MAGIC -- CREATE TABLE IF NOT EXISTS databricks_practice.user_data.actions (
# MAGIC --     user_id STRING,
# MAGIC --     action_type CHAR(1),
# MAGIC --     action_ts TIMESTAMP,
# MAGIC --     item_id STRING,
# MAGIC --     device STRING,
# MAGIC --     b2c BOOLEAN
# MAGIC -- )
# MAGIC -- USING delta;
# MAGIC

# COMMAND ----------


final_df = df0.union(df1).union(df2).union(df3).union(df4)
final_df.write.format("delta").mode("overwrite").saveAsTable(catalog_schema_path + '.actions')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN databricks_practice.user_data;
