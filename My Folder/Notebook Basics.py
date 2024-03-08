# Databricks notebook source
spark

# COMMAND ----------

tb_bruden_df = spark.read.csv('/Volumes/default/default/training/TB_Burden_Country.csv')

# COMMAND ----------

tb_bruden_df.write.format('delta').mode('overwrite').save('/Volumes/default/default/training/TB_Burden_Country_Delta')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Derar AlHussein Course

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Hello World from SQL"

# COMMAND ----------

# MAGIC %run ./Notebook2

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets/'

# COMMAND ----------

dbutils.help() 

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets/')
print(files)

# COMMAND ----------

display(files)

# COMMAND ----------


