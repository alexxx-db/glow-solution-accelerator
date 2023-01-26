# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Convert Delta Table to VCF
# MAGIC 
# MAGIC Take the delta table and write it back to VCF (for input to Hail)
# MAGIC 
# MAGIC note: most of the job writing back to a single VCF is single threaded, 
# MAGIC 
# MAGIC but it requires the full dataset to be read into memory on a spark cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run notebook(s) to set everything up

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %md ##### Enforce using Photon

# COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", True)

# COMMAND ----------

spark.conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled", True)
spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", 20)
spark.conf.set("io.compression.codecs", "io.projectglow.sql.util.BGZFCodec")

# COMMAND ----------

spark.read.format("delta").load(output_delta) \
                          .limit(100) \
                          .write \
                          .mode("overwrite") \
                          .format("bigvcf") \
                          .save(output_vcf_small)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check output VCF

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 100 $output_vcf

# COMMAND ----------

# MAGIC %md
# MAGIC ##### write vcf on full dataset

# COMMAND ----------

spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", 20)

# COMMAND ----------

vcf_df = spark.read.format("delta").load(output_delta)

# COMMAND ----------

# FYI Photon doesn't support the below repartition() operation
vcf_df = spark.read.format("delta").load(output_delta) \
                          .repartition(n_partitions)

# COMMAND ----------

# We are collecting all the partitions one by one onto the driver node; instead, can we use the executors to write the partitions to a temp directory
# then assemble the files in a temp directory

# COMMAND ----------

vcf_df.write \
      .mode("overwrite") \
      .format("bigvcf") \
      .save(output_vcf)

# COMMAND ----------

# spark.read.format("delta").load(output_delta) \
#                           .repartition(n_partitions) \
#                           .write \
#                           .mode("overwrite") \
#                           .format("bigvcf") \
#                           .save(output_vcf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check output

# COMMAND ----------

delta_vcf = spark.read.format("vcf").load(output_vcf)

# COMMAND ----------

delta_vcf.count()
