# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

from pyspark import SparkContext,SparkConf

# COMMAND ----------

# from pyspark.sql import SQLContext

# COMMAND ----------

conf=SparkConf().setAppName("HW2Q31")

# COMMAND ----------

sc=SparkContext.getOrCreate()

# COMMAND ----------

# sqlContext = SQLContext(sc)

# COMMAND ----------

business_file=sc.textFile("/FileStore/tables/business.csv")

# COMMAND ----------

# business_df.show()

# COMMAND ----------

def business_mapper(row):
  business_details=row.split("::")
  temp_ans=[]
#   if "Stanford, CA" in business_details[1]:
  temp_ans.append((str(business_details[0]),str(business_details[1]),str(business_details[2])))
  return temp_ans

# COMMAND ----------

business_df=business_file.flatMap(business_mapper).toDF()

# COMMAND ----------

business_df=business_df.select(business_df._1.alias('business_id'),business_df._2.alias('full_address'),business_df._3.alias('categories'))

# COMMAND ----------

# business_df.select('business_id','full_address').show()

# COMMAND ----------

# business_df.createOrReplaceTempView("business_table")

# COMMAND ----------

# business_df_filter=spark.sql("select business_id,full_address,categories from business_table where full_address like '%Standford, CA%'")

# COMMAND ----------

# business_df_filter=business_df.select("business_id","full_address")
# business_df.registerTempTable("business")

# COMMAND ----------

# business_df_filter=sqlContext.sql("select * from business where full_address like '%Standford, CA%'")

# COMMAND ----------

business_df_filter=business_df.filter(business_df.full_address.contains("Stanford, CA")).distinct()

# COMMAND ----------

review_file=sc.textFile("/FileStore/tables/review.csv")

# COMMAND ----------

review_df=review_file.map(lambda x:x.split("::")).map(lambda x:(str(x[0]),str(x[1]),str(x[2]),str(x[3]))).toDF()

# COMMAND ----------

# print review_df.take(10)

# COMMAND ----------

review_df=review_df.select(review_df._1.alias('review_id'),review_df._2.alias('user_id'),review_df._3.alias('business_id'),review_df._4.alias('ratings'))

# COMMAND ----------

join_df = business_df_filter.join(review_df, business_df_filter.business_id == review_df.business_id)

# COMMAND ----------

# join_df.show()

# COMMAND ----------

for item in join_df.take(join_df.count()):
  print item[4]+"\t"+item[6]

# COMMAND ----------


