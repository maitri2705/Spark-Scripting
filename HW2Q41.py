# Databricks notebook source
from pyspark.sql.functions import mean,desc

# COMMAND ----------

from pyspark import SparkContext,SparkConf

# COMMAND ----------

conf=SparkConf().setAppName("HW2Q41")

# COMMAND ----------

sc=SparkContext.getOrCreate()

# COMMAND ----------

business_file=sc.textFile("/FileStore/tables/business.csv")

# COMMAND ----------

def business_mapper(row):
  business_details=row.split("::")
  temp_ans=[]
#   if "Stanford, CA" in business_details[1]:
  temp_ans.append((str(business_details[0]),str(business_details[1]),str(business_details[2])))
  return temp_ans

# COMMAND ----------

business_df=business_file.flatMap(business_mapper).toDF().distinct()

# COMMAND ----------

business_df=business_df.select(business_df._1.alias('b_id'),business_df._2.alias('full_address'),business_df._3.alias('categories'))

# COMMAND ----------

review_file=sc.textFile("/FileStore/tables/review.csv")

# COMMAND ----------

review_df=review_file.map(lambda x:x.split("::")).map(lambda x:(str(x[0]),str(x[1]),str(x[2]),str(x[3]))).toDF()

# COMMAND ----------

review_df=review_df.select(review_df._1.alias('review_id'),review_df._2.alias('user_id'),review_df._3.alias('business_id'),review_df._4.alias('ratings'))

# COMMAND ----------

review_df_top=review_df.groupBy("business_id").agg(mean("ratings").alias("avg_rating"))

# COMMAND ----------

review_df_top=review_df_top.sort(desc("avg_rating")).limit(10)

# COMMAND ----------

join_df = review_df_top.join(business_df, business_df.b_id == review_df_top.business_id)

# COMMAND ----------

for item in join_df.take(join_df.count()):
  print item[0]+"\t"+item[3]+"\t"+item[4]+"\t"+str(item[1])

# COMMAND ----------


