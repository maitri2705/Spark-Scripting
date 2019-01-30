# Databricks notebook source
from pyspark import SparkContext,SparkConf

# COMMAND ----------

conf=SparkConf().setAppName("HW2Q4")

# COMMAND ----------

sc = SparkContext.getOrCreate()

# COMMAND ----------

business_RDD=sc.textFile("/FileStore/tables/business.csv")

# COMMAND ----------

def business_mapper(row):
  business_details=row.split("::")
  temp_ans=[]
  temp_ans.append((str(business_details[0]),str(business_details[1]+":"+business_details[2])))
  return temp_ans

# COMMAND ----------

business_detail_RDD=business_RDD.flatMap(business_mapper).distinct()

# COMMAND ----------

# print business_detail_RDD.take(10)

# COMMAND ----------

review_RDD=sc.textFile("/FileStore/tables/review.csv")

# COMMAND ----------

def review_mapper(row):
  review_details=row.split("::")
  temp_ans=[]
  if len(review_details)==4:
    temp_ans.append((str(review_details[2]),float(review_details[3])))
  return temp_ans

# COMMAND ----------

# print review_RDD.take(10)

# COMMAND ----------

review_detail_RDD=sc.parallelize(review_RDD.flatMap(review_mapper).groupByKey().mapValues(lambda x: round(sum(x) / len(x),1)).top(10,lambda item:item[1]))

# COMMAND ----------

# print review_detail_RDD.take(review_detail_RDD.count())

# COMMAND ----------

final_output_RDD=review_detail_RDD.join(business_detail_RDD)

# COMMAND ----------

# print final_output_RDD.take(10)

# COMMAND ----------

for item in final_output_RDD.take(final_output_RDD.count()):
  print item[0]+"\t"+item[1][1].split(":")[0]+"\t"+item[1][1].split(":")[1]+"\t"+str(item[1][0])

# COMMAND ----------


