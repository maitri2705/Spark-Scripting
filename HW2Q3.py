# Databricks notebook source
from pyspark import SparkContext,SparkConf

# COMMAND ----------

conf=SparkConf().setAppName("HW2Q3")

# COMMAND ----------

sc = SparkContext.getOrCreate()

# COMMAND ----------

business_RDD=sc.textFile("/FileStore/tables/business.csv")

# COMMAND ----------

# print business_RDD.take(business_RDD.count())

# COMMAND ----------

def business_mapper(row):
  business_details=row.split("::")
  temp_ans=[]
  if "Stanford, CA" in business_details[1]:
    temp_ans.append((str(business_details[0]),str(business_details[1])))
  return temp_ans

# COMMAND ----------

business_detail_RDD=business_RDD.flatMap(business_mapper).distinct()

# COMMAND ----------

# print business_detail_RDD.take(business_detail_RDD.count())

# COMMAND ----------

review_RDD=sc.textFile("/FileStore/tables/review.csv")

# COMMAND ----------

def review_mapper(row):
  review_details=row.split("::")
  temp_ans=[]
  temp_ans.append((str(review_details[2]),str(review_details[1]+":"+review_details[3])))
  return temp_ans

# COMMAND ----------

# review_detail_RDD=review_RDD.flatMap(lambda x:list((x.split("::")[2],str(x.split("::")[1]+":"+x.split("::")[3]))))

# COMMAND ----------

review_detail_RDD2=review_RDD.flatMap(review_mapper)

# COMMAND ----------

# print review_detail_RDD2.take(review_detail_RDD2.count())

# COMMAND ----------

final_output_RDD=business_detail_RDD.leftOuterJoin(review_detail_RDD2).values().map(lambda (key1,key2):(key2.split(":")[0],key2.split(":")[1]))

# COMMAND ----------

# print final_output_RDD.take(final_output_RDD.count())

# COMMAND ----------

# print(final_output_RDD.count())
for item in final_output_RDD.take(final_output_RDD.count()):
  print item[0]+"\t"+item[1]

# COMMAND ----------


