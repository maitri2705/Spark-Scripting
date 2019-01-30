# Databricks notebook source
from pyspark import SparkContext,SparkConf

# COMMAND ----------

from pyspark.sql.functions import collect_list,col

# COMMAND ----------

conf=SparkConf().setAppName("HW2Q11")

# COMMAND ----------

sc=SparkContext.getOrCreate()

# COMMAND ----------

text_file=sc.textFile("/FileStore/tables/soc_LiveJournal1Adj_1_-33625.txt")

# COMMAND ----------

def mutual_friend_mapper(line):
  user_friends=line.split("\t")
  users=user_friends[0]
  friends=user_friends[1].split(",")
  friends_second_list=list(friends)
  mutual_friend_list=[]
  for friend in friends:
    if friend!='':
      friends_second_list.remove(friend)
      friends_first_list=list(friends_second_list)
      friends_first_list=[str(a) for a in friends_first_list]
      if(int(friend)>int(users)):
        mutual_friend_list.append(((int(users),int(friend)),friends_first_list))
        friends_second_list.append(friend)
      else:
        mutual_friend_list.append(((int(friend),int(users)),friends_first_list))
        friends_second_list.append(friend)
  return mutual_friend_list

# COMMAND ----------

friend_pair_df=text_file.flatMap(mutual_friend_mapper).toDF()

# COMMAND ----------

friend_pair_df=friend_pair_df.select(friend_pair_df._1.alias('friend_pair'),friend_pair_df._2.alias('friends_list'))

# COMMAND ----------

# friend_pair_df.show()

# COMMAND ----------

def common_friend(x):
  return len(list(set(x[0]) & set(x[1])))

# COMMAND ----------

common_friend_udf=udf(common_friend)

# COMMAND ----------

# common_friend_pair=friend_pair_df.groupBy("friend_pair").agg(collect_list("friends_list").alias("friend_number"))
common_friend_pair=friend_pair_df.groupBy("friend_pair").agg(collect_list('friends_list').alias('friends_list1')).withColumn('friends_list2', common_friend_udf(col('friends_list1')))

# COMMAND ----------

common_friend_pair.count()
# common_friend_pair.show(common_friend_pair.count(),False)

# COMMAND ----------

common_friend_pair=common_friend_pair.select("friend_pair","friends_list2")

# COMMAND ----------

common_friend_pair.show()

# COMMAND ----------


