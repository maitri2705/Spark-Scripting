# Databricks notebook source
from pyspark import SparkContext,SparkConf

# COMMAND ----------

from pyspark.sql.functions import collect_list,col,desc

# COMMAND ----------

conf=SparkConf().setAppName("HW2Q21")

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
        mutual_friend_list.append((int(users),int(friend),friends_first_list))
        friends_second_list.append(friend)
      else:
        mutual_friend_list.append((int(friend),int(users),friends_first_list))
        friends_second_list.append(friend)
  return mutual_friend_list

# COMMAND ----------

friend_pair_df=text_file.flatMap(mutual_friend_mapper).toDF()

# COMMAND ----------

friend_pair_df=friend_pair_df.select(friend_pair_df._1.alias('user1'),friend_pair_df._2.alias('user2'),friend_pair_df._3.alias('friends_list'))

# COMMAND ----------

# friend_pair_df.show()

# COMMAND ----------

def common_friend(x):
  return len(list(set(x[0]) & set(x[1])))

# COMMAND ----------

common_friend_udf=udf(common_friend)

# COMMAND ----------

common_friend_pair=friend_pair_df.groupBy("user1","user2").agg(collect_list('friends_list').alias('friends_list1')).withColumn('friends_list2', common_friend_udf(col('friends_list1')))

# COMMAND ----------

common_friend_pair=common_friend_pair.select("user1","user2","friends_list2")

# COMMAND ----------

commonfriend_top=common_friend_pair.sort(desc("friends_list2")).limit(10)

# COMMAND ----------

commonfriend_top.show()

# COMMAND ----------

user_data_file=sc.textFile("/FileStore/tables/userdata_1_-2c564.txt")

# COMMAND ----------

def userdata_mapper(x):
  user_detail=x.split(',')
  temp_ans=[]
  temp_ans.append((user_detail[0],user_detail[1],user_detail[2],user_detail[3]+","+user_detail[4]+","+user_detail[5]+","+user_detail[6]+","+user_detail[7]))
  return temp_ans

# COMMAND ----------

user_data_df=user_data_file.flatMap(userdata_mapper).toDF()

# COMMAND ----------

# user_data_df.show()

# COMMAND ----------

user_data_df=user_data_df.select(user_data_df._1.alias('userid'),user_data_df._2.alias('firstName'),user_data_df._3.alias('lastName'),user_data_df._4.alias('full_address'))

# COMMAND ----------

# user_data_df.show()

# COMMAND ----------

first_user_info=commonfriend_top.join(user_data_df,commonfriend_top.user1==user_data_df.userid)

# COMMAND ----------

# first_user_info.show()

# COMMAND ----------

first_user_info=first_user_info.select(first_user_info["friends_list2"].alias("friends_number"),first_user_info["user2"].alias("user2"),first_user_info["firstName"].alias('user1_firstname'),first_user_info['lastName'].alias('user1_lastname'),first_user_info['full_address'].alias('user1_full_address'))

# COMMAND ----------

# first_user_info.show()

# COMMAND ----------

second_user_info=first_user_info.join(user_data_df,first_user_info.user2==user_data_df.userid)

# COMMAND ----------

second_user_info=second_user_info.select(second_user_info["friends_number"].alias("friends_number"),second_user_info["user1_firstname"].alias('user1_firstname'),second_user_info['user1_lastname'].alias('user1_lastname'),second_user_info['user1_full_address'].alias('user1_full_address'),second_user_info["firstname"].alias('user2_firstname'),second_user_info['lastname'].alias('user2_lastname'),second_user_info['full_address'].alias('user2_full_address'))

# COMMAND ----------

second_user_info.show()

# COMMAND ----------

for item in second_user_info.take(second_user_info.count()):
  print str(item[0])+"\t"+item[1]+"\t"+item[2]+"\t"+item[3]+"\t"+item[4]+"\t"+item[5]+"\t"+item[6]

# COMMAND ----------


