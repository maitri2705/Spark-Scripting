# Databricks notebook source
from pyspark import SparkContext, SparkConf

# COMMAND ----------

conf = SparkConf().setAppName("HW2Q1")

# COMMAND ----------

# sc.stop()

# COMMAND ----------

sc = SparkContext.getOrCreate()

# COMMAND ----------

text_file = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj_1_-33625.txt")


# COMMAND ----------

def mutual_friend_mapper(line):
    user_friends = line.split("\t")
    users = user_friends[0]
    friends = user_friends[1].split(",")
    friends_second_list = list(friends)
    mutual_friend_list = []
    for friend in friends:
        if friend != '':
            friends_second_list.remove(friend)
            friends_first_list = list(friends_second_list)
            friends_first_list = [str(a) for a in friends_first_list]
            if (int(friend) > int(users)):
                mutual_friend_list.append(((int(users), int(friend)), friends_first_list))
                friends_second_list.append(friend)
            else:
                mutual_friend_list.append(((int(friend), int(users)), friends_first_list))
                friends_second_list.append(friend)
    return mutual_friend_list


# COMMAND ----------

friend_pair_RDD = text_file.flatMap(mutual_friend_mapper)
mutual_friends_RDD = friend_pair_RDD.reduceByKey(lambda list1, list2: list(set(list1).intersection(list2)))

# COMMAND ----------

# print mutual_friends_RDD.count()

# COMMAND ----------

for item in mutual_friends_RDD.take(mutual_friends_RDD.count()):
    print str(item[0][0]) + "," + str(item[0][1]) + "\t" + str(len(item[1]))

# COMMAND ----------
