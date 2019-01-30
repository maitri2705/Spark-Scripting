# Databricks notebook source
from pyspark import SparkContext, SparkConf

# COMMAND ----------

conf = SparkConf().setAppName("HW2Q2")

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

# print mutual_friends_RDD.take(mutual_friends_RDD.count()) 

# COMMAND ----------

mutual_friends_len = sc.parallelize(
    friend_pair_RDD.reduceByKey(lambda list1, list2: len(list(set(list1).intersection(list2)))).top(10,
                                                                                                    key=lambda items:
                                                                                                    items[1]))

# COMMAND ----------

print mutual_friends_len.take(mutual_friends_len.count())

# COMMAND ----------

userdata_file = sc.textFile("/FileStore/tables/userdata_1_-2c564.txt")


# COMMAND ----------

def friend_data_mapper(line):
    friends_data = line.split(",")
    data_list = []
    if len(friends_data) == 10:
        data_list.append((int(str(friends_data[0])), str(
            friends_data[1] + ":" + friends_data[2] + ":" + friends_data[3] + "," + friends_data[4] + "," +
            friends_data[5] + "," + friends_data[6] + "," + friends_data[7])))
    #   data_list=[str(a) for a in data_list]
    return data_list


# COMMAND ----------

userdata_RDD = userdata_file.flatMap(friend_data_mapper)

# COMMAND ----------

# print userdata_RDD.take(1)

# COMMAND ----------

mutual_friends_len1 = mutual_friends_len.map(lambda (key1, key2): (key1[0], (key1[1], key2)))

# COMMAND ----------

# print mutual_friends_len1.take(1)

# COMMAND ----------

first_join_RDD = mutual_friends_len1.leftOuterJoin(userdata_RDD)

# COMMAND ----------

print first_join_RDD.take(first_join_RDD.count())

# COMMAND ----------

mutual_friends_len2 = first_join_RDD.map(lambda (key1, (key2, key3)): (key2[0], (key3, key2[1])))

# COMMAND ----------

second_join_RDD = mutual_friends_len2.leftOuterJoin(userdata_RDD)

# COMMAND ----------

print second_join_RDD.take(second_join_RDD.count())

# COMMAND ----------

final_output_RDD = second_join_RDD.map(lambda (key1, ((key2, key3), key4)): (key3, key2, key4))

# COMMAND ----------

print final_output_RDD.take(final_output_RDD.count())

# COMMAND ----------

for item in final_output_RDD.take(final_output_RDD.count()):
    user1_details = item[1].split(":")
    user2_details = item[2].split(":")
    print str(item[0]) + "\t" + str(user1_details[0]) + "\t" + str(user1_details[1]) + "\t" + str(
        user1_details[2]) + "\t" + str(user2_details[0]) + "\t" + str(user2_details[1]) + "\t" + str(user2_details[2])

# COMMAND ----------
