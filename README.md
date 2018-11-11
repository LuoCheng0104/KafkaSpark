# KafkaSpark
This project simply realized that the kafka producer sent log messages to Sparkstream, 
realized IP attribution query in spark, the query results were stored in mysql table, and the abnormal access, 
that is, the over-accessed IP per unit time, was put into the blacklist table.

version:

hadoop 2.7.4

spark 2.1.0

kafka 2.11-2.0.0

Producer Program submission method:

java -cp /home/bd/software/kafka_2.11-2.0.0/libs/*:/home/bd/lc/jar/KafkaSpark-x.jar RandomIPSender /home/bd/lc/data/logs/*

Spark Program submission method:

spark-submit --master yarn-client --class Receiver /home/bd/lc/jar/KafkaSpark-x.jar  /lc/ip_address_all.txt

Program submission may have some error, you can refer to the blog
https://blog.csdn.net/baidu_27485577/article/details/83181110
