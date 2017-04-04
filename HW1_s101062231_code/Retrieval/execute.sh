# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/WordCount/Output/
#hadoop jar WordCount.jar wordcount.WordCount /user/shared/WordCount/Input /user/TA/WordCount/Output
#hdfs dfs -cat /user/TA/WordCount/Output/part-*

hdfs dfs -rm -r ./scoreOutput
hadoop jar Retrieval.jar Retrieval.Retrieval ./tableOutput ./scoreOutput
#hdfs dfs -cat ./scoreOutput/part-*