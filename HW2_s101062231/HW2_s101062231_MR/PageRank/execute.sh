# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/WordCount/Output/
#hadoop jar WordCount.jar wordcount.WordCount /user/shared/WordCount/Input /user/TA/WordCount/Output
#hdfs dfs -cat /user/TA/WordCount/Output/part-*

hdfs dfs -rm -r ./ranking
hadoop jar PageRank.jar PageRank.PageRank
#hdfs dfs -cat ./ranking/part-*
