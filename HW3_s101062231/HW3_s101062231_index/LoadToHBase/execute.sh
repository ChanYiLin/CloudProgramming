hdfs dfs -getmerge PageRankSpark/part-* PageRank
hdfs dfs -getmerge tableOutput/part-* Inverted
#hadoop jar HBaseExample.jar hBaseExample.HBaseExample /home/cp2016/shared/lab7/input/eng.txt /home/cp2016/shared/lab7/input/math.txt
hadoop jar HBaseExample.jar hBaseExample.HBaseExample ./Inverted ./PageRank
