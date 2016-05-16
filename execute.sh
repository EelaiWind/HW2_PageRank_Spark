INPUT=/shared/HW2/sample-in/input-1G
OUTPUT=/user/s101062105/hw2/spark_output

time spark-submit --class PageRank --num-executors 30 target/scala-2.10/pagerank-application_2.10-1.0.jar ${INPUT} ${OUTPUT}

hadoop fs -getmerge ${OUTPUT} output.log
