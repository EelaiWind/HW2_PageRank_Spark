INPUT=/shared/HW2/sample-in/input-10G
OUTPUT=/user/s101062105/hw2/spark_output

spark-submit --class PageRank --num-executors 10 --driver-memory 3G target/scala-2.10/pagerank-application_2.10-1.0.jar ${INPUT} ${OUTPUT}

hadoop fs -getmerge ${OUTPUT} output.log
