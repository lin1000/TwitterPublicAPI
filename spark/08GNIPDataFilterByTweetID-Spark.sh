nohup spark-submit --master "spark://10.120.175.161:7077" --executor-memory 512M --total-executor-cores 48 08GNIPDataFilterByTweetID-Spark.py $1 > 08GNIPDataFilterByTweetID-Spark.log 2>&1 &
