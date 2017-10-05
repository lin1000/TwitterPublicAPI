# From Twiiter Public Twitter API to Social Network Analysis Experiements

 
##   Configurations
In this sample, I will leave content of twitter4j.properties as dummy data. please replace the data in twitter4j.properties with your own and make sure the .jar and .properties in a same directory.

##  Feature List

- [x] (java) To connect to Public Twitter API using your own keys and secretid
- [x] (java) Given twitter handle, you can find the followers' handle list
- [x] (java) Twitter API Key Resoure Control by managing the concurrency and locking mechanism to maximize the rate litmit utilization
- [x] (java) Executor Thread pool to submit concurrent tasks
- [x] (python) Random Sampling Account and then output as csv file in 01SamplingAccount folder
- [x] (python) Read through full account list and then output as csv file in 01FullAccount folder
- [x] (python) Compose a gnip query rule with interested accounts that aligning with rule limitations
- [x] (python) Create a historical job that can sent to gnip
- [x] (python) Generate csv files group by rule tags 
- [x] (spark) Generate json/csv files group by rule tags (accerelate processing speed by parallelizing)
```
spark-submit --master "local[*]" --executor-memory 2G --total-executor-cores 20 06GNIPDataGroupByRuleTag-Spark.py > 06GNIPDataGroupByRuleTag-Spark.log 2>&1 
```
- [x] (spark) Generate json/csv files filter by influencee account (accerelate processing speed by parallelizing)
- [ ] (spark) Speark GraphX to analyze the social networking of random sampled followers

- [x] (java8) CountTweets
```
export MAVEN_OPTS="-ea"
mvn exec:java@0002 -Dexec.args="./output/collect-follower-day4/modelpress.followers.json Scanner"
```

- [x] (java8) CountTweetsParaller : Use parallels stream to parse json object 
```
export MAVEN_OPTS="-ea"
 mvn exec:java@0003 -Dexec.args="./output/collect-follower-day4/modelpress.followers.json Parallels"
```

## Language 

Java (Concurrency, Twitter API)
Python (Data Processing)
Spark (Data Processing)