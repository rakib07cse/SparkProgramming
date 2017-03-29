### Flume Configuration

``` shell
agent.sources = tailEventLogSource

# agent.sinks = loggerSink
agent.sinks = avroSink
agent.channels = memChannel
 
# Bind the source and sink to the channel
agent.sources.tailEventLogSource.channels = memChannel
agent.sinks.avroSink.channel = memChannel
 
 
# Define the type and options for each sources
agent.sources.tailEventLogSource.type = exec
agent.sources.tailEventLogSource.command = tail -f /opt/log.txt
 
# Define the type and options for the channel
agent.channels.memChannel.type = memory # file
agent.channels.memChannel.capacity = 10000
agent.channels.memChannel.transactionCapacity = 1000
 
 
# Define the type and options for the sink
# Note: namenode is the hostname the hadoop namenode server
# agent.sinks.loggerSink.type = logger
agent.sinks.avroSink.type = avro
agent.sinks.avroSink.channel = memChannel
agent.sinks.avroSink.hostname = localhost
agent.sinks.avroSink.port = 10000
```
Command for lunching Flume 
```
flume-ng agent -n agent -c conf -f /usr/local/flume/conf/flume-spark-stream.conf
```

### Build
```shell
mvn package -DskipTests
```

### Usage

``` shell
# submit the jar to yarn
/usr/local/spark/bin/spark-submit --master yarn --deploy-mode cluster --class com.ringid.sparkStreaming.FlumeEventReceiver.SparkStreamer /opt/FlumeEventReceiver-1.0.0-SNAPSHOT.jar localhost 10000

# kill the application
yarn application -kill application_Id
```

Extracting logs from HDFS's parquet file - 

``` shell
scala > val sqlContext = new org.apache.spark.sql.SQLContext(sc)
scala > val eventLogDF = sqlContext.read.parquet("/user/flume-spark/eventlog")
scala > eventLogDF.show()

+--------------------+----------+---------+--------+--------------------+--------------------+
|                args|      date|eventType|logLevel|          methodName|           requestId|
+--------------------+----------+---------+--------+--------------------+--------------------+
|Map(sessionUserId...|2017-02-01|        R|    INFO|         isFeedSaved|13fe6f40-e8c2-11e...|
|Map(userId -> 51263)|2017-02-01|        R|    INFO|    getUserBasicInfo|13fe9650-e8c2-11e...|
|Map(sessionUserId...|2017-02-01|        R|    INFO|         isFeedSaved|13fee470-e8c2-11e...|
|Map(userId -> 51263)|2017-02-01|        R|    INFO| getTotalFriendCount|13fff5e0-e8c2-11e...|
| Map(calleeId -> 28)|2017-02-01|        R|    INFO|   getMissedCallList|17ff8520-e8c2-11e...|
|   Map(userId -> 28)|2017-02-01|        R|    INFO|    getSuggestionIDs|185c2410-e8c2-11e...|
|               Map()|2017-02-01|        R|    INFO|       getUsersByIDs|18d76e90-e8c2-11e...|
|   Map(userId -> 28)|2017-02-01|        R|    INFO|getMutualFriendsC...|18d88000-e8c2-11e...|
|Map(userUpdateTim...|2017-02-01|        R|    INFO|        getMyFriends|1b5eb5b0-e8c2-11e...|
|   Map(userId -> 28)|2017-02-01|        R|    INFO|      getFriendCount|1b62ad50-e8c2-11e...|
|Map(friendId -> 5...|2017-02-01|        R|    INFO| getMutualFriendsIDs|13f68002-e8c2-11e...|
|Map(userId -> 51263)|2017-02-01|        R|    INFO|           getRingID|13f68004-e8c2-11e...|
|Map(pivotId -> 0,...|2017-02-01|        R|    INFO|          getFriends|13f68003-e8c2-11e...|
|  Map(userId -> 436)|2017-02-01|        R|    INFO|getMutualFriendsC...|13f71c40-e8c2-11e...|
|                null|2017-02-01|       18|    INFO|                null|13f68000-e8c2-11e...|
|Map(userId -> 51263)|2017-02-01|        R|    INFO|    getUserBasicInfo|13faecd0-e8c2-11e...|
|Map(sessionUserId...|2017-02-01|        R|    INFO|         isFeedSaved|13fb3af0-e8c2-11e...|
|Map(userId -> 51263)|2017-02-01|        R|    INFO|    getUserBasicInfo|13fb8910-e8c2-11e...|
|Map(sessionUserId...|2017-02-01|        R|    INFO|         isFeedSaved|13fbd730-e8c2-11e...|
|Map(userId -> 51263)|2017-02-01|        R|    INFO|    getUserBasicInfo|13fbfe40-e8c2-11e...|
+--------------------+----------+---------+--------+--------------------+--------------------+
only showing top 20 rows
```
