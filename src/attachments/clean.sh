#!/bin/bash
#这个文件是删除Kafka集群缓存的所有数据，并重启kafka集群
killall -9 java
ssh kafka@kafka03 killall -9 java
ssh kafka@kafka04 killall -9 java

rm -rf /tmp/hsperfdata_kafka /tmp/kafka-logs /tmp/zookeeper /tmp/kafka-streams
ssh kafka@kafka03 rm -rf /tmp/hsperfdata_kafka /tmp/kafka-logs /tmp/zookeeper /tmp/kafka-streams
ssh kafka@kafka04 rm -rf /tmp/hsperfdata_kafka /tmp/kafka-logs /tmp/zookeeper /tmp/kafka-streams

mkdir -p /tmp/zookeeper
ssh kafka@kafka03 mkdir -p /tmp/zookeeper
ssh kafka@kafka04 mkdir -p /tmp/zookeeper

echo "1" > /tmp/zookeeper/myid
ssh kafka@kafka03 "echo \"2\" > /tmp/zookeeper/myid"
ssh kafka@kafka04 "echo \"3\" > /tmp/zookeeper/myid"

zkServer.sh start &
ssh kafka@kafka03 zkServer.sh start & 
ssh kafka@kafka04 zkServer.sh start & 

kafka-server-start.sh -daemon /home/kafka/kafka/config/server.properties
ssh kafka@kafka03 kafka-server-start.sh -daemon /home/kafka/kafka/config/server.properties
ssh kafka@kafka04 kafka-server-start.sh -daemon /home/kafka/kafka/config/server.properties
