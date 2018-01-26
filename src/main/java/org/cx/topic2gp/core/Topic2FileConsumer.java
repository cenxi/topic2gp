package org.cx.topic2gp.core;

import org.cx.topic2gp.util.Log;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 消费者
 * Created by 冯曦 on 2017/12/17.
 */
public class Topic2FileConsumer {
    private final ConsumerConnector consumer;
    private final String topic;
    private final String zookeeper="kafka04:2181";
    private int threadCount;
    private String tableName;

    /**
     * @param groupId 消费者组
     * @param topic     主题
     * @param threadCount   线程数
     */
    public Topic2FileConsumer(String groupId, String topic, String tableName, int threadCount) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        /*设置超时，超时系统会抛出超时异常,从而退出正在的线程*/
        props.put("consumer.timeout.ms",110*1000+"");
        /**可以取值为smallest,largest**/
        props.put("auto.offset.reset", "smallest");
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        this.topic = topic.toLowerCase();
        this.threadCount = threadCount;
        this.tableName = tableName;
    }

    /**
     * 消费kafka的数据
     *
     */
    public void consume() throws FileNotFoundException {
        Log.info(this.toString());
        Log.info("Starting consuming...");
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(threadCount));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        Object lock = new Object();
        ConcurrentLinkedQueue<String> msgQueue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < streams.size(); i++) {
            new Topic2FileThread(streams.get(i), "th" + i, topic,tableName,lock,msgQueue).start();
        }
    }

    @Override
    public String toString() {
        return "Topic2FileConsumer{" +
                "topic='" + topic + '\'' +
                ", zookeeper='" + zookeeper + '\'' +
                ", threadCount=" + threadCount +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
