package com.jsy.manager;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Author: jsy
 * @Date: 2021/5/30 23:30
 */
public class TestKafka {
    public static void main(String[] args) {
        //1.创建kafka streaming的配置对象
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // 指定key默认的序列化器和反序列化器
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // 流处理应用名
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-dsl");
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);  // 线程数量 默认为1

        //2.dsl编程
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> kStream = streamsBuilder.stream("flink_kafka");// kstream【数据流】反映了flink_kafka topic中的record
        // kstream k: record k  v: record v
        // flatMap展开或者铺开 v ---> word[]
        KTable<String, Long> kTable = kStream
                .flatMap((String k, String v) -> {
                    String[] words = v.split(" ");
                    List<KeyValue<String, String>> list = new ArrayList<>();
                    for (String word : words) {
                        KeyValue<String, String> keyValue = new KeyValue<String, String>(k, word); //k: record k v: Hello
                        list.add(keyValue);
                    }
                    return list;
                })
                // 将v相同的键值对归为一类
                .groupBy((k, v) -> v)
                // 统计k相同的v的数量
                .count();
        // 将计算的结果输出保存到t10的topic中 k: word【string】 v: count【long】
        kTable.toStream().to("flink_kafka2", Produced.with(Serdes.String(), Serdes.Long()));
        //3. 创建kafka Streaming应用
        Topology topology = streamsBuilder.build();
        // 打印输出topology的关系图
        System.out.println(topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        //4. 启动
        kafkaStreams.start();
    }
}
