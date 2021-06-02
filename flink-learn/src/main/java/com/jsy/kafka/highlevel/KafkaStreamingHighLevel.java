package com.jsy.kafka.highlevel;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

/**
 * http://kafka.apache.org/documentation/streams/
 *
 * Kafka Streams DSL（Domain Specific Language）
 *
 * 注：创建Kafka Streaming Topology有两种方式
 * <p>
 * - low-level：Processor API
 * - high-level：Kafka Streams DSL(DSL：提供了通用的数据操作算子，如：map, filter, join, and aggregations等)
 * <p>
 * dsl api 高级api
 *
 * @Author: jsy
 * @Date: 2021/5/30 22:57
 */

/*
生产者
bin/kafka-console-producer.sh --broker-list node1:9092,node2:9092,node3:9092 --topic flink_kafka
消费者
bin/kafka-console-consumer.sh --bootstrap-server node1:9092,node2:9092,node3:9092 --topic flink_kafka2
用这个消费者,类型正确才能展示出来
bin/kafka-console-consumer.sh --bootstrap-server node1:9092,node2:9092,node3:9092 \
    --topic flink_kafka2 \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

造数据
Hello World
Hello Spark
Hello Scala
how are you

 */
public class KafkaStreamingHighLevel {
    public static void main(String[] args) {
        //1.创建kafka streaming的配置对象
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
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
        // TODO 方法一
        // KTable<String, Long> kTable = kStream
        //         .flatMap((String k, String v) -> {
        //             String[] words = v.split(" ");
        //             List<KeyValue<String, String>> list = new ArrayList<>();
        //             for (String word : words) {
        //                 KeyValue<String, String> keyValue = new KeyValue<String, String>(k, word); //k: record k v: Hello
        //                 System.out.println("keyValue = " + keyValue);
        //                 list.add(keyValue);
        //             }
        //             return list;
        //         })
        //         // 将v相同的键值对归为一类
        //         .groupBy((k, v) -> v)
        //         // 统计k相同的v的数量
        //         .count();

        // TODO 方法2
        // 流处理
        // Hello World
        // k: Hello World v: Hello
        // k: Hello World v: World
        KTable<String, Long> kTable = kStream.flatMapValues(v -> Arrays.asList(v.split(" ")))
                // hello hello
                // world word
                .groupBy((k, v) -> v)
                // hello 1
                // ....
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

/*
 * 拓扑图
 *
// 打印kafka streaming拓扑关系图
System.out.println(builder.build().describe());

-->  :输出
<--  :输入


Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [input])
      --> KSTREAM-FLATMAPVALUES-0000000001
    Processor: KSTREAM-FLATMAPVALUES-0000000001 (stores: [])
      --> KSTREAM-KEY-SELECT-0000000002
      <-- KSTREAM-SOURCE-0000000000
    Processor: KSTREAM-KEY-SELECT-0000000002 (stores: [])
      --> KSTREAM-FILTER-0000000006
      <-- KSTREAM-FLATMAPVALUES-0000000001
    Processor: KSTREAM-FILTER-0000000006 (stores: [])   // kafka自动提供的过滤k为空的数据的过滤器
      --> KSTREAM-SINK-0000000005
      <-- KSTREAM-KEY-SELECT-0000000002
    // shuffle：key相同的结果保存到topic相同分区
    Sink: KSTREAM-SINK-0000000005 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition)
      <-- KSTREAM-FILTER-0000000006

  Sub-topology: 1
    Source: KSTREAM-SOURCE-0000000007 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition])
      --> KSTREAM-AGGREGATE-0000000004
    Processor: KSTREAM-AGGREGATE-0000000004 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000003])// 自动状态管理,默认提供
      --> KTABLE-TOSTREAM-0000000008
      <-- KSTREAM-SOURCE-0000000007
    Processor: KTABLE-TOSTREAM-0000000008 (stores: [])
      --> KSTREAM-SINK-0000000009
      <-- KSTREAM-AGGREGATE-0000000004
    Sink: KSTREAM-SINK-0000000009 (topic: output)
      <-- KTABLE-TOSTREAM-0000000008

1. 在kafka streaming拓扑关系图中有两个子拓扑Sub-topology: 0和Sub-topology: 1
*
2. Sub-topology: 0的KSTREAM-SOURCE-0000000000会将input topic中的record作为数据源，
    * 然后经过处理器（Processor）KSTREAM-FLATMAPVALUES-0000000001、KSTREAM-KEY-SELECT-0000000002、
    * KSTREAM-FILTER-0000000006（过滤掉key为空的中间结果）,最终将处理完成的结果存放到
    * topic KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition中。为什么这里需要*-repartition的topic呢？
    * 主要原因是保证在shuffle结束后key相同的record存放在*-repartition相同的分区中，
    * 这样就为下一步的统计做好了准备
    *
3. Sub-topology: 1的KSTREAM-SOURCE-0000000007将*-repartitiontopic中的record作为数据源，
    * 然后经过ProcessorKSTREAM-AGGREGATE-0000000004进行聚合操作，并且将聚合的状态信息存放大
    * topicKSTREAM-AGGREGATE-STATE-STORE-0000000003中，继续经过ProcessorKTABLE-TOSTREAM-0000000008，
    * 最终将处理完成的结果存放到output中


 */