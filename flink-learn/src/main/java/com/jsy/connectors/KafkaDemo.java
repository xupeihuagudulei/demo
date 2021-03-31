package com.jsy.connectors;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Flink-Connectors-KafkaConsumer/Source + KafkaProducer/Sink
 * <p>
 * Flink 里已经提供了一些绑定的 Connector，例如 kafka source 和 sink，Es sink 等。
 * 读写 kafka、es、rabbitMQ 时可以直接使用相应 connector 的 api 即可，虽然该部分是
 * Flink 项目源代码里的一部分，但是真正意义上不算作 Flink 引擎相关逻辑，并且该部分没
 * 有打包在二进制的发布包里面。所以在提交 Job 时候需要注意， job 代码 jar 包中一定要
 * 将相应的 connetor 相关类打包进去，否则在提交作业时就会失败，提示找不到相应的类，或
 * 初始化某些类异常。
 * <p>
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html
 *
 * @Author: jsy
 * @Date: 2021/3/26 8:10
 */
public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        /**
         * 以下参数都必须/建议设置上
         * 1.订阅的主题
         * 2.反序列化规则
         * 3.消费者属性-集群地址
         * 4.消费者属性-消费者组id(如果不设置,会有默认的,但是默认的不方便管理)
         * 5.消费者属性-offset重置规则,如earliest/latest...
         * 6.动态分区检测(当kafka的分区数变化/增加时,Flink能够检测到!)
         * 7.如果没有设置Checkpoint,那么可以设置自动提交offset,后续学习了Checkpoint会把offset随着做Checkpoint的时候提交到Checkpoint和默认主题中
         */

        //TODO 1.source
        //准备kafka连接参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "node1:9092");//集群地址
        props.setProperty("group.id", "flink");//消费者组id
        // latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费
        // earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        props.setProperty("auto.offset.reset", "latest");
        props.setProperty("flink.partition-discovery.interval-millis", "5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
        props.setProperty("enable.auto.commit", "true");//自动提交(提交到默认主题,后续学习了Checkpoint后随着Checkpoint存储在Checkpoint和默认主题中)
        props.setProperty("auto.commit.interval.ms", "2000");//自动提交的时间间隔
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("flink_kafka", new SimpleStringSchema(), props);
        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 2.transformation
        SingleOutputStreamOperator<String> etlDS = kafkaDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.contains("success");
            }
        });

        //TODO 3.sink
        kafkaDS.print();

        etlDS.print();
        Properties props2 = new Properties();
        props2.setProperty("bootstrap.servers", "node1:9092");
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>("flink_kafka2", new SimpleStringSchema(), props2);
        etlDS.addSink(kafkaSink);

        //TODO 4.execute
        env.execute();
    }
}
//控制台生产者 ---> flink_kafka主题 --> Flink -->etl ---> flink_kafka2主题--->控制台消费者
//准备主题 /export/server/kafka/bin/kafka-topics.sh --create --zookeeper node1:2181 --replication-factor 2 --partitions 3 --topic flink_kafka
//准备主题 /export/server/kafka/bin/kafka-topics.sh --create --zookeeper node1:2181 --replication-factor 2 --partitions 3 --topic flink_kafka2
//启动控制台生产者发送数据 /export/server/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic flink_kafka
//log:2020-10-10 success xxx
//log:2020-10-10 success xxx
//log:2020-10-10 success xxx
//log:2020-10-10 fail xxx
//启动控制台消费者消费数据 /export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --topic flink_kafka2 --from-beginning
//启动程序FlinkKafkaConsumer
//观察控制台输出结果

/**
 * 笔记：
 * kafka consumer 反序列化数据
 *      SimpleStringSchema()    按字符串方式进行序列化，反序列化
 *      TypeInformationSerializationSchema<>()  基于flink的TypeInformation创建schema
 *      JSONKeyValueDeserializationSchema()     使用jackson反序列化json格式消息，并返回ObjectNode,可以使用 .get("property")方法来访问字段
 *
 * kafka consumer 消费起始位置
 *      setStartFromGroupOffsets - default  从kafka记录的group.id的位置开始读取，如果没有根据auto.offset.reset设置
 *      setStartFromEarliest                从kafka最早的位置读取
 *      setStartFromLatest                  从kafka最新的数据开始读取
 *      setStartFromTimeStamp(long)         从时间戳大于或等于指定时间戳的位置开始读取
 *      setStartFromSpecificOffsets         从指定分区的offset位置开始读取，如果指定的offset中不存在某个分区，该分区从group offset位置开始读取。
 *
 *      tips：
 *          作业故障，从checkpoint自动恢复，以及手动savepoint时，消费的位置从保存状态中恢复，与该配置无关
 *
 * kafka consumer topic partition 自动发现
 *      内部单独的线程获取kafka meta信息进行更新
 *      flink.partition-discover.interval-millis:发现时间间隔，默认false，设置非负值开启
 *
 *      分区发现    消费的topic进行了partition扩容，新发现的分区，从earliest位置开始读取
 *      topic发现   支持正则扫描topic名字
 *
 * kafka consumer commit offset方式
 *      开启 checkpoint 时 offset 是 Flink 通过状态 state 管理和恢复的，并不是从 kafka 的 offset 位置恢复。
 *      在 checkpoint 机制下，作业从最近一次checkpoint 恢复，本身是会回放部分历史数据，导致部分数据重复消费，
 *      Flink 引擎仅保证计算状态的精准一次，要想做到端到端精准一次需要依赖一些幂等的存储系统或者事务操作。
 *
 */
