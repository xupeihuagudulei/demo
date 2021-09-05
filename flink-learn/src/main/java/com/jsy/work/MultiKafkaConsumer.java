package com.jsy.work;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author: jsy
 * @Date: 2021/5/30 17:16
 */

/*
测试用的两个kafka配置
/export/server/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic flink_kafka

/export/server/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic flink_kafka2

Yarn集群跑起来  需要改文件读取路径，传全量的包
/export/server/flink/bin/yarn-session.sh -n 2 -tm 800 -s 1 -d
/export/server/flink/bin/flink run --class com.jsy.work.MultiKafkaConsumer /root/MultiKafkaConsumer.jar

-- 没通
/export/server/flink/bin/flink run -d --yarnname jsy-test --yarnship /export/server/flink/lib -yD env.java.opts="-Xloggc:<LOG_DIR>/gc.log -XX:+PrintGCDetails -XX:-OmitStackTraceInFastThrow -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=20M"  --jobmanager yarn-cluster --class com.jsy.work.MultiKafkaConsumer /root/jar/flink-learn-1.0-SNAPSHOT.jar


 */
@Slf4j
public class MultiKafkaConsumer {

    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        //准备kafka连接参数
        DataStream<String> kafkaDS = initKafkaDS(env);

        kafkaDS.print("union kafka").setParallelism(10);

        // //TODO 2.transformation
        // SingleOutputStreamOperator<String> etlDS = kafkaDS.filter(new FilterFunction<String>() {
        //     @Override
        //     public boolean filter(String value) throws Exception {
        //         return value.contains("success");
        //     }
        // });
        //
        // //TODO 3.sink
        // kafkaDS.print();
        //
        // etlDS.print();
        // Properties props2 = new Properties();
        // props2.setProperty("bootstrap.servers", "node1:9092");
        // FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>("flink_kafka2", new SimpleStringSchema(), props2);
        // etlDS.addSink(kafkaSink);

        //TODO 4.execute
        env.execute();
    }

    private static DataStream<String> initKafkaDS(StreamExecutionEnvironment env) {

        DataStream<String> kafkaDS = null;
        Path path = new Path("E:\\GitHub\\demo\\flink-learn\\src\\main\\resources\\kafka.conf");
        InputStreamReader inputStreamReader = null;
        try {
            inputStreamReader = new InputStreamReader(path.getFileSystem().open(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("get kafka conf file error.", e);
        }
        Config config = ConfigFactory.parseReader(inputStreamReader);
        Set<String> keys = config.entrySet().stream()
                .map(Map.Entry::getKey)
                .map(t -> t.substring(0, t.indexOf(".")))
                .collect(Collectors.toSet());

        List<DataStream<String>> kafkaDSList = Lists.newArrayList();

        for (String key : keys) {
            Config kafkaConf = config.getConfig(key);
            Set<Map.Entry<String, ConfigValue>> entries = kafkaConf.entrySet();
            Properties props = new Properties();
            String topic = "";
            for (Map.Entry<String, ConfigValue> entry : entries) {
                if ("topic".equals(entry.getKey())) {
                    topic = entry.getValue().unwrapped().toString();
                }
                props.setProperty(entry.getKey(), entry.getValue().unwrapped().toString());
            }

            System.out.println("topic " + topic + " : ");
            for (Map.Entry<Object, Object> propEntry : props.entrySet()) {
                System.out.println(propEntry.getKey() + "----------" + propEntry.getValue());
            }
            FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
            kafkaDSList.add(env.addSource(kafkaSource).setParallelism(5));
        }
        if (kafkaDSList.isEmpty()) {
            return kafkaDS;
        }
        if (kafkaDSList.size() == 1) {
            return kafkaDSList.get(0);
        }
        DataStream<String> unionKafkaDS = kafkaDSList.get(0);
        for (int i = 1; i < kafkaDSList.size(); i++) {
            unionKafkaDS = unionKafkaDS.union(kafkaDSList.get(i));
        }
        return unionKafkaDS;
    }

}
