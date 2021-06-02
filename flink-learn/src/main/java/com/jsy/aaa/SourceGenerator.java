package com.jsy.aaa;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * kafka生成数据，参考
 * https://github.com/wuchong/flink-sql-submit
 * https://www.cnblogs.com/HeCG95/p/12218650.html
 *
 * @Author: jsy
 * @Date: 2021/6/1 23:51
 */
/*
新建topic
bin/kafka-topics.sh --create --zookeeper node1:2181,node2:2181,node3:2181 --replication-factor 1 --partitions 1 --topic user_behavior
查看topic
bin/kafka-topics.sh --zookeeper node1:2181,node2:2181,node3:2181 --describe --topic user_behavior

# 生产者
bin/kafka-console-producer.sh --broker-list node1:9092,node2:9092,node3:9092 --topic user_behavior
# 消费者
bin/kafka-console-consumer.sh --bootstrap-server node1:9092,node2:9092,node3:9092 --topic user_behavior --from-beginning


测试jar包
java -cp flink-learn-1.0-SNAPSHOT.jar com.jsy.aaa.SourceGenerator 1

把上面的Jar 包复制到Kafka根目录下：
java -cp flink-learn-1.0-SNAPSHOT.jar com.jsy.aaa.SourceGenerator 1 | bin/kafka-console-producer.sh --broker-list node1:9092,node2:9092,node3:9092 --topic user_behavior

*
*/

public class SourceGenerator {
    // private static final long SPEED = 1000; // 每秒1000条
    private static final long SPEED = 3; // 每秒1000条

    public static void main(String[] args) {
        long speed = SPEED;
        if (args.length > 0) {
            speed = Long.valueOf(args[0]);
        }
        long delay = 1000_000 / speed; // 每条耗时多少毫秒

        try (InputStream inputStream = SourceGenerator.class.getClassLoader().getResourceAsStream("user_behavior.log")) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            long start = System.nanoTime();
            while (reader.ready()) {
                String line = reader.readLine();
                System.out.println(line);

                long end = System.nanoTime();
                long diff = end - start;
                while (diff < (delay * 1000)) {
                    Thread.sleep(1);
                    end = System.nanoTime();
                    diff = end - start;
                }
                start = end;
            }
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
