package com.jsy.aaa.udf.official;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/table/kafka/
 *
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/table/print/
 *
 * @Author: jsy
 * @Date: 2021/7/1 23:30
 */
public class SqlFromKafka {

    private static String kafkaTable = "CREATE TABLE KafkaTable (\n" +
            "  `user_id` STRING,\n" +
            "  `item_id` STRING,\n" +
            "  `category_id` STRING,\n" +
            "  `behavior` STRING,\n" +
            "  `ts` STRING\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'user_behavior',\n" +
            "  'properties.bootstrap.servers' = 'node1:9092',\n" +
            "  'properties.group.id' = 'testGroup',\n" +
            "  'scan.startup.mode' = 'latest-offset',\n" +
            "  'format' = 'json'\n" +
            ")";

    private static final String PRINT_SINK_SQL = "create table sink_print ( \n" +
            "  `user_id` STRING,\n" +
            "  `item_id` STRING,\n" +
            "  `category_id` STRING,\n" +
            "  `behavior` STRING,\n" +
            "  `ts` STRING\n" +
            ") with ('connector' = 'print' )";

    private static final String GENERAL_PRINT_SINK_SQL = "CREATE TABLE print_table WITH ('connector' = 'print')\n" +
            "LIKE KafkaTable (EXCLUDING ALL)";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql(kafkaTable);
        // tableEnvironment.executeSql(PRINT_SINK_SQL);
        // tableEnvironment.executeSql("insert into sink_print select * from KafkaTable");

        tableEnvironment.executeSql(GENERAL_PRINT_SINK_SQL);
        tableEnvironment.executeSql("insert into print_table select * from KafkaTable");



        // try {
        //     env.execute();
        // } catch (Exception e) {
        //     e.printStackTrace();
        // }
    }

}
