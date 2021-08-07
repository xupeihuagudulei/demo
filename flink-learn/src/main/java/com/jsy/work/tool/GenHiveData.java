package com.jsy.work.tool;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: jsy
 * @Date: 2021/8/3 0:09
 */

/*
CREATE EXTERNAL TABLE `t_metric`(
  `user_id` string COMMENT 'user_id',
  `metric` string COMMENT 'metric')
COMMENT '每天事件表'
PARTITIONED BY (
  `pt_d` varchar(20) COMMENT '天分区')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://node1:8020/user/hive/warehouse/t_metric'
TBLPROPERTIES (
  'bucketing_version'='2',
  'transient_lastDdlTime'='1628006493')

* */
public class GenHiveData {

    private static String dimTable = "CREATE TABLE dimTable (\n" +
            "  id int,\n" +
            "  user_name STRING,\n" +
            "  age INT,\n" +
            "  gender STRING,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "   'connector'='jdbc',\n" +
            "   'username'='root',\n" +
            "   'password'='root',\n" +
            "   'url'='jdbc:mysql://localhost:3306/aspirin',\n" +
            "   'table-name'='user_data_for_join'\n" +
            ")";

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

    private static final String GENERAL_PRINT_SINK_SQL = "CREATE TABLE print_table WITH ('connector' = 'print')\n" +
            "LIKE KafkaTable (EXCLUDING ALL)";


    private static String wideTable = "CREATE TABLE wideTable (\n" +
            "  id int,\n" +
            "  site STRING,\n" +
            "  user_name STRING,\n" +
            "  age INT,\n" +
            "  ts STRING,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "   'connector'='jdbc',\n" +
            "   'username'='root',\n" +
            "   'password'='root',\n" +
            "   'url'='jdbc:mysql://localhost:3306/aspirin',\n" +
            "   'table-name'='wide_table'\n" +
            ")";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEvn = StreamTableEnvironment.create(env);

        tEvn.executeSql(kafkaTable);
        // tevn.executeSql(PRINT_SINK_SQL);
        // tevn.executeSql("insert into sink_print select * from KafkaTable");

        tEvn.executeSql(GENERAL_PRINT_SINK_SQL);
        tEvn.executeSql("insert into print_table select * from KafkaTable");


        tEvn.getConfig().setSqlDialect(SqlDialect.HIVE);

        tEvn.getConfig().setSqlDialect(SqlDialect.DEFAULT);




    }
}
