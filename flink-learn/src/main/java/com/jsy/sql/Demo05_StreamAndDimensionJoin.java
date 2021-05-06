package com.jsy.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * 流表与维表join
 * 流表 -- kafka
 * 维表 -- MySQL
 *
 * <p>
 * 流与维表的join会碰到两个问题：
 * <p>
 * 第一个是性能问题。因为流速要是很快，每一条数据都需要到维表做下join，但是维表的数据是存在第三方存储系统，如果实时访问第三方存储系统，不仅join的性能会差，每次都要走网络io；还会给第三方存储系统带来很大的压力，有可能会把第三方存储系统搞挂掉。
 * <p>
 * 所以解决的方法就是维表里的数据要缓存，可以全量缓存，这个主要是维表数据不大的情况，还有一个是LRU缓存，维表数据量比较大的情况。
 *
 * @Author: jsy
 * @Date: 2021/5/6 21:53
 */
public class Demo05_StreamAndDimensionJoin {

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
            "  `user` STRING,\n" +
            "  `site` STRING,\n" +
            "  `time` STRING\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'test-old',\n" +
            "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'properties.group.id' = 'testGroup',\n" +
            "  'scan.startup.mode' = 'earliest-offset',\n" +
            "  'format' = 'json'\n" +
            ")";

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
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment.executeSql(dimTable);
        tableEnvironment.executeSql(kafkaTable);
        tableEnvironment.executeSql(wideTable);
        Table mysqlTable = tableEnvironment.from("dimTable").select("id, user_name, age, gender");
        Table kafkaTable = tableEnvironment.from("KafkaTable").select($("user"), $("site"), $("time"));

        String joinSql = "insert into wideTable " +
                " select " +
                "   dimTable.id as `id`, " +
                "   t.site as site, " +
                "   dimTable.user_name as user_name, " +
                "   dimTable.age as age, " +
                "   t.`time` as ts " +
                "from KafkaTable as t " +
                "left join dimTable on dimTable.user_name = t.`user`";
        tableEnvironment.executeSql(joinSql);

        env.execute();
    }
}
