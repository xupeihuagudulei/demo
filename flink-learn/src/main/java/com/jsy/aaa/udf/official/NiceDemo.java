package com.jsy.aaa.udf.official;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Random;

/**
 * @Author: jsy
 * @Date: 2021/7/1 23:47
 */
public class NiceDemo {
    private static final String PRINT_SINK_SQL = "create table sink_print ( \n" +
            " sales BIGINT," +
            " r_num BIGINT " +
            ") with ('connector' = 'print' )";

    private static final String KAFKA_SQL = "CREATE TABLE t2 (\n" +
            " user_id VARCHAR ," +
            " item_id VARCHAR," +
            " category_id VARCHAR," +
            " behavior VARCHAR," +
            " proctime TIMESTAMP(3)," +
            " ts VARCHAR" +
            ") WITH (" +
            " 'connector' = 'kafka'," +
            " 'topic' = 'ods_kafka'," +
            " 'properties.bootstrap.servers' = 'localhost:9092'," +
            " 'properties.group.id' = 'test1'," +
            " 'format' = 'json'," +
            " 'scan.startup.mode' = 'earliest-offset'" +
            ")";

    public static void main(String[] args) {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsEnv.enableCheckpointing(5000);
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        tEnv.getConfig().getConfiguration().setBoolean("table.dynamic-table-options.enabled", true);

// 接收来自外部数据源的 DataStream
        DataStream<Tuple4<String, String, String, Long>> ds = bsEnv.addSource(new SourceFunction<Tuple4<String, String, String, Long>>() {
            @Override
            public void run(SourceContext<Tuple4<String, String, String, Long>> out) throws Exception {
                Random random = new Random();

                while (true) {
                    int sale = random.nextInt(1000);
                    out.collect(new Tuple4<>("product_id", "category", "product_name", Long.valueOf(sale)));
                    Thread.sleep(100L);
                }
            }

            @Override
            public void cancel() {

            }
        });
// 把 DataStream 注册为表，表名是 “ShopSales”
        tEnv.createTemporaryView("ShopSales", ds, "product_id, category, product_name, sales");
        tEnv.createTemporaryView("aa", ds);
        String topSql = "insert into sink_print SELECT * " +
                "FROM (" +
                "   SELECT sales," +
                "       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num" +
                "   FROM ShopSales ) " +
                "WHERE row_num = 1";
        // Table table2 = tEnv.sqlQuery(topSql);
//        tEnv.toRetractStream(table2, Row.class).print("########");

        tEnv.executeSql(PRINT_SINK_SQL);
        tEnv.executeSql(topSql);
        try {
            bsEnv.execute("aaaa");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
