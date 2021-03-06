package com.jsy.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink Table&SQL 案例 - 将DataStream数据转Table和View然后使用sql进行统计查询
 * <p>
 * - 依赖
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/
 * 程序结构  官网示例
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/common.html
 *
 * @Author: jsy
 * @Date: 2021/3/28 23:33
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();// 默认就是blink，流模式
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        //TODO 1.source
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        //TODO 2.transformation
        // 将DataStream数据转Table和View,然后查询
        // Expression 表达式  在内存里搞了个表  tableA
        Table tableA = tenv.fromDataStream(orderA, $("user"), $("product"), $("amount"));  // ds 风格
        tableA.printSchema();
        System.out.println(tableA);

        tenv.createTemporaryView("tableB", orderB, $("user"), $("product"), $("amount")); // sql 风格

        //查询:tableA中amount>2的和tableB中amount>1的数据最后合并
        /*
select * from tableA where amount > 2
union
 select * from tableB where amount > 1
         */
        String sql = "select * from " + tableA + " where amount > 2 \n" + // 没有tableA表 ，只有tableA变量，从tableA变量里查
                "union \n" +
                " select * from tableB where amount > 1";

        Table resultTable = tenv.sqlQuery(sql);
        resultTable.printSchema();
        System.out.println(resultTable);//UnnamedTable$1

        //将Table转为DataStream
        // DataStream<Order> resultDS = tenv.toAppendStream(resultTable, Order.class);//union all使用toAppendStream
        DataStream<Tuple2<Boolean, Order>> resultDS = tenv.toRetractStream(resultTable, Order.class);//union使用toRetractStream
        //toAppendStream → 将计算后的数据append到结果DataStream中去
        //toRetractStream  → 将计算后的新的数据在DataStream原数据的基础上更新true或是删除false
        //类似StructuredStreaming中的append/update/complete

        //TODO 3.sink
        resultDS.print();

        //TODO 4.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        public Long user;
        public String product;
        public int amount;
    }

}
