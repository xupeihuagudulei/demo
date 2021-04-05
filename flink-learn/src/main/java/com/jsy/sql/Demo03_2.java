package com.jsy.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 使用Flink SQL来统计5秒内 每个用户的 订单总数、订单的最大金额、订单的最小金额
 * 也就是每隔5秒统计最近5秒的每个用户的订单总数、订单的最大金额、订单的最小金额
 * 要求：使用事件时间+WM+FlinkSql和Table中的window
 * <p>
 * -- table 风格,面向对象风格
 *
 * @Author: jsy
 * @Date: 2021/4/6 0:01
 */
public class Demo03_2 {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        //TODO 1.source
        DataStreamSource<Order> orderDS = env.addSource(new RichSourceFunction<Order>() {
            private Boolean isRunning = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        //TODO 2.transformation
        //需求:事件时间+Watermarker+FlinkSQL和Table的window完成订单统计
        DataStream<Order> orderDSWithWatermark = orderDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((order, recordTimestamp) -> order.getCreateTime())
        );

        //将DataStream-->View/Table,注意:指定列的时候需要指定哪一列是时间
        tenv.createTemporaryView("t_order", orderDSWithWatermark, $("orderId"), $("userId"), $("money"), $("createTime").rowtime());
        //Table table = tenv.fromDataStream(orderDSWithWatermark, $("orderId"), $("userId"), $("money"), $("createTime").rowtime());
        //table.groupBy().select();
/*
select  userId, count(orderId) as orderCount, max(money) as maxMoney,min(money) as minMoney
from t_order
group by userId,
tumble(createTime, INTERVAL '5' SECOND)
 */
//https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/tableApi.html
        Table resultTable = tenv.from("t_order")
                .window(Tumble.over(lit(5).second())
                        .on($("createTime"))
                        .as("tumbleWindow"))
                // 按照窗口分钟，再按照userId 分组
                .groupBy($("tumbleWindow"), $("userId"))
                .select(
                        $("userId"),
                        $("orderId").count().as("orderCount"),
                        $("money").max().as("maxMoney"),
                        $("money").min().as("minMoney")
                );

        DataStream<Tuple2<Boolean, Row>> resultDS = tenv.toRetractStream(resultTable, Row.class);

        //TODO 3.sink
        resultDS.print();

        //TODO 4.execute
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime;//事件时间
    }
}
