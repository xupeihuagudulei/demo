package com.jsy.watermaker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * 演示基于事件时间的窗口计算+WaterMaker解决一定程度上的数据乱序/延迟到达的问题
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/event_timestamps_watermarks.html
 * <p>
 * WaterMaker就是给数据再额外的加的一个时间列
 * 也就是WaterMaker是个时间戳!
 * <p>
 * WaterMaker = 数据的事件时间  -  最大允许的延迟时间或乱序时间
 * 注意:后面通过源码会发现,准确来说:
 * <p>
 * - 不是某条数据的了，因为数据可能乱序，水位线就下降
 * WaterMaker = 当前窗口的最大的事件时间  -  最大允许的延迟时间或乱序时间
 * 这样可以保证WaterMaker水位线会一直上升(变大),不会下降
 * <p>
 * <p>
 * * ---也就是说Watermaker是用来触发窗口计算的！
 * Watermaker如何触发窗口计算的？
 * 窗口计算的触发条件为:
 * 1.窗口中有数据  2.Watermaker >= 窗口的结束时间
 * Watermaker = 当前窗口的最大的事件时间  -  最大允许的延迟时间或乱序时间
 * 也就是说只要不断有数据来,就可以保证Watermaker水位线是会一直上升/变大的,不会下降/减小的
 * 所以最终一定是会触发窗口计算的
 * 注意:
 * 上面的触发公式进行如下变形:
 * Watermaker >= 窗口的结束时间
 * Watermaker = 当前窗口的最大的事件时间  -  最大允许的延迟时间或乱序时间
 * 当前窗口的最大的事件时间  -  最大允许的延迟时间或乱序时间  >= 窗口的结束时间
 * 当前窗口的最大的事件时间  >= 窗口的结束时间 +  最大允许的延迟时间或乱序时间
 *
 * @Author: jsy
 * @Date: 2021/3/26 22:31
 */
public class WaterMakerDemo01 {

    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStreamSource<Order> orderDS = env.addSource(new SourceFunction<Order>() {
            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(2);
                    int money = random.nextInt(101);
                    //随机模拟延迟 时间戳随机减0-5秒
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    ctx.collect(new Order(orderId, userId, money, eventTime));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        //TODO 2.transformation
        //老版本API
        /*DataStream<Order> watermakerDS = orderDS.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(3)) {//最大允许的延迟时间或乱序时间
                    @Override
                    public long extractTimestamp(Order element) {
                        return element.eventTime;
                        //指定事件时间是哪一列,Flink底层会自动计算:
                        //Watermaker = 当前最大的事件时间 - 最大允许的延迟时间或乱序时间
                    }
        });*/
        //注意:下面的代码使用的是Flink1.12中新的API
        //每隔5s计算最近5s的数据求每个用户的订单总金额,要求:基于事件时间进行窗口计算+Watermaker
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//在新版本中默认就是EventTime，老版本需要设置下
        //设置Watermarker = 当前最大的事件时间 - 最大允许的延迟时间或乱序时间
        SingleOutputStreamOperator<Order> orderDSWithWatermark = orderDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))//指定maxOutOfOrderness最大无序度/最大允许的延迟时间/乱序时间
                        .withTimestampAssigner((order, timestamp) -> order.getEventTime())//指定事件时间列
        );

        SingleOutputStreamOperator<Order> result = orderDSWithWatermark.keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("money");

        //TODO 3.sink
        result.print();

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
        private Long eventTime;
    }
}
