package com.jsy.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Flink cep demo
 * https://blog.csdn.net/weixin_45417821/article/details/114841948
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/libs/cep/
 *
 * @Author: jsy
 * @Date: 2021/6/2 23:33
 */
public class CEPDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据并转换成POJO类型
        String filePath = "E:\\GitHub\\demo\\flink-learn\\src\\main\\resources\\cep_data.csv";
        DataStream<OrderEvent> orderEventStream = env.readTextFile(filePath)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                // 升序时间戳
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //1.定义一个带时间限制的模式
        Pattern<OrderEvent, OrderEvent> start = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("end").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.seconds(5));

        //2.定义侧输出流标签，用来标示超时事件
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout") {
        };

        KeyedStream<OrderEvent, Long> keyedStream = orderEventStream.keyBy(OrderEvent::getOrderId);

        //3.将pattern 应用到输入数据流 得到pattern stream
        PatternStream<OrderEvent> pattern = CEP.pattern(keyedStream, start);

        //4.调用select方法，实现对匹配复杂事件和超时复杂事件的提取和处理
        SingleOutputStreamOperator<OrderResult> resultStream = pattern.select(orderTimeoutTag, new OrderTimeoutSelect1(), new OrderPaySelect1());

        resultStream.print("payed normally ");
        resultStream.getSideOutput(orderTimeoutTag).print("timeOUT");

        env.execute("FlinkCEP");
    }

    //实现自定义的超时事件处理函数
    public static class OrderTimeoutSelect1 implements PatternTimeoutFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            Long timeoutOrderId = pattern.get("start").iterator().next().getOrderId();
            return new OrderResult(timeoutOrderId, "timeout" + timeoutTimestamp);
        }
    }

    //实现自定义的正常匹配事件处理函数
    public static class OrderPaySelect1 implements PatternSelectFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            Long payedOrderId = pattern.get("end").iterator().next().getOrderId();
            return new OrderResult(payedOrderId, "payed");
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class OrderEvent {

        private Long orderId;

        private String eventType;

        private String password;

        private Long timestamp;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class OrderResult {

        private Long orderId;

        private String result;

    }
}
