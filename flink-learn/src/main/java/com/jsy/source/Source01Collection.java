package com.jsy.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * DataStream-Source-基于集合
 * 以流来处理批
 *
 * @Author: jsy
 * @Date: 2021/3/24 22:53
 */
public class Source01Collection {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStream<String> ds1 = env.fromElements("hadoop spark flink", "hadoop spark flink");
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("hadoop spark flink", "hadoop spark flink"));
        DataStream<Long> ds3 = env.generateSequence(1, 100);
        DataStream<Long> ds4 = env.fromSequence(1, 100);

        //TODO 2.transformation

        //TODO 3.sink
        ds1.print();
        ds2.print();
        ds3.print();
        ds4.print();

        //TODO 4.execute
        env.execute();
    }
}
