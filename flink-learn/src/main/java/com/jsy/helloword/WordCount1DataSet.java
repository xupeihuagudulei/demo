package com.jsy.helloword;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink-DataSet-API-实现WordCount
 *
 * @Author: jsy
 * @Date: 2021/3/21 20:51
 */
public class WordCount1DataSet {

    public static void main(String[] args) throws Exception {
        //TODO 0.env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //TODO 1.source
        // 相当于4行数据
        DataSet<String> lines = env.fromElements("kafka hadoop spark", "kafka hadoop spark", "kafka hadoop", "kafka");


        //TODO 2.transformation
        //切割
        DataSet<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //value表示每一行数据
                String[] arr = value.split(" ");
                for (String word : arr) {
                    out.collect(word);
                }
            }
        });

        //记为1
        /**
         * 泛型只保留了结果
         */
        DataSet<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //value就是每一个单词
                return Tuple2.of(value, 1);
            }
        });

        //分组
        UnsortedGrouping<Tuple2<String, Integer>> grouped = wordAndOne.groupBy(0);

        //聚合
        AggregateOperator<Tuple2<String, Integer>> result = grouped.sum(1);

        //TODO 3.sink
        result.print();
    }

}
