package com.jsy.aaa.udf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * udf 的第二种写法
 *
 * @Author: jsy
 * @Date: 2021/5/6 23:32
 */
public class UseUdfTest2 {

    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        //TODO 1.source
        DataStream<UseUdfTest2.WC> wordsDS = env.fromElements(
                new UseUdfTest2.WC("Hello", 1, 3),
                new UseUdfTest2.WC("World", 1, 4),
                new UseUdfTest2.WC("HelloWorld", 1, 5)
        );

        //TODO 2.transformation
        //将DataStream转为View或Table
        tenv.createTemporaryView("t_words", wordsDS, $("word"), $("frequency"), $("status"));

        // 注册UDF
        tenv.registerFunction("IsStatusFive", new IsStatus(5));
        Table wordWithCount = tenv.sqlQuery("SELECT * FROM t_words WHERE IsStatusFive(status)");

        //转为DataStream
        tenv.toRetractStream(wordWithCount, UseUdfTest2.WC.class).print();// 有聚合，肯定有更新，不能用append

        //TODO 4.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WC {
        public String word;
        // 频率
        public long frequency;
        public int status;
    }

}

