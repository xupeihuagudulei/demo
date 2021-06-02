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
import static org.apache.flink.table.api.Expressions.call;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/tableapi/#group-window-aggregate
 *
 * @Author: jsy
 * @Date: 2021/5/6 23:32
 * <p>
 * https://blog.51cto.com/mapengfei/2572888
 */
public class UdafTest {

    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        /**
         * 表函数
         */
        tenv.registerFunction("avgTemp", new AggTemp());
        tenv.registerFunction("myAggFunc", new MyMinMax());

        //TODO 1.source
        DataStream<UdafTest.WC> wordsDS = env.fromElements(
                new UdafTest.WC("Hello", 1, "手机 pc ios"),
                new UdafTest.WC("Hello", 5, "android xiaomi"),
                new UdafTest.WC("Hello", 3, "huawei zte")
        );

        // 看效果
        wordsDS.print("source table:");

        //TODO 2.transformation
        // todo 2.1 table
        Table table = tenv.fromDataStream(wordsDS);
        table.printSchema();

        Table select = table.groupBy($("word"))
                .aggregate(call("myAggFunc", $("frequency")).as("agg","num"))
                .select($("word"),$("agg"), $("num"));

        // Table select = table.joinLateral(call("customTypeSplit", $("device")).as("device1", "size"))
        //         .select($("word"), $("device1"), $("size"));
        tenv.toRetractStream(select, UdafTest.WCWithLength.class).print();

        // todo 2.2 sql
        // //将DataStream转为View或Table
        // tenv.createTemporaryView("t_words", wordsDS, $("word"), $("frequency"), $("device"));
        // String sql = "select word,device1,size\n " +
        //         "from t_words ,LATERAL TABLE(customTypeSplit(device)) as customTypeSplit(device1, size)";
        // //执行sql
        // Table resultTable = tenv.sqlQuery(sql);
        //
        // //转为DataStream
        // tenv.toRetractStream(resultTable, UdafTest.WCWithLength.class).print();

        //TODO 4.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WC {
        private String word;
        // 频率
        private Integer frequency;

        private String device;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WCWithLength {
        private String word;

        private Integer agg;

        private Integer num;


    }
}





