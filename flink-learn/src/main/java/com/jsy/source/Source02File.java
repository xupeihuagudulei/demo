package com.jsy.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * DataStream-Source-基于本地/HDFS的文件/文件夹/压缩文件
 *
 * @Author: jsy
 * @Date: 2021/3/24 22:53
 */
public class Source02File {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStream<String> ds1 = env.readTextFile("data/input/words.txt");
        // 读文件夹
        DataStream<String> ds2 = env.readTextFile("data/input/dir");
        // 读压缩文件
        DataStream<String> ds3 = env.readTextFile("data/input/wordcount.txt.gz");


        //TODO 2.transformation

        //TODO 3.sink
        ds1.print();
        ds2.print();
        ds3.print();

        //TODO 4.execute
        env.execute();
    }
}
