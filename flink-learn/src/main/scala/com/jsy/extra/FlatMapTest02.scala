package com.jsy.extra

import com.jsy.extra.udf.FlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala.tableConversions
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableResult}
import org.apache.flink.types.Row

import java.lang

/**
 * 解析复杂json，然后flatmap
 *
 * @Author: jsy
 * @Date: 2021/7/6 7:13
 */

/*
/export/server/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic input_kafka
# 消息
{"funcName":"test","resultMap":{"result":{"cover":"/data/test/log.txt"},"isSuccess":true,"timestamp":1610549997263}}

{
    "funcName": "test",
    "resultMap": {
        "result": {
            "cover": "/data/test/log.txt"
        },
        "isSuccess": true,
        "timestamp": 1610549997263
    }
}


*/
object FlatMapTest02 {

  def main(args: Array[String]): Unit = {
    //TODO 0.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    tenv.registerFunction("FlatMapFunction", new FlatMapFunction)

    //TODO 1.source
    // 直接连接kafka，获取的就是一个表
    val inputTable: TableResult = tenv.executeSql(
      """
        |CREATE TABLE kafka_source (
        |    funcName STRING,
        |    resultMap STRING,
        |    proctime as PROCTIME()
        |) WITH (
        |    'connector' = 'kafka', -- 使用 kafka connector
        |    'topic' = 'input_kafka',  -- kafka topic
        |    'properties.bootstrap.servers' = 'node1:9092',  -- broker连接信息
        |    'properties.group.id' = 'jason_flink_test', -- 消费kafka的group_id
        |    'scan.startup.mode' = 'latest-offset',  -- 读取数据的位置
        |    'format' = 'json',  -- 数据源格式为 json
        |    'json.fail-on-missing-field' = 'true', -- 字段丢失任务不失败
        |    'json.ignore-parse-errors' = 'false'  -- 解析失败跳过
        |)
        |""".stripMargin)

    //TODO 2.transformation
    // 解析sql

    val etlResult: Table = tenv.sqlQuery(
      """
        |select funcName,resultMap,proctime from kafka_source
        |""".stripMargin)





    etlResult.printSchema()

    //TODO 3.sink


    //TODO 4.execute
    env.execute

  }

}
