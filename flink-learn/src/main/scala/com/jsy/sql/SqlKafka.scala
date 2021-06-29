package com.jsy.sql

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableResult}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.types.Row

import java.lang

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/table/kafka/#how-to-create-a-kafka-table
 *
 * @Author: jsy
 * @Date: 2021/6/30 5:51 
 */
object SqlKafka {

  def main(args: Array[String]): Unit = {

    //TODO 0.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //TODO 1.source
    // 直接连接kafka，获取的就是一个表
    val inputTable: TableResult = tenv.executeSql(
      """
        |CREATE TABLE user_behavior (
        |    `user_id` STRING,
        |    `item_id` STRING,
        |    `category_id` STRING,
        |    `behavior` STRING,
        |    `ts` STRING
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'user_behavior',
        |    'properties.bootstrap.servers' = 'node1:9092',
        |    'properties.group.id' = 'testGroup',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'json'
        |)
        |""".stripMargin)

    //TODO 2.transformation
    //编写sql过滤出行为为pv的数据
    val sql = "select * from user_behavior where behavior='pv'"
    val etlResult: Table = tenv.sqlQuery(sql)

    //TODO 3.sink
    val resultDS: DataStream[Tuple2[lang.Boolean, Row]] = tenv.toRetractStream(etlResult, classOf[Row])
    resultDS.print

    val outputTable: TableResult = tenv.executeSql(
      """
        |CREATE TABLE output_kafka (
        |    `user_id` STRING,
        |    `item_id` STRING,
        |    `category_id` STRING,
        |    `behavior` STRING,
        |    `ts` STRING
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'output_kafka',
        |    'properties.bootstrap.servers' = 'node1:9092',
        |    'format' = 'json',
        |    'sink.partitioner' = 'round-robin'
        |)
        |""".stripMargin)

    tenv.executeSql("insert into output_kafka select * from " + etlResult)

    //TODO 4.execute
    env.execute

  }
}

//case class Behavior(
//                     userId: String, //用户id
//                     itemId: String, //
//                     categoryId: String, //类别id
//                     behavior: String, //国家
//                     ts: String //时间戳
//                   )
