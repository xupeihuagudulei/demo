package com.jsy.sql

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.{Random, UUID}

/**
 * @Author: jsy
 * @Date: 2021/6/29 21:52 
 */
object SqlLocal {

  def main(args: Array[String]): Unit = {
    //TODO 0.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance.inStreamingMode.build // 默认就是blink，流模式
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //TODO 1.source
    val orderA: DataStream[Order] = env.
      fromCollection(Array(new Order(1L, "beer", 3),
        new Order(1L, "diaper", 4),
        new Order(3L, "rubber", 2)))

    val orderB: DataStream[Order] = env
      .fromCollection(List(new Order(2L, "pen", 3),
        new Order(2L, "rubber", 3),
        new Order(4L, "beer", 1)))

    //TODO 2.transformation
    // 将DataStream数据转Table和View,然后查询
    // Expression 表达式  在内存里搞了个表  tableA
    val tableA: Table = tenv.fromDataStream(orderA, $("user"), $("product"), $("amount")) // ds 风格
    tableA.printSchema()
    System.out.println(tableA)

    tenv.createTemporaryView("tableB", orderB, $("user"), $("product"), $("amount")) // sql 风格

    //查询:tableA中amount>2的和tableB中amount>1的数据最后合并
    /*
select * from tableA where amount > 2
union
select * from tableB where amount > 1
     */
    val sql: String =
    s"""
       |select * from $tableA  where amount > 2
       |union
       |select * from tableB where amount > 1
       |""".stripMargin

    val resultTable: Table = tenv.sqlQuery(sql)
    resultTable.printSchema()
    System.out.println(resultTable) //UnnamedTable$1

    //将Table转为DataStream
    // DataStream<Order> resultDS = tenv.toAppendStream(resultTable, Order.class);//union all使用toAppendStream
    val resultDS: DataStream[(Boolean, Order)] = tenv.toRetractStream(resultTable) //union使用toRetractStream
    //toAppendStream → 将计算后的数据append到结果DataStream中去
    //toRetractStream  → 将计算后的新的数据在DataStream原数据的基础上更新true或是删除false
    //类似StructuredStreaming中的append/update/complete

    //TODO 3.sink
    resultDS.print

    //TODO 4.execute
    env.execute
  }

}

case class Order(
                     user: Long,
                     product: String,
                     amount: Int
                   )
