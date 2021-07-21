package com.jsy.extra.udf

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/**
 * @Author: jsy
 * @Date: 2021/7/6 23:46 
 */
//class FlatMapFunction extends TableFunction[(String, String, String)] {
class FlatMapFunction extends TableFunction[Row] {

  def eval(str: String): Unit = {

    val array: JSONArray = JSON.parseArray(str)

    for (i <- 0 until array.size) {

      val jSONObject = array.getJSONObject(i)
      collect(Row.of(jSONObject.get("address").toString, jSONObject.get("city").toString))
    }

  }

  //def main(args: Array[String]): Unit = {
  //  eval("[{\"address\":\"北京市海淀区\",\"city\":\"beijing\"},{\"address\":\"北京市海淀区\",\"city\":\"beijing\"},{\"address\":\"北京市海淀区\",\"city\":\"beijing\"}]")
  //}
}
