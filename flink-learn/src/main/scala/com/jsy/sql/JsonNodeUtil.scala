//package com.jsy.sql
//
//import java.util
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.typeutils.RowTypeInfo
//import org.apache.flink.api.scala.typeutils.Types
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
//import org.apache.flink.types.Row
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable
//
///**
// *
// * 给定JSON串，和目标schema描述，生成对于的 Row
// * https://www.jianshu.com/p/848a399d977e
// */
//
//object JsonNodeUtil {
//
//  def main(args: Array[String]): Unit = {
//
//    //    objects_test()
//
//    //    oneList_test()
//
//    twoList_test()
//
//    oneList_no_flatMap_test()
//
//    //    twoList_no_flatmap_test()
//
//  }
//
//  val objects =
//
//    """
//
//{"a":"1","b":{"c":"2","d":"3"},"e":{"f":"4","g":"5"}}
//
//    """.stripMargin
//
//      .getBytes()
//
//  val oneList =
//
//    """
//
//{"a":"1","b":[{"c":"2","d":"3"},{"c":"4","d":"5"}],"e":"6","f":{"g":"7","h":"8"}}
//
//    """.stripMargin
//
//      .getBytes()
//
//  val twoList =
//
//    """
//
//{"a":"1","b":[{"c":"2","d":"3"},{"c":"4","d":"5"}],"e":"6","f":[{"g":"7","h":"8"},{"g":"9","h":"10"}],"i":{"j":"11","k":"12"}}
//
//    """.stripMargin
//
//      .getBytes()
//
//  /**
//   *
//   * 嵌套对象打平 和嵌套list一个不打平一个打平
//   *
//   */
//
//  def twoList_no_flatmap_test() = {
//
//    val objectMapper = new ObjectMapper()
//
//    val json = twoList
//
//    val map = new util.LinkedHashMap[String, String]()
//
//    map.put("a", TableSchemaUtil.STRING)
//
//    map.put("b", TableSchemaUtil.OBJECT_ARRAY)
//
//    map.put("b_c", TableSchemaUtil.STRING)
//
//    map.put("b_d", TableSchemaUtil.STRING)
//
//    map.put("e", TableSchemaUtil.STRING)
//
//    map.put("f", TableSchemaUtil.OBJECT_ARRAY)
//
//    map.put("f_g", TableSchemaUtil.STRING)
//
//    map.put("f_h", TableSchemaUtil.STRING)
//
//    map.put("i_j", TableSchemaUtil.STRING)
//
//    map.put("i_k", TableSchemaUtil.STRING)
//
//    map.put("tableName", TableSchemaUtil.STRING)
//
//    val rows: util.ArrayList[Row] = getRows(objectMapper, json, map)
//
//    rows
//
//  }
//
//  /**
//   *
//   * 嵌套对象打平 和嵌套list不打平
//   *
//   */
//  def oneList_no_flatMap_test() = {
//
//    val objectMapper = new ObjectMapper
//
//    val json = oneList
//
//    val map = new util.LinkedHashMap[String, String]()
//
//    map.put("a", TableSchemaUtil.STRING)
//
//    map.put("b", TableSchemaUtil.OBJECT_ARRAY)
//
//    map.put("b_c", TableSchemaUtil.STRING)
//
//    map.put("b_d", TableSchemaUtil.STRING)
//
//    map.put("e", TableSchemaUtil.STRING)
//
//    map.put("f_g", TableSchemaUtil.STRING)
//
//    map.put("f_h", TableSchemaUtil.STRING)
//
//    val rows: util.ArrayList[Row] = getRows(objectMapper, json, map)
//
//    rows
//
//  }
//
//  /**
//   *
//   * 嵌套对象 和嵌套多个list  打平
//   *
//   * 多个数组的一定要指定 一个 tableName 列
//   *
//   * map.put("tableName", TableSchemaUtil.STRING)
//   *
//   * 后续根据这个tableName 进行查询分表
//   *
//   * @return
//   *
//   */
//
//  def twoList_test() = {
//
//    val objectMapper = new ObjectMapper
//
//    val json = twoList
//
//    val map = new util.LinkedHashMap[String, String]()
//
//    map.put("a", TableSchemaUtil.STRING)
//
//    map.put("b_c", TableSchemaUtil.STRING)
//
//    map.put("b_d", TableSchemaUtil.STRING)
//
//    map.put("e", TableSchemaUtil.STRING)
//
//    map.put("f_g", TableSchemaUtil.STRING)
//
//    map.put("f_h", TableSchemaUtil.STRING)
//
//    map.put("i_j", TableSchemaUtil.STRING)
//
//    map.put("i_k", TableSchemaUtil.STRING)
//
//    map.put("tableName", TableSchemaUtil.STRING)
//
//    val rows: util.ArrayList[Row] = getRows(objectMapper, json, map)
//
//    rows
//
//  }
//
//  /**
//   *
//   * 嵌套对象 和嵌套list  打平
//   *
//   * @return
//   *
//   */
//
//  def oneList_test() = {
//
//    val objectMapper = new ObjectMapper
//
//    val json = oneList
//
//    val map = new util.LinkedHashMap[String, String]()
//
//    map.put("a", TableSchemaUtil.STRING)
//
//    map.put("b_c", TableSchemaUtil.STRING)
//
//    map.put("b_d", TableSchemaUtil.STRING)
//
//    map.put("e", TableSchemaUtil.STRING)
//
//    map.put("f_g", TableSchemaUtil.STRING)
//
//    map.put("f_h", TableSchemaUtil.STRING)
//
//    val rows: util.ArrayList[Row] = getRows(objectMapper, json, map)
//
//    rows
//
//  }
//
//  /**
//   *
//   * 嵌套对象打平
//   *
//   * @return
//   *
//   */
//
//  def objects_test() = {
//
//    val objectMapper = new ObjectMapper
//
//    val json = objects
//
//    val map = new util.LinkedHashMap[String, String]()
//
//    map.put("a", TableSchemaUtil.STRING)
//
//    map.put("b_c", TableSchemaUtil.STRING)
//
//    map.put("b_d", TableSchemaUtil.STRING)
//
//    map.put("e_f", TableSchemaUtil.STRING)
//
//    map.put("e_g", TableSchemaUtil.STRING)
//
//    val rows: util.ArrayList[Row] = getRows(objectMapper, json, map)
//
//    rows
//
//  }
//
//  def getRows(objectMapper: ObjectMapper, json: Array[Byte], map: util.LinkedHashMap[String, String]) = {
//
//    val objectNode: ObjectNode = objectMapper.readValue(json, classOf[ObjectNode])
//
//    //最外层的数据
//
//    val rootColums = new ObjectNode(JsonNodeFactory.instance)
//
//    //嵌套的数组
//
//    val tables = mutable.LinkedHashMap[String, ArrayNode]()
//
//    val list = new util.ArrayList[Row]()
//
//    //解析json
//
//    parse(objectNode, rootColums, tables, "")
//
//    val saclaLinkMap = mutable.LinkedHashMap[String, String]()
//
//    map.asScala.foreach { case (k: String, v: String) => saclaLinkMap += (k -> v) }
//
//    //对json转化成 row的时候选择打平跳过
//
//    val feidMapScala = TableSchemaUtil.toFlinkType(saclaLinkMap)
//
//    val types: Array[TypeInformation[_]] = feidMapScala.map(_._2).toArray
//
//    val fieldNames: Array[String] = feidMapScala.map(_._1).toArray
//
//    val info = new RowTypeInfo(types, fieldNames)
//
//    val tableSize = tables.size
//
//    if (tableSize == 0 && rootColums.size() > 0) {
//
//      val row = JsonToRowUtil.convertRow(rootColums, info)
//
//      list.add(row)
//
//    }
//
//    //循环每个嵌套的数组 每个数组理解为一个表
//
//    for ((table, value) <- tables) { //循环每个表
//
//      //判断表是否需要打平
//
//      if (map.containsKey(table) && map.get(table).startsWith(TableSchemaUtil.OBJECT_ARRAY)) {
//
//        //不需要打平就把list数据弄成一个row 数组
//
//        val tableLine = rootColums.deepCopy() //每一行初始化的ObjectNode
//
//        if (tableSize > 1) { //如果有多个table 就要加一列table 名字做区分
//
//          tableLine.put("tableName", table) //把每行数据都加一个table名字
//
//        }
//
//        tableLine.put(table, value)
//
//        val row = JsonToRowUtil.convertRow(tableLine, info)
//
//        list.add(row)
//
//      } else {
//
//        val tableLines = value.elements() //表中的所有行
//
//        while (tableLines.hasNext) { //循环每一行
//
//          val line = tableLines.next() //获取每一行
//
//          val child = line.fields() //每一行的所有列
//
//          val tableLine = rootColums.deepCopy() //每一行初始化的ObjectNode
//
//          if (tableSize > 1) { //如果有多个table 就要加一列table 名字做区分
//
//            tableLine.put("tableName", table) //把每行数据都加一个table名字
//
//          }
//
//          while (child.hasNext) { //循环每一列
//
//            val colum = child.next() //获取每一列
//
//            val columName = colum.getKey //列名
//
//            val filedFullName = table + "_" + columName //组合列名
//
//            val columValue = colum.getValue.asText() //列对应的值
//
//            //          println(filedFullName, columValue)
//
//            val dataValue = colum.getValue.asText()
//
//            tableLine.put(filedFullName, dataValue)
//
//          }
//
//          val row = JsonToRowUtil.convertRow(tableLine, info)
//
//          list.add(row)
//
//        }
//
//      }
//
//    }
//
//    //    returnJson.put("root",json)
//
//    list
//
//  }
//
//  def parse(objectNode: ObjectNode,
//
//            objectSchema: ObjectNode,
//
//            listSchema: mutable.LinkedHashMap[String, ArrayNode],
//
//            parentName: String = ""
//
//           ): Unit = {
//
//    val fieldNames = objectNode.fieldNames()
//
//    //得到第一层
//
//    while (fieldNames.hasNext) {
//
//      val field = fieldNames.next()
//
//      var node = objectNode.get(field)
//
//      val filedFullName = if (parentName.length > 0) {
//
//        parentName + "_" + field
//
//      } else {
//
//        field
//
//      }
//
//      if (node.isObject) {
//
//        parse(objectNode.`with`(field), objectSchema, listSchema, filedFullName)
//
//      } else if (node.isArray) {
//
//        val list: ArrayNode = node.asInstanceOf[ArrayNode]
//
//        if (list.size() > 0) {
//
//          //获取第0个解析一下
//
//          //判断list里面是否还有嵌套，如果没有则直接去
//
//          if (false) {
//
//            //            node = list.get(0)
//
//            //            parse(node,objectSchema,listSchema,filedFullName)
//
//          } else {
//
//            listSchema += (filedFullName -> list)
//
//          }
//
//        } else { //不为空则填充默认值
//
//        }
//
//      } else {
//
//        val dataValue = node.asText()
//
//        //        println(filedFullName,dataValue)
//
//        objectSchema.put(filedFullName, dataValue.toString)
//
//        //        val dataType = schema.get(filedFullName)
//
//      }
//
//    }
//
//  }
//
//}
