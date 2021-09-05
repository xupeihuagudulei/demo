package com.jsy.join

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Author: jsy
 * @Date: 2021/8/31 21:18
 */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val random = scala.util.Random
    val col1 = Range(1, 50).map(idx => (random.nextInt(10), s"user$idx"))
    val col2 = Array((0, "BJ"), (1, "SH"), (2, "GZ"), (3, "SZ"), (4, "TJ"), (5, "CQ"), (6, "HZ"), (7, "NJ"), (8, "WH"), (0, "CD"))
    val rdd1: RDD[(Int, String)] = sc.makeRDD(col1)
    val rdd2: RDD[(Int, String)] = sc.makeRDD(col2)
    val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
    println(rdd3.dependencies)
    val rdd4: RDD[(Int, (String, String))] = rdd1.partitionBy(new HashPartitioner(3)).join(rdd2.partitionBy(new HashPartitioner(3)))
    println(rdd4.dependencies)
    sc.stop()
  }
}