package com.jsy.test

import scala.util.control.Breaks.breakable

/**
 * @Author: jsy
 * @Date: 2021/7/10 21:33 
 */
object Break {

  def main(args: Array[String]): Unit = {
    val times = null
    breakable(
      if (times==null) Break
    )
    print("sss")
  }
}
