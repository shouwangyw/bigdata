package com.yw.p06_exception

/**
  * 捕获异常
  */
object TryCatchException {
  def main(args: Array[String]): Unit = {
    try {
      var i = 10 / 0
    } catch {
      case ex: Exception => println(ex.getMessage)
    } finally {
      println("始终会执行")
    }
  }
}
