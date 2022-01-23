package com.yw.p04_trait

/**
  * 定义具体方法和抽象方法
  */
trait Logger3 {
  // 抽象方法
  def log(msg: String)

  // 具体方法（该方法依赖于抽象方法log)
  def info(msg: String) = log("INFO: " + msg)

  def warn(msg: String) = log("WARN: " + msg)

  def error(msg: String) = log("ERROR: " + msg)
}

class ConsoleLogger3 extends Logger3 {
  override def log(msg: String): Unit = println(msg)
}

object LoggerTrait3 {
  def main(args: Array[String]): Unit = {
    val logger = new ConsoleLogger3

    logger.log("这是一条日志信息")
    logger.info("这是一条普通信息")
    logger.warn("这是一条警告信息")
    logger.error("这是一条错误信息")
  }
}
