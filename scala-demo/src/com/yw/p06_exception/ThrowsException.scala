package com.yw.p06_exception

/**
  * 抛出异常
  */
object ThrowsException {
  def main(args: Array[String]): Unit = {
    throw new Exception("这是一个异常")
  }
}
