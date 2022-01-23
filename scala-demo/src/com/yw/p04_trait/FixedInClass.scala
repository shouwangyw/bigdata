package com.yw.p04_trait

/**
  * 实例对象混入 trait
  */
trait LoggerMix {
  def log(msg: String) = println(msg)
}

class UserService

object FixedInClass {
  def main(args: Array[String]): Unit = {
    val service = new UserService with LoggerMix

    service.log("你好")
  }
}
