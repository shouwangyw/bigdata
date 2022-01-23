package com.yw.p05_case

/**
  * 偏函数
  */
object Case09PartialFunction {
  // func1是一个输入参数为Int类型，返回值为String类型的偏函数
  val func1: PartialFunction[Int, String] = {
    case 1 => "一"
    case 2 => "二"
    case 3 => "三"
    case _ => "其它"
  }

  def main(args: Array[String]): Unit = {
    println(func1(1))
    val list = List(1, 2, 3, 4, 5, 6)
    // 使用偏函数操作
    val result = list.filter {
      case x if x > 3 => true
      case _ => false
    }
    println(result)
  }
}
