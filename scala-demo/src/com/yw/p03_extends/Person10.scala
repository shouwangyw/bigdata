package com.yw.p03_extends

/**
  * 匿名内部类
  */
abstract class Person10 {
  def sayHello: Unit
}

object Main10 {
  def main(args: Array[String]): Unit = {
    // 直接用new来创建一个匿名内部类对象
    val p1 = new Person10 {
      override def sayHello: Unit = println("这是一个匿名内部类")
    }
    p1.sayHello
  }
}
