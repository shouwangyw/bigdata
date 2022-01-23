package com.yw.p03_extends

/**
  * 单例对象实现继承
  */
class Person2 {
  var name = "super"

  def getName = this.name
}

object Student2 extends Person2

object Main2 {
  def main(args: Array[String]): Unit = {
    println(Student2.getName)
  }
}
