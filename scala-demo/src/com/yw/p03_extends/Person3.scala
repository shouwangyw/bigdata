package com.yw.p03_extends

/**
  * override 和 super
  */
class Person3 {
  val name = "super"

  def getName = name
}

class Student3 extends Person3 {
  // 重写 val 字段
  override val name: String = "child"

  // 重写 getName 方法
  override def getName: String = "hello, " + super.getName
}

object Main3 {
  def main(args: Array[String]): Unit = {
    println(new Student3().getName)
  }
}
