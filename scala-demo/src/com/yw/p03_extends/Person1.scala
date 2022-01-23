package com.yw.p03_extends

/**
  * 实现简单继承
  */
class Person1 {
  var name = "super"

  def getName = this.name
}

class Student1 extends Person1

object Main1 {
  def main(args: Array[String]): Unit = {
    val p1 = new Person1()
    val p2 = new Student1()

    p2.name = "张三"

    println(p1.getName)
    println(p2.getName)
  }
}
