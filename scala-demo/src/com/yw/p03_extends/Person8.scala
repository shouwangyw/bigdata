package com.yw.p03_extends

/**
  * 调用父类的constructor
  */
class Person8(var name: String) {
  println("name: " + name)
}

// 直接在子类的类名后面调用父类构造器
class Student8(name: String, var clazz: String) extends Person8(name)

object Main8 {
  def main(args: Array[String]): Unit = {
    val s1 = new Student8("zhangsan", "32")
    println(s"${s1.name} - ${s1.clazz}")
  }
}
