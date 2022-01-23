package com.yw.p07_extractor

/**
  * 提取器
  */
object ExtractorTest {
  def main(args: Array[String]): Unit = {
    val zhangsan = Student("张三", 23)
    zhangsan match {
      case Student(name, age) => println(s"姓名: $name 年龄: $age")
      case _ => println("未匹配")
    }
  }
}

class Student {
  var name: String = _
  var age: Int = _

  // 实现一个辅助构造器
  def this(name: String, age: Int) = {
    this()
    this.name = name
    this.age = age
  }
}

object Student {
  def apply(name: String, age: Int): Student = new Student(name, age)

  // 实现一个解构器
  def unapply(arg: Student): Option[(String, Int)] = Some(arg.name, arg.age)
}