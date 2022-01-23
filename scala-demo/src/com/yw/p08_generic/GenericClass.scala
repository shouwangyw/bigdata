package com.yw.p08_generic

/**
  * 泛型类
  */
// 类名后面的方括号，就表示这个类可以使用两个类型、分别是T和S
class Pair[T, S](val first: T, val second: S)

case class People(var name: String, val age: Int)

class GenericClass {
  def main(args: Array[String]): Unit = {
    val p1 = new Pair[String, Int]("张三", 23)
    val p2 = new Pair[String, String]("李四", "1984-01-12")
    val p3 = new Pair[People, People](People("张三", 20), People("李四", 14))
  }
}
