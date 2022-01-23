package com.yw.p08_generic

/**
  * 上下界：上界
  */
class Person(var name: String, val age: Int)

// 类名后面的指定泛型的范围 ---- 上界
class Pair1[T <: Person, S <: Person](val first: T, val second: S) {
  def chat(msg: String) = println(s"${first.name}对${second.name}说: $msg")
}

object GenericLimitUp {
  def main(args: Array[String]): Unit = {
    val p = new Pair1[Person, Person](new Person("张三", 33), new Person("李四", 24))
    p.chat("你好")
  }
}
