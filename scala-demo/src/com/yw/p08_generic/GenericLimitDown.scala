package com.yw.p08_generic

/**
  * 上下界: 下界
  */
class Person(var name: String, val age: Int)

class PoliceMan(name: String, age: Int) extends Person(name, age)

class SuperMan(name: String) extends PoliceMan(name, -1)

class Pair2[T <: Person, S >: PoliceMan <: Person](val first: T, val second: S) {
  def chat(msg: String) = println(s"${first.name}对${second.name}说: $msg")
}

object GenericLimitUp {
  def main(args: Array[String]): Unit = {
//    // 编译错误：第二个类型参数必须是Person的子类（包括本身）、Policeman的父类（包括本身）
//    val p = new Pair2[Person, SuperMan](new Person("张三", 33), new SuperMan("李四"))
//    p.chat("你好")
  }
}
