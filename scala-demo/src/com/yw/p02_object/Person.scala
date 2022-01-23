package com.yw.p02_object

/**
  * object 的 apply 方法
  */
class Person(var name: String, var age: Int) {
  override def toString: String = s"Person($name, $age)"
}

object Person {
  // 实现apply方法，返回的是伴生类的对象
  def apply(name: String, age: Int): Person = new Person(name, age)

  // apply方法支持重载
  def apply(name: String): Person = new Person(name, 20)

  def apply(age: Int): Person = new Person("xx", age)

  def apply(): Person = new Person("xxx", 30)
}
object Main {
  def main(args: Array[String]): Unit = {
    val p1 = Person("张三", 23)
    val p2 = Person("李四")
    val p3 = Person(100)
    val p4 = Person()

    println(p1)
    println(p2)
    println(p3)
    println(p4)
  }
}
