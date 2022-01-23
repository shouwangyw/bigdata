package com.yw.p02_object

/**
 * scala 中的 伴生对象
 */
class Dog {
  val id = 1
  private var name = "Tom"
  def printName(): Unit = {
    println(Dog.CONSTANT + name)
  }
}
object Dog {
  // 伴生对象中的私有属性
  private val CONSTANT = "汪汪汪 : "

  def main(args: Array[String]): Unit = {
    val dog = new Dog
    // 访问私有的字段name
    dog.name = "123"
    dog.printName()
  }
}
