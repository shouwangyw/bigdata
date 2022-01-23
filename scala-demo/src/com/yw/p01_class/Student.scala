package com.yw.p01_class

/**
 * 类的构造器
 */
class Student(val name: String, val age: Int) {
  val address: String = "shenzhen"
  // 定义一个参数的辅助构造器
  def this(name: String) {
    // 辅助构造器的第一行必须调用主构造器或其他辅助构造器或者super父类的构造器
    this(name, 20)
  }
  def this(age: Int) {
    this("xxx", age)
  }
}
