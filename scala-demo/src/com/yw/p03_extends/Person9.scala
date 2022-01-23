package com.yw.p03_extends

/**
  * 抽象类
  */
abstract class Person9(val name: String) {
  // 抽象方法
  def sayHello: String

  def sayBye: String

  // 抽象字段
  def address: String
}

class Student9(name: String) extends Person9(name) {
  // 重写抽象方法或字段，def前不必加override关键字
  def sayHello: String = "Hello, " + name

  def sayBye: String = "Bye, " + name

  // 重写抽象字段
  override def address: String = "shenzhen"
}

object Main9 {
  def main(args: Array[String]): Unit = {
    val s = new Student9("Tom")
    println(s.sayHello)
    println(s.sayBye)
    println(s.address)
  }
}