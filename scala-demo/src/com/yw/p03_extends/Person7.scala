package com.yw.p03_extends

/**
  * 访问修饰符：protected
  */
class Person7 {
  protected[this] var name = "super"

  def getName = {
    this.name // 正确
  }

  def sayHello(p: Person7): Unit = {
//    println(p.name) // 编译错误
  }
}

object Person7 {
  def sayHello(p: Person7) = {
//    println(p.name) // 编译错误
  }
}

class Student7 extends Person7 {
  def showName = {
    println(name) // 正确
  }

//  def sayHello(p: Person7): Unit = {
//    println(p.name) // 编译错误
//  }
}
