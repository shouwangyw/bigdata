package com.yw.p03_extends

/**
  * 访问修饰符：private
  */
class Person6 {
  private[this] var name = "super"

  def getName = this.name // 正确

  def sayHello(p: Person6): Unit = {
//    println("Hello " + p.name) // 报错，无法访问
  }
}
object Main6 {
//  def showName(p: Person6) = println(p.name) // 报错，无法访问
}
