package com.yw.p03_extends

/**
  * getClass 和 classOf
  */
class Person5

class Student5 extends Person5

object Main5 {
  def main(args: Array[String]): Unit = {
    val p: Person5 = new Student5
    println(p.isInstanceOf[Person5]) // true
    println(p.getClass == classOf[Person5]) // false
    println(p.getClass == classOf[Student5]) // true
  }
}
