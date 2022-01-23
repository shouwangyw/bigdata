package com.yw.p03_extends

/**
  * isInstanceOf 和 asInstanceOf
  */
class Person4

class Student4 extends Person4

object Main4 {
  def main(args: Array[String]): Unit = {
    val s1: Person4 = new Student4

    // 判断 s1 是否为 Student4 类型
    if (s1.isInstanceOf[Student4]) {
      // 将s1转换为Student4类型
      val s2 = s1.asInstanceOf[Student4]
      println(s2)
    }
  }
}
