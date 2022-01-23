package com.yw.p05_case

/**
  * 样例类
  */
// 定义一个样例类
case class CasePerson(name: String, age: Int)
// 使用var指定成员变量是可变的
case class CaseStudent(var name: String, var age: Int)

object Case06Class {
  def main(args: Array[String]): Unit = {
    // 1. 使用new创建实例
    val zhangsan = new CasePerson("张三", 23)
    println(zhangsan)

    // 2. 使用类名直接创建实例
    val lisi = CasePerson("李四", 34)
    println(lisi)

    // 3. 样例类默认的成员变量都是val的，除非手动指定变量为var类型
//    lisi.age = 22 // 编译错误！age默认为val类型

    val xiaoming = CaseStudent("小明", 13)
    xiaoming.age = 24
    println(xiaoming)
  }
}
