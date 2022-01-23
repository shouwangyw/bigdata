package com.yw.p01_class

import java.util.Date

/**
 * 类的定义
 */
class Customer {
  // _表示使用默认值进行初始化
  // String类型默认值是null，Int类型默认值是0，Boolean类型默认值是false
  var name: String = _
  var sex: String = _
  // val变量不能使用_来进行初始化，因为val是不可变的，所以必须手动指定一个默认值
  val registerDate: Date = new Date

  def sayHi(msg: String) = {
    println(msg)
  }
}
object Main {
  // main方法必须要放在一个scala的object（单例对象）中才能执行
  def main(args: Array[String]): Unit = {
    val customer = new Customer
    customer.name = "张三"
    customer.sex = "男"

    println(s"姓名: ${customer.name}, 性别: ${customer.sex}, 注册时间: ${customer.registerDate}")
    // 对象调用方法
    customer.sayHi("你好!")
  }
}
