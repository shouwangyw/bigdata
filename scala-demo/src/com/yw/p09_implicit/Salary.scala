package com.yw.p09_implicit

/**
  * 员工领薪水
  */
object Company {
  // 在object中定义隐式值 注意：同一类型的隐式值只允许出现一次，否则会报错
  implicit val x = "zhangsan"
  implicit val y = 10000.00
}

class Boss {
  // 定义一个用implicit修饰的参数 类型为String
  // 注意参数匹配的类型，它需要的是String类型的隐式值
  def callName(implicit name: String): String = {
    name + " is coming!"
  }

  // 定义一个用implicit修饰的参数，类型为Double
  // 注意参数匹配的类型，它需要的是Double类型的隐式值
  def getMoney(implicit money: Double): String = {
    "当前薪水: " + money
  }
}

object Salary extends App {
  // 使用import导入定义好的隐式值，注意：必须先加载否则会报错
  import Company.{x, y}

  val boss = new Boss
  println(boss.callName + boss.getMoney)
}
