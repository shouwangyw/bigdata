package com.yw.p09_implicit

/**
  * 一个类隐式转换成具有相同方法的多个类
  */
class C

class A(c: C) {
  def readBook(): Unit = {
    println("A: 好诗好诗..")
  }
}

class B(c: C) {
  def readBook(): Unit = {
    println("B: 看不懂")
  }

  def writeBook(): Unit = {
    println("B: 不会写..")
  }
}

object AB {
  // 创建一个类转换为2个类的隐式转换
  implicit def C2A(c: C) = new A(c)

  implicit def C2B(c: C) = new B(c)
}

object Multi {
  def main(args: Array[String]): Unit = {
    // 导包
    // 1. import AB._ 会将AB类下的所有隐式转换导进来
    // 2. import AB.C2A 只导入C类到A类的的隐式转换方法
    // 3. import AB.C2B 只导入C类到B类的的隐式转换方法
    import AB._

    val c = new C
    // 由于A类与B类中都有readBook()，只能导入其中一个，否则调用共同方法时代码报错
    // c.readBook();

    // C类可以执行B类中的writeBook()
    c.writeBook()
  }
}
