package com.yw.p09_implicit

/**
  * 超人变身
  */
class Man(val name: String)

class SuperMan(val name: String) {
  def heat = print(s"${name}超人打怪兽")
}

object SuperMan {
  // 隐式转换方法
  implicit def man2SuperMan(man: Man) = new SuperMan(man.name)

  def main(args: Array[String]): Unit = {
    val hero = new Man("Tom")
    // Man具备了SuperMan的方法
    hero.heat
  }
}
