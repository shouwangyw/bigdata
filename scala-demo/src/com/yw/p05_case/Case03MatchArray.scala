package com.yw.p05_case

/**
  * 匹配数组
  */
object Case03MatchArray extends App {
  val arr = Array(1, 3, 5)

  arr match {
    case Array(1, x, y) => println(x + "---" + y)
    case Array(1, _*) => println("1...")
    case Array(0) => println("only 0")
    case _ => println("something else")
  }
}
