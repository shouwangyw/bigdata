package com.yw.p05_case

/**
  * 匹配元组
  */
object Case05MatchTuple extends App {
  val tuple = (1, 3, 5)
  tuple match {
    case (1, x, y) => println(s"1,$x,$y")
    case (2, x, y) => println(s"$x,$y")
    case _ => println("others")
  }
}
