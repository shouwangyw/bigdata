package com.yw.p05_case

/**
  * 匹配集合
  */
object Case04MatchList extends App {
  val list = List(0, 3, 6)
  list match {
    case 0 :: Nil => println("only 0")
    case 0 :: tail => println("0....")
    case x :: y :: z :: Nil => println(s"x:$x y:$y z:$z")
    case _ => println("something else")
  }
}
