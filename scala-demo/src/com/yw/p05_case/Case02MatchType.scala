package com.yw.p05_case

import scala.util.Random

/**
  * 匹配类型
  */
object Case02MatchType extends App {
  val arr = Array("hello", 1, -2.0, Case02MatchType)

  val value = arr(Random.nextInt(arr.length))
  println(value)

  value match {
    case x: Int => println("Int => " + x)
    case y: Double if (y >= 0) => println("Double => " + y)
    case z: String => println("String => " + z)
    case _ => throw new Exception("not match exception")
  }
}
