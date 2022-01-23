package com.yw.p05_case

/**
  * Option 类型
  */
object Case08Option {
  def main(args: Array[String]): Unit = {
    val map = Map("a" -> 1, "b" -> 2)
    val value: Option[Int] = map.get("b")
    val v1 = value match {
      case Some(i) => i
      case None => 0
    }
    println(v1)
    // 更好的方式
    val v2 = map.getOrElse("c", 0)
    println(v2)
  }
}
