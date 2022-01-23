package com.yw.p05_case

import scala.util.Random

/**
  * 匹配字符串
  */
object Case01MatchString extends App {
  val arr = Array("hadoop", "zookeeper", "spark", "storm")

  val name = arr(Random.nextInt(arr.length))
  println(name)

  name match {
    case "hadoop" => println("大数据分布式存储和计算框架")
    case "zookeeper" => println("大数据分布式协调服务框架")
    case "spark" => println("大数据分布式内存计算框架")
    case _ => println("啥也不是")
  }
}
