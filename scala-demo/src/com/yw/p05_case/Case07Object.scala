package com.yw.p05_case

import scala.util.Random

/**
  * 样例对象
  */
case class SendMessage(text: String)

//  消息如果没有任何参数，就可以定义为样例对象
case object StartTask

case object PauseTask

case object StopTask

case class SubmitTask(id: String, name: String)

case class HeartBeat(time: Long)

case object CheckTimeOutTask

object Case07Object extends App {
  val arr = Array(CheckTimeOutTask, HeartBeat(10000), SubmitTask("0001", "task-0001"))

  arr(Random.nextInt(arr.length)) match {
    case SubmitTask(id, name) => println(s"id = $id, name = $name")
    case HeartBeat(time) => println(s"time = $time")
    case CheckTimeOutTask => println("检查超时")
  }
}