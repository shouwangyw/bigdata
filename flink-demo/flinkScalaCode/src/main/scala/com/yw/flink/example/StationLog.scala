package com.yw.flink.example

/**
  * 基站日志数据
  * sid ： 基站id
  * callOut :主叫
  * callIn : 被叫
  * callType ： 通话类型
  * callTime : 呼叫时间，时间戳
  * duration : 通话时长
  */
case class StationLog(sid: String, callOut: String, callIn: String, var callType: String, callTime: Long, duration: Long)
