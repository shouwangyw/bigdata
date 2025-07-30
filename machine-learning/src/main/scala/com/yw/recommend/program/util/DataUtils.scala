package com.yw.recommend.program.util

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

/**
  * 日期工具类
  */
object DataUtils {

  //将字符串变成Date类型
  def getData(dateStr: String) = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val format = new SimpleDateFormat(pattern)
    try {
      format.parse(dateStr)
    } catch {
      case exception: ParseException => println(exception.getMessage)
    }
    format.parse("1970-01-01 00:00:00")
  }

  //获取两个日期之间的差值
  def getDayDiff(dateStr: String) = {
    val startDate = getData(dateStr)
    val endDate = new Date()
    val between = endDate.getTime - startDate.getTime
    val day = between / 1000 / 3600 / 24
    day
  }

  //获取两个日期之间的最大值
  def getMaxDate(dataStrA: String, dataStrB: String) = {
    val dateA = getData(dataStrA)
    val dateB = getData(dataStrB)
    if(dateA.getTime > dateB.getTime) dataStrA
    else dataStrB
  }
}
