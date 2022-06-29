package com.yw.musichw.util

import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * 日期相关工具
  */
object DateUtils {
  /**
    * 将字符串格式化成"yyyy-MM-dd HH:mm:ss"格式
    */
  def formatDate(stringDate: String): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var formatDate = ""
    try {
      formatDate = sdf.format(sdf.parse(stringDate))
    } catch {
      case e: Exception => {
        try {
          val bigDecimal = new BigDecimal(stringDate)
          val date = new Date(bigDecimal.longValue())
          formatDate = sdf.format(date)
        } catch {
          case e: Exception => {
            formatDate
          }
        }
      }
    }
    formatDate
  }

  /**
    * 获取输入日期的前几天的日期
    *
    * @param currentDate 当前时间 yyyyMMdd
    * @param i           获取前几天的日期
    * @return yyyyMMdd
    */
  def getCurrentDatePreDate(currentDate: String, i: Int) = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date: Date = sdf.parse(currentDate)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -i)
    val preDate = calendar.getTime
    sdf.format(preDate)
  }
}
