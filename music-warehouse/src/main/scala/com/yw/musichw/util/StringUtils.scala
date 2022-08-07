package com.yw.musichw.util

/**
  * @author yangwei
  */
object StringUtils {
  def checkString(str: String) = {
    if (str == null || "".equals(str)) "" else str
  }

  def containsAny(str: String, arr: Array[String]): Boolean = {
    arr.foreach(s => if (str.contains(s)) true)
    false
  }
}
