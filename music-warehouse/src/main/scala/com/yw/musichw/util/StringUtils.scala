package com.yw.musichw.util

/**
  * @author yangwei
  */
object StringUtils {
  def checkString(str: String) = {
    if (str == null || "".equals(str)) "" else str
  }
}
