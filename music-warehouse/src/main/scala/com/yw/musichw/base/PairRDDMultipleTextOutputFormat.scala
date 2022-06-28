package com.yw.musichw.base

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
  * @author yangwei
  */
class PairRDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  // 1. 文件名：根据key和value自定义输出文件名。name：对应的part-0001文件名
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    val fileName = key.asInstanceOf[String]
    fileName
  }
  // 2. 文件内容：默认同时输出key和value，这里指定不输出key。
  override def generateActualKey(key: Any, value: Any): String = {
    null
  }
}
