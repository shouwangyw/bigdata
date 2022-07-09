package com.yw.spark.example.partition

import org.apache.spark.Partitioner

/**
  *
  * @author yangwei
  */
class MyPartitioner(num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    System.identityHashCode(key) % num.abs
  }
}
