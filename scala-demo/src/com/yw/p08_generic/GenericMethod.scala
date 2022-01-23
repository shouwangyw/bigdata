package com.yw.p08_generic

/**
  * 泛型方法
  */
object GenericMethod {
  // 不考虑泛型的支持
  def getMiddle(arr: Array[Int]) = arr(arr.length / 2)

//  def main(args: Array[String]): Unit = {
//    val arr1 = Array(1, 2, 3, 4, 5)
//    println(getMiddle(arr1))
//  }

  // 考虑泛型的支持
  def getMiddle[A](arr: Array[A]) = arr(arr.length / 2)

  def main(args: Array[String]): Unit = {
    val arr1 = Array(1, 2, 3, 4, 5)
    val arr2 = Array("a", "b", "c", "d", "e", "f")

    println(getMiddle[Int](arr1))
    println(getMiddle[String](arr2))

    // 简写
    println(getMiddle(arr1))
    println(getMiddle(arr2))
  }
}
