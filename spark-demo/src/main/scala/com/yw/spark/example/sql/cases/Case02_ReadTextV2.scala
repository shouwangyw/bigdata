package com.yw.spark.example.sql.cases

import com.yw.spark.example.sql.Person
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * @author yangwei
  */
object Case02_ReadTextV2 {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 添加隐式转换
    import spark.implicits._

    val rdd1: RDD[Array[String]] = sc.textFile(this.getClass.getClassLoader.getResource("person.txt").getPath)
      .map(x => x.split(" "))
    // 将 rdd 与样例类进行关联
    val personRDD: RDD[Person] = rdd1.map(x => Person(x(0), x(1), x(2).toInt))

    // 将 rdd 转成 DataFrame
    val personDF = personRDD.toDF

    /**
      * root
      * |-- id: string (nullable = true)
      * |-- name: string (nullable = true)
      * |-- age: integer (nullable = false)
      */
    personDF.printSchema()

    /**
      * +---+-------+---+
      * | id|   name|age|
      * +---+-------+---+
      * |  1| youyou| 38|
      * |  2|   Tony| 25|
      * |  3|laowang| 18|
      * |  4|   dali| 30|
      * +---+-------+---+
      */
    personDF.show()

    /************************** DSL风格语法 start *************************/
    // 1. 查询指定字段
    personDF.select("name").show
    personDF.select($"name").show

    // 2. 实现 age+1
    personDF.select($"name", $"age", $"age" + 1).show

    // 3. 实现 age>30 过滤
    personDF.filter($"age" > 30).show

    // 4. 按照 age 分组统计
    personDF.groupBy("age").count.show

    // 5. 按照age分组统计次数降序
    personDF.groupBy("age").count().sort($"age".desc).show
    /************************** DSL风格语法 end *************************/

    println("-----------------------------------------------")

    /************************** SQL风格语法 start *************************/
    // 1. DataFrame注册成表
    personDF.createTempView("person")

    // 2. 使用SparkSession调用sql方法统计查询
    spark.sql("select * from person").show
    spark.sql("select name from person").show
    spark.sql("select name, age from person").show
    spark.sql("select * from person where age > 30").show
    spark.sql("select count(*) from person where age > 30").show
    spark.sql("select age, count(*) from person group by age").show
    spark.sql("select age, count(*) as count from person group by age").show
    spark.sql("select * from person order by age desc").show
    /************************** SQL风格语法 end *************************/

    spark.stop()
  }
}


