package com.yw.iceberg.example

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author yangwei
  */
object SparkIcebergDML {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      // 指定hadoop catalog，catalog名称为hadoop_prod
      .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://node01:8020/spark_iceberg")
      .getOrCreate()

    // 1.准备数据，使用DataFrame Api 写入Iceberg表及分区表
    val nameJsonList = List[String](
      "{\"id\":1,\"name\":\"zs\",\"age\":18,\"loc\":\"beijing\"}",
      "{\"id\":2,\"name\":\"ls\",\"age\":19,\"loc\":\"shanghai\"}",
      "{\"id\":3,\"name\":\"ww\",\"age\":20,\"loc\":\"beijing\"}",
      "{\"id\":4,\"name\":\"ml\",\"age\":21,\"loc\":\"shanghai\"}")

    import spark.implicits._
    val df: DataFrame = spark.read.json(nameJsonList.toDS)

    // 2. 创建普通表df_tbl1，并将数据写入到Iceberg表，其中DF中的列就是Iceberg表中的列
    df.writeTo("hadoop_prod.default.df_tbl1").create()

    // 3. 查询表 hadoop_prod.default.df_tbl1 中的数据，并查看数据存储结构
    spark.read.table("hadoop_prod.default.df_tbl1").show()
    /*
    +---+---+--------+----+
    |age| id|     loc|name|
    +---+---+--------+----+
    | 18|  1| beijing|  zs|
    | 19|  2|shanghai|  ls|
    | 20|  3| beijing|  ww|
    | 21|  4|shanghai|  ml|
    +---+---+--------+----+
     */

    // 4. 创建分区表 df_tbl2, 并将数据写入到Iceberg表，其中DF中的列就是Iceberg表中的列
    df.sortWithinPartitions($"loc") //写入分区表，必须按照分区列进行排序
      .writeTo("hadoop_prod.default.df_tbl2")
      .partitionedBy($"loc") //这里可以指定多个列为联合分区
      .create()

    // 5.查询分区表 hadoop_prod.default.df_tbl2 中的数据，并查看数据存储结构
    spark.read.table("hadoop_prod.default.df_tbl2").show()
    /*
    +---+---+--------+----+
    |age| id|     loc|name|
    +---+---+--------+----+
    | 18|  1| beijing|  zs|
    | 19|  2|shanghai|  ls|
    | 20|  3| beijing|  ww|
    | 21|  4|shanghai|  ml|
    +---+---+--------+----+
     */

    spark.close()
  }
}
