package com.yw.iceberg.example

import org.apache.spark.sql.SparkSession

/**
  * Spark与iceberg整合DDL操作
  *
  * 报错：java.lang.IllegalStateException: Incoming records violate the writer assumption that records are clustered by spec and by partition within each spec. Either cluster the incoming records or switch to fanout writers.
Encountered records that belong to already closed files:
partition 'register_ts_year=2020' in spec [
  1000: register_ts_year: year(4)
]
  * @author yangwei
  */
object Case02_SparkIcebergDDL {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      // 指定hadoop catalog，catalog名称为hadoop_prod
      .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://mycluster/spark_iceberg")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    /******************** case01 ********************/
//    // 1. 创建普通表
//    spark.sql(
//      """
//        | create table if not exists hadoop_prod.default.normal_tbl(id int,name string,age int) using iceberg
//      """.stripMargin)
//
//    // 2. 创建分区表，以 loc 列为分区字段
//    spark.sql(
//      """
//        | create table if not exists hadoop_prod.default.partition_tbl(id int,name string,age int,loc string) using iceberg partitioned by (loc)
//      """.stripMargin)
//
//    // 3. 向分区表中插入数据
//    spark.sql(
//      """
//        | insert into table hadoop_prod.default.partition_tbl values (1,"zs",18,"bj"),(3,"ww",20,"sz"),(2,"ls",19,"sh"),(4,"ml",21,"gz")
//      """.stripMargin)
//
//    // 4. 查询
//    spark.sql("select * from hadoop_prod.default.partition_tbl").show()

    /******************** case02 ********************/
//    // 创建分区表 partition_tbl1，指定分区为year
//    spark.sql(
//      """
//        | create table if not exists hadoop_prod.default.partition_tbl1(id int ,name string,age int,register_ts timestamp) using iceberg
//        | partitioned by (years(register_ts))
//      """.stripMargin)
//
//    // 插入数据
//    spark.sql(
//      """
//        | insert into hadoop_prod.default.partition_tbl1 values
//        | (1,'zs',18,cast(1508469830 as timestamp)),
//        | (2,'ls',19,cast(1604559630 as timestamp)),
//        | (3,'ww',20,cast(1606096230 as timestamp)),
//        | (4,'ml',21,cast(1639920630 as timestamp)),
//        | (5,'tq',22,cast(1648279630 as timestamp)),
//        | (6,'gb',23,cast(1676843830 as timestamp))
//      """.stripMargin)
//
//    // 查询
//    spark.sql("select * from hadoop_prod.default.partition_tbl1").show()

    /******************** case03 ********************/
    // 创建分区表 partition_tbl2，指定分区为months，会按照“年-月”分区
//    spark.sql(
//      """
//        | create table if not exists hadoop_prod.default.partition_tbl2(id int ,name string,age int,register_ts timestamp) using iceberg
//        | partitioned by (months(register_ts))
//      """.stripMargin)
//
//    // 插入数据
//    spark.sql(
//      """
//        | insert into hadoop_prod.default.partition_tbl2 values
//        | (1,'zs',18,cast(1578469830 as timestamp)),
//        | (2,'ls',19,cast(1604559630 as timestamp)),
//        | (3,'ww',20,cast(1623096230 as timestamp)),
//        | (4,'ml',21,cast(1638920630 as timestamp)),
//        | (5,'tq',22,cast(1639279630 as timestamp)),
//        | (6,'gb',23,cast(1676843830 as timestamp))
//      """.stripMargin)
//
//    // 查询
//    spark.sql("select * from hadoop_prod.default.partition_tbl2").show()

    /******************** case04 ********************/
//    // 创建分区表 partition_tbl3，指定分区为days，会按照“年-月-日”分区
//    spark.sql(
//      """
//        | create table if not exists hadoop_prod.default.partition_tbl3(id int ,name string,age int,register_ts timestamp) using iceberg
//        | partitioned by (days(register_ts))
//      """.stripMargin)
//
//    // 插入数据
//    spark.sql(
//      """
//        | insert into hadoop_prod.default.partition_tbl3 values
//        | (1,'zs',18,cast(1578469830 as timestamp)),
//        | (2,'ls',19,cast(1604559630 as timestamp)),
//        | (3,'ww',20,cast(1623096230 as timestamp)),
//        | (4,'ml',21,cast(1638920630 as timestamp)),
//        | (5,'tq',22,cast(1639279630 as timestamp)),
//        | (6,'gb',23,cast(1676843830 as timestamp))
//      """.stripMargin)
//
//    // 查询
//    spark.sql("select * from hadoop_prod.default.partition_tbl3").show()

    /******************** case05 ********************/
//    // 创建分区表 partition_tbl4，指定分区为hours，会按照“年-月-日-时”分区
//    spark.sql(
//      """
//        | create table if not exists hadoop_prod.default.partition_tbl4(id int ,name string,age int,register_ts timestamp) using iceberg
//        | partitioned by (hours(register_ts))
//      """.stripMargin)
//
//    // 插入数据
//    spark.sql(
//      """
//        | insert into hadoop_prod.default.partition_tbl4 values
//        | (1,'zs',18,cast(1578469830 as timestamp)),
//        | (2,'ls',19,cast(1604559630 as timestamp)),
//        | (3,'ww',20,cast(1623096230 as timestamp)),
//        | (4,'ml',21,cast(1638920630 as timestamp)),
//        | (5,'tq',22,cast(1639279630 as timestamp)),
//        | (6,'gb',23,cast(1676843830 as timestamp))
//      """.stripMargin)
//
//    // 查询
//    spark.sql("select * from hadoop_prod.default.partition_tbl4").show()

    /******************** case06 create table ... as select ... ********************/
//    // 创建表
//    spark.sql("create table hadoop_prod.default.my_tb1(id int,name string,age int) using iceberg")
//    // 向表中插入数据
//    spark.sql("insert into table hadoop_prod.default.my_tb1 values (1,'zs',18),(3,'ww',20),(2,'ls',19),(4,'ml',21)")
//    // 查询数据
//    spark.sql("select * from hadoop_prod.default.my_tb1").show()
//    // 查询插入
//    spark.sql("create table hadoop_prod.default.my_tb2 using iceberg as select id,name,age from hadoop_prod.default.my_tb1")
//    // 查询
//    spark.sql("select * from hadoop_prod.default.my_tb2").show()

    /******************** case07 replace table ... as select ... ********************/
//    // 创建表
//    spark.sql("create table hadoop_prod.default.my_tb3(id int,name string,age int) using iceberg")
//    // 向表中插入数据
//    spark.sql("insert into table hadoop_prod.default.my_tb3 values (1,'zs',18),(3,'ww',20),(2,'ls',19),(4,'ml',21)")
//    // 查询数据
//    spark.sql("select * from hadoop_prod.default.my_tb3").show()
//    // 查询插入
//    spark.sql("replace table hadoop_prod.default.my_tb2 using iceberg as select * from hadoop_prod.default.my_tb3")
//    // 查询
//    spark.sql("select * from hadoop_prod.default.my_tb2").show()

    /******************** case08 drop table ********************/
//    spark.sql("drop table if exists hadoop_prod.default.my_tb1")
//    spark.sql("drop table if exists hadoop_prod.default.my_tb2")
//    spark.sql("drop table if exists hadoop_prod.default.my_tb3")
//    spark.sql("drop table if exists hadoop_prod.default.normal_tbl")
//    spark.sql("drop table if exists hadoop_prod.default.partition_tbl")
//    spark.sql("drop table if exists hadoop_prod.default.partition_tbl1")
//    spark.sql("drop table if exists hadoop_prod.default.partition_tbl2")
//    spark.sql("drop table if exists hadoop_prod.default.partition_tbl3")
//    spark.sql("drop table if exists hadoop_prod.default.partition_tbl4")

    /******************** case09 ********************/
//    // 创建表
//    spark.sql("create table hadoop_prod.default.test(id int,name string,age int) using iceberg")
//    // 向表中插入数据
//    spark.sql("insert into table hadoop_prod.default.test values (1,'zs',18),(2,'ls',19),(3,'ww',20)")
//    // 查询
//    spark.sql("select * from hadoop_prod.default.test").show()
//    // 添加字段，给 test 表增加列: gender、loc
//    spark.sql("alter table hadoop_prod.default.test add column gender string, loc string")
//    // 删除字段，给 test 表删除列: age
//    spark.sql("alter table hadoop_prod.default.test drop column age")
//    // 再次查询
//    spark.sql("select * from hadoop_prod.default.test").show()
    /*
    最终表展示的列少了age列，多了gender、loc列
    +---+----+------+----+
    | id|name|gender| loc|
    +---+----+------+----+
    |  1|  zs|  null|null|
    |  2|  ls|  null|null|
    |  3|  ww|  null|null|
    +---+----+------+----+
     */
//    // 重命名列
//    spark.sql("alter table hadoop_prod.default.test rename column gender to xxx")
//    // 再次查询
//    spark.sql("select * from hadoop_prod.default.test").show()
    /*
    最终表展示的列 gender列变成了xxx列
    +---+----+----+----+
    | id|name| xxx| loc|
    +---+----+----+----+
    |  1|  zs|null|null|
    |  2|  ls|null|null|
    |  3|  ww|null|null|
    +---+----+----+----+
     */
    /******************** case10 ********************/
    // 创建普通表
    spark.sql("create table if not exists hadoop_prod.default.my_tab(id int,name string,loc string,ts timestamp) using iceberg")
    // 向表中插入数据，并查询
    spark.sql("insert into hadoop_prod.default.my_tab values (1,'zs','shenzhen',cast(1608469830 as timestamp))")
    spark.sql("select * from hadoop_prod.default.my_tab").show()
    // 将表loc列添加为分区列，并插入数据，再查询
    spark.sql("alter table hadoop_prod.default.my_tab add partition field loc")
    spark.sql("insert into hadoop_prod.default.my_tab values (2,'li','wuhan',cast(1634559630 as timestamp))")
    spark.sql("select * from hadoop_prod.default.my_tab").show()
//    // 将ts列进行转换作为分区列，插入数据并查询
//    spark.sql("alter table hadoop_prod.default.my_tab add partition field years(ts)")
//    spark.sql("insert into hadoop_prod.default.my_tab values (3,'ww','beijing',cast(1576843830 as timestamp))")
//    spark.sql("select * from hadoop_prod.default.my_tab").show()
//    // 删除分区loc，插入数据并查询
//    spark.sql("alter table hadoop_prod.default.my_tab drop partition field loc")
//    spark.sql("insert into hadoop_prod.default.my_tab values (4,'zl','shanghai',cast(1639920630 as timestamp))")
//    spark.sql("select * from hadoop_prod.default.my_tab").show()
//    // 删除分区删除分区years(ts)，插入数据并查询
//    spark.sql("alter table hadoop_prod.default.my_tab drop partition field years(ts)")
//    spark.sql("insert into hadoop_prod.default.my_tab values (5,'tq','shanghai',cast(1634559630 as timestamp))")
//    spark.sql("select * from hadoop_prod.default.my_tab").show()

    spark.close()
  }
}
