package com.yw.iceberg.example

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.spark.sql.SparkSession

/**
  *
  * @author yangwei
  */
object SparkIcebergDQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      // 指定hadoop catalog，catalog名称为hadoop_prod
      .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://node01:8020/spark_iceberg")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    /******************** case01 ********************/
//    // 1. SQL 方式读取Iceberg中的数据
//    spark.sql("select * from hadoop_prod.default.my_tb1").show()
//    // 2. 使用DataFrame方式,建议使用SQL方式
//    // 方式一
//    val df1: DataFrame = spark.table("hadoop_prod.default.my_tb1")
//    df1.show()
//    // 方式二
//    val df2: DataFrame = spark.read.format("iceberg").load("hdfs://node01:8020/spark_iceberg/default/my_tb1")
//    df2.show()

    /******************** case02 ********************/
    // 查看Iceberg表快照信息
    spark.sql("select * from hadoop_prod.default.my_tb1.snapshots").show(false)
    /*
    +-----------------------+-------------------+---------+---------+-----------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |committed_at           |snapshot_id        |parent_id|operation|manifest_list                                                                                                                |summary                                                                                                                                                                                                                                                                                         |
    +-----------------------+-------------------+---------+---------+-----------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |2022-07-30 00:15:07.942|8409100036511820619|null     |append   |hdfs://node01:8020/spark_iceberg/default/my_tb1/metadata/snap-8409100036511820619-1-e7b3103f-2152-4046-a7fd-6da6bf051944.avro|{spark.app.id -> local-1659111301205, added-data-files -> 4, added-records -> 4, added-files-size -> 3400, changed-partition-count -> 1, total-records -> 4, total-files-size -> 3400, total-data-files -> 4, total-delete-files -> 0, total-position-deletes -> 0, total-equality-deletes -> 0}|
    +-----------------------+-------------------+---------+---------+-----------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    // 查看Iceberg表历史信息
    spark.sql("select * from hadoop_prod.default.my_tb1.history").show(false)
    /*
    +-----------------------+-------------------+---------+-------------------+
    |made_current_at        |snapshot_id        |parent_id|is_current_ancestor|
    +-----------------------+-------------------+---------+-------------------+
    |2022-07-30 00:15:07.942|8409100036511820619|null     |true               |
    +-----------------------+-------------------+---------+-------------------+
     */
    // 查看Iceberg表中的data files
    spark.sql("select * from hadoop_prod.default.my_tb1.files").show(false)
    /*
    +-------+---------------------------------------------------------------------------------------------------------------+-----------+-------+------------+------------------+---------------------------+------------------------+------------------------+----------------+-------------------------------+-------------------------------+------------+-------------+------------+-------------+
    |content|file_path                                                                                                      |file_format|spec_id|record_count|file_size_in_bytes|column_sizes               |value_counts            |null_value_counts       |nan_value_counts|lower_bounds                   |upper_bounds                   |key_metadata|split_offsets|equality_ids|sort_order_id|
    +-------+---------------------------------------------------------------------------------------------------------------+-----------+-------+------------+------------------+---------------------------+------------------------+------------------------+----------------+-------------------------------+-------------------------------+------------+-------------+------------+-------------+
    |0      |hdfs://node01:8020/spark_iceberg/default/my_tb1/data/00000-0-9e7b2d89-b416-4989-bfa5-19d06c3630be-00001.parquet|PARQUET    |0      |1           |850               |{1 -> 47, 2 -> 49, 3 -> 47}|{1 -> 1, 2 -> 1, 3 -> 1}|{1 -> 0, 2 -> 0, 3 -> 0}|{}              |{1 ->    , 2 -> zs, 3 ->    }|{1 ->    , 2 -> zs, 3 ->    }|null        |[4]          |null        |0            |
    |0      |hdfs://node01:8020/spark_iceberg/default/my_tb1/data/00001-1-eb9bf19b-f35d-4639-a2bc-2f57a8170b89-00001.parquet|PARQUET    |0      |1           |850               |{1 -> 47, 2 -> 49, 3 -> 47}|{1 -> 1, 2 -> 1, 3 -> 1}|{1 -> 0, 2 -> 0, 3 -> 0}|{}              |{1 ->    , 2 -> ww, 3 ->    }|{1 ->    , 2 -> ww, 3 ->    }|null        |[4]          |null        |0            |
    |0      |hdfs://node01:8020/spark_iceberg/default/my_tb1/data/00002-2-6fc9de13-e8d3-4c8b-8400-dcafd6a4c1c3-00001.parquet|PARQUET    |0      |1           |850               |{1 -> 47, 2 -> 49, 3 -> 47}|{1 -> 1, 2 -> 1, 3 -> 1}|{1 -> 0, 2 -> 0, 3 -> 0}|{}              |{1 ->    , 2 -> ls, 3 ->    }|{1 ->    , 2 -> ls, 3 ->    }|null        |[4]          |null        |0            |
    |0      |hdfs://node01:8020/spark_iceberg/default/my_tb1/data/00003-3-16d1dbb7-5607-42ad-9e49-5f9c3e81cb3c-00001.parquet|PARQUET    |0      |1           |850               |{1 -> 47, 2 -> 49, 3 -> 47}|{1 -> 1, 2 -> 1, 3 -> 1}|{1 -> 0, 2 -> 0, 3 -> 0}|{}              |{1 ->    , 2 -> ml, 3 ->    }|{1 ->    , 2 -> ml, 3 ->    }|null        |[4]          |null        |0            |
    +-------+---------------------------------------------------------------------------------------------------------------+-----------+-------+------------+------------------+---------------------------+------------------------+------------------------+----------------+-------------------------------+-------------------------------+------------+-------------+------------+-------------+
     */
    // 查询Manifests
    spark.sql("select * from hadoop_prod.default.my_tb1.manifests").show(false)
    /*
    +-----------------------------------------------------------------------------------------------------+------+-----------------+-------------------+----------------------+-------------------------+------------------------+-------------------+
    |path                                                                                                 |length|partition_spec_id|added_snapshot_id  |added_data_files_count|existing_data_files_count|deleted_data_files_count|partition_summaries|
    +-----------------------------------------------------------------------------------------------------+------+-----------------+-------------------+----------------------+-------------------------+------------------------+-------------------+
    |hdfs://node01:8020/spark_iceberg/default/my_tb1/metadata/e7b3103f-2152-4046-a7fd-6da6bf051944-m0.avro|5993  |0                |8409100036511820619|4                     |0                        |0                       |[]                 |
    +-----------------------------------------------------------------------------------------------------+------+-----------------+-------------------+----------------------+-------------------------+------------------------+-------------------+
     */
    // 查询指定快照数据
    spark.read.option("snapshot-id", 8409100036511820619L).format("iceberg")
        .load("hdfs://node01:8020/spark_iceberg/default/my_tb1")
        .show()
    /*
    +---+----+---+
    | id|name|age|
    +---+----+---+
    |  1|  zs| 18|
    |  3|  ww| 20|
    |  2|  ls| 19|
    |  4|  ml| 21|
    +---+----+---+
     */
//    // SQL 方式指定查询快照ID 数据
//    spark.sql("call hadoop_prod.system.set_current_snapshot('default.my_tb1', 8409100036511820619L)").show()

    // call hadoop_prod.system.set_current_snapshot('default.my_tb1', 2564591926167696280L);
    // 根据时间戳查询数据
    spark.read.option("as-of-timestamp", "1659466148000")
        .format("iceberg")
        .load("hdfs://node01:8020/spark_iceberg/default/my_tb1")
        .show()
    /*
    +---+----+---+
    | id|name|age|
    +---+----+---+
    |  1|  zs| 18|
    |  3|  ww| 20|
    |  2|  ls| 19|
    |  4|  ml| 21|
    +---+----+---+
     */

    // 查询快照
    spark.sql("select * from hadoop_prod.default.my_tb1.snapshots").show()
    /*
    +--------------------+-------------------+-------------------+---------+--------------------+--------------------+
    |        committed_at|        snapshot_id|          parent_id|operation|       manifest_list|             summary|
    +--------------------+-------------------+-------------------+---------+--------------------+--------------------+
    |2022-07-30 00:15:...|8409100036511820619|               null|   append|hdfs://node01:802...|{spark.app.id -> ...|
    |2022-08-03 08:39:...|2564591926167696280|8409100036511820619|   append|hdfs://node01:802...|{spark.app.id -> ...|
    +--------------------+-------------------+-------------------+---------+--------------------+--------------------+
     */
    // 回滚快照
    spark.sql("select * from hadoop_prod.default.my_tb1").show() // 回滚前
    val conf = new Configuration()
    val catalog = new HadoopCatalog(conf, "hdfs://node01:8020/spark_iceberg")
    val table = catalog.loadTable(TableIdentifier.of("default", "my_tb1"))
    table.manageSnapshots().rollbackTo(8409100036511820619L).commit()
    spark.sql("select * from hadoop_prod.default.my_tb1").show() // 回滚后

    spark.close()
  }
}
