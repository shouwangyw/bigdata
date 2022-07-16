package com.yw.spark.example.sql.cases

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *
  * @author yangwei
  */
object Case08_ReadCsvWriteMySQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df: DataFrame = spark.read.format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ") // 时间转换
      .option("header", "true") // 第一行数据都是head(字段属性的意思)
//      .option("multiLine", "true") // 数据可能换行
      .load(this.getClass.getClassLoader.getResource("data").getPath)

    df.createOrReplaceTempView("job_detail")
    spark.sql("select job_name,job_url,job_location,job_salary,job_company,job_experience,job_class,job_given,job_detail,company_type,company_person,search_key,city from job_detail where job_company = '北京无极慧通科技有限公司'").show(80)

    val props = new Properties()
    props.put("user", "root")
    props.put("password", "123456")

    df.write.mode(SaveMode.Append).jdbc(
      "jdbc:mysql://192.168.254.132:3306/mydb?useSSL=false&useUnicode=true&characterEncoding=UTF-8",
      "mydb.jobdetail_copy", props
    )
  }
}
