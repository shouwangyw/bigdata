package com.yw.spark.example.core.cases

import java.sql.{Connection, DriverManager}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
case class JobDetail(
                      jobId: String,
                      jobName: String,
                      jobUrl: String,
                      jobLocation: String,
                      jobSalary: String,
                      jobCompany: String,
                      jobExperience: String,
                      jobClass: String,
                      jobGiven: String,
                      jobDetail: String,
                      companyType: String,
                      companyPerson: String,
                      searchKey: String,
                      city: String
                    )

object Case03_JdbcOperate {
  val getConn: () => Connection = () => {
    DriverManager.getConnection("jdbc:mysql://192.168.254.132:3306/mydb?characterEncoding=UTF-8", "root", "123456")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 创建RDD，这个RDD会记录以后从MySQL中读数据
    val jdbcRDD: JdbcRDD[JobDetail] = new JdbcRDD(
      sc,
      getConn,
      "select * from jobdetail where job_id >= ? and job_id <= ?",
      1,
      75000,
      8,
      rs => {
        val jobId = rs.getString(1)
        val jobName = rs.getString(2)
        val jobUrl = rs.getString(3)
        val jobLocation = rs.getString(4)
        val jobSalary = rs.getString(5)
        val jobCompany = rs.getString(6)
        val jobExperience = rs.getString(7)
        val jobClass = rs.getString(8)
        val jobGiven = rs.getString(9)
        val jobDetail = rs.getString(10)
        val companyType = rs.getString(11)
        val companyPerson = rs.getString(12)
        val searchKey = rs.getString(13)
        val city = rs.getString(14)
        JobDetail(jobId, jobName, jobUrl, jobLocation, jobSalary, jobCompany, jobExperience, jobClass, jobGiven,
          jobDetail, companyType, companyPerson, searchKey, city)
      }
    )
    val searchKey: RDD[(String, Iterable[JobDetail])] = jdbcRDD.groupBy(x => x.searchKey)

    /**
      * 需求一：求取每个搜索关键字下的职位数量，并将结果入库mysql
      */
    // 第一种实现方式
    val searchKeyRDD: RDD[(String, Int)] = searchKey.map(x => (x._1, x._2.size))

    // 第二种实现方式
    // 求取每个搜索关键字出现的岗位人数
    val resultRDD: RDD[(String, Int)] = jdbcRDD.map(x => (x.searchKey, 1)).reduceByKey(_ + _).filter(x => x._1 != null)
    // 数据量变少，缩减分区个数
    val resultRDD2: RDD[(String, Int)] = resultRDD.coalesce(2)
    // 将统计的结果写回去mysql
    resultRDD2.foreachPartition(it => {
      val conn = getConn()
      conn.setAutoCommit(false)
      val statement = conn.prepareStatement("insert into job_count(search_name, job_num) values(?, ?)")
      // 遍历
      it.foreach(x => {
        statement.setString(1, x._1)
        statement.setInt(2, x._2)
        statement.addBatch()
      })
      statement.executeBatch()
      conn.commit()
      statement.close()
      conn.close()
    })

    /**
      * 需求二：求取每个搜索关键字岗位下最高薪资的工作信息，以及最低薪资下的工作信息
      */
    val getEachJobs: RDD[(String, Iterable[JobDetail])] = jdbcRDD.groupBy(x => x.searchKey)
    val maxJobDetail: RDD[JobDetail] = getEachJobs.map(x => {
      val value: Iterable[JobDetail] = x._2
      val array: Array[JobDetail] = value.toArray
      array.maxBy(x => {
        val jobSalary: String = x.jobSalary
        val result = if (StringUtils.isNotBlank(jobSalary) && jobSalary.contains("k") && jobSalary.contains("-")
          && jobSalary.replace("k", "").split("-").length >= 2) {
          val strs: Array[String] = jobSalary.replace("k", "").split("-")
          val ret = if (strs.length >= 2) strs(1).toInt else 0
          ret
        } else 0
        result
      })
    })
    val details: Array[JobDetail] = maxJobDetail.collect()
    details.foreach(x => {
      println(x.jobId + "\t" + x.jobSalary + "\t" + x.searchKey + "\t" + x.jobCompany)
    })

    sc.stop()
  }
}
