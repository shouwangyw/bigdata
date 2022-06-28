package com.yw.musichw.util

import com.typesafe.config.{Config, ConfigFactory}

/**
  * @author yangwei
  */
object ConfigUtils {
  /**
    * ConfigFactory.load() 默认加载 classpath 下的
    * application.conf, application.json 和 application.properties 文件
    */
  lazy val load: Config = ConfigFactory.load()
  val LOCAL_RUN = load.getBoolean("local.run")
  val HIVE_METASTORE_URIS = load.getString("hive.metastore.uris")
  val HIVE_DATABASE = load.getString("hive.database")
  val HDFS_CLIENT_LOG_PATH = load.getString("clientlog.hdfs.path")
  val MYSQL_URL = load.getString("mysql.url")
  val MYSQL_USER = load.getString("mysql.user")
  val MYSQL_PASSWORD = load.getString("mysql.password")
}
