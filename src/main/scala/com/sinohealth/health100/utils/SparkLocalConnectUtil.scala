package com.sinohealth.health100.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkLocalConnectUtil {

  def createContextAndSqlContext(): SparkSession.Builder = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf =new SparkConf()
    conf.setAppName("Health100")
    conf.setMaster("yarn-client")
    conf.set("deploy.mode", "cluster")
    conf.set("spark.yarn.dist.files", "hdfs://cdh2:8020/cmh/yarn-site.xml,hdfs://cdh2:8020/cmh/hdfs-site.xml");
    conf.setJars(Array("hdfs://cdh2:8020/cmh/ojdbc14-10.2.0.5.jar"))
    conf.set("spark.executor.instances", "15")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.sql.broadcastTimeout", "1800")
    conf.set("spark.driver.maxResultSize","2g")
    SparkSession.builder().config(conf).appName("Health100").enableHiveSupport()
  }

  def createStreamingContext(): StreamingContext = {
    val conf = new SparkConf()
    conf.setAppName("StreamingJob")
    conf.setMaster("yarn")
    conf.set("deploy.mode", "cluster")
    conf.set("spark.yarn.dist.files", "hdfs://hadoop-cluster1/nyh/yarn-site.xml,hdfs://hadoop-cluster1/nyh/hdfs-site.xml")
    conf.setJars(Array("hdfs://hadoop-cluster1/nyh/haha.jar", "hdfs://hadoop-cluster1/nyh/sqljdbc4-4.0.jar", "hdfs://hadoop-cluster1/nyh/ojdbc14-10.2.0.5.jar"
      , "hdfs://hadoop-cluster1/nyh/kafka_2.11-0.8.2.1.jar", "hdfs://hadoop-cluster1/nyh/spark-streaming-kafka-0-8_2.11-2.1.0.jar"
      , "hdfs://hadoop-cluster1/nyh/zkclient-0.3.jar"
      , "hdfs://hadoop-cluster1/nyh/metrics-core-3.1.2.jar"
      , "hdfs://hadoop-cluster1/nyh/metrics-graphite-3.1.2.jar"
    ))

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc

  }

  //为了构造由DataFrame变成hive表时，拼写建表语句的时候要把DataFrame的字段属性文字变成hive表的字段属性描述类型
  def hiveTypeChange(columnTypeName: String): String = {
    val result = columnTypeName match {
      case i if (i.contains("DecimalType(") && i.contains(",0)")) => "int"
      case "StringType" => "String"
      case "TimestampType" => "String"
      case "DoubleTpye" => "double"
      case "IntegerType" => "int"
      case "LongType" => "bigint"
      case "FloatType" => "float"
      case i if i.contains("DecimalType") => "double"
      case _ => "String"
    }
    result
  }

}
