package com.sinohealth.health100.task

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 肝炎，通过甲肝乙肝丙肝等数据分析得出
  * --->>> 切记:
  * 这里是用spark-submit提交到yarn集群的版本,注意SparkConf不需要设置太多参数,因为spark-submit已经带上了,
  * 假如这里添加了其他参数,会出现很多无法想象无法定位的bug和问题,如果要本地调试,那么才需要设置更多参数
  */
object HepatitisTask {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      return
    } else {
      val targetTable = args.apply(0)
      val mode = args.apply(1)
      System.setProperty("HADOOP_USER_NAME", "hdfs")
      val conf = new SparkConf().setAppName("Health100")
      val ss = SparkSession.builder().config(conf).appName("Health100").enableHiveSupport().getOrCreate()
      ss.sql("use mn")
      if (mode.equals("create")) {
        // 创建
        ss.sql("create table " + targetTable + " as " +
          "select vid,system,'肝炎' as classify,is_special,period,if(sum(eval_result) > 0, 1, 0) as eval_result from " +
          "lis_test_result_aggregation " +
          "group by vid,is_special,system,period")
      } else if (mode.equals("insert")) {
        // 增量写法
        ss.sql("insert into table " + targetTable + " " +
          "select vid,system,'肝炎' as classify,is_special,period,if(sum(eval_result) > 0, 1, 0) as eval_result from " +
          "lis_test_result_aggregation " +
          "group by vid,is_special,system,period")
      }

      ss.sql("select count(1) from " + targetTable).show()
      ss.close()
    }
  }

}

