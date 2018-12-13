package com.sinohealth.health100.task

import com.sinohealth.health100.udaf.HepatitisBUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 乙肝数据分析
  * --->>> 切记:
  * 这里是用spark-submit提交到yarn集群的版本,注意SparkConf不需要设置太多参数,因为spark-submit已经带上了,
  * 假如这里添加了其他参数,会出现很多无法想象无法定位的bug和问题,如果要本地调试,那么才需要设置更多参数
  */
object HepatitisBTask {

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
      // 注册UDAF
      ss.udf.register("eval", new HepatitisBUDAF)
      if (mode.equals("create")) {
        // 特殊情况下 - 初始化表写法
        ss.sql("create table " + targetTable + " as " +
          "select vid,system,classify,is_special,period,eval(items_name_comm,results_discrete) as eval_result from " +
          "(select vid,items_name_comm,results_discrete,is_special,system,classify,period from lis_test_result where classify = '乙肝' and is_special=1 and clean_status=0) temp " +
          "group by vid,is_special,system,classify,period")
      } else if (mode.equals("insert")) {
        // 增量写法
        ss.sql("insert into table " + targetTable + " " +
          "select vid,system,classify,is_special,period,eval(items_name_comm,results_discrete) as eval_result from " +
          "(select vid,items_name_comm,results_discrete,is_special,system,classify,period from lis_test_result where classify = '乙肝' and is_special=1 and clean_status=0) temp " +
          "group by vid,is_special,system,classify,period")
      }

      ss.sql("select count(1) from " + targetTable).show()
      ss.close()
    }


  }

}

