package com.sinohealth.health100

import java.util

import cn.hutool.core.date.DateUtil
import com.sinohealth.health100.udaf.HepatitisBUDAF
import com.sinohealth.health100.utils.SparkLocalConnectUtil
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

object LisTestResultTask {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      return
    } else {
      val firstMonth = args.apply(0)
      val lastMonth = args.apply(1)
      val monthList = new util.ArrayList[String]()
      var d = DateUtil.parse(firstMonth, "yyyyMM")
      while (d.getTime <= DateUtil.parse(lastMonth, "yyyyMM").getTime) {
        monthList.add(DateUtil.format(d, "yyyyMM"))
        d = DateUtil.offsetMonth(d, 1)
      }

      System.setProperty("HADOOP_USER_NAME", "hdfs")
      val ss = SparkLocalConnectUtil.createContextAndSqlContext().getOrCreate()
      ss.sql("use mn")

      ss.udf.register("eval", new HepatitisBUDAF)

      for (month <- monthList) {
        // 正常情况下
//        ss.sql("insert into lis_test_result_aggregation " +
//          "select x.vid,x.classify,max(x.is_exception) as result from " +
//          "(select vid,classify," +
//          "(CASE WHEN(results_discrete) = '-2' THEN 1 WHEN(results_discrete = '2') THEN 1 ELSE 0 END) as is_exception " +
//          "from lis_test_result_" + month +
//          "where is_special = 0 and clean_status = '0') x group by x.vid,x.classify")
        // 特殊情况下

        // 特殊情况下
        val df = ss.sql("select vid,items_name_comm,results_discrete,system,classify from lis_test_result_201606 where is_special=1 and clean_status=0").toDF()
        // 临时表
        df.createOrReplaceTempView("x")
        df.persist(StorageLevel.MEMORY_ONLY_SER)

        ss.sql("select vid,system,classify,eval(items_name_comm,results_discrete) from x group by vid,system,classify").show()

      }

//      val df201606 = ss.sql("create table customer_test_result as " +
//        "(select x.vid,x.classify,max(x.is_exception) as result from " +
//        "(select a.vid,b.classify," +
//        "(CASE WHEN(a.results_discrete) = '-2' THEN 1 WHEN(a.results_discrete = '2') THEN 1 ELSE 0 END) as is_exception " +
//        "from lis_test_result_201606 a " +
//        "inner join bas_test_item b on a.small_category = b.small_category " +
//        "where b.is_special = 0 and a.clean_status = '0') x group by x.vid,x.classify)")

      ss.close()

    }


  }

}

