package com.sinohealth.health100

import cn.hutool.core.date.DateUtil
import cn.hutool.core.util.StrUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 客户清洗程序
  */
object FutianUserTest {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new SparkConf().setAppName("Test")
    val ss = SparkSession.builder().config(conf).appName("Test").enableHiveSupport().getOrCreate()
    ss.sql("use mn")
    val sql =
      """
        |create table fujian_zd_juzheng as
        |select t.vid ,
        |max(case when t.first_level_classify = 'BI-RADS评分' and (t.serverity=0 or t.serverity= 1 ) then 1 else 0 end  ) as `BI-RADS评分01`,
        |max(case when t.first_level_classify = 'BI-RADS评分' and (t.serverity=2 or t.serverity= 3 ) then 1 else 0 end  ) as `BI-RADS评分23`,
        |max(case when t.first_level_classify = 'BI-RADS评分' and (t.serverity=4 or t.serverity= 5 ) then 1 else 0 end  ) as `BI-RADS评分45`,
        |max(case when t.first_level_classify = '鼻骨改变' and (t.serverity=0 or t.serverity= 1 ) then 1 else 0 end  ) as `鼻骨改变01`
        |from  biz_diagnose_result_detail_ext t inner join
        | (select vid from fujian_vid) v on t.vid  =v.vid group by
        |t.vid
      """.stripMargin

    ss.sql(sql)

    ss.sql("select count(1) from fujian_zd_juzheng").show()
    ss.close()
  }

}

