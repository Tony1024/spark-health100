package com.sinohealth.health100.etl

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * 检验数据清洗程序
  */
object LisTestResult {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      return
    } else {
      val targetTable = args.apply(0)
      val mode = args.apply(1)
      System.setProperty("HADOOP_USER_NAME", "hdfs")
      val conf = new SparkConf().setAppName("Health100-Etl")
      val ss = SparkSession.builder().config(conf).appName("Health100-Etl").enableHiveSupport().getOrCreate()

      ss.sql("use mn")
      val unitDF = ss.sql("select unit,unit_comm from bas_test_item_unit")
      val checkItemDF = ss.sql("select field_comment,field_comm from bas_check_item where clean_status = 0 and del_flag = 0")
      val initResultCommDF = ss.sql("select init_result,init_result_comm from bas_check_item_init where init_result is not null and init_result_comm is not null")
      unitDF.createOrReplaceTempView("unit_dist")
      unitDF.persist(StorageLevel.MEMORY_ONLY_SER)
      checkItemDF.createOrReplaceTempView("check_item_dist")
      checkItemDF.persist(StorageLevel.MEMORY_ONLY_SER)
      initResultCommDF.createOrReplaceTempView("init_result_dist")
      initResultCommDF.persist(StorageLevel.MEMORY_ONLY_SER)

      if (mode.equals("create")) {
        // 创建
        ss.sql("use health100_source")
        ss.sql("create table " + targetTable + " as " +
          "select x.vid,x.cid,x.item_id,x.field_comment as item_name,x.field_results as results,x.zcz_xx as normal_l," +
          "x.zcz_sx as normal_h,x.dw as unit,x.init_results as init_result,x.in_factory,x.op_datetime as create_time," +
          "y.init_result_comm,if(x.yysj is not null,x.yysj,'1970-01-01 08:00:00') as book_time," +
          "z.field_comm,w.unit_comm,if(x.field_comment is null,0,if(z.field_comm is null or z.field_comm = '', 1, 0)) as clean_status " +
          "from" +
          "(SELECT b.vid,b.cid,b.item_id,b.field_comment,b.field_results,b.zcz_xx,b.zcz_sx,b.dw,b.init_results,b.in_factory,b.op_datetime,v.yysj FROM jj_comm_jc_result201808 b LEFT JOIN view_yyqkb v ON b.vid = v.vid) x " +
          "left join init_result_dist y on x.init_results = y.init_result " +
          "left join check_item_dist z on x.field_comment = z.field_comment " +
          "left join unit_dist w on x.dw = w.unit")
      } else if (mode.equals("insert")) {
        // 增量写法
        ss.sql("insert into table " + targetTable + " " +
          "select x.vid,x.cid,x.item_id,x.field_comment as item_name,x.field_results as results,x.zcz_xx as normal_l," +
          "x.zcz_sx as normal_h,x.dw as unit,x.init_results as init_result,x.in_factory,x.op_datetime as create_time," +
          "y.init_result_comm,if(x.yysj as book_time is not null,x.yysj,'1970-01-01 08:00:00')," +
          "z.field_comm,if(x.field_comment is null,0,if(z.field_comm is null or z.field_comm = '', 1, 0)) as clean_status" +
          "from" +
          "(SELECT b.vid,b.cid,b.item_id,b.field_comment,b.field_results,b.zcz_xx,b.zcz_sx,b.dw,b.init_results,b.in_factory,b.op_datetime,v.yysj FROM jj_comm_jc_result201808 b LEFT JOIN view_yyqkb v ON b.vid = v.vid) x " +
          "left join init_result_dist y on x.init_results = y.init_result " +
          "left join check_item_dist z on x.field_comment = z.field_comment " +
          "left join unit_dist w on x.dw = w.unit")
      }

      ss.sql("select count(1) from " + targetTable).show()
      ss.close()
    }
  }

}

