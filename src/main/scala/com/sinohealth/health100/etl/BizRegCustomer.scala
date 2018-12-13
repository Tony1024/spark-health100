package com.sinohealth.health100.etl

import cn.hutool.core.date.DateUtil
import cn.hutool.core.util.StrUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 客户清洗程序
  */
object BizRegCustomer {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      return
    } else {
      val targetTable = args.apply(0)
      val mode = args.apply(1)
      System.setProperty("HADOOP_USER_NAME", "hdfs")
      val conf = new SparkConf().setAppName("Health100-Etl")
      val ss = SparkSession.builder().config(conf).appName("Health100-Etl").enableHiveSupport().getOrCreate()
      ss.sql("use health100_source")
      ss.udf.register("Age", (birthday: String, bookTime: String) =>
        if (StrUtil.isNotBlank(birthday) && StrUtil.isNotBlank(bookTime)) {
          Integer.valueOf(DateUtil.age(DateUtil.parse(birthday), DateUtil.parse(bookTime)))
        } else null
      )
      if (mode.equals("create")) {
        // 创建
        ss.sql("create table " + targetTable + " as " +
          "select now() as clean_date,bz_sfzhm as id_card,if(bz_bm1 is not null,bz_bm1,bz_bm2) as department," +
          "dwdm as company_code,vid,cid,cust_name as name,cust_xb as sex,cust_csrq as birth_date,cust_zy as job," +
          "cust_gzhy as job_industry,yysj as book_time,yydjr as book_person,yydjsj as book_reg_time," +
          "jjzh as shop_no,status,qtdjr as other_reg_person,qtdjsj as other_reg_time,tjsj as body_check_time," +
          "tjzzsj as body_check_org_time,member_type,print_time," +
          "Age(cust_csrq,yysj) as age, if(cust_csrq is null, 1, 0) as clean_status from view_yyqkb")
      } else if (mode.equals("insert")) {
        // 增量写法
        ss.sql("insert into table " + targetTable + " " +
          "select now() as clean_date,bz_sfzhm as id_card,if(bz_bm1 is not null,bz_bm1,bz_bm2) as department," +
          "dwdm as company_code,vid,cid,cust_name as name,cust_xb as sex,cust_csrq as birth_date,cust_zy as job," +
          "cust_gzhy as job_industry,yysj as book_time,yydjr as book_person,yydjsj as book_reg_time," +
          "jjzh as shop_no,status,qtdjr as other_reg_person,qtdjsj as other_reg_time,tjsj as body_check_time," +
          "tjzzsj as body_check_org_time,member_type,print_time," +
          "Age(cust_csrq,yysj) as age, if(cust_csrq is null, 1, 0) as clean_status from view_yyqkb")
      }

      ss.sql("select count(1) from " + targetTable).show()
      ss.close()
    }
  }

}

