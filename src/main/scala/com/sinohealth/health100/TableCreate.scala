package com.sinohealth.health100

import java.util.Properties

import com.sinohealth.health100.utils.SparkLocalConnectUtil

object TableCreate {

  def main(args: Array[String]): Unit = {

    var ss = SparkLocalConnectUtil.createContextAndSqlContext().getOrCreate()
    ss.sql("use mn")
    var prop2 = new Properties()
    prop2.setProperty("url", "jdbc:mysql://192.168.16.101:3000/health100_source201801?characterEncoding=utf8")
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "Zk2025##")
    var PhlDF = ss.read.jdbc(prop2.getProperty("url"), "(select * from " + "biz_diagnose_result" + " where 1=2) s ", prop2)

    var keyCount = PhlDF.schema.fields.length - 2

    var str = "create table  if not exists " + "biz_diagnose_result" + " ( "
    PhlDF.schema.fields.foreach(
      x => {
        str += x.name + " " + SparkLocalConnectUtil.hiveTypeChange(x.dataType.toString()) + ", "
      }
    )
    str = str.substring(0, str.length() - 2)
    str += ") "
    str += "partitioned by (period string)"
    str += " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' "
    println(str)
    ss.sql(str)
    ss.close()

  }

}
