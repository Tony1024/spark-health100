package com.sinohealth.health100.task

import com.sinohealth.health100.utils.SparkLocalConnectUtil
import org.apache.spark.sql.functions
import org.apache.spark.storage.StorageLevel

object BizDiagnoseResultDetailExtTask {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val sparkSession = SparkLocalConnectUtil.createContextAndSqlContext().getOrCreate()
    sparkSession.sql("use mn")
    val df = sparkSession.sql("select * from biz_diagnose_result_detail")
    val result = df.withColumn("sub_diagnose_result_comm", functions.explode(functions.split(df("diagnose_result_comm"), ",|，"))).toDF()
    result.createOrReplaceTempView("biz_diagnose_detail_temp_view")
    result.persist(StorageLevel.MEMORY_ONLY_SER)

    sparkSession.sql("create table biz_diagnose_result_detail_ext as " +
      "select a.*,b.first_level_classify,b.second_level_classify,b.diagnose_comm as diagnose_result_comm_comm,b.serverity,b.system,b.sex " +
      "from biz_diagnose_detail_temp_view a " +
      "left join bas_diagnose_classify b on a.sub_diagnose_result_comm = b.diagnose")

    sparkSession.close()

  }

}
