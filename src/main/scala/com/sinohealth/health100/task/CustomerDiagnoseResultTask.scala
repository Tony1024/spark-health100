package com.sinohealth.health100.task

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object CustomerDiagnoseResultTask {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new SparkConf().setAppName("Health100")
    val ss = SparkSession.builder().config(conf).appName("Health100").enableHiveSupport().getOrCreate()
    ss.sql("use mn")

    /**
      * 高血压
      *
      * 收缩压	      关系	    舒张压
      * ≥140mmHg	  和	      <90mmHg
      * 140-159mmHg	和（或）	90-99mmHg
      * 160-179mmHg	和（或）	100-109mmHg
      * ≥180mmHg 	  和（或）	≥110mmHg
      * ≥140mmHg	  和（或）	≥90mmHg
      * 91-140mmHg	和	      61-90mmHg
      * ≤90mmHg	    和	      ≤60mmHg
      *
      */
    val time1 = System.currentTimeMillis() / 1000
    logger.info("高血压数据开始计算:")
    val bloodSql = "insert into table health_customer_diagnose_result " +
      "select a.vid,b.id as diagnose_id,a.result as diagnose_name,0,a.period,3 from " +
      "(select x.vid,x.period," +
      "IF(x.results>=140 and y.results <90,'单纯收缩期高血压'," +
      "IF(x.results >=140 and x.results <=159 or y.results >=90 and y.results <=99,'1级高血压（轻度）'," +
      "IF(x.results >=160 and x.results <=179 or y.results >=100 and y.results <=109,'2级高血压（中度）'," +
      "IF(x.results >=180 or y.results >=110,'3级高血压（重度）'," +
      "IF(x.results >=140 or y.results >=90,'高血压'," +
      "IF(x.results >=91 and x.results <=140 or y.results >=61 and y.results <=90,'血压正常'," +
      "IF(x.results <=90 or y.results <=60,'低血压',''))))))) as result from" +
      "(select vid,period,max(results) as results from pacs_check_result where item_name_comm = '收缩压' and results is not null group by vid,item_name_comm,period) x," +
      "(select vid,period,max(results) as results from pacs_check_result where item_name_comm = '舒张压' and results is not null group by vid,item_name_comm,period) y " +
      "where x.vid = y.vid) a " +
      "left join health_diagnose_check_dist b on a.result = b.name where a.result != ''"
    ss.sql(bloodSql)
    logger.info("高血压数据计算完成，耗时:[{}] 秒", (System.currentTimeMillis() / 1000 - time1))

    /**
      * BMI BMI值=体重（kg）÷ 身高的平方（m）
      **/
    val time2 = System.currentTimeMillis() / 1000
    logger.info("BMI数据开始计算:")
    val bmiSql = "insert into table health_customer_diagnose_result " +
      "select a.vid,b.id as diagnose_id,a.result as diagnose_name,0,a.period,3 from " +
      "(select vid,period,IF(bmi < 18.5, '偏瘦',IF(bmi >= 18.5 and bmi <= 23.9, '标准身型', IF(bmi >= 24 and bmi <= 27.9, '超重', '肥胖'))) as result from (" +
      "select x.vid,x.period,x.results / (y.results * y.results * 0.01 * 0.01) as bmi from " +
      "(select vid,period,max(results) as results from pacs_check_result where item_name_comm = '体重' and results is not null group by vid,item_name_comm,period) x," +
      "(select vid,period,max(results) as results from pacs_check_result where item_name_comm = '身高' and results is not null group by vid,item_name_comm,period) y " +
      "where x.vid = y.vid) src) a " +
      "left join health_diagnose_check_dist b on a.result = b.name"
    ss.sql(bmiSql)
    logger.info("BMI数据计算完成，耗时:[{}] 秒", (System.currentTimeMillis() / 1000 - time2))

    /**
      * 前列腺肿瘤标志物检测异常
      */
    val time3 = System.currentTimeMillis() / 1000
    logger.info("前列腺肿瘤标志物检测异常 数据开始计算:")
    val sql = "insert into table health_customer_diagnose_result " +
      "select x.vid,y.id as diagnose_id,x.result as diagnose_name,0,x.period,2 from" +
      "(select vid,period,result from " +
      "(select vid,period,IF((x >= normal_h and x <= 10 and y >= 0.25) or (x >= normal_h and x <= 10 and y < 0.25 or x > 10) ,'前列腺肿瘤标志物检测异常','') as result from " +
      "    (select a.vid,a.period,a.results as x,a.normal_h,b.results as y from " +
      "    (select vid,period,max(results) as results,normal_h from lis_test_result where items_name_comm = '前列腺特异性抗原' group by vid,normal_h,period) a," +
      "    (select vid,period,max(results) as results from lis_test_result where items_name_comm = '游离前列腺特异性抗原（F-PSA）/前列腺特异性抗原（PSA）' group by vid,period) b" +
      "    where a.vid = b.vid) src) temp where result != '') x " +
      "left join " +
      "health_diagnose_test_dist y on x.result = y.name and y.type = '特殊'"
    ss.sql(sql)
    logger.info("前列腺肿瘤标志物检测异常 数据计算完成，耗时:[{}] 秒", (System.currentTimeMillis() / 1000 - time3))

    /**
      * 美年的诊断
      */
    val time4 = System.currentTimeMillis() / 1000
    logger.info("美年的诊断 数据开始计算:")
    val diagnoseSql = "insert into table health_customer_diagnose_result " +
      "select a.vid,b.id as diagnose_id,a.diagnose_result_comm as diagnose_name,0,a.period,1 from " +
      "biz_diagnose_result_detail a " +
      "inner join health_diagnose_origin_dist b on a.diagnose_result_comm = b.name"
    ss.sql(diagnoseSql)
    logger.info("美年的诊断 数据计算完成，耗时:[{}] 秒", (System.currentTimeMillis() / 1000 - time4))

    /**
      * 检验数据【一个异常所有异常的规则】
      */
    val time5 = System.currentTimeMillis() / 1000
    logger.info("检验数据【一个异常所有异常的规则】开始计算:")
    val testSql = "insert into table health_customer_diagnose_result " +
      "select a.vid,b.id as diagnose_id,a.diagnose_name,0,period,2 from (" +
      "select vid,period,small_category,diagnose_small as diagnose_name from (" +
      "select vid,period,max(results_discrete) as result,small_category,diagnose_small from (" +
      "select a.vid,a.period,a.results_discrete,a.small_category,b.diagnose_small from lis_test_result a inner join health_diagnose_test_category b on a.small_category = b.small_category where b.type != '特殊'" +
      ") temp group by vid,small_category,diagnose_small,period" +
      ") t where t.result = '2') a inner join health_diagnose_test_dist b on a.diagnose_name = b.name"
    ss.sql(testSql)
    logger.info("检验数据【一个异常所有异常的规则】计算完成，耗时:[{}] 秒", (System.currentTimeMillis() / 1000 - time5))

    /**
      * 检验数据特殊的规则
      */
    val time6 = System.currentTimeMillis() / 1000
    logger.info("检验数据【特殊的规则】开始计算:")
    // 组合情况
    val combinationSql = "insert into table health_customer_diagnose_result " +
      "select vid,diagnose_id,diagnose_name,0,period,2 from (" +
      "select a.vid,a.period,a.diagnose_name,a.item_names,a.result,b.diagnose_id from (" +
      "select vid,period,diagnose_name,concat_ws(',',collect_list(basic)) as item_names,concat_ws(',',collect_list(results_discrete)) as result from " +
      "(" +
      "select a.vid,a.period,a.results_discrete,b.group_id,b.sort,b.basic,b.diagnose_name from (" +
      "select a.vid,a.period,a.results_discrete,b.name from lis_test_result a inner join health_test_item_dist b on a.items_name_comm = b.name group by a.vid,a.results_discrete,b.name,a.period" +
      ") a inner join health_diagnose_basis_group b on a.name = b.basic where b.is_combination = 1 order by a.vid asc,b.group_id asc,b.sort desc" +
      ") x group by vid,diagnose_name,period) a inner join health_diagnose_judgement_result b on a.diagnose_name = b.diagnose_name and a.item_names = b.item_names and a.result = b.result" +
      ") t "
    ss.sql(combinationSql)
    // 非组合情况
    val notCombinationSql = "insert into table health_customer_diagnose_result " +
      "select vid,diagnose_id,diagnose_name,0,period,2 from (" +
      "select a.vid,a.period,a.diagnose_name,a.item_names,a.result,b.diagnose_id from (" +
      "select vid,period,diagnose_name,concat_ws(',',collect_list(basic)) as item_names,concat_ws(',',collect_list(results_discrete)) as result from " +
      "(" +
      "select a.vid,a.period,a.results_discrete,b.group_id,b.sort,b.basic,b.diagnose_name from (" +
      "select a.vid,a.period,a.results_discrete,b.name from lis_test_result a inner join health_test_item_dist b on a.items_name_comm = b.name group by a.vid,a.results_discrete,b.name,a.period" +
      ") a inner join health_diagnose_basis_group b on a.name = b.basic where b.is_combination = 0 order by a.vid asc,b.group_id asc,b.sort desc" +
      ") x group by vid,diagnose_name,period) a inner join health_diagnose_judgement_result b on a.diagnose_name = b.diagnose_name and a.item_names = b.item_names and a.result = b.result" +
      ") t "
    ss.sql(notCombinationSql)

    logger.info("检验数据【特殊的规则】数据计算完成，耗时:[{}] 秒", (System.currentTimeMillis() / 1000 - time6))


  }

}
