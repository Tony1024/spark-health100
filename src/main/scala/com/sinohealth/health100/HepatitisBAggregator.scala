package com.sinohealth.health100

import java.util.Properties

import com.sinohealth.health100.udaf.HepatitisBUDAF
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object HepatitisBAggregator {

  val list = Array[Integer](16, 21, 19, 20, 23, 18, 17, 1, 11, 8, 3, 24, 25, 26, 27, 29, 4, 5, 7, 12, 13)

  case class LisTestResult(id: Int, cid: String, vid: String, itemId: String, itemFt: String, bigCategory: String,
                           smallCategory: String, itemName: String, itemsNameComm: String, unit: String, unitComm: String,
                           results: String, resultsDiscrete: String, normalL: String, normalH: String, createTime: String,
                           groupId: Int, cleanStatus: String, bookTime: String, classify: String, system: String, isSpecial: Int, period: String)

  case class SubLisTestResult(vid: String, itemsNameComm: String, resultsDiscrete: String, classify: String, system: String)

  object HepatitisBFunction extends Aggregator[LisTestResult, Int, Int] {
    override def zero: Int = 0

    override def reduce(b: Int, a: LisTestResult): Int = {
      b | getValue(a.itemsNameComm, a.resultsDiscrete)
    }

    override def merge(b1: Int, b2: Int): Int = b1 | b2

    override def finish(reduction: Int): Int = finalResult(reduction)

    override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

    override def outputEncoder: Encoder[Int] = Encoders.scalaInt

    private def finalResult(result: Integer) = if (list.contains(result)) 1 else 0

    private def getValue(itemName: String, result: String) = {
      if (StringUtils.isNotBlank(itemName)) itemName.trim match {
        case "乙肝表面抗原" =>
          if (StringUtils.isNotBlank(result) && result == "2") 16 else 0
        case "乙肝表面抗体" =>
          if (StringUtils.isNotBlank(result) && result == "2") 8 else 0
        case "乙肝e抗原" =>
          if (StringUtils.isNotBlank(result) && result == "2") 4 else 0
        case "乙肝e抗体" =>
          if (StringUtils.isNotBlank(result) && result == "2") 2 else 0
        case "乙肝核心抗体" =>
          if (StringUtils.isNotBlank(result) && result == "2") 1 else 0
        case _ => 0
      }
      else 0
    }

  }

  def main(args: Array[String]): Unit = {
    val url = "jdbc:mysql://192.168.16.101:3000/health100_source201605?useUnicode=true&amp;zeroDateTimeBehavior=convertToNull"
    val user = "root"
    val passwd = "Zk2025##"
    val table = "lis_test_result"
    val props = new Properties()
    props.put("user", user)
    props.put("password", passwd)
    // 58
    val conf = new SparkConf().set("spark.executor.memory", "30000m").setAppName("aggregator").setMaster("spark://192.168.16.58:7077")
    val sc = new SparkContext(conf)
    val ss = SparkSession.builder().appName("aggregator").config("spark.debug.maxToStringFields", "100").config("spark.some.config.option", "some-value").getOrCreate()
    val df = ss.read.jdbc(url, table, props)
    // local
    //    val conf = new SparkConf().setAppName("aggregator").setMaster("local")
    //    val sc = new SparkContext(conf)
    //    val ss = SparkSession.builder().appName("aggregator").config("spark.some.config.option", "some-value").getOrCreate()
    //    val df = ss.read.jdbc(url, table, props)

    ss.udf.register("eval", new HepatitisBUDAF)
    // 临时表
    df.createOrReplaceTempView("table")
    df.persist(StorageLevel.MEMORY_ONLY_SER)

    ss.sql("select vid,system,classify,eval(itemsNameComm,resultsDiscrete) as finalResult from (select vid,items_name_comm as itemsNameComm,results_discrete as resultsDiscrete,system,classify from table where clean_status=0) x group by vid,system,classify").show()

    //    import ss.implicits._
    //    val ds = ss.sql("select vid,items_name_comm as itemsNameComm,results_discrete as resultsDiscrete,system,classify from table where clean_status=0").as[LisTestResult]
    //    ds.show()
    //    val result = HepatitisBFunction.toColumn.name("finalResult")
    //    df.select(df.col("vid"), df.col("system"), df.col("classify"), result).groupBy("itemsNameComm", "resultsDiscrete")

    ss.stop()
    sc.stop()

  }

}
