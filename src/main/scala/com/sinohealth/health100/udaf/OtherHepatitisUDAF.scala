package com.sinohealth.health100.udaf

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class OtherHepatitisUDAF extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = {
    StructType(StructField("itemName", StringType) :: StructField("result", StringType) :: Nil)
  }

  override def bufferSchema: StructType = {
    StructType(StructField("output", IntegerType) :: Nil)
  }

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getInt(0) | getValue(input.getString(0), input.getString(1)))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getInt(0) | buffer2.getInt(0))
  }

  override def evaluate(buffer: Row): Any = buffer.getInt(0)

  private def getValue(itemName: String, result: String) = {
    if (StringUtils.isNotBlank(itemName)) itemName.trim match {
      case "甲肝病毒抗体IgM" =>
        if (StringUtils.isNotBlank(result) && result == "2") 1 else 0
      case "丙肝病毒抗体IgM" =>
        if (StringUtils.isNotBlank(result) && result == "2") 1 else 0
      case "戊肝病毒抗体IgM" =>
        if (StringUtils.isNotBlank(result) && result == "2") 1 else 0
      case "庚肝病毒抗体IgM" =>
        if (StringUtils.isNotBlank(result) && result == "2") 1 else 0
      case "风疹病毒抗体IgM测定" =>
        if (StringUtils.isNotBlank(result) && result == "2") 1 else 0
      case "弓形体抗体IgM测定" =>
        if (StringUtils.isNotBlank(result) && result == "2") 1 else 0
      case "巨细胞病毒IgM抗体" =>
        if (StringUtils.isNotBlank(result) && result == "2") 1 else 0
      case "单纯疱疹病毒Ⅱ型抗体IgM" =>
        if (StringUtils.isNotBlank(result) && result == "2") 1 else 0
      case "单纯疱疹病毒Ⅰ型抗体IgM" =>
        if (StringUtils.isNotBlank(result) && result == "2") 1 else 0
      case _ => 0
    }
    else 0
  }

}
