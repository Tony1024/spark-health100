package com.sinohealth.health100.udaf

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class HepatitisBUDAF extends UserDefinedAggregateFunction {

  val list = Array[Integer](16, 21, 19, 20, 23, 18, 17, 1, 11, 8, 3, 24, 25, 26, 27, 29, 4, 5, 7, 12, 13)

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

  override def evaluate(buffer: Row): Any = finalResult(buffer.getInt(0))

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
