package com.github.leafty.hudi

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.types.{DoubleType, IntegerType}


object RowTransformationsRegistry {

  type RowTransformation = (DataFrame ⇒ DataFrame)

  def toTimestamp(name: String, fmt: String): RowTransformation =
    df ⇒ df.withColumn(name, to_timestamp(col(name), fmt))

  def toInt(name: String): RowTransformation =
    df ⇒ df.withColumn(name, col(name).cast(IntegerType))

  def toDouble(name: String): RowTransformation =
    df ⇒ df.withColumn(name, col(name).cast(DoubleType))

}
