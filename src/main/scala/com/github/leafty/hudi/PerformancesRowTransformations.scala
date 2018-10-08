package com.github.leafty.hudi

import org.apache.spark.sql.functions.{col, date_add}
import org.apache.spark.sql.types.{DateType, TimestampType}

import scala.language.implicitConversions


/**
  *
  */
trait PerformancesRowTransformations {

  import RowTransformationsRegistry._

  val curr_date: RowTransformation = toTimestamp("curr_date", "MM/dd/yyyy")

  val foreclosure_amount: RowTransformation = toDouble("foreclosure_amount")

  val remain_to_mat: RowTransformation = toInt("remain_to_mat")

  val curr_date_inc = date_add(col("curr_date").cast(DateType), 1).cast(TimestampType)
}

object PerformancesRowTransformations extends PerformancesRowTransformations