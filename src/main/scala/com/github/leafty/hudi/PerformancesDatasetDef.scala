package com.github.leafty.hudi

import org.apache.spark.sql.functions.{concat_ws, lit, col}
import org.apache.spark.sql.{Column, DataFrame}


class PerformancesDatasetDef(location: Option[String] = None)
  extends DatasetDef("performances", HoodieKeys.ROW_KEY, "curr_date", location) {

}

object PerformancesDatasetDef {

  final val ID = "id_2"

  val customTrans = new PerformancesRowTransformations {}

  implicit class Mapper(df : DataFrame) extends DatasetMapperFromRaw(df) {

    override def rowKeyColumn: Column = concat_ws("--", col(ID), col("curr_date"))

    override def partitionColumn: Column = concat_ws("/", lit("parent"), col(ID))

  }
}