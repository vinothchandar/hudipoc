package com.github.leafty.hudi

import org.apache.spark.sql.functions.{concat_ws, lit}
import org.apache.spark.sql.{Column, DataFrame}


class PerformancesDatasetDef(location: Option[String] = None) extends DatasetDef("performances", HoodieKeys.ROW_KEY, "curr_date", location)
  with DatasetMapperFromRaw {

    final val ID = "id_2"

    override def rowKeyColumn(df: DataFrame): Column = concat_ws("--", df(ID), df("curr_date"))

    override def partitionColumn(df: DataFrame): Column = concat_ws("/", lit("parent"), df(ID))
  }