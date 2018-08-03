package com.github.leafty.hudi

import org.apache.spark.sql.functions.{concat_ws, lit}
import org.apache.spark.sql.{Column, DataFrame}


class PerformancesDatasetDef(location: Option[String] = None) extends DatasetDef("performances", HoodieKeys.ROW_KEY, "curr_date", location)
  with DatasetMapperFromRaw {

    override def rowKeyColumn(df: DataFrame): Column = concat_ws("--", df("id_2"), df("curr_date"))

    override def partitionColumn(df: DataFrame): Column = concat_ws("/", lit("parent"), df("id_2"))
  }