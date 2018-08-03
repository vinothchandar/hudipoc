package com.github.leafty.hudi

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, concat_ws, lit}


class AcquisitionsDatasetDef(location: Option[String] = None) extends DatasetDef("acquisitions", HoodieKeys.ROW_KEY, "start_date", location)
    with DatasetMapperFromRaw {

    override def rowKeyColumn(df: DataFrame): Column = col("id")

    override def partitionColumn(df: DataFrame): Column = concat_ws("/", lit("seller"), df("seller"))
  }