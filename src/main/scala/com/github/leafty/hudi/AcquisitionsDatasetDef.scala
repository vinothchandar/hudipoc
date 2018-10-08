package com.github.leafty.hudi

import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.{Column, DataFrame}


class AcquisitionsDatasetDef(location: Option[String] = None)
  extends DatasetDef("acquisitions", HoodieKeys.ROW_KEY, "start_date", location) with DatasetDefExt {

  final val ID = "id"

  override val mapper : DatasetMapperFromRaw = new DatasetMapperFromRaw {

      override def rowKeyColumn: Column = col(ID)

      override def partitionColumn: Column = concat_ws("/", lit("seller"), col("seller"))
  }

  override def transform : DataFrame ⇒ DataFrame = mapper.mapFromRaw
}

