package com.github.leafty.hudi

import org.apache.spark.sql.{Column, DataFrame}


/**
  * Encapsulates mapping from raw to Hoodie format
  */
abstract class DatasetMapperFromRaw(df : DataFrame) {

  def rowKeyColumn: Column

  def partitionColumn: Column

  def mapFromRaw : DataFrame =
    df.withColumn(HoodieKeys.PARTITION_KEY, partitionColumn)
      .withColumn(HoodieKeys.ROW_KEY, rowKeyColumn)
}
