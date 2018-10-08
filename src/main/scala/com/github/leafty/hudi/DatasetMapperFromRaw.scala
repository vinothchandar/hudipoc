package com.github.leafty.hudi

import org.apache.spark.sql.{Column, DataFrame}


/**
  * Encapsulates mapping from raw to Hoodie format
  */
abstract class DatasetMapperFromRaw {

  def rowKeyColumn: Column

  def partitionColumn: Column

  def mapFromRaw(df : DataFrame): DataFrame =
    df.withColumn(HoodieKeys.PARTITION_KEY, partitionColumn)
      .withColumn(HoodieKeys.ROW_KEY, rowKeyColumn)
}
