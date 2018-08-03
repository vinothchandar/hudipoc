package com.github.leafty.hudi

import org.apache.spark.sql.{Column, DataFrame}


/**
  * Encapsulates mapping from raw to Hoodie format
  */
trait DatasetMapperFromRaw {

  def rowKeyColumn(df: DataFrame) : Column

  def partitionColumn(df: DataFrame) : Column

  def mapFromRaw(df: DataFrame) : DataFrame =
    df.withColumn(HoodieKeys.PARTITION_KEY, partitionColumn(df))
      .withColumn(HoodieKeys.ROW_KEY, rowKeyColumn(df))
}
