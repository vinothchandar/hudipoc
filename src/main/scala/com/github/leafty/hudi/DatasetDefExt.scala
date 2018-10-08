package com.github.leafty.hudi

import org.apache.spark.sql.DataFrame


trait DatasetDefExt {

  val mapper : DatasetMapperFromRaw = null

  val transformer : RowTransformations = null

  def transform : DataFrame ⇒ DataFrame

}
