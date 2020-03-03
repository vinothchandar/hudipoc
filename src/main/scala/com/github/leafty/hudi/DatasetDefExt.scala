package com.github.leafty.hudi

import org.apache.spark.sql.DataFrame

// dont follow this.. there are transformations before the final data frame is prepped for write into Hudi, correct?
// If so, should this be coupled with the data set?
trait DatasetDefExt {

  val mapper : DatasetMapperFromRaw = null

  val transformer : RowTransformations = null

  def transform : DataFrame ⇒ DataFrame

}
