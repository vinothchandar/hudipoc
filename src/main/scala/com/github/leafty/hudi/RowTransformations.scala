package com.github.leafty.hudi

import com.github.leafty.hudi.RowTransformationsRegistry.RowTransformation
import org.apache.spark.sql.DataFrame


trait RowTransformations {

  def transformations: List[RowTransformation]

  def applyTransformations(df: DataFrame): DataFrame
}

