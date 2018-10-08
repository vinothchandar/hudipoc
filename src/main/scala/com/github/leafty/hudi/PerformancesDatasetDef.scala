package com.github.leafty.hudi

import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.{Column, DataFrame}


class PerformancesDatasetDef(location: Option[String] = None)
  extends DatasetDef("performances", HoodieKeys.ROW_KEY, "curr_date", location) with DatasetDefExt {

  final val ID = "id_2"

  override val mapper: DatasetMapperFromRaw = new DatasetMapperFromRaw() {

      override def rowKeyColumn: Column = concat_ws("--", col(ID), col("curr_date"))

      override def partitionColumn: Column = concat_ws("/", lit("parent"), col(ID))

  }

  override val transformer : RowTransformations = new RowTransformations with PerformancesRowTransformations {

      def transformations = List(curr_date, foreclosure_amount, remain_to_mat)

      import com.github.mrpowers.spark.daria.sql.DataFrameExt._

      /**
        * Can be overridden for more control over application of transformations
        * #design Resulting data frame has same cardinality
        */
      def applyTransformations(df: DataFrame): DataFrame = df.composeTransforms(transformations)
  }

  override def transform : DataFrame ⇒ DataFrame = mapper.mapFromRaw _ andThen transformer.applyTransformations
}

