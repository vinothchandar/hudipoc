package com.github.leafty.hudi

import com.uber.hoodie.DataSourceWriteOptions
import com.uber.hoodie.config.HoodieWriteConfig


/**
  * Defines a Hoodie dataaset
  *
  * @param name table name
  *             @see [[HoodieWriteConfig.TABLE_NAME]]
  * @param primaryKey name of field uniquely identifying a record.
  *                   If there is no natural/business "id" then this can be synthesized from multiple fields.
  *             @see [[DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY]]
  * @param mergeBy name of field used to merge updates (records with same [[primaryKey]]) in different ... updates
  *             @see [[DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY]]
  */
case class DatasetDef(name: String, primaryKey: String, mergeBy: String) {
  def asMap: Map[String, String] = Map(
    HoodieWriteConfig.TABLE_NAME -> name,
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> primaryKey,
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> mergeBy
  )
}
