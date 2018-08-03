package com.github.leafty.hudi

import com.uber.hoodie.common.util.FSUtils
import com.uber.hoodie.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import com.uber.hoodie.config.HoodieWriteConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


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
case class DatasetDef(name: String, primaryKey: String, mergeBy: String, location : Option[String] = None) {

  def asMap: Map[String, String] = Map(
    HoodieWriteConfig.TABLE_NAME -> name,
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> primaryKey,
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> mergeBy
  )

  def hasNewCommits(implicit session: SparkSession) : Boolean = {
    import DataSetDef._
    HoodieDataSourceHelpers.hasNewCommits(getFs(location.get), location.get, "000")
  }

  def latestCommit(implicit session: SparkSession) : String = {
    import DataSetDef._
    HoodieDataSourceHelpers.latestCommit(getFs(location.get), location.get)
  }

  def listCommitsSince(implicit session: SparkSession) : List[String] = {
    import DataSetDef._
    import scala.collection.JavaConverters._
    HoodieDataSourceHelpers.listCommitsSince(getFs(location.get), location.get,  "000").asScala.toList
  }

  def writeReplace(df: DataFrame)(implicit commonOpts: Map[String, String]): Unit =
      df.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .options(this.asMap)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(this.location.get)

  def writeAppend(df: DataFrame)(implicit commonOpts: Map[String, String]): Unit =
      df.write
      .format("com.uber.hoodie")
      .options(commonOpts)
      .options(this.asMap)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(this.location.get)

  def read(commitTime: String)(implicit session: SparkSession) : DataFrame  = {
      session.read
      .format("com.uber.hoodie")
      .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitTime)
      .load(this.location.get)

  }
}

object DataSetDef {

  def getFs(path: String)(implicit session: SparkSession) = FSUtils.getFs(path, session.sparkContext.hadoopConfiguration)
}

