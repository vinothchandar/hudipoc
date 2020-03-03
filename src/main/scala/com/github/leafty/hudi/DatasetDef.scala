package com.github.leafty.hudi

import org.apache.hudi.common.util.FSUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object HoodieKeys {

  val ROW_KEY: String = "_row_key"
  val PARTITION_KEY: String = "partition"

}

/**
  * Defines a Hoodie dataaset
  *
  * @param name table name
  * @see [[HoodieWriteConfig.TABLE_NAME]]
  * @param rowKey name of field uniquely identifying a record.
  *               If there is no natural/business "id" then this can be synthesized from multiple fields.
  * @see [[DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY]]
  * @param mergeByKey name of field used to merge updates (records with same [[rowKey]]) in different ... updates
  * @see [[DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY]]
  */
// TableDef probably is a better fit for how we renamed everything now.
// While this is a good start.. In the general case, the key may be derived from  the incoming data using the KeyGenerator abstraction
// Same for the mergeByKey, Its the common case to use a field's value, but can be complex using a custom HoodieRecordPayload implementation.
// Should table type be here as well?
// if you are registeing the table into Hive, then a HiveSyncConfig can also be part of this..
abstract case class DatasetDef(name: String, rowKey: String, mergeByKey: String, location: Option[String] = None) {

  /**
    * #todo How is [[DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY]] used exactly?
    * Is it for merging updates?
    */
  protected def asMap: Map[String, String] = Map(
    HoodieWriteConfig.TABLE_NAME -> name,
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> rowKey,
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> mergeByKey
  )

  /**
    *
    */
  def hasNewCommits(commitTimestamp: String)(implicit session: SparkSession): Boolean = {
    import DataSetDef._
    HoodieDataSourceHelpers.hasNewCommits(getFs(location.get), location.get, commitTimestamp)
  }

  /**
    *
    */
  def latestCommit(implicit session: SparkSession): String = {
    import DataSetDef._
    HoodieDataSourceHelpers.latestCommit(getFs(location.get), location.get)
  }

  /**
    *
    */
  def listCommitsSince(instantTimestamp: String)(implicit session: SparkSession): List[String] = {
    import DataSetDef._

    import scala.collection.JavaConverters._
    HoodieDataSourceHelpers.listCommitsSince(getFs(location.get), location.get, instantTimestamp).asScala.toList
  }

  import DataSetDef._

  /**
    *
    */
  def writeReplace(df: DataFrame)(implicit commonOpts: Map[String, String]): Unit =
    df.write
      .format(apacheHudiFormat)
      .options(commonOpts)
      .options(this.asMap)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(this.location.get)

  /**
    *
    */
  def writeAppend(df: DataFrame)(implicit commonOpts: Map[String, String]): Unit =
    df.write
      .format(apacheHudiFormat)
      .options(commonOpts)
      .options(this.asMap)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(this.location.get)

  /**
    * #todo Is this api really needed or there is a better way?
    * Why UPSERT only here or for all "appends"?
    */
  def writeUpsert(df: DataFrame)(implicit commonOpts: Map[String, String]): Unit =
    df.write
      .format(apacheHudiFormat)
      .options(commonOpts)
      .options(this.asMap)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(this.location.get)

  /**
    *
    * @param commitTime a real commit or will real all
    */
  def read(commitTime: Option[String] = None)(implicit session: SparkSession): DataFrame = {
    commitTime match {
      case Some(c) ⇒ session.read
        .format(apacheHudiFormat)
        .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, c)
        .load(this.location.get)

      case None ⇒ session.read
        .format(apacheHudiFormat)
        .load(this.location.get + "/*/*/*")
    }
  }
}

object DataSetDef {

  val apacheHudiFormat = "org.apache.hudi"

  lazy implicit val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    // Should this be in the dataset def above? this is very similar to the rowKey
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> HoodieKeys.PARTITION_KEY
  )

  def getFs(path: String)(implicit session: SparkSession) = FSUtils.getFs(path, session.sparkContext.hadoopConfiguration)
}

