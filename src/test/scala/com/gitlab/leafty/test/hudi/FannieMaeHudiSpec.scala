package com.gitlab.leafty.test.hudi

import com.github.leafty.hudi.DatasetDef
import com.uber.hoodie.DataSourceWriteOptions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{concat_ws, lit}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.junit.rules.TemporaryFolder

import scala.concurrent.Promise

class FannieMaeHudiSpec extends AsyncBaseSpec {

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com.uber.hoodie").setLevel(Level.WARN)

  lazy implicit val spark: SparkSession = getSparkSession

  "test data sets" should {
    "contain acquisitions" in {
      val df = getAcquisitions
      df.count() shouldBe 8
    }

    "contain performances" in {
      val df = getPerformances

      // row counts from each csv file
      val count = 40 + 16 + 40 + 58 + 31 + 12 + 51 + 36

      df.count() shouldBe count
    }

    "contain acquisitions splits" in {
      val (df1, df2) = getAcquisitionsSplit
      df1.columns should contain theSameElementsAs getAcquisitions.columns
      df2.columns should contain theSameElementsAs getAcquisitions.columns
      df1.count() shouldBe 4
      df2.count() shouldBe 4
    }

    "contain performances splits" in {
      val map = getPerformancesSplit
      map should have size 8

      // row counts from each csv file
      val counts = Seq(40, 16, 40, 58, 31, 12, 51, 36)
      val sizes = for { (df1, df2, df3) <- map.values } yield { df1.count() + df2.count() + df3.count() }

      sizes should contain theSameElementsAs counts
    }
  }

  lazy implicit val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition"
  )

  /**
    * How is [[DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY]] used exactly?
    * Is it for merging updates?
    */
  val acquisitionsDs = DatasetDef("acquisitions", "id", "start_date", Some(tmpLocation))

    /**
      * #todo why use "id" for "acquisitionsOpts" and not use "id_2" here ?
      */
  val performancesDs = DatasetDef("performances", "_row_key", "curr_date", Some(tmpLocation))


  def tmpLocation : String = {
    val folder = new TemporaryFolder()
    folder.create()
    folder.getRoot.getAbsolutePath
  }

  "hudi" should {

    val performancesCommitInstantTime1: Promise[String] = Promise()
    val performancesCommitInstantTime2: Promise[String] = Promise()
    val performancesCommitInstantTime3: Promise[String] = Promise()

    "ingest first half of 'acquisitions'" in {

      val (df, _) = getAcquisitionsSplit

      acquisitionsDs.writeReplace(df)

      val hasNewCommits = acquisitionsDs.hasNewCommits
      hasNewCommits shouldBe true

      // read back data from hudi
      val hudiDf = acquisitionsDs.read()

      hudiDf.count() shouldBe df.count()
    }

    "ingest first half of 'performances' (1/2)" in {
      // Group 1 from acquisitions, 1st third
      val (acquisitionsDf, _) = getAcquisitionsSplit
      val ids = acquisitionsDf.select(acquisitionsDf("id")).distinct().collect().map(_.getString(0))

      val map = getPerformancesSplit
      val dfs = for { id <- ids } yield map(id)._1

      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getPerformances.schema)
      val insertDf = dfs.fold(emptyDF){ (df1, df2) => df1.union(df2) }

      performancesDs.writeReplace(insertDf)

      performancesCommitInstantTime1.success(performancesDs.latestCommit)

      val hasNewCommits = performancesDs.hasNewCommits
      hasNewCommits shouldBe true

      // read back data from hudi
      val hudiDf = performancesDs.read()

      hudiDf.count() shouldBe insertDf.count()
    }

    "ingest first half of 'performances' (2/2)" in {
      // Group 1 from acquisitions, 2nd third
      val (acquisitionsDf, _) = getAcquisitionsSplit
      val ids = acquisitionsDf.select(acquisitionsDf("id")).distinct().collect().map(_.getString(0))

      val map = getPerformancesSplit
      val dfs = for { id <- ids } yield map(id)._2

      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getPerformances.schema)
      val insertDf = dfs.fold(emptyDF){ (df1, df2) => df1.union(df2) }

      performancesDs.writeAppend(insertDf)

      performancesCommitInstantTime2.success(performancesDs.latestCommit)

      val commitCount = performancesDs.listCommitsSince.length
      commitCount shouldBe 2

      // read back data from hudi (using incremental view)
      performancesCommitInstantTime1.isCompleted shouldBe true
      for {
        commitTime <- performancesCommitInstantTime1.future

        hudiDf = performancesDs.read(Some(commitTime))

      } yield {

        hudiDf.count() shouldBe insertDf.count()
      }
    }

    "have ingested first half of ds_0001 x ds_0002 consistently" in {
      val acquisitionsDf = acquisitionsDs.read()

      val performancesDf = performancesDs.read()

      val joinedDf = acquisitionsDf.join(performancesDf, acquisitionsDf("id") === performancesDf("id_2"), "inner")

      joinedDf.count() shouldBe performancesDf.count()

      val a_ids = acquisitionsDf.select(acquisitionsDf("id")).distinct().collect().map(_.getString(0))
      val j_ids = joinedDf.select(joinedDf("id")).distinct().collect().map(_.getString(0))

      a_ids should contain theSameElementsAs j_ids
    }

    "ingest second half of 'acquisitions'" in {
      val (_, df) = getAcquisitionsSplit

      acquisitionsDs.writeAppend(df)

      val commitCount = acquisitionsDs.listCommitsSince.length
      commitCount shouldBe 2

      // read back data from hudi
      val hudiDf = acquisitionsDs.read()

      hudiDf.count() shouldBe getAcquisitions.count()
    }

    "ingest second half of 'performances' (1/2)" in {
      // Group 2 from acquisitions, 1st third
      val (_, acquisitionsDf) = getAcquisitionsSplit
      val ids = acquisitionsDf.select(acquisitionsDf("id")).distinct().collect().map(_.getString(0))

      val map = getPerformancesSplit
      val dfs = for { id <- ids } yield map(id)._1

      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getPerformances.schema)
      val insertDf = dfs.fold(emptyDF){ (df1, df2) => df1.union(df2) }

      performancesDs.writeAppend(insertDf)

      performancesCommitInstantTime3.success(performancesDs.latestCommit)

      val commitCount = performancesDs.listCommitsSince.length
      commitCount shouldBe 3

      // read back data from hudi (using incremental view)
      performancesCommitInstantTime2.isCompleted shouldBe true

      for {
        commitTime <- performancesCommitInstantTime2.future
        hudiDf = performancesDs.read(Some(commitTime))

      } yield {

        hudiDf.count() shouldBe insertDf.count()
      }
    }

    "ingest second half of 'performances' (2/2)" in {
      // Upsert all performances data
      val insertDf = getPerformances

      //performancesDs.writeAppend(insertDf)
      insertDf.write
        .format("com.uber.hoodie")
        .options(commonOpts)
        .options(performancesDs.asMap)
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL) // #todo UPSERT only here or for all "appends"?
        .mode(SaveMode.Append)
        .save(performancesDs.location.get)

      val commitCount = performancesDs.listCommitsSince.length
      commitCount shouldBe 4

      // read back data from hudi (using incremental view)
      performancesCommitInstantTime3.isCompleted shouldBe true
      for {
        commitTime <- performancesCommitInstantTime3.future
        hudiDf = performancesDs.read(Some(commitTime))

      } yield {

        hudiDf.count() shouldBe insertDf.count()
      }
    }

    "have ingested second half of ds_0001 x ds_0002 consistently" in {
      val acquisitionsDf = acquisitionsDs.read()

      val performancesDf = performancesDs.read()

      val joinedDf = acquisitionsDf.join(performancesDf, acquisitionsDf("id") === performancesDf("id_2"), "inner")

      joinedDf.count() shouldBe performancesDf.count()
      performancesDf.count() shouldBe getPerformances.count()

      val a_ids = acquisitionsDf.select(acquisitionsDf("id")).distinct().collect().map(_.getString(0))
      val j_ids = joinedDf.select(joinedDf("id")).distinct().collect().map(_.getString(0))

      a_ids should contain theSameElementsAs j_ids
    }
  }

  /**
    * @return acquisitions split in two halves
    */
  def getAcquisitionsSplit: (DataFrame, DataFrame) = {
    val df1 = getAcquisitions
    val thr = df1.count() / 2

    val rdd2 = df1.rdd.zipWithUniqueId()
    val rdd3_1 = rdd2.filter{ case (_, rank) => rank < thr }.map(_._1)
    val rdd3_2 = rdd2.filter{ case (_, rank) => rank >= thr }.map(_._1)

    val df4_1 = spark.createDataFrame(rdd3_1, df1.schema)
    val df4_2 = spark.createDataFrame(rdd3_2, df1.schema)

    (df4_1, df4_2)
  }

  /**
    * @return performances, with each group split in thirds
    */
  def getPerformancesSplit: Map[String, (DataFrame, DataFrame, DataFrame)] = {
    val df1 = getPerformances
    val ids = df1.select(df1("id_2")).distinct().collect().map(_.getString(0))

    val mapped = for { id <- ids } yield id -> df1.filter(df1("id_2") === id)

    val splitMapped = for { (id, df) <- mapped } yield {
      val rdd = df.rdd.zipWithUniqueId()

      val thr1 = df.count() / 3
      val thr2 = 2 * df.count() / 3

      val rdd_1 = rdd.filter{ case (_, rank) => rank < thr1 }.map(_._1)
      val rdd_2 = rdd.filter{ case (_, rank) => rank >= thr1 && rank < thr2 }.map(_._1)
      val rdd_3 = rdd.filter{ case (_, rank) => rank >= thr2 }.map(_._1)

      val df_1 = spark.createDataFrame(rdd_1, df.schema)
      val df_2 = spark.createDataFrame(rdd_2, df.schema)
      val df_3 = spark.createDataFrame(rdd_3, df.schema)

      id -> (df_1, df_2, df_3)
    }

    splitMapped.toMap
  }

  def getAcquisitions: DataFrame = {
    val url = getClass.getResource("/ds_0001")
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load(url.getPath)

    // Add partition column
    val partitionColumn = concat_ws("/", lit("seller"), df("seller"))
    df.withColumn("partition", partitionColumn)
  }

  def getPerformances: DataFrame = {
    val url = getClass.getResource("/ds_0002")
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load(url.getPath)

    // Add partition column
    val partitionColumn = concat_ws("/", lit("parent"), df("id_2"))

    // Add row key
    val rowKeyColumn = concat_ws("--", df("id_2"), df("curr_date"))

    df.withColumn("partition", partitionColumn)
      .withColumn("_row_key", rowKeyColumn)
  }

  protected def getSparkSession: SparkSession = {
    val builder = SparkSession.builder()
      .appName("test-hudi")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    builder.getOrCreate()
  }

}
