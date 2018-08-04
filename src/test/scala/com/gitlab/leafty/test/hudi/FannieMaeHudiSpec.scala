package com.gitlab.leafty.test.hudi

import com.github.leafty.hudi._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.rules.TemporaryFolder

import scala.concurrent.Promise


/**
  *
  */
class FannieMaeHudiSpec extends AsyncBaseSpec {

  lazy val log = Logger.getLogger("hudi.test")

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
      val dfs = getAcquisitions_2Split

      dfs(0).columns should contain theSameElementsAs getAcquisitions.columns
      dfs(1).columns should contain theSameElementsAs getAcquisitions.columns

      dfs(0).count() shouldBe 4
      dfs(1).count() shouldBe 4
    }

    "contain performances splits" in {
      val map = getPerformances_3Split
      map should have size 8

      // row counts from each csv file
      val counts = Seq(40, 16, 40, 58, 31, 12, 51, 36)
      val sizes = for {(df1, df2, df3) <- map.values} yield {
        df1.count() + df2.count() + df3.count()
      }

      sizes should contain theSameElementsAs counts
    }
  }

  val acquisitionsDs = new AcquisitionsDatasetDef(Some(tmpLocation))

  val performancesDs = new PerformancesDatasetDef(Some(tmpLocation))

  def tmpLocation: String = {
    val folder = new TemporaryFolder()
    folder.create()
    folder.getRoot.getAbsolutePath
  }

  def getIds(df: DataFrame, id: String, distinct: Boolean = true) : Array[String] = {
    (if (distinct)
      df.select(df(id)).distinct()
    else
      df.select(df(id))).collect().map(_.getString(0))
  }


  import DataSetDef._

  "hudi" should {

    object CommitRuntime {
      var performancesCommits: List[Promise[String]] = List.empty

      def init = performancesCommits = List(Promise(), Promise(), Promise())
    }

    /**
      * Ingest the first part
      */
    "ingest 'acquisitions': first 1/2" in {

      CommitRuntime.init

      val df = getAcquisitions_2Split(0)

      acquisitionsDs.writeReplace(df)

      acquisitionsDs.hasNewCommits("000") shouldBe true

      acquisitionsDs.listCommitsSince("000").length shouldBe 1

      acquisitionsDs.read().count() shouldBe df.count()
    }

    "for (1), ingest their 'performances': first 1/3" in {

      // Group 1 from acquisitions, 1st third
      val acquisitionsDf = getAcquisitions_2Split(0)

      val ids = getIds(acquisitionsDf, "id")

      val map = getPerformances_3Split
      val dfs = for {id <- ids} yield map(id)._1

      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getPerformances.schema)
      val insertDf = dfs.fold(emptyDF) { (df1, df2) => df1.union(df2) }

      log.info(s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, "id_2", false).mkString(", ")}""".stripMargin)

      performancesDs.writeReplace(insertDf)

      CommitRuntime.performancesCommits(0).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 1

      performancesDs.read().count() shouldBe insertDf.count()
    }

    "for (1), ingest their 'performances': second 1/3" in {
      // Group 1 from acquisitions, 2nd third
      val acquisitionsDf = getAcquisitions_2Split(0)
      val ids = getIds(acquisitionsDf, "id")

      val map = getPerformances_3Split
      val dfs = for {id <- ids} yield map(id)._2

      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getPerformances.schema)
      val insertDf = dfs.fold(emptyDF) { (df1, df2) => df1.union(df2) }

      log.info(s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, "id_2", false).mkString(", ")}""".stripMargin)

      performancesDs.writeAppend(insertDf)

      CommitRuntime.performancesCommits(1).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 2

      // read back data from hudi (using incremental view)
      //performancesCommitInstantTime1.isCompleted shouldBe true
      for {
        commitTime <- CommitRuntime.performancesCommits(0).future

        hudiDf = performancesDs.read(Some(commitTime))

      } yield {

        hudiDf.count() shouldBe insertDf.count()
      }
    }

    "for (1), ingest their 'performances': third 1/3" in {

      // Group 1 from acquisitions, 3rd third
      val acquisitionsDf = getAcquisitions_2Split(0)
      val ids = getIds(acquisitionsDf, "id")

      val map = getPerformances_3Split
      val dfs = for {id <- ids} yield map(id)._3

      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getPerformances.schema)
      val insertDf = dfs.fold(emptyDF) { (df1, df2) => df1.union(df2) }

      log.info(s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, "id_2", false).mkString(", ")}""".stripMargin)

      performancesDs.writeAppend(insertDf)

      CommitRuntime.performancesCommits(2).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 3

      // read back data from hudi (using incremental view)
      //performancesCommitInstantTime1.isCompleted shouldBe true
      for {
        commitTime <- CommitRuntime.performancesCommits(1).future

        hudiDf = performancesDs.read(Some(commitTime))

      } yield {

        hudiDf.count() shouldBe insertDf.count()
      }
    }

    "check have ingested first half of ds_0001 x ds_0002 consistently" in {

      acquisitionsDs.listCommitsSince("000").length shouldBe 1

      val acquisitionsDf = acquisitionsDs.read()

      performancesDs.listCommitsSince("000").length shouldBe 3

      val performancesDf = performancesDs.read()

      val joinedDf = acquisitionsDf.join(performancesDf, acquisitionsDf("id") === performancesDf("id_2"), "inner")

      joinedDf.count() shouldBe performancesDf.count()

      val a_ids = getIds(acquisitionsDf, "id")
      val j_ids = getIds(joinedDf, "id")

      a_ids should contain theSameElementsAs j_ids
    }

    /**
      * Ingest the second part
      */
    "ingest 'acquisitions': second 1/2" in {

      CommitRuntime.init

      val df = getAcquisitions_2Split(1)

      acquisitionsDs.writeAppend(df)

      acquisitionsDs.hasNewCommits("000") shouldBe true

      acquisitionsDs.listCommitsSince("000").length shouldBe 2

      acquisitionsDs.read().count() shouldBe getAcquisitions.count()
    }

    "for (2), ingest their 'performances' : first (1/3)" in {

      // Group 1 from acquisitions, 1st third
      val acquisitionsDf = getAcquisitions_2Split(1)

      val ids = getIds(acquisitionsDf, "id")

      val map = getPerformances_3Split
      val dfs = for {id <- ids} yield map(id)._1

      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getPerformances.schema)
      val insertDf = dfs.fold(emptyDF) { (df1, df2) => df1.union(df2) }

      log.info(s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, "id_2", false).mkString(", ")}""".stripMargin)

      performancesDs.writeAppend(insertDf)

      CommitRuntime.performancesCommits(0).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 1 + 3

      //#todo - continue from previous 3 commits
      // performancesDs.read().count() shouldBe insertDf.count()
    }

    "for (2), ingest their 'performances': second 1/3" in {
      // Group 1 from acquisitions, 2nd third
      val acquisitionsDf = getAcquisitions_2Split(1)
      val ids = getIds(acquisitionsDf, "id")

      val map = getPerformances_3Split
      val dfs = for {id <- ids} yield map(id)._2

      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getPerformances.schema)
      val insertDf = dfs.fold(emptyDF) { (df1, df2) => df1.union(df2) }

      log.info(s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, "id_2", false).mkString(", ")}""".stripMargin)

      performancesDs.writeAppend(insertDf)

      CommitRuntime.performancesCommits(1).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 2 + 3

      // read back data from hudi (using incremental view)
      //performancesCommitInstantTime1.isCompleted shouldBe true
      for {
        commitTime <- CommitRuntime.performancesCommits(0).future

        hudiDf = performancesDs.read(Some(commitTime))

      } yield {

        hudiDf.count() shouldBe insertDf.count()
      }
    }

    "for (2), ingest their 'performances': third 1/3" in {

      // Group 1 from acquisitions, 3rd third
      val acquisitionsDf = getAcquisitions_2Split(1)
      val ids = getIds(acquisitionsDf, "id")

      val map = getPerformances_3Split
      val dfs = for {id <- ids} yield map(id)._3

      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], getPerformances.schema)
      val insertDf = dfs.fold(emptyDF) { (df1, df2) => df1.union(df2) }

      log.info(s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, "id_2", false).mkString(", ")}""".stripMargin)

      performancesDs.writeAppend(insertDf)

      CommitRuntime.performancesCommits(2).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 3 + 3

      // read back data from hudi (using incremental view)
      //performancesCommitInstantTime1.isCompleted shouldBe true
      for {
        commitTime <- CommitRuntime.performancesCommits(1).future

        hudiDf = performancesDs.read(Some(commitTime))

      } yield {

        hudiDf.count() shouldBe insertDf.count()
      }
    }

    "check have ingested second half of ds_0001 x ds_0002 consistently" in {

      acquisitionsDs.listCommitsSince("000").length shouldBe 2

      val acquisitionsDf = acquisitionsDs.read()

      performancesDs.listCommitsSince("000").length shouldBe 6

      val performancesDf = performancesDs.read()

      val joinedDf = acquisitionsDf.join(performancesDf, acquisitionsDf("id") === performancesDf("id_2"), "inner")

      joinedDf.count() shouldBe performancesDf.count()

      val a_ids = getIds(acquisitionsDf, "id")
      val j_ids = getIds(joinedDf, "id")

      a_ids should contain theSameElementsAs j_ids

      performancesDf.count() shouldBe getPerformances.count()
    }
  }

  /**
    * @return acquisitions split in two halves
    */
  def getAcquisitions_2Split: List[DataFrame] = {
    val df1 = getAcquisitions
    val thr = df1.count() / 2

    val rdd2 = df1.rdd.zipWithUniqueId()
    val rdd3_1 = rdd2.filter { case (_, rank) => rank < thr }.map(_._1)
    val rdd3_2 = rdd2.filter { case (_, rank) => rank >= thr }.map(_._1)

    val df4_1 = spark.createDataFrame(rdd3_1, df1.schema)
    val df4_2 = spark.createDataFrame(rdd3_2, df1.schema)

    List(df4_1, df4_2)
  }

  /**
    * @return performances, with each group split in thirds
    */
  def getPerformances_3Split: Map[String, (DataFrame, DataFrame, DataFrame)] = {
    val df1 = getPerformances
    val ids = getIds(df1, "id_2")

    val mapped = for {id <- ids} yield id -> df1.filter(df1("id_2") === id)

    val splitMapped = for {(id, df) <- mapped} yield {
      val rdd = df.rdd.zipWithUniqueId()

      val thr1 = df.count() / 3
      val thr2 = 2 * df.count() / 3

      val rdd_1 = rdd.filter { case (_, rank) => rank < thr1 }.map(_._1)
      val rdd_2 = rdd.filter { case (_, rank) => rank >= thr1 && rank < thr2 }.map(_._1)
      val rdd_3 = rdd.filter { case (_, rank) => rank >= thr2 }.map(_._1)

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

    acquisitionsDs.mapFromRaw(df)
  }

  def getPerformances: DataFrame = {
    val url = getClass.getResource("/ds_0002")
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load(url.getPath)

    performancesDs.mapFromRaw(df)
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
