package com.gitlab.leafty.test.hudi

import com.uber.hoodie.common.HoodieTestDataGenerator
import com.uber.hoodie.common.util.FSUtils
import com.uber.hoodie.config.HoodieWriteConfig
import com.uber.hoodie.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.junit.rules.TemporaryFolder
import org.scalatest.BeforeAndAfterAll

class FannieMaeHudiSpec extends AsyncBaseSpec with BeforeAndAfterAll {

  Logger.getLogger("org.apache").setLevel(Level.WARN)

  lazy val spark: SparkSession = getSparkSession

  import spark.implicits._

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
    spark.read
      .format("csv")
      .option("header", "true")
      .load(url.getPath)
  }

  def getPerformances: DataFrame = {
    val url = getClass.getResource("/ds_0002")
    spark.read
      .format("csv")
      .option("header", "true")
      .load(url.getPath)
  }

  protected def getSparkSession: SparkSession = {
    val builder = SparkSession.builder()
      .appName("test-hudi")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    builder.getOrCreate()
  }

  override protected def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

}
