package com.gitlab.leafty.test.hudi

import com.uber.hoodie.common.HoodieTestDataGenerator
import com.uber.hoodie.common.util.FSUtils
import com.uber.hoodie.config.HoodieWriteConfig
import com.uber.hoodie.{DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.junit.rules.TemporaryFolder
import org.scalatest.BeforeAndAfterAll

class HudiSpec extends AsyncBaseSpec with BeforeAndAfterAll {

  Logger.getLogger("org.apache").setLevel(Level.WARN)

  lazy val spark: SparkSession = getSparkSession

  import spark.implicits._

  "spark" should {
    "work" in {
      val ds = spark.createDataset(spark.sparkContext.range(0, 100, numSlices = 4))
      val s = ds.select(sum(ds(ds.columns(0)))).as[Long]

      s.count() shouldBe 1
      s.collect().head shouldBe (100*99/2)
    }
  }

  "hudi" should {
    "handle one write operation" in {
      import scala.collection.JavaConversions._

      val commonOpts = Map(
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4",
        DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
        DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
        DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "timestamp",
        HoodieWriteConfig.TABLE_NAME -> "hoodie_test"
      )

      val dataGen = new HoodieTestDataGenerator()
      val folder = new TemporaryFolder()
      folder.create()
      val basePath = folder.getRoot.getAbsolutePath
      val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)

      val records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("001", 100)).toList
      val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2).toDS())
      inputDF1.write.format("com.uber.hoodie")
        .options(commonOpts)
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Overwrite)
        .save(basePath)

      HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000") shouldBe true
      val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

      // Read RO View
      val hoodieROViewDF1 = spark.read.format("com.uber.hoodie")
        .load(basePath + "/*/*/*/*")
      hoodieROViewDF1.count() shouldBe 100
    }
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
