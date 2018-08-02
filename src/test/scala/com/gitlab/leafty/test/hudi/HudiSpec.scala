package com.gitlab.leafty.test.hudi

import com.uber.hoodie.common.HoodieTestDataGenerator
import com.uber.hoodie.common.util.FSUtils
import com.uber.hoodie.config.HoodieWriteConfig
import com.uber.hoodie.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
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
    "work" ignore {
      val ds = spark.createDataset(spark.sparkContext.range(0, 100, numSlices = 4))
      val s = ds.select(sum(ds(ds.columns(0)))).as[Long]

      s.count() shouldBe 1
      s.collect().head shouldBe (100*99/2)
    }
  }

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "timestamp",
    HoodieWriteConfig.TABLE_NAME -> "hoodie_test"
  )

  "hudi" should {
    "handle one write operation" ignore {
      import scala.collection.JavaConversions._

      val dataGen = new HoodieTestDataGenerator()
      val folder = new TemporaryFolder()
      folder.create()
      val basePath = folder.getRoot.getAbsolutePath
      val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)

      val records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("001", 100)).toList
      val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2).toDS())
      inputDF1.show()
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

    "handle a CopyOnWrite data set" ignore {
      import scala.collection.JavaConversions._

      val dataGen = new HoodieTestDataGenerator()
      val folder = new TemporaryFolder()
      folder.create()
      val basePath = folder.getRoot.getAbsolutePath
      val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)

      // Insert Operation
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

      val records2 = DataSourceTestUtils.convertToStringList(dataGen.generateUpdates("001", 100)).toList
      val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2).toDS())
      val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

      // Upsert Operation
      inputDF2.write.format("com.uber.hoodie")
        .options(commonOpts)
        .mode(SaveMode.Append)
        .save(basePath)

      val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

      HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size() shouldBe 2

      // Read RO View
      val hoodieROViewDF2 = spark.read.format("com.uber.hoodie")
        .load(basePath + "/*/*/*/*");
      hoodieROViewDF2.count() shouldBe 100 // still 100, since we only updated


      // Read Incremental View
      val hoodieIncViewDF2 = spark.read.format("com.uber.hoodie")
        .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
        .load(basePath)

      hoodieIncViewDF2.count() shouldBe uniqueKeyCnt // 100 records must be pulled
      val countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
      countsPerCommit.length shouldBe 1
      countsPerCommit(0).get(0) shouldBe commitInstantTime2
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
