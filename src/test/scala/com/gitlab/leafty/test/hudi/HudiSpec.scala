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

  // #todo use specs2 to run "sequential"ly

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

    /**
      * Split each csv under data_hub/ds_0001 in half (1/2) and then each group in roughly 1/3 rds
      */
    "ingest first 1/2 of 'acquisitions' ds_0001 in 2 commits" in {

      /**
        * ... and ingest 2 of the three 3rds
        */

      fail("Not implemented yet")
    }

    /**
      * Split each csv under data_hub/ds_0002 in roughly 1/3 rds
      */
    "ingest first 1/2 of 'performances' ds_0002 in 2 commits" in {

      /**
        * ... and ingest 2 of the three 3rds
        */
      fail("Not implemented yet")
    }

    "check ingested data for ds_0001 is consistent" in {


      fail("Not implemented yet")
    }

    "check ingested data for ds_0002 is consistent" in {


      fail("Not implemented yet")
    }

    "check ingested data for ds_0001 x ds_0002 is consistent" in {

      // ... where 'x' means `natural join` by `id`

      fail("Not implemented yet")
    }

    "ingest second 1/2 of 'acquisitions' ds_0001 ..." in {

      /**
        * ... in three 3rds
        */

      fail("Not implemented yet")
    }

    "ingest second 1/2 of 'performances' ds_0002 ..." in {

      /**
        * ... in three 3rds
        */
      fail("Not implemented yet")
    }

    /**
      * Repeat consistency checks for the second half of ingested data.
      */
    "check ingested data for ds_0001 is consistent" in {


      fail("Not implemented yet")
    }

    "check ingested data for ds_0002 is consistent" in {


      fail("Not implemented yet")
    }

    "check ingested data for ds_0001 x ds_0002 is consistent" in {

      // ... where 'x' means `natural join` by `id`

      fail("Not implemented yet")
    }

    /**
      * Done checking all ingestion is consistent
      */

    /**
      * Update records in the last "3rd" above and re-ingest
      */


    /**
      * Re-apply all checks above for consistency of ingestion: essentially must show that ingesting updates does
      * not duplicate but update records by "primary key".
      *
      * Note: my understanding is that this can only be achieved / observed after sync-ing with `hive`!
      * But up to you if you think `hive` is not strictly necessary.
      */

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
