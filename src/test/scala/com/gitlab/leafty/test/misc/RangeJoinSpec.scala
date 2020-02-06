package com.gitlab.leafty.test.misc

import com.gitlab.leafty.test.hudi.AsyncBaseSpec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class RangeJoinSpec extends AsyncBaseSpec {

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hudi").setLevel(Level.WARN)

  lazy val spark: SparkSession = getSparkSession

  import spark.implicits._

  private lazy val pointsData: Dataset[Trn] = spark
    .createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(10, 1),
          Row(11, 5),
          Row(20, 7),
          Row(22, 14),
          Row(30, 20),
          Row(40, 29)
        )),
      new StructType().add("amount", IntegerType).add("time", IntegerType)
    )
    .as[Trn]

  private lazy val rangesData: Dataset[Range] = spark
    .createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(0, 2),
          Row(6, 7),
          Row(11, 15),
          Row(19, 22),
          Row(27, 31)
        )),
      new StructType().add("start", IntegerType).add("end", IntegerType)
    )
    .as[Range]

  "spark range join" should {
    "work" in {
      //val expr: Expression = (pointsData($"time") === rangesData($"start")).expr
      val df = pointsData
        .join(rangesData, $"time" >= $"start" && $"time" <= $"end", "inner")
        .select("*")
      val result = df.collect()
      result.length shouldBe 5
    }
  }

  protected def getSparkSession: SparkSession = {
    val builder = SparkSession
      .builder()
      .appName("hudipoc")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    builder.getOrCreate()
  }
}
