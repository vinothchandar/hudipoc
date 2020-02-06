package com.gitlab.leafty.test.misc

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import com.gitlab.leafty.test.hudi.AsyncBaseSpec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

class RangeJoinSpec extends AsyncBaseSpec {

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hudi").setLevel(Level.WARN)

  lazy val spark: SparkSession = getSparkSession

  import spark.implicits._

  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def parseDt(str: String): Timestamp =
    new Timestamp(
      LocalDateTime.from(dtf.parse(str)).toInstant(ZoneOffset.UTC).toEpochMilli)

  private lazy val pointsData: Dataset[Trn] = spark
    .createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(100, parseDt("2019-12-20 09:10:12")),
          Row(101, parseDt("2019-12-26 09:10:12")), // outside ranges

          Row(200, parseDt("2020-01-03 00:00:00")),
          Row(201, parseDt("2020-01-01 00:00:00")), // outside ranges
          Row(202, parseDt("2020-01-05 00:00:00")), // outside ranges

          Row(300, parseDt("2020-01-10 00:00:00")),
          Row(301, parseDt("2020-01-08 00:00:00")), // outside ranges

          Row(400, parseDt("2020-01-17 00:00:00")),
          Row(401, parseDt("2020-01-16 00:00:00")), // outside ranges

          Row(500, parseDt("2020-01-24 00:00:00")),
          Row(501, parseDt("2020-01-26 00:00:00")), // outside ranges
          Row(502, parseDt("2020-01-26 00:00:00"))  // outside ranges
        )),
      new StructType().add("amount", IntegerType).add("time", TimestampType)
    )
    .as[Trn]

  private lazy val rangesData: Dataset[Range] = spark
    .createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(parseDt("2019-12-20 00:00:00"), parseDt("2019-12-21 00:00:00")),
          Row(parseDt("2019-12-27 00:00:00"), parseDt("2019-12-28 00:00:00")),
          Row(parseDt("2020-01-03 00:00:00"), parseDt("2020-01-04 00:00:00")),
          Row(parseDt("2020-01-09 00:00:00"), parseDt("2020-01-10 00:00:00")),
          Row(parseDt("2020-01-17 00:00:00"), parseDt("2020-01-18 00:00:00")),
          Row(parseDt("2020-01-24 00:00:00"), parseDt("2020-01-25 00:00:00"))
        )),
      new StructType().add("start", TimestampType).add("end", TimestampType)
    )
    .as[Range]

  "spark range join" should {
    "work" in {
      //val expr: Expression = (pointsData($"time") === rangesData($"start")).expr
      val ds = pointsData
        .join(rangesData, $"time" >= $"start" && $"time" <= $"end", "inner")
        .select("*")
      val result = ds.collect()
      ds.explain(true)

      ds.show()
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
