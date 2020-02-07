package com.gitlab.leafty.test.misc

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

trait RangeJoinMockData {

  val session: SparkSession
  import session.implicits._

  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def parseDt(str: String): Timestamp =
    new Timestamp(
      LocalDateTime.from(dtf.parse(str)).toInstant(ZoneOffset.UTC).toEpochMilli)

  lazy val pointsData: Dataset[Trn] = session
    .createDataFrame(
      session.sparkContext.parallelize(
        Seq(
          Row(BigDecimal(100.00), parseDt("2019-12-20 09:10:12")),
          Row(BigDecimal(101.00), parseDt("2019-12-26 09:10:12")), // outside ranges
          //
          Row(BigDecimal(200.00), parseDt("2020-01-03 00:00:00")),
          Row(BigDecimal(201.00), parseDt("2020-01-01 00:00:00")), // outside ranges
          Row(BigDecimal(202.00), parseDt("2020-01-05 00:00:00")), // outside ranges
          //
          Row(BigDecimal(300.00), parseDt("2020-01-09 16:20:00")),
          Row(BigDecimal(301.00), parseDt("2020-01-08 00:00:00")), // outside ranges
          //
          Row(BigDecimal(400.00), parseDt("2020-01-17 19:45:00")),
          Row(BigDecimal(401.00), parseDt("2020-01-16 00:00:00")), // outside ranges
          //
          Row(BigDecimal(500.00), parseDt("2020-01-24 13:30:20")),
          Row(BigDecimal(501.00), parseDt("2020-01-26 00:00:00")), // outside ranges
          Row(BigDecimal(502.00), parseDt("2020-01-26 00:00:00"))  // outside ranges
        )),
      new StructType().add("amount", DecimalType(10, 2)).add("time", TimestampType)
    )
    .as[Trn]

  lazy val rangesData: Dataset[Range] = session
    .createDataFrame(
      session.sparkContext.parallelize(
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

}
