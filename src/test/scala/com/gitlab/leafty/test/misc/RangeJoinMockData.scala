package com.gitlab.leafty.test.misc

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait RangeJoinMockData {

  val session: SparkSession
  import session.implicits._
  import domain._

  val ctg001 = "ctg001"

  lazy val trnsData: Dataset[Trn] = session
    .createDataFrame(
      session.sparkContext.parallelize(Seq(
        Trn(ctg001, "100.00", "2019-12-20 09:10:12"),
        Trn(ctg001, "101.01", "2019-12-26 09:10:12"), // outside ranges
        //
        Trn(ctg001, "200.00", "2020-01-03 00:00:00"),
        Trn("ctg002", "200.00", "2020-01-03 00:00:00"), // different ctgId so will skipped
        Trn(ctg001, "201.23", "2020-01-01 00:00:00"), // outside ranges
        Trn(ctg001, "202.56", "2020-01-05 00:00:00"), // outside ranges
        //
        Trn(ctg001, "300.00", "2020-01-10 16:20:00"),
        Trn(ctg001, "301.03", "2020-01-08 00:00:00"), // outside ranges
        //
        Trn(ctg001, "400.00", "2020-01-17 19:45:00"),
        Trn("ctg002", "400.00", "2020-01-17 19:45:00"), // different ctgId so will skipped
        Trn(ctg001, "401.62", "2020-01-16 00:00:00"), // outside ranges
        //
        Trn(ctg001, "500.00", "2020-01-24 13:30:20"),
        Trn(ctg001, "501.83", "2020-01-26 00:00:00"), // outside ranges
        Trn(ctg001, "502.57", "2020-01-26 00:00:00") // outside ranges
      ))
    )
    .as[Trn]
    .alias("trns")

  lazy val scenario1_trnsData: Dataset[Trn] = session
    .createDataFrame(
      session.sparkContext.parallelize(Seq(
        Trn(ctg001, "1233.00", "2021-01-01 00:00:00"),
        Trn(ctg001, "1233.00", "2021-01-08 00:00:00"),
        Trn(ctg001, "1233.00", "2021-01-15 00:00:00"),
        Trn(ctg001, "1233.00", "2021-01-22 00:00:00")
      ))
    )
    .as[Trn]
    .alias("trns")

  lazy val scenario7_trnsData: Dataset[Trn] = session
    .createDataFrame(
      session.sparkContext.parallelize(Seq(
        Trn(ctg001, "451.90", "2021-08-06 00:00:00"),
        Trn(ctg001, "451.90", "2021-08-13 00:00:00"),
        Trn(ctg001, "451.90", "2021-08-20 00:00:00")
      ))
    )
    .as[Trn]
    .alias("trns")

  lazy val scenario13_trnsData: Dataset[Trn] = session
    .createDataFrame(
      session.sparkContext.parallelize(Seq(
        Trn(ctg001, "2213.0", "2022-01-21 00:00:00"),
        Trn(ctg001, "2213.0", "2022-01-28 00:00:00"),
        Trn(ctg001, "2213.0", "2022-02-11 00:00:00")
      ))
    )
    .as[Trn]
    .alias("trns")

  lazy val scenario20_trnsData: Dataset[Trn] = session
    .createDataFrame(
      session.sparkContext.parallelize(Seq(
        Trn(ctg001, "190.56", "2022-09-06 00:00:00"),
        Trn(ctg001, "210.56", "2022-09-11 00:00:00"),
        Trn(ctg001, "185.56", "2022-09-14 00:00:00"),
        Trn(ctg001, "200.56", "2022-09-23 00:00:00")
      ))
    )
    .as[Trn]
    .alias("trns")

  import DateTimeUtils._
  private def makeRangeRow(ts: Timestamp, ctgId: String) = Row(ctgId, addDays(ts, -2), addDays(ts, 2))

  private def makeDates(date: String,
                        count: Int,
                        dayCount: Int): Seq[Timestamp] = {
    val start = parseDate(date)
    (0 until count).map(i ⇒ addDays(start, i * dayCount))
  }

  def rangesWeeklyData(startDate: String, ctgId: String): Dataset[Range] =
    session
      .createDataFrame(
        session.sparkContext.parallelize(
          makeDates(startDate, 6, 7).map((ts: Timestamp) =>
            makeRangeRow(ts, ctgId))),
        new StructType()
          .add("ctgId", StringType)
          .add("start", TimestampType)
          .add("end", TimestampType)
      )
      .as[Range]
      .alias("ranges")

}
