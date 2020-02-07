package com.gitlab.leafty.test.misc

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, OffsetDateTime}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object DateTimeUtils {

  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val dt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val offset = OffsetDateTime.now().getOffset()

  def parseDt(str: String): Timestamp =
    new Timestamp(LocalDateTime.from(dtf.parse(str)).toInstant(offset).toEpochMilli)

  def parseDate(str: String): Timestamp =
    new Timestamp(LocalDate.from(dt.parse(str)).atStartOfDay(offset.normalized()).toInstant().toEpochMilli)

  def addDays(ts: Timestamp, d: Int): Timestamp = {
    import java.util.Calendar
    val cal = Calendar.getInstance
    cal.setTimeInMillis(ts.getTime)
    cal.add(Calendar.DAY_OF_WEEK, d)
    new Timestamp(cal.getTime.getTime)
  }

  def addToEOD(ts: Timestamp): Timestamp = {
    import java.util.Calendar
    val cal = Calendar.getInstance
    cal.setTimeInMillis(ts.getTime)
    cal.add(Calendar.MILLISECOND, 60*60*24*1000 - 1)
    new Timestamp(cal.getTime.getTime)
  }
}

trait RangeJoinMockData {

  val session: SparkSession
  import DateTimeUtils._
  import session.implicits._

  lazy val trnsData: Dataset[Trn] = session
    .createDataFrame(
      session.sparkContext.parallelize(
        Seq(
          Row("ctg001", BigDecimal(100.00), parseDt("2019-12-20 09:10:12")),
          Row("ctg001", BigDecimal(101.01), parseDt("2019-12-26 09:10:12")), // outside ranges
          //
          Row("ctg001", BigDecimal(200.00), parseDt("2020-01-03 00:00:00")),
          Row("ctg002", BigDecimal(200.00), parseDt("2020-01-03 00:00:00")), // different ctgid
          Row("ctg001", BigDecimal(201.23), parseDt("2020-01-01 00:00:00")), // outside ranges
          Row("ctg001", BigDecimal(202.56), parseDt("2020-01-05 00:00:00")), // outside ranges
          //
          Row("ctg001", BigDecimal(300.00), parseDt("2020-01-10 16:20:00")),
          Row("ctg001", BigDecimal(301.03), parseDt("2020-01-08 00:00:00")), // outside ranges
          //
          Row("ctg001", BigDecimal(400.00), parseDt("2020-01-17 19:45:00")),
          Row("ctg001", BigDecimal(401.62), parseDt("2020-01-16 00:00:00")), // outside ranges
          //
          Row("ctg001", BigDecimal(500.00), parseDt("2020-01-24 13:30:20")),
          Row("ctg001", BigDecimal(501.83), parseDt("2020-01-26 00:00:00")), // outside ranges
          Row("ctg001", BigDecimal(502.57), parseDt("2020-01-26 00:00:00"))  // outside ranges
        )),
      new StructType().add("ctgId", StringType).add("amount", DecimalType(10, 2)).add("time", TimestampType)
    )
    .as[Trn].alias("trns")

  private def makeRangeRow(ts: Timestamp) = Row("ctg001", ts, addToEOD(ts))

  private def makeDates(date: String, count: Int, dayCount: Int): Seq[Timestamp] = {
    val start = parseDate(date)
    (0 until count).map(i ⇒ addDays(start, i * dayCount))
  }

  def rangesWeeklyData(startDate: String): Dataset[Range] = session
    .createDataFrame(
      session.sparkContext.parallelize(
        makeDates(startDate, 6, 7).map(makeRangeRow)),
      new StructType().add("ctgId", StringType).add("start", TimestampType).add("end", TimestampType)
    )
    .as[Range].alias("ranges")

}
