package com.gitlab.leafty.test.misc

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer

trait RangeJoinMockData {

  val session: SparkSession
  import domain._
  import session.implicits._

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


  import DateTimeUtils._
  def makeRangeRow(ts: Timestamp, ctgId: String, amt: Double) = Row(ctgId, addDays(ts, -2),
      addDays(ts, 2), addAmtRange(amt, false), addAmtRange(amt, true))

  def makeDates(date: String, count: Int, recurrenceDays: Int, coverageDays: Int): Seq[Timestamp]= {
    var start = parseDate(date)

    //@TODO refactor!!!
    var list: ListBuffer[Timestamp] = ListBuffer.empty[Timestamp]

    for (j <- 0 until coverageDays) {
      println("###Start date: " + start)
      for (i <- 0 until count) {
        val ts = addDays(start, i * recurrenceDays)
        list.append(ts)
        println("Generated new date: " + ts.toString)
      }
      start = addDays(start, 1)
    }
    list

  }

  def calculateMedian(list: List[Any]): Double = {
    val count = list.size
    val median: Double = if (count % 2 == 0) {
      val l = (count / 2 - 1).toInt
      val r = (l + 1).toInt
      (list(l).toString.toDouble + list(r).toString.toDouble) / 2
    } else list((count / 2).toInt).toString.toDouble
    println("Calculated median " + median)
    median
  }

  def addAmtRange(amount: Double, direction: Boolean): Double = {
    if (direction)
      amount * 1.2
    else
      amount * 0.8
  }

}
