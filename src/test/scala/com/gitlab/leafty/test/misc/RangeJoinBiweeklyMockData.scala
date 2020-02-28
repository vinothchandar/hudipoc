package com.gitlab.leafty.test.misc

import java.sql.Timestamp

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{DoubleType, StringType, StructType, TimestampType}

/**
  *
  */
trait RangeJoinBiweeklyMockData extends RangeJoinMockData {
     import domain._
     import session.implicits._

     lazy val scenario1_bw_trnsData: Dataset[Trn] = session
          .createDataFrame(
               session.sparkContext.parallelize(Seq(
                    Trn(ctg001, "1100.00", "2019-05-10 00:00:00"),
                    Trn(ctg001, "1100.00", "2019-05-24 00:00:00"),
                    Trn(ctg001, "1100.00", "2019-06-07 00:00:00"),
                    Trn(ctg001, "1100.00", "2019-06-21 00:00:00")
               ))
          )
          .as[Trn]
          .alias("trns")

     lazy val scenario8_bw_trnsData: Dataset[Trn] = session
          .createDataFrame(
               session.sparkContext.parallelize(Seq(
                    Trn(ctg001, "100.00", "2018-09-07 00:00:00"),
                    Trn(ctg001, "100.00", "2018-09-25 00:00:00"),
                    Trn(ctg001, "100.00", "2018-10-10 00:00:00"),
                    Trn(ctg001, "113.00", "2018-10-22 00:00:00"),
                    Trn(ctg001, "85.00", "2018-11-04 00:00:00"),
                    Trn(ctg001, "100.00", "2018-11-19 00:00:00")
               ))
          )
          .as[Trn]
          .alias("trns")

     lazy val scenario9_bw_trnsData: Dataset[Trn] = session
          .createDataFrame(
               session.sparkContext.parallelize(Seq(
                    Trn(ctg001, "1500.00", "2019-07-02 00:00:00"),
                    Trn(ctg001, "2200.00", "2019-07-21 00:00:00"),
                    Trn(ctg001, "3500.00", "2019-08-14 00:00:00"),
                    Trn(ctg001, "2200.00", "2019-08-27 00:00:00"),
                    Trn(ctg001, "1500.00", "2019-09-10 00:00:00"),
                    Trn(ctg001, "2200.00", "2019-09-23 00:00:00")
               ))
          )
          .as[Trn]
          .alias("trns")

     def rangesBiWeeklyData(startDate: String, ctgId: String, amt: Double): Dataset[Range] =
          session
               .createDataFrame(
                    session.sparkContext.parallelize(
                         makeDates(startDate, 7, 14, 14).map((ts: Timestamp) =>
                              makeRangeRow(ts, ctgId, amt))),
                    new StructType()
                         .add("ctgId", StringType)
                         .add("start", TimestampType)
                         .add("end", TimestampType)
                         .add("startAmt", DoubleType) //@TODO - fix type to Decimal
                         .add("endAmt", DoubleType)
               )
               .as[Range]
               .alias("ranges")
}
