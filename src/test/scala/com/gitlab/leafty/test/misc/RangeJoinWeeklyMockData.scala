package com.gitlab.leafty.test.misc

import java.sql.Timestamp

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{DoubleType, StringType, StructType, TimestampType}

/**
  *
  */
trait RangeJoinWeeklyMockData extends RangeJoinMockData {
     import domain._
     import session.implicits._

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

     lazy val scenario21_trnsData: Dataset[Trn] = session
          .createDataFrame(
               session.sparkContext.parallelize(Seq(
                    Trn(ctg001, "300.8", "2022-10-10 00:00:00"),
                    Trn(ctg001, "800.8", "2022-10-14 00:00:00"),
                    Trn(ctg001, "700.8", "2022-10-20 00:00:00"),
                    Trn(ctg001, "500.8", "2022-10-28 00:00:00")
               ))
          )
          .as[Trn]
          .alias("trns")

     lazy val scenario22_trnsData: Dataset[Trn] = session
          .createDataFrame(
               session.sparkContext.parallelize(Seq(
                    Trn(ctg001, "1500.11", "2022-11-01 00:00:00"),
                    Trn(ctg001, "600.11", "2022-11-08 00:00:00"),
                    Trn(ctg001, "500.11", "2022-11-15 00:00:00"),
                    Trn(ctg001, "1000.11", "2022-11-25 00:00:00")
               ))
          )
          .as[Trn]
          .alias("trns")

     lazy val scenario23_trnsData: Dataset[Trn] = session
          .createDataFrame(
               session.sparkContext.parallelize(Seq(
                    Trn(ctg001, "150.88", "2022-12-02 00:00:00"),
                    Trn(ctg001, "150.88", "2022-12-09 00:00:00"),
                    Trn(ctg001, "300.88", "2022-12-16 00:00:00"),
                    Trn(ctg001, "150.88", "2022-12-23 00:00:00")
               ))
          )
          .as[Trn]
          .alias("trns")

     lazy val scenario24_trnsData: Dataset[Trn] = session
          .createDataFrame(
               session.sparkContext.parallelize(Seq(
                    Trn(ctg001, "300.6", "2023-01-06 00:00:00"),
                    Trn(ctg001, "200.6", "2023-01-13 00:00:00"),
                    Trn(ctg001, "100.6", "2023-01-20 00:00:00"),
                    Trn(ctg001, "300.6", "2023-01-27 00:00:00")
               ))
          )
          .as[Trn]
          .alias("trns")

     lazy val scenario27_trnsData: Dataset[Trn] = session
          .createDataFrame(
               session.sparkContext.parallelize(Seq(
                    Trn(ctg001, "250.66", "2023-04-07 00:00:00"),
                    Trn(ctg001, "250.66", "2023-04-14 00:00:00"),
                    Trn(ctg001, "250.66", "2023-04-17 00:00:00"),
                    Trn(ctg001, "250.66", "2023-04-28 00:00:00")
               ))
          )
          .as[Trn]
          .alias("trns")

     lazy val scenario28_trnsData: Dataset[Trn] = session
          .createDataFrame(
               session.sparkContext.parallelize(Seq(
                    Trn(ctg001, "119.44", "2023-05-01 00:00:00"),
                    Trn(ctg001, "119.44", "2023-05-12 00:00:00"),
                    Trn(ctg001, "119.44", "2023-05-15 00:00:00"),
                    Trn(ctg001, "119.44", "2023-05-26 00:00:00")
               ))
          )
          .as[Trn]
          .alias("trns")

     def rangesWeeklyData(startDate: String, ctgId: String, amt: Double): Dataset[Range] =
          session
               .createDataFrame(
                    session.sparkContext.parallelize(
                         makeDates(startDate, 5, 7, 7).map((ts: Timestamp) =>
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
