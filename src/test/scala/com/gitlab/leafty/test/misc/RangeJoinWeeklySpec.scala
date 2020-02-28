package com.gitlab.leafty.test.misc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

/**
  *
  */
class RangeJoinWeeklySpec extends RangeJoinSpec with RangeJoinWeeklyMockData {
     Logger.getLogger("org.apache").setLevel(Level.WARN)
     Logger.getLogger("org.apache.hudi").setLevel(Level.WARN)

     import session.implicits._

     "range join" should {
          "work for `Scenario 1 Weekly - Salary Deposit regular deposit and regular amount`" in {

               val median = calculateMedian(scenario1_trnsData.sort("amount").select("amount").collect().map(_ (0)).toList)

               val rangesData = rangesWeeklyData("2021-01-01", ctg001, median)
               val results: _root_.scala.Array[_root_.org.apache.spark.sql.Row] = matchRecurringTransactions(scenario1_trnsData, rangesData)

               results.length shouldBe 4
          }
          "work for `Scenario 7 Weekly -  Second transaction from cut off date is missing`" in {

               val median = calculateMedian(scenario7_trnsData.sort("amount").select("amount").collect().map(_ (0)).toList)

               val rangesData = rangesWeeklyData("2021-08-06", ctg001, median)
               val results: _root_.scala.Array[_root_.org.apache.spark.sql.Row] = matchRecurringTransactions(scenario7_trnsData, rangesData)

               results.length shouldBe 3
          }

          "work for `Scenario 13 Weekly -  second transaction from cut off date is missing`" in {
               val median = calculateMedian(scenario13_trnsData.sort("amount").select("amount").collect().map(_ (0)).toList)

               val rangesData = rangesWeeklyData("2022-01-21", ctg001, median)
               rangesData.sort("start").show(150, false)

               val results: _root_.scala.Array[_root_.org.apache.spark.sql.Row] = matchRecurringTransactions(scenario13_trnsData, rangesData)

               results.length shouldBe 3
          }

          "work for `Scenario 20 Weekly -  both date and amounts are in date range for two consecutive biweekly. <>2 days. And amount is 80% to120%`" in {

               val median = calculateMedian(scenario20_trnsData.sort("amount").select("amount").collect().map(_ (0)).toList)
               val rangesData = rangesWeeklyData("2022-09-06", ctg001, median)
               println ("### Scenario 20 Data ranges")
               rangesData.sort("start").show(150, false)

               //Not needed as amount ranges are captured in join query
               //               val validatedTransactions = validateTransactionsByAmount(scenario20_trnsData.sort("amount").toDF())
               //               validatedTransactions.count() shouldBe 4

               val results: Array[Row] = matchRecurringTransactions(scenario20_trnsData, rangesData)
               results.length shouldBe 4
          }

          "work for `Scenario 21 Weekly -  dates are in range and amounts are not in date range for two consecutive biweekly. <>2 `"in {

               val median = calculateMedian(scenario21_trnsData.sort("amount").select("amount").collect().map(_ (0))
                    .toList)
               val rangesData = rangesWeeklyData("2022-10-10", ctg001, median)
               println ("### Scenario 21 Data ranges")
               rangesData.sort("start").show(150, false)

               val results: Array[Row] = matchRecurringTransactions(scenario21_trnsData, rangesData)
               results.length shouldBe 2
          }

          "work for `Scenario 22 Weekly -  dates are in range and amounts are not in date range for two consecutive " +
               "biweekly. >3 days `" in {

               val median = calculateMedian(scenario22_trnsData.sort("amount").select("amount").collect().map(_ (0))
                    .toList)
               val rangesData = rangesWeeklyData("2022-11-01", ctg001, median)
               println ("### Scenario 22 Data ranges")
               rangesData.sort("start").show(150, false)

               val results: Array[Row] = matchRecurringTransactions(scenario22_trnsData, rangesData)
               results.length shouldBe 0
          }

          "work for `Scenario 23 Weekly -  Validate Weeklys data when one transaction amount was not in range`" in {

               val median = calculateMedian(scenario23_trnsData.sort("amount").select("amount").collect().map(_ (0))
                    .toList)
               val rangesData = rangesWeeklyData("2022-12-02", ctg001, median)
               println ("### Scenario 22 Data ranges")
               rangesData.sort("start").show(150, false)

               val results: Array[Row] = matchRecurringTransactions(scenario23_trnsData, rangesData)
               results.length shouldBe 3
          }

          "work for `Scenario 24 Weekly -  Validate Weeklys data when Two consecutive transaction amount was not in range`" in {

               val median = calculateMedian(scenario24_trnsData.sort("amount").select("amount").collect().map(_ (0))
                    .toList)
               val rangesData = rangesWeeklyData("2023-01-06", ctg001, median)
               println ("### Scenario 22 Data ranges")
               rangesData.sort("start").show(150, false)

               val results: Array[Row] = matchRecurringTransactions(scenario24_trnsData, rangesData)
               results.length shouldBe 3
          }
          "work for `Scenario 27 Weekly - Weekly payment : One of the transcation was out of date range, rest " +
               "transaction was in range`" in {

               val median = calculateMedian(scenario27_trnsData.sort("amount").select("amount").collect().map(_ (0))
                    .toList)
               val rangesData = rangesWeeklyData("2023-04-07", ctg001, median)
               println ("### Scenario 22 Data ranges")
               rangesData.sort("start").show(150, false)

               val results: Array[Row] = matchRecurringTransactions(scenario27_trnsData, rangesData)
               results.length shouldBe 4
          }

          "work for `Scenario 28 Weekly - Weekly payment : two of the transcation was out of date range, rest transaction was in range`" in {

               val median = calculateMedian(scenario28_trnsData.sort("amount").select("amount").collect().map(_ (0))
                    .toList)
               val rangesData = rangesWeeklyData("2023-05-01", ctg001, median)
               println ("### Scenario 22 Data ranges")
               rangesData.sort("start").show(150, false)

               val results: Array[Row] = matchRecurringTransactions(scenario28_trnsData, rangesData)
               results.length shouldBe 4
          }
     }
}
