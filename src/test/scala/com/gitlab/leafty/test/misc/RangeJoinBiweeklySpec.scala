package com.gitlab.leafty.test.misc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row

/**
  *
  */
class RangeJoinBiweeklySpec extends RangeJoinSpec with RangeJoinBiweeklyMockData {

     Logger.getLogger("org.apache").setLevel(Level.WARN)
     Logger.getLogger("org.apache.hudi").setLevel(Level.WARN)

     "range join" should {
          "work for `Scenario 1 BiWeekly - Salary Deposit regular deposit and regular amount`" in {

               val median = calculateMedian(scenario1_bw_trnsData.sort("amount").select("amount").collect().map(_ (0))
                    .toList)
               val rangesData = rangesBiWeeklyData("2019-05-10", ctg001, median)
               println("### Scenario 22 Data ranges")
               rangesData.sort("start").show(150, false)

               val results: Array[Row] = matchRecurringTransactions(scenario1_bw_trnsData, rangesData)
               results.length shouldBe 4
          }

          "work for `Scenario-8 BiWeekly transactions and both date and amounts are in date range for two consecutive biweekly. <>2 days. And amount is 80% to120%`" in {

               val median = calculateMedian(scenario8_bw_trnsData.sort("amount").select("amount").collect().map(_ (0))
                    .toList)
               val rangesData = rangesBiWeeklyData("2018-09-07", ctg001, median)
               println("### Scenario 22 Data ranges")
               rangesData.sort("start").show(150, false)

               val results: Array[Row] = matchRecurringTransactions(scenario8_bw_trnsData, rangesData)
               results.length shouldBe 6
          }

          "work for `Scenario-9 BiWeekly transactions  date is in range  and amounts are not in range for two consecutive biweekly. <>2 days. And amount is <80% to Amount >120%`" in {

               val median = calculateMedian(scenario9_bw_trnsData.sort("amount").select("amount").collect().map(_ (0))
                    .toList)
               val rangesData = rangesBiWeeklyData("2019-07-02", ctg001, median)
               println("### Scenario 22 Data ranges")
               rangesData.sort("start").show(150, false)

               val results: Array[Row] = matchRecurringTransactions(scenario9_bw_trnsData, rangesData)
               results.length shouldBe 3
          }
     }
}
