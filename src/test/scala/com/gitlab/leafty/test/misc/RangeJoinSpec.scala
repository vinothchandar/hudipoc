package com.gitlab.leafty.test.misc

import java.util

import com.gitlab.leafty.test.hudi.AsyncBaseSpec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Resource https://docs.databricks.com/delta/join-performance/range-join.html
  */
class RangeJoinSpec extends AsyncBaseSpec with RangeJoinMockData {

     Logger.getLogger("org.apache").setLevel(Level.WARN)
     Logger.getLogger("org.apache.hudi").setLevel(Level.WARN)

     lazy val session: SparkSession = getSparkSession

     import session.implicits._

     "range join" should {
          "work for `point in interval range joins`" in {
               val median = calculateMedian(trnsData.sort("amount").select("amount").collect().map(_ (0)).toList)

               val rangesData = rangesWeeklyData("2019-12-20", ctg001, median)
               val results: _root_.scala.Array[_root_.org.apache.spark.sql.Row] = matchRecurringTransactions(trnsData, rangesData)

//               results.length shouldBe 5 //without wiggle room of +/- 2 days
               results.length shouldBe 2 //with wiggle room of +/- 2 days

          }

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

          "work for `Scenario 21 Weekly -  dates are in range and amounts are not in date range for two consecutive biweekly. <>2 "in {

               val median = calculateMedian(scenario21_trnsData.sort("amount").select("amount").collect().map(_ (0))
                    .toList)
               val rangesData = rangesWeeklyData("2022-10-10", ctg001, median)
               println ("### Scenario 21 Data ranges")
               rangesData.sort("start").show(150, false)

               val results: Array[Row] = matchRecurringTransactions(scenario21_trnsData, rangesData)
               results.length shouldBe 2
          }

         "work for `Scenario 22 Weekly -  dates are in range and amounts are not in date range for two consecutive " +
              "biweekly. >3 days "in {

               val median = calculateMedian(scenario22_trnsData.sort("amount").select("amount").collect().map(_ (0))
                    .toList)
               val rangesData = rangesWeeklyData("2022-11-01", ctg001, median)
               println ("### Scenario 22 Data ranges")
               rangesData.sort("start").show(150, false)

               val results: Array[Row] = matchRecurringTransactions(scenario22_trnsData, rangesData)
               results.length shouldBe 0
          }
     }


     private def validateTransactionsByAmount(sortedTransactions: DataFrame): DataFrame = {

//          val medianAmount = sortedTransactions.stat.approxQuantile("amount", Array(0.5), 0.0)
//          println("### Found median amount with Spark approxQuantiles - wrong value: " + medianAmount(0))

          val list = sortedTransactions.select("amount").collect().map(_ (0)).toList
          val median = calculateMedian(list)

          //Validate transactions are within median amount range
          val validatedTransactions = sortedTransactions.filter(sortedTransactions("amount") >= 0.8 * median
               and sortedTransactions("amount") <= 1.2 * median)
          validatedTransactions.show()
          validatedTransactions
     }

     protected def matchRecurringTransactions(transactions: Dataset[domain.Trn], rangesData: Dataset[domain.Range]) = {
          /**
            * https://hyp.is/y6NjHjheEeqEhG-j_u3puA/docs.databricks.com/delta/join-performance/range-join.html
            */
          val ds = transactions
               .join(
                    rangesData,
                    ($"trns.ctgId" === $"ranges.ctgId") and ($"time" between($"start", $"end")) and ($"amount" between
                         ($"startAmt", $"endAmt") ),
                    "inner")
               .select($"trns.ctgId", $"amount", $"time", $"start", $"end", $"startAmt", $"endAmt")
               .dropDuplicates("ctgId", "amount", "time")

          /**
            * #todo This uses `CartesianProduct` if not joining by `ctgId`.
            */
          ds.explain(true)

          val results = ds.collect()

          ds.show()
          results
     }

     protected def getSparkSession: SparkSession = {

          val conf = new SparkConf()
               .setAppName("hudipoc")
               .setMaster("local[2]")
               .set("spark.ui.enabled", "false")
//               .set("spark.serializer", classOf[KryoSerializer].getName)
               .set("setWarnUnregisteredClasses", "true")
               .set("spark.kryo.registrationRequired", "true")
               .set("spark.kryo.registrator", classOf[serde.CustomRegistrator].getName)

          SparkSession.builder().config(conf).getOrCreate()
     }
}
