package com.gitlab.leafty.test.misc

import com.gitlab.leafty.test.hudi.AsyncBaseSpec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}

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

               val rangesData = rangesWeeklyData("2019-12-20", ctg001)
               val results: _root_.scala.Array[_root_.org.apache.spark.sql.Row] = matchRecurringTransactions(trnsData, rangesData)

//               results.length shouldBe 5 //without wiggle room of +/- 2 days
               results.length shouldBe 12 //with wiggle room of +/- 2 days

          }

          "work for `Scenario 1 Weekly - Salary Deposit regular deposit and regular amount`" in {

               val rangesData = rangesWeeklyData("2021-01-01", ctg001)
               val results: _root_.scala.Array[_root_.org.apache.spark.sql.Row] = matchRecurringTransactions(scenario1_trnsData, rangesData)

               results.length shouldBe 4
          }
          "work for `Scenario 7 Weekly -  Second transaction from cut off date is missing`" in {

               val rangesData = rangesWeeklyData("2021-08-06", ctg001)
               val results: _root_.scala.Array[_root_.org.apache.spark.sql.Row] = matchRecurringTransactions(scenario7_trnsData, rangesData)

               results.length shouldBe 3
          }

          "work for `Scenario 13 Weekly -  second transaction from cut off date is missing`" in {

               val rangesData = rangesWeeklyData("2022-01-21", ctg001)
               val results: _root_.scala.Array[_root_.org.apache.spark.sql.Row] = matchRecurringTransactions(scenario13_trnsData, rangesData)

               results.length shouldBe 3
          }

          "work for `Scenario 20 Weekly -  both date and amounts are in date range for two consecutive biweekly. <>2 days. And amount is 80% to120%`" in {

               val rangesData = rangesWeeklyData("2022-09-06", ctg001)
               println ("### Scenario 20 Data ranges")
               rangesData.show(false)
               val results: _root_.scala.Array[_root_.org.apache.spark.sql.Row] = matchRecurringTransactions(scenario20_trnsData, rangesData)

               results.length shouldBe 3
          }
     }

     protected def matchRecurringTransactions(transactions: Dataset[domain.Trn], rangesData: Dataset[domain.Range]) = {
          /**
            * https://hyp.is/y6NjHjheEeqEhG-j_u3puA/docs.databricks.com/delta/join-performance/range-join.html
            */
          val ds = transactions
               .join(
                    rangesData,
                    ($"trns.ctgId" === $"ranges.ctgId") and ($"time" between($"start", $"end")),
                    "inner")
               .select($"trns.ctgId", $"amount", $"time", $"start", $"end")

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
               .set("spark.serializer", classOf[KryoSerializer].getName)
               .set("setWarnUnregisteredClasses", "true")
               .set("spark.kryo.registrationRequired", "true")
               .set("spark.kryo.registrator", classOf[serde.CustomRegistrator].getName)

          SparkSession.builder().config(conf).getOrCreate()
     }
}
