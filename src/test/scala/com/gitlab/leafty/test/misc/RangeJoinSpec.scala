package com.gitlab.leafty.test.misc

import java.sql.Timestamp

import com.gitlab.leafty.test.hudi.AsyncBaseSpec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}
import org.apache.spark.sql.{Encoders, SparkSession}

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

      /**
        * https://hyp.is/y6NjHjheEeqEhG-j_u3puA/docs.databricks.com/delta/join-performance/range-join.html
        */
      val rangesData = rangesWeeklyData("2019-12-20", ctg001)
      val ds = trnsData
        .join(
          rangesData,
          ($"trns.ctgId" === $"ranges.ctgId") and ($"time" between ($"start", $"end")),
          "inner")
        .select($"trns.ctgId", $"amount", $"time", $"start", $"end")

      /**
        * #todo This uses `CartesianProduct` if not joining by `ctgId`.
        */
      ds.explain(true)

      val results = ds.collect()

      ds.show()
      results.length shouldBe 5
    }
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
