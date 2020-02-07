package com.gitlab.leafty.test.misc

import com.gitlab.leafty.test.hudi.AsyncBaseSpec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  *
  */
class RangeJoinSpec extends AsyncBaseSpec with RangeJoinMockData {

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hudi").setLevel(Level.WARN)

  lazy val session: SparkSession = getSparkSession

  import session.implicits._

  "spark range join" should {
    "work" in {
      val ds = pointsData
        .join(rangesData, $"time" between ($"start", $"end"), "inner")
        .select("*")

      /**
        * #todo This uses `CartesianProduct` !
        */
      ds.explain(true)
      ds.show()

      ds.collect().length shouldBe 5
    }
  }

  protected def getSparkSession: SparkSession = {
    val builder = SparkSession
      .builder()
      .appName("hudipoc")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    builder.getOrCreate()
  }
}
