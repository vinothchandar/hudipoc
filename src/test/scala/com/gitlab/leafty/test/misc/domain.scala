package com.gitlab.leafty.test.misc

import java.sql.Timestamp

import DateTimeUtils._

object domain {

  type CtgId = String

  object Trn {

    def amount(s: String) : BigDecimal = BigDecimal(s).setScale(2)

    /**
      * Marshalling helper for building objects from string based formats like CSV and Json
      */
    def apply(ctgId: CtgId, amt: String, time: String): Trn = new Trn(ctgId, amount(amt), parseDt(time))

    def apply(ctgId: CtgId, amt: BigDecimal, time: Timestamp): Trn = new Trn(ctgId, amt.setScale(2), time)
  }

  case class Trn(ctgId: CtgId, amount: BigDecimal, time: Timestamp)

  case class Range(ctgId: CtgId, start: Timestamp, end: Timestamp)
}

