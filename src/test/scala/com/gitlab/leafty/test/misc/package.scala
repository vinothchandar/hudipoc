package com.gitlab.leafty.test

import java.sql.Timestamp


package object misc {

  type CtgId = String

  case class Trn(ctgId: CtgId, amount: BigDecimal, time: Timestamp)

  case class Range(ctgId: CtgId, start: Timestamp, end: Timestamp)

}
