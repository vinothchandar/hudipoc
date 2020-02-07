package com.gitlab.leafty.test

import java.sql.Timestamp


package object misc {

  case class Trn(amount: BigDecimal, time: Timestamp)

  case class Range(start: Timestamp, end: Timestamp)

}
