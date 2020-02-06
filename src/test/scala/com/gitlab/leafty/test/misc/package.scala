package com.gitlab.leafty.test

import java.sql.Timestamp


package object misc {

  case class Trn(amount: Integer, time: Timestamp)

  case class Range(start: Timestamp, end: Timestamp)

}
