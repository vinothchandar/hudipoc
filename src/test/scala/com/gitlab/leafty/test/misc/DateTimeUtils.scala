package com.gitlab.leafty.test.misc

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, OffsetDateTime}


object DateTimeUtils {

  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val dt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val offset = OffsetDateTime.now().getOffset()

  def parseDt(str: String): Timestamp =
    new Timestamp(LocalDateTime.from(dtf.parse(str)).toInstant(offset).toEpochMilli)

  def parseDate(str: String): Timestamp =
    new Timestamp(LocalDate.from(dt.parse(str)).atStartOfDay(offset.normalized()).toInstant().toEpochMilli)

  def addDays(ts: Timestamp, d: Int): Timestamp = {
    import java.util.Calendar
    val cal = Calendar.getInstance
    cal.setTimeInMillis(ts.getTime)
    cal.add(Calendar.DAY_OF_WEEK, d)
    new Timestamp(cal.getTime.getTime)
  }

  def addToEOD(ts: Timestamp): Timestamp = {
    import java.util.Calendar
    val cal = Calendar.getInstance
    cal.setTimeInMillis(ts.getTime)
    cal.add(Calendar.MILLISECOND, 60*60*24*1000 - 1)
    new Timestamp(cal.getTime.getTime)
  }
}
