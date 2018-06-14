import java.sql.Timestamp
import java.util.Calendar._
import java.util.Calendar.MINUTE

import scala.io.StdIn

package object apps {

  def goodBye(implicit sc: org.apache.spark.SparkContext) {
    println("Press any key to quit..")
    StdIn.readLine()
    sc.stop()
  }

  def firstInstant(in: Timestamp) = {
    val cal = getInstance()
    cal.setTime(in)
    cal.set(SECOND, 0)
    cal.set(MILLISECOND, 0)
    new Timestamp(cal.getTime.getTime)
  }


  def nextInstant(in: Timestamp) = {
    val cal = getInstance()
    cal.setTime(in)
    cal.add(MINUTE,  1)
    cal.set(SECOND, 0)
    cal.set(MILLISECOND, 0)
    new Timestamp(cal.getTime.getTime)
  }

  def addMinute(ts: Timestamp, minutes : Int = 1) = {
    val cal = getInstance()
    cal.setTime(ts)
    cal.add(MINUTE, minutes)
    new Timestamp(cal.getTime.getTime)
  }

  def intervals(start: Timestamp, end: Timestamp) = {
    val startInstant = firstInstant(start)
    val lastInstant = nextInstant(end)

    val delta = ((lastInstant.getTime - startInstant.getTime) / (1000 * 60)).toInt
    val intervals = for (i <- 0 to delta) yield addMinute(startInstant, i)
    intervals
  }

  def dayOfTheWeek(ts: Timestamp) = {
      val cal = getInstance()
      cal.setTime(ts)
      cal.get(DAY_OF_WEEK)
  }
}
