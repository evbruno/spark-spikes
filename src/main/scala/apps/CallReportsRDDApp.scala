package apps

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CallReportsRDDApp extends App {

  val spark: SparkSession = SparkSession.builder()
      .appName(s"SparkApp-${getClass.getName}")
      .master("local[*]")
      .getOrCreate()

  implicit val sc: SparkContext = spark.sparkContext

  val lines: RDD[String] = sc.textFile("src/main/resources/apps/calls_2017_12.csv.gz")

  println(s"count: ${lines.count}")

  val timestampsRDD: RDD[(Timestamp, Timestamp)] = lines
    .filter(!_.startsWith("starttime"))
    .map(line => {
      val args = line.split(",")
      (Timestamp.valueOf(args(0)), Timestamp.valueOf(args(1)))
    })
    .persist()

  timestampsRDD.take(10).foreach(println)

  val intervalsRDD: RDD[Timestamp] = timestampsRDD.flatMap {
    case (init, end) => intervals(init, end)
  }

  //implicit val ordTS = Ordering.by { f :Timestamp => f.getTime }
  //intervalsRDD.takeOrdered(10).foreach(println)

  val mostCommonIntervals = intervalsRDD
      .map((_, 1))
      .countByKey()
      .toSeq
      .sortBy(- _._2)

  println("mostCommonIntervals:")
  mostCommonIntervals.take(10).foreach(println)

  val callsPerDOW = timestampsRDD
    .keys
    .map(ts => (dayOfTheWeek(ts), 1))
    .countByKey()

  val dow = Map(
    1 -> "SUN",
    2 -> "MON",
    3 -> "TUE",
    4 -> "WED",
    5 -> "THU",
    6 -> "FRI",
    7 -> "SAT"
  )

  println("busiest days of the week:")
  callsPerDOW
    .toSeq
    .sortBy(_._1)
    .foreach(d => println(s"${dow(d._1)} -> ${d._2}"))

  goodBye

}