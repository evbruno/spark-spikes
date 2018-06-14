package apps

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CallReportsRDDApp extends App {

  val spark: SparkSession = SparkSession.builder()
      .appName(s"SparkApp-${getClass.getName}")
      .master("local[4]")
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
  
  println("busiest days of the week:")
  callsPerDOW
    .toSeq
    .sortBy(_._1)
    .foreach(println)

  goodBye

}