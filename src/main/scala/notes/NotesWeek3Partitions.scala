package notes

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

import scala.io.StdIn

object NotesWeek3Partitions extends App {

  lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkClassNotes")
  lazy val sc: SparkContext = new SparkContext(conf)

  val ls = List(
    (101, "Bern"),
    (101, "Thun"),
    (102, "Lausanne"),
    (102, "Geneve"),
    (102, "Nyon"),
    (103, "Zurich"),
    (103, "St-Gallen"),
    (103, "Chur")
  )

  val locations: RDD[(Int, String)] = sc.parallelize(ls)

  println("locations:")
  println(locations.toDebugString)
  println(locations.dependencies)

  // --------

  val partitioner0 = new RangePartitioner(3, locations)
  val locationsPart0: RDD[(Int, String)] = locations.partitionBy(partitioner0).persist()

  println("locationsPart0:")
  println(locationsPart0.toDebugString)
  println(locationsPart0.dependencies)

  println("locationsPart0#mapValues:")
  locationsPart0.mapValues(n => n.toUpperCase).foreach(println)

  // --------

  val partitioner1 = new HashPartitioner(3)
  val locationsPart1: RDD[(Int, String)] = locations.partitionBy(partitioner1).persist()

  println("locationsPart1:")
  println(locationsPart1.toDebugString)

  println("locationsPart1#mapValues:")
  locationsPart1.mapValues(n => n.toUpperCase).foreach(println)

  println("Press any key to quit..")
  StdIn.readLine()
  sc.stop()

}
