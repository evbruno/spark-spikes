package notes

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn

object NotesWeek4SparkSession extends App {

  val spark = SparkSession
    .builder()
    .appName("My App")
    //.config()
    .master("local[*]")
    .getOrCreate()

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
  val locations: RDD[(Int, String)] = spark.sparkContext.parallelize(ls)

  import spark.implicits._

  val locationDF: DataFrame = locations.toDF("id", "location")
  locationDF.show(10)

  println("#locationDF.printSchema()")
  locationDF.printSchema()

  case class MyLocation(myId: Long, myDesc: String)

  val myLocationsRDD: RDD[MyLocation] = locations.map(t => MyLocation(t._1, t._2))
  val myLocationDF: DataFrame = myLocationsRDD.toDF()

  println("#myLocationDF.printSchema()")
  myLocationDF.printSchema()

  myLocationDF.show(10)

  val peopleDF = spark.read.json("src/main/resources/people.json")
  peopleDF.show(5)

  println("#peopleDF.printSchema()")
  peopleDF.printSchema()

  peopleDF.createOrReplaceTempView("people_table")

  val peopleFiltered: DataFrame = spark.sql("select * from people_table where age > 17")
  peopleFiltered.show(5)

  println("Press any key to quit..")
  StdIn.readLine()
  spark.stop()

}
