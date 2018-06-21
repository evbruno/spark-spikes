package notes

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn

object NotesWeek4DataFrames extends App {

  // types

  type UserId = Int
  type Tarif = Int

  val AG = 1
  val DemiTarif = 2
  val DemiTarifVisa = 3

  case class SubsType(name: String, tarif: Tarif)

  case class Listing(street: String, zip: Int, price: Int)

  // app

  val spark = SparkSession
    .builder()
    .appName("My App")
    //.config()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val as = List(
    ((101), SubsType("Ruetli", AG)),
    ((102), SubsType("Brelaz", DemiTarif)),
    ((103), SubsType("Gress", DemiTarifVisa)),
    ((104), SubsType("Schatten", DemiTarif))
  )

  val abos: RDD[(UserId, SubsType)] = spark.sparkContext.parallelize(as)
  val subscriptionDF = abos.toDF("id", "tarif")

  subscriptionDF.printSchema()
  subscriptionDF.show()

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
  val locationDF: DataFrame = locations.toDF("id", "location")

  println("# locationDF show id = 102")
  locationDF.filter("id = 102").show()

  println("# locationDF show id >= 102")
  locationDF.filter($"id" >= 102).show()

  println("# subscriptionDF show id < 102")
  subscriptionDF.filter(subscriptionDF("id") < 102).show()

  println("# subscriptionDF show tarif.nam = Schatten")
  subscriptionDF.filter($"tarif.name" === "Schatten").show()

  locationDF
    .groupBy($"id")
    .count()
    .show()

  import org.apache.spark.sql.functions._

  val listingsDF = spark.sparkContext.parallelize(List(
    Listing("street 1 A", 1111, 10),
    Listing("street 1 B", 1111, 20),
    Listing("street 1 C", 1111, 30),
    Listing("street 2 A", 2222, 11),
    Listing("street 2 B", 2222, 21),
    Listing("street 3 A", 3333, 33),
    Listing("street 3 B", 3333, 34)
  )).toDF()

  listingsDF
    .groupBy($"zip")
    .max("price")
    .show()

  listingsDF
    .groupBy($"zip")
    .avg("price")
    .show()

  listingsDF
    .filter($"price" > 15)
    .groupBy($"zip")
    //.agg(count($"price"))
    .agg(sum($"price"))
    .orderBy($"zip".asc)
    .show()

  val zipIndex = spark.sparkContext.parallelize(List(
    (1111, 10.4),
    (2222, 10.3),
    (3333, 10.2),
    (4444, 10.1)
  )).toDF("zip", "index")

  listingsDF.join(zipIndex, "zip").show()

  println("Press any key to quit..")
  StdIn.readLine()
  spark.stop()

}
