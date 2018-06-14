package notes

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object NotesWeek2Tarifs extends App {

  lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkClassNotes")
  lazy val sc: SparkContext = new SparkContext(conf)

  type UserId = Int

  abstract class Tarif extends Serializable

  case object AG extends Tarif

  case object DemiTarif extends Tarif

  case object DemiTarifVisa extends Tarif

  val as = List(
    ((101), ("Ruetli", AG)),
    ((102), ("Brelaz", DemiTarif)),
    ((103), ("Gress", DemiTarifVisa)),
    ((104), ("Schatten", DemiTarif))
  )
  val abos: RDD[(UserId, (String, Tarif))] = sc.parallelize(as)
  val subscriptions = abos

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

  println(s"abos#count=${abos.count()}")
  println(s"locations#count=${locations.count()}")

  val join1 = locations.join(subscriptions).collect()
  println("> locations join subscriptions")
  join1.foreach(println)

  val join2 = subscriptions.join(locations).collect()
  println("> subscriptions join locations")
  join2.foreach(println)

  //  val oJoin1 = locations.leftOuterJoin(subscriptions).collect()
  //  println("> locations left outer join subscriptions")
  //  oJoin1.foreach(println)

  val oJoin2 = subscriptions.rightOuterJoin(locations).collect()
  println("> subscriptions right outer join locations")
  oJoin2.foreach(println)

  val oJoin1 = subscriptions.leftOuterJoin(locations).collect()
  println("> subscriptions left outer join locations")
  oJoin1.foreach(println)

  val oJoin3 = locations.leftOuterJoin(subscriptions).collect()
  println("> locations left outer join subscriptions")
  oJoin3.foreach(println)

  def hasNoLeftValue[A, B, C](rddVal: (A, (B, Option[C]))) = rddVal._2._2.isEmpty

  val usersThatDoesntUseTarif = subscriptions
    .leftOuterJoin(locations)
//     .filter { case (userId : UserId, optLocation) => optLocation._2.isEmpty }
//    .filter(_._2._2.isEmpty)
    .filter(hasNoLeftValue)
    .collect()

  println("> usersThatDoesntUseTarif")
  usersThatDoesntUseTarif.foreach(println)

  sc.stop()
}
