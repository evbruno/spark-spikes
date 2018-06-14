package notes

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

object NotesWeek2 extends App {

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("My app")
  val sc: SparkContext = new SparkContext(conf)

  case class Taco(kind: String, price: Double)

  val list = List(
    Taco("Carnitas", 2.25),
    Taco("Bacon", 2.25),
    Taco("Corn", 1.75),
    Taco("Barbacoa", 2.5),
    Taco("Chicken", 2)
  )

  val zeroValue = 0d

  println("list fold=" + list.foldLeft(zeroValue)(_ + _.price))

  val grouped: Map[Double, List[Taco]] = list.groupBy(t => t.price)
  println("list group=" + grouped)
  println("list group(2.25)=" + grouped(2.25))

  val rdd: RDD[Taco] = sc.parallelize(list)

  println("rdd aggregate=" + rdd.aggregate(0d)(_ + _.price, _ + _))

  object Carnitas extends Taco("Carnitas", 2.25)

  object Bacon extends Taco("Bacon", 2.25)

  object Corn extends Taco("Corn", 1.75)

  object Barbacoa extends Taco("Barbacoa", 2.5)

  object Chicken extends Taco("Chicken", 2)

  val order: List[Taco] = List(Carnitas, Corn, Barbacoa, Chicken, Carnitas, Corn, Barbacoa, Carnitas, Corn, Barbacoa)
  val orderRDD: RDD[(String, Taco)] = sc.parallelize(order.map(t => (t.kind, t)))

  orderRDD.groupByKey().foreach(println)

  println("-----")

  orderRDD.mapValues(_.kind).foreach(println)
  println(orderRDD.toDebugString)

  println("Press any key to quit..")
  StdIn.readLine()
  sc.stop()
}
