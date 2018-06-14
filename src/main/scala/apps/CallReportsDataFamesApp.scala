package apps

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object CallReportsDataFrameApp extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName(s"SparkApp-${getClass.getName}")
    .master("local[*]")
    .getOrCreate()

  implicit val sc: SparkContext = spark.sparkContext

  import org.apache.spark.sql.types._

  val customSchema = StructType(Array(
    StructField("startTime", TimestampType, true),
    StructField("endTime", TimestampType, true),
    StructField("source", StringType, true),
    StructField("direction", StringType, true)))

  val dataFrame: DataFrame = spark.read.format("CSV")
    .schema(customSchema)
    .option("header", "true")
    //.option("timestampFormat", "yyyy-MM-dd HH:mm:ss.S Z")
    .load("src/main/resources/apps/calls_2017_12.csv.gz")

  println(s"all count: ${dataFrame.count}")

  val incomingCalls: Dataset[Row] = dataFrame
    .select("source", "direction", "startTime", "startTime")
    .where("direction = 'IN'")

  incomingCalls.show(20)

  goodBye

}