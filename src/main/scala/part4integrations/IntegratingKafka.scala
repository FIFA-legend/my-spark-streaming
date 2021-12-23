package part4integrations

import common.carsSchema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object IntegratingKafka {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

  def readFromKafka() = {
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()

    kafkaDF
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") // without checkpoints the writing will fail
      .start()
      .awaitTermination()
  }

  /**
    * Exercise: write the whole data structures to Kafka as JSON.
    * Use struct columns and the to_json function.
    */
  def writeCarsToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsToKafkaDF = carsDF
      .select(
        col("Name").as("key"),
        struct("Name", "Miles_per_Gallon", "Cylinders", "Displacement", "Horsepower", "Weight_in_lbs", "Acceleration", "Year", "Origin").as("JSON")
      )
      .select(col("key"), to_json(col("JSON")).as("value"))

    carsToKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeCarsToKafka()
  }

}
