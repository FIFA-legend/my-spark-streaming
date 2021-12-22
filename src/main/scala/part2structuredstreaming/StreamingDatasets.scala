package part2structuredstreaming

import common._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  // include encoders for DF -> DS transformations

  import spark.implicits._

  def readCars(): Dataset[Car] = {
    // useful for DF -> DS transformations
    val carEncoder = Encoders.product[Car]

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with a single string column "value"
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // DF with multiple columns
      .as[Car] // encoder can be passed implicitly with spark.implicits
  }

  def showCarNames() = {
    val carsDS: Dataset[Car] = readCars()

    // transformations here
    val carNamesDF: DataFrame = carsDS.select(col("Name")) // DF

    // collection transformation maintain type info
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
    * Exercises
    *
    * 1) Count how many POWERFUL cars we have in the DS (HP > 140)
    * 2) Average HP for the entire dataset (use the complete output mode)
    * 3) Count the cars by their origin field
    */

  // 1
  def countPowerfulCars() = {
    val carsDS = readCars()

    val powerfulCarsDS = carsDS.filter(_.Horsepower.getOrElse(0L) > 140)

    powerfulCarsDS.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // 2 (complete mode makes us count average for all cars, not only batch)
  def countAverageHorsepower() = {
    val carsDS = readCars()

    val averageHorsepowerDS = carsDS.map(_.Horsepower).agg(avg(col("value")))

    averageHorsepowerDS.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def countAverageHorsepower2() = {
    val carsDS = readCars()

    carsDS.select(avg(col("Horsepower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // 3
  def countCarsByOrigin() = {
    val carsDS = readCars()

    val countCarsByOrigin = carsDS
      .groupByKey(_.Origin)
      .count()

    countCarsByOrigin.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //countPowerfulCars()
    //countAverageHorsepower()
    countCarsByOrigin()
  }

}
