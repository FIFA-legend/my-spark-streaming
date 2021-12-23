package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket() = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
      .selectExpr("purchase.*")
  }

  def aggregatePurchasesByTumblingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time")) // tumbling window: sliding duration == window duration
      .agg(sum(col("quantity")).as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregatePurchasesBySlidingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // struct column: has fields {start, end}
      .agg(sum(col("quantity")).as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
    * Exercises
    * 1) Show the best selling product of every day. + quantity sold.
    * 2) Show the best selling product of every 24 hours, updated every hour.
    */

  def exercise1() = {
    val purchaseDF = readPurchasesFromSocket()

    val bestSellingProductOfDayDF = purchaseDF
      .groupBy(window(col("time"), "1 day").as("time"), col("item"))
      .agg(sum(col("quantity")).as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      )
      .orderBy(col("time"), col("totalQuantity").desc)

    bestSellingProductOfDayDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def exercise2() = {
    val purchaseDF = readPurchasesFromSocket()

    val bestSellingProductByDaysDF = purchaseDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time"), col("item"))
      .agg(sum(col("quantity")).as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      )
      .orderBy(col("time"), col("totalQuantity").desc)

    bestSellingProductByDaysDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /*
    For window functions, window start at Jan 1 1970, 0 AM GMT
   */

  def main(args: Array[String]): Unit = {
    exercise2()
  }

}
