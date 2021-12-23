package part4integrations

import common.{Car, carsSchema}
import org.apache.spark.sql.{Dataset, SparkSession}

object IntegratingJDBC {

  // DBs don't support writing and reading in streaming way, but we can do it in batch mode.

  val spark = SparkSession.builder()
    .appName("Integrating JDBC")
    .master("local[2]")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val username = "docker"
  val password = "docker"

  import spark.implicits._

  def writeStreamToPostgres() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // each executor can control the batch
        // batch os a STATIC Dataset/DataFrame

        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", username)
          .option("password", password)
          .option("dbtable", "public.cars")
          .save()
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamToPostgres()
  }

}
