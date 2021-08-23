package homeworks

import org.apache.spark.sql.{SaveMode, SparkSession}

object TestDeployApp {

  // test deploy app with input output command line arguments
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Need to have 2 arguments: input and output paths")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("TestDeployApp")
      .getOrCreate()

    import spark.implicits._

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val bestComediesDF = moviesDF.select(
      $"Title",
      $"IMDB_Rating".as("Rating"),
      $"Release_Date".as("Release")
    )
      .where(($"Major_Genre" === "Comedy") and ($"IMDB_Rating" > 6.5))
      .orderBy($"Rating".desc_nulls_last)

    bestComediesDF.show()

    bestComediesDF.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save(args(1))

  }
}
