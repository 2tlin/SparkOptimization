package homeworks

import org.apache.spark.sql.functions.{array_contains, upper}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object ColumnPruning {
  val spark = SparkSession.builder()
    .appName("Column Pruning")
    .master("local[2]")
    .getOrCreate()

  val sc = spark.sparkContext
  import spark.implicits._

  def readDF(path: String): DataFrame = spark.read
    .option("inferSchema", "true")
    .json(path)

  val guitarPlayerDF: DataFrame = readDF("src/main/resources/data/guitarPlayers/guitarPlayers.json")
  val bandsDF: DataFrame = readDF("src/main/resources/data/bands/bands.json")
  val guitarsDF: DataFrame = readDF("src/main/resources/data/guitars/guitars.json")

  val joinCondition: Column = guitarPlayerDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitarPlayerDF.join(bandsDF, joinCondition, "inner")

//  guitaristsBandsDF.explain()
  /*
  == Physical Plan ==
  *(2) BroadcastHashJoin [band#7L], [id#23L], Inner, BuildLeft
  :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#34]
  :  +- *(1) Project [band#7L, guitars#8, id#9L, name#10]
  :     +- *(1) Filter isnotnull(band#7L)
  :        +- FileScan json [band#7L,guitars#8,id#9L,name#10] Batched: false, DataFilters: [isnotnull(band#7L)], Format: JSON, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
  +- *(2) Project [hometown#22, id#23L, name#24, year#25L]
     +- *(2) Filter isnotnull(id#23L)
        +- FileScan json [hometown#22,id#23L,name#24,year#25L] Batched: false, DataFilters: [isnotnull(id#23L)], Format: JSON, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<hometown:string,id:bigint,name:string,year:bigint>
   */

  // COLUMN PRUNING
  val guitaristsWithoutBandsDF: DataFrame = guitarPlayerDF.join(bandsDF, joinCondition, "left_anti")
  // guitaristsWithoutBandsDF.explain()
  /*
  == Physical Plan ==
  *(2) BroadcastHashJoin [band#7L], [id#23L], LeftAnti, BuildRight
  :- FileScan json [band#7L,guitars#8,id#9L,name#10] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
  +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#31]
     +- *(1) Project [id#23L] <-- Column pruning = cut off columns that are not relevant
        +- *(1) Filter isnotnull(id#23L)
           +- FileScan json [id#23L] Batched: false, DataFilters: [isnotnull(id#23L)], Format: JSON, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>
   */

  // Project and filter pushdown
  val namesDF: DataFrame = guitaristsBandsDF.select(guitarPlayerDF.col("name"), bandsDF.col("name"))
  // namesDF.explain()
  /*
  == Physical Plan ==
  *(2) Project [name#10, name#24]
  +- *(2) BroadcastHashJoin [band#7L], [id#23L], Inner, BuildRight
     :- *(2) Project [band#7L, name#10] <-- COLUMNS PRUNUNG -->
     :  +- *(2) Filter isnotnull(band#7L)
     :     +- FileScan json [band#7L,name#10] Batched: false, DataFilters: [isnotnull(band#7L)], Format: JSON, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,name:string>
     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#42]
        +- *(1) Project [id#23L, name#24]
           +- *(1) Filter isnotnull(id#23L)
              +- FileScan json [id#23L,name#24] Batched: false, DataFilters: [isnotnull(id#23L)], Format: JSON, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>
   */

  val rockDF = guitarPlayerDF
    .join(bandsDF, joinCondition)
    .join(guitarsDF, array_contains(guitarPlayerDF.col("guitars"), guitarsDF.col("id")))

  val essentialsDF = rockDF.select(guitarPlayerDF.col("name"), bandsDF.col("name"), upper(guitarsDF.col("make")))
  essentialsDF.explain()
  /*
  == Physical Plan ==
  *(3) Project [name#10, name#24, upper(make#39) AS upper(make)#147]
  +- BroadcastNestedLoopJoin BuildRight, Inner, array_contains(guitars#8, id#38L)
     :- *(2) Project [guitars#8, name#10, name#24]
     :  +- *(2) BroadcastHashJoin [band#7L], [id#23L], Inner, BuildRight
     :     :- *(2) Project [band#7L, guitars#8, name#10]
     :     :  +- *(2) Filter isnotnull(band#7L)
     :     :     +- FileScan json [band#7L,guitars#8,name#10] Batched: false, DataFilters: [isnotnull(band#7L)], Format: JSON, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [IsNotNull(band)], ReadSchema: struct<band:bigint,guitars:array<bigint>,name:string>
     :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#58]
     :        +- *(1) Project [id#23L, name#24]
     :           +- *(1) Filter isnotnull(id#23L)
     :              +- FileScan json [id#23L,name#24] Batched: false, DataFilters: [isnotnull(id#23L)], Format: JSON, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,name:string>
     +- BroadcastExchange IdentityBroadcastMode, [id=#48]
        +- FileScan json [id#38L,make#39] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:bigint,make:string>

   */



  def main(args: Array[String]): Unit = {

  }
}
