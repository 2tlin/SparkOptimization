package homeworks

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Bucketing {
  val spark = SparkSession.builder()
    .appName("Pre Partitioning")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext
  import spark.implicits._

  // deactivate auto-broadcasting
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  /*
  Bucketing - writing technic which splits dataframe into chunks.
  The chunks have the property that the identical keys get on (продолжать / садиться) the same bucket.
  Bucketing is - available on a dataframe writer – applicable to file based on datasources.
   */

  val large = spark.range(1000000).selectExpr("id * 5 as id").repartition(10)
  val small = spark.range(10000).selectExpr("id * 3 as id").repartition(3)

  val joined = large.join(small, "id")
  joined.explain()
  /*
  == Physical Plan ==
  *(5) Project [id#2L]
  +- *(5) SortMergeJoin [id#2L], [id#6L], Inner
     :- *(2) Sort [id#2L ASC NULLS FIRST], false, 0
     :  +- Exchange hashpartitioning(id#2L, 200), true, [id=#40]
     :     +- Exchange RoundRobinPartitioning(10), false, [id=#39]
     :        +- *(1) Project [(id#0L * 5) AS id#2L]
     :           +- *(1) Range (0, 1000000, step=1, splits=1)
     +- *(4) Sort [id#6L ASC NULLS FIRST], false, 0
        +- Exchange hashpartitioning(id#6L, 200), true, [id=#47]
           +- Exchange RoundRobinPartitioning(3), false, [id=#46]
              +- *(3) Project [(id#4L * 3) AS id#6L]
                 +- *(3) Range (0, 10000, step=1, splits=1)
   */

  // bucketing
  large.write // bucketing and the storing them to disk is very expensive as a shuffle
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_large")

  // saveAsTable method will store a dataframe in a form of files on a Spark "warehouse" directory
  // which Spark will create automatically
  small.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_small")

  val bucketedLarge: DataFrame = spark.table("bucketed_large") // something like prepartitioning
  val bucketedSmall: DataFrame = spark.table("bucketed_small")

  val bucketedJoin: DataFrame = bucketedLarge.join(bucketedSmall, "id")
  bucketedJoin.explain()
  /*
  == Physical Plan ==
  *(3) Project [id#11L]
  +- *(3) SortMergeJoin [id#11L], [id#13L], Inner
     :- *(1) Sort [id#11L ASC NULLS FIRST], false, 0
     :  +- *(1) Project [id#11L]
     :     +- *(1) Filter isnotnull(id#11L)
     :        +- *(1) ColumnarToRow
     :           +- FileScan parquet default.bucketed_large[id#11L] Batched: true, DataFilters: [isnotnull(id#11L)], Format: Parquet, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
     +- *(2) Sort [id#13L ASC NULLS FIRST], false, 0
        +- *(2) Project [id#13L]
           +- *(2) Filter isnotnull(id#13L)
              +- *(2) ColumnarToRow
                 +- FileScan parquet default.bucketed_small[id#13L] Batched: true, DataFilters: [isnotnull(id#13L)], Format: Parquet, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4

   */

  // bucketing for groups
  // scenario without bucketing
  val flightsDF: Dataset[Row] = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/flights/flights.json")
    .repartition(2) // RoundRobinPartitioning(2)

  val mostDelayedDF: Dataset[Row] = flightsDF
    .filter("origin = 'DEN' and arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)

  mostDelayedDF.explain()
  /*
  == Physical Plan ==
  *(4) Sort [avg(arrdelay)#53 DESC NULLS LAST], true, 0
  +- Exchange rangepartitioning(avg(arrdelay)#53 DESC NULLS LAST, 200), true, [id=#111]
     +- *(3) HashAggregate(keys=[origin#27, dest#24, carrier#18], functions=[avg(arrdelay#17)])
        +- Exchange hashpartitioning(origin#27, dest#24, carrier#18, 200), true, [id=#107]
           +- *(2) HashAggregate(keys=[origin#27, dest#24, carrier#18], functions=[partial_avg(arrdelay#17)])
              +- Exchange RoundRobinPartitioning(2), false, [id=#103]
                 +- *(1) Project [arrdelay#17, carrier#18, dest#24, origin#27]
                    +- *(1) Filter (((isnotnull(origin#27) AND isnotnull(arrdelay#17)) AND (origin#27 = DEN)) AND (arrdelay#17 > 1.0))
                       +- FileScan json [arrdelay#17,carrier#18,dest#24,origin#27] Batched: false, DataFilters: [isnotnull(origin#27), isnotnull(arrdelay#17), (origin#27 = DEN), (arrdelay#17 > 1.0)], Format: JSON, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [IsNotNull(origin), IsNotNull(arrdelay), EqualTo(origin,DEN), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string,origin:string>
   */

  // scenario with bucketing
  flightsDF.write
    .partitionBy("origin")
    .bucketBy(4, "dest", "carrier")
    .saveAsTable("flights_bucketed") // just as long as a shuffle -> 2 shuffles

  val bucketedFlights: DataFrame = spark.table("flights_bucketed")

  val mostDelayed2 = bucketedFlights
    .filter("origin = 'DEN' and arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)

  mostDelayed2.explain()
  /*
  == Physical Plan ==
  *(2) Sort [avg(arrdelay)#155 DESC NULLS LAST], true, 0
  +- Exchange rangepartitioning(avg(arrdelay)#155 DESC NULLS LAST, 200), true, [id=#258]
     +- *(1) HashAggregate(keys=[origin#129, dest#126, carrier#120], functions=[avg(arrdelay#119)])
        +- *(1) HashAggregate(keys=[origin#129, dest#126, carrier#120], functions=[partial_avg(arrdelay#119)])
           +- *(1) Project [arrdelay#119, carrier#120, dest#126, origin#129]
              +- *(1) Filter (isnotnull(arrdelay#119) AND (arrdelay#119 > 1.0))
                 +- *(1) ColumnarToRow
                    +- FileScan parquet default.flights_bucketed[arrdelay#119,carrier#120,dest#126,origin#129] Batched: true, DataFilters: [isnotnull(arrdelay#119), (arrdelay#119 > 1.0)], Format: Parquet, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [isnotnull(origin#129), (origin#129 = DEN)], PushedFilters: [IsNotNull(arrdelay), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string>, SelectedBucketsCount: 4 out of 4

   */

  // bucket pruning
  val the10: Dataset[Row] = bucketedLarge.filter($"id" === 10)
  the10.show()
  /*
  +---+
  | id|
  +---+
  | 10|
  +---+
   */
  the10.explain()
  /*
  == Physical Plan ==
  *(1) Project [id#11L]
  +- *(1) Filter (isnotnull(id#11L) AND (id#11L = 10))
     +- *(1) ColumnarToRow
        +- FileScan parquet default.bucketed_large[id#11L] Batched: true, DataFilters: [isnotnull(id#11L), (id#11L = 10)], Format: Parquet, Location: InMemoryFileIndex[file:/C:/Programming/Scala/OnlineCources/Spark/RockTheJVM/spark-optimization-ma..., PartitionFilters: [], PushedFilters: [IsNotNull(id), EqualTo(id,10)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 1 out of 4
   */

  def main(args: Array[String]): Unit ={

  }
}
