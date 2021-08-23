package homeworks

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.lang

object BroadcastJoins {
  val spark = SparkSession.builder()
    .appName("Broadcast Joins")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  // defining small DF
  val rows: RDD[Row] = sc.parallelize(List(
    Row(0, "zero"),
    Row(1, "first"),
    Row(2, "second"),
    Row(3, "third"),
  ))

  val rowsSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("order", StringType)
  ))

  val lookupTable: DataFrame = spark.createDataFrame(rows, rowsSchema)

  // defining large DF
  val table: Dataset[lang.Long] = spark.range(1, 100000000) // whenever you create Range there will be id column

  // the innocent join
  val joined = table.join(lookupTable, "id")
  joined.explain()
  /*
  == Physical Plan ==
  *(5) Project [id#6L, order#3]
  +- *(5) SortMergeJoin [id#6L], [cast(id#2 as bigint)], Inner
     :- *(2) Sort [id#6L ASC NULLS FIRST], false, 0
     :  +- Exchange hashpartitioning(id#6L, 200), true, [id=#27]
     :     +- *(1) Range (1, 100000000, step=1, splits=8)
     +- *(4) Sort [cast(id#2 as bigint) ASC NULLS FIRST], false, 0
        +- Exchange hashpartitioning(cast(id#2 as bigint), 200), true, [id=#33]
           +- *(3) Filter isnotnull(id#2)
              +- *(3) Scan ExistingRDD[id#2,order#3]
   */
  // joined.show() // takes an ice age
  /*
  +---+------+
  | id| order|
  +---+------+
  |  1| first|
  |  3| third|
  |  2|second|
  +---+------+
   */

  // a smarter join
  val joinedSmart = table.join(broadcast(lookupTable), "id")
  joinedSmart.explain()
  /*
  == Physical Plan ==
  *(2) Project [id#6L, order#3]
  +- *(2) BroadcastHashJoin [id#6L], [cast(id#2 as bigint)], Inner, BuildRight
     :- *(2) Range (1, 100000000, step=1, splits=8)
     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint))), [id=#64]
        +- *(1) Filter isnotnull(id#2)
           +- *(1) Scan ExistingRDD[id#2,order#3]
  */
//  joinedSmart.show()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 30) // switch off auto broadcast

  // auto-broadcast detection
  val bigTable = spark.range(1, 100000000)
  val smallTable = spark.range(1, 1000)
  val joinedNumbers = bigTable.join(smallTable, "id") // BroadcastExchange HashedRelationBroadcastMode
  joinedNumbers.explain()
  /*
  == Physical Plan ==
  *(2) Project [id#12L]
  +- *(2) BroadcastHashJoin [id#12L], [id#14L], Inner, BuildRight
     :- *(2) Range (1, 100000000, step=1, splits=8)
     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false])), [id=#88]
        +- *(1) Range (1, 1000, step=1, splits=8)
   */

  def main(args: Array[String]): Unit = {
  Thread.sleep(1000000) // 2 minutes
  }
}
