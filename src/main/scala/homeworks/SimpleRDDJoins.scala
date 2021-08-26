package homeworks

import generator.DataGenerator
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SimpleRDDJoins {
  val spark = SparkSession.builder()
    .appName("RDD Joins")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
  Scenario
  We are runnung a nation-wide standart exam (> 1M students)
    - every candidate can attempt the exam 5 times
    - each attempt is scored 0 - 10
    - final score is the max of all attempts
  Goal: the number of candidates who passed the exam
    - passed = at least one attempt above 9.0
  Data:
    - candidates: candidate ID (long), candidate name (string)
    - exam scores: candidate ID (long), attempt score (double)
   */

  val rootFolder = "src/main/resources/generated/examData"

  // DataGenerator.generateExamData(rootFolder, 1000000, 5)

  def readIds(): RDD[(Long, String)] = sc.textFile(s"$rootFolder/examIds.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1))
    }
    .partitionBy(new HashPartitioner(10))

  def readExamScores(): RDD[(Long, Double)] = sc.textFile(s"$rootFolder/examScores.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1).toDouble)
    }

  // goal: the number of candidates who passed the exam = at least one attempt above 9.0
  def plainJoin(): Long = {
    val candidates: RDD[(Long, String)] = readIds() // Pair (key, value) RDD
    val scores: RDD[(Long, Double)] = readExamScores() // Pair (key, value) RDD

    // simple join
    // every candidate will appear 5 times in this joined dataframe because
    // it will have 5 attempts in this scores RDD
    val joined: RDD[(Long, (Double, String))] = scores.join(candidates)
    // for every key I will reduce the values. The values here in the joined RDD will be appear
    // between (score attempt, candidate name), String). This joined will be RDD[(Long, (Double, String))]
    // It means that for every candidate Id which is Long we have an exam attempt which is Double and
    // candidate's name
    // I want to reduce by key by comparing this tuples (score attempt, candidate name).
    // and filter which takes tuple by candidate id and then use first parameter which is score attempt to compare with 9.0
    val finalScores: RDD[(Long, (Double, String))] = joined
      .reduceByKey((pair1, pair2) => if (pair1._1 > pair2._1) pair1 else pair2)
      .filter(pair => pair._2._1 > 9.0)

    finalScores.count()
  }

  def preAggregateJoin(): Long = {
    val candidates: RDD[(Long, String)] = readIds() // Pair (key, value) RDD
    val scores: RDD[(Long, Double)] = readExamScores() // Pair (key, value) RDD

    // do aggregation first and then to do a join -> 10% perf increase

    // For every candidate id we will have a maximum score
    val maxScores: RDD[(Long, Double)] = scores.reduceByKey(Math.max)
    val finalScores = maxScores.join(candidates).filter(pair => pair._2._1 > 9.0)

    finalScores.count()
  }

  def preFiltering() = {
    val candidates: RDD[(Long, String)] = readIds() // Pair (key, value) RDD
    val scores: RDD[(Long, Double)] = readExamScores() // Pair (key, value) RDD

    // do filtering first before a join
    val maxScores: RDD[(Long, Double)] = scores.reduceByKey(Math.max)
    val finalScores = maxScores.join(candidates)

    finalScores.count()
  }

  def coPartitioning() = {
    val candidates: RDD[(Long, String)] = readIds() // Pair (key, value) RDD
    val scores: RDD[(Long, Double)] = readExamScores() // Pair (key, value) RDD

    // receive current partitioner from candidates
    val partitionerForScores = candidates.partitioner match {
      case None => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }

    val repartitionedScores = scores.partitionBy(partitionerForScores)

    // with copartitioning Spark will not shuffle candidates anymore
    // if both RDDs share the same partitioner
    val joined: RDD[(Long, (Double, String))] = repartitionedScores.join(candidates)

    val finalScores: RDD[(Long, (Double, String))] = joined
      .reduceByKey((pair1, pair2) => if (pair1._1 > pair2._1) pair1 else pair2)
      .filter(pair => pair._2._1 > 9.0)

    finalScores.count()
  }

  def combined() = {
    val candidates: RDD[(Long, String)] = readIds() // Pair (key, value) RDD
    val scores: RDD[(Long, Double)] = readExamScores() // Pair (key, value) RDD

    // receive current partitioner from candidates
    val partitionerForScores = candidates.partitioner match {
      case None => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }

    val repartitionedScores = scores.partitionBy(partitionerForScores)

    // do filtering first before a join
    val maxScores: RDD[(Long, Double)] = repartitionedScores.reduceByKey(Math.max)
    val finalScores = maxScores.join(candidates)

    finalScores.count()
  }

  def main(args: Array[String]): Unit = {
    println(s"finalScores for plainJoin = ${plainJoin()}") // 376560
    println(s"finalScores for preAggregateJoin = ${preAggregateJoin()}") // 376560
    println(s"finalScores for preFiltering = ${preFiltering()}") // 1000001
    println(s"finalScores for coPartitioning = ${coPartitioning()}") // 376560
    println(s"finalScores for combined = ${combined()}") // 1000001

    Thread.sleep(1000000) // about 20 minutes to analyze Spark UI
  }
}
