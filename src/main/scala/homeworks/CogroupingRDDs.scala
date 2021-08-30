package homeworks

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CogroupingRDDs {
  val spark = SparkSession.builder()
    .appName("Cogrouping RDDs")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
  Take all the student attempts
  - if a student passed (at least one attempt > 9.0), send them an email "PASSED"
  - else send them an email "FAILED"
  In order to do that we need to join all the files examEmails + examIds + examScores
  by their candidateId
  */

  // DataGenerator.generateExamData(rootFolder, 1000000, 5)

  val rootFolder = "src/main/resources/generated/examData"


  def readIds(): RDD[(Long, String)] = sc.textFile(s"$rootFolder/examIds.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1)) // (candidate id, candidate name)
    }

  def readExamScores(): RDD[(Long, Double)] = sc.textFile(s"$rootFolder/examScores.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1).toDouble) // (candidate id, attempt score)
    }

  def readExamEmails(): RDD[(Long, String)] = sc.textFile(s"$rootFolder/examEmails.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1)) // (candidate id, attempt email)
    }

  def plainJoin() = {
    // before doing a join for scores I am going to reduce by key and just obtain
    // the maximum attempt for every single candidate because if I would send to candidate
    // an email with FAILED I need to make sure that the maximum attempt less then 9.0
    val scores = readExamScores().reduceByKey(Math.max) // will return the maximum of the 5 attempts
    val candidates = readIds()
    val emails = readExamEmails()

    val results: RDD[(Long, (String, String))] = candidates
      .join(scores) // RDD[(candidate id, (candidate name, candidate maximum attempt)]
      .join(emails) // RDD[(candidate id, ((candidate name, candidate maximum attempt), email))]
      .mapValues {
        case ((_, maxAttempt), email) =>
          if (maxAttempt >= 9.0) (email, "PASSED")
          else (email, "FAILED")
      }
    results.count()
    results.count()
  }

  def coGroupedJoin() = {
    val scores = readExamScores().reduceByKey(Math.max) // will return the maximum of the 5 attempts
    val candidates = readIds()
    val emails = readExamEmails()

    /*
    Cogroup function will take a number of other RDDs. It is very similar to a join except
    that cogroup will make sure that these RDDs are a part of cogroup which share the same partitioner.
    That means that we will co-partition all these 3 RDDs.
    And after the 3 RDDs share the same partitioner cogroup() will basically do a full outer join between the three RDDs.
     */
    val results: RDD[(Long, Option[(String, String)])] = candidates.cogroup(scores, emails) // RDD[(candidate id, (Iterable[candidate name], Iterable[candidate maximum attempt], Iterable[candidate email]))]
      .mapValues {
        case (nameIter, maxAttemptIter, emailIter) =>
          val name = nameIter.headOption
          val maxScore = maxAttemptIter.headOption
          val email = emailIter.headOption

          for {
            e <- email
            s <- maxScore
          } yield (e, if (s >= 9.0) "PASSED" else "FAILED")
      }
    results.count()
  }



  def main(args: Array[String]): Unit = {
    plainJoin()
    coGroupedJoin()
    Thread.sleep(1000000) // about 20 minutes to use Spark UI
  }

}
