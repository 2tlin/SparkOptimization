package homeworks

import generator.DataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import part5rddtransformations.ReusingObjects.MutableTextStats

object ReusingObjects {
  val spark = SparkSession.builder()
    .appName("Reusing Objects")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  /*
  Analyze text
  we use generateText function from DataGenerator
  to receive millions batches of text from data sources with id and some test like:
  "35 // some text

  Aggregated stats per each data source id:
  - the number of lines in total
  - total numbers of words
  - length of the longest word
  - the number of occurrences of the word "imperdiet"

  Results should be VERY FAST. Time is critical
   */

  val textPath = "src/main/resources/data/lipsum/words.txt"
  val criticalWord = "imperdiet"

  def generateData() = {
    DataGenerator.generateText("src/main/resources/data/lipsum/words.txt", 60000000, 3000000, 200)
  }

  val text: RDD[(String, String)] = sc.textFile(textPath).map { line =>
    val tokens = line.split("//")
    (tokens(0), tokens(1))
  }


  // version 1 using case classes
  case class TextStats(nLines: Int, nWords: Int, maxWordLength: Int, occurrences: Int)

  object TextStats {
    val zero = TextStats(0, 0, 0, 0)
  }

  def collectStats(): collection.Map[String, TextStats] = {
    def aggregateNewRecord(textStats: TextStats, record: String): TextStats = {
      val newWords = record.split(" ")
      val longestWord = newWords.maxBy(_.length)
      val newOccurrunces = newWords.count(_ == criticalWord)

      TextStats( // we create an new instance for every single record which we aggragate in our data
        textStats.nLines + 1,
        textStats.nWords + newWords.length,
        if (longestWord.length > textStats.maxWordLength) longestWord.length else textStats.maxWordLength,
        textStats.occurrences + newOccurrunces
      )
    }

    def combineStats(stats1: TextStats, stats2: TextStats): TextStats = {
      TextStats(
        stats1.nLines + stats2.nLines,
        stats1.nWords + stats2.nWords,
        if(stats1.maxWordLength > stats2.maxWordLength) stats1.maxWordLength else stats2.maxWordLength,
        stats1.occurrences + stats2.occurrences
      )
    }

    val aggregate: RDD[(String, TextStats)] = text.aggregateByKey(TextStats.zero)(aggregateNewRecord, combineStats)
    aggregate.collectAsMap()
  }

  // version 2 using mutable data structures
  case class mutableTextStats(var nLines: Int, var nWords: Int, var maxWordLength: Int, var occurrences: Int)
    extends Serializable

  object mutableTextStats extends Serializable {
    def zero = new mutableTextStats(0, 0, 0, 0)
  }


  def collectStats2(): collection.Map[String, MutableTextStats] = {
    def aggregateNewRecord(textStats: MutableTextStats, record: String): MutableTextStats = {
      val newWords = record.split(" ")
      val longestWord = newWords.maxBy(_.length)
      val newOccurrunces = newWords.count(_ == criticalWord)

      textStats.nLines += 1
      textStats.nWords += newWords.length
      textStats.maxWordLength = if (longestWord.length > textStats.maxWordLength) longestWord.length else textStats.maxWordLength
      textStats.occurrences += newOccurrunces

      textStats
    }

    def combineStats(stats1: MutableTextStats, stats2: MutableTextStats): MutableTextStats = {

      stats1.nLines += stats2.nLines
      stats1.nWords += stats2.nWords
      stats1.maxWordLength = if(stats1.maxWordLength > stats2.maxWordLength) stats1.maxWordLength else stats2.maxWordLength
      stats1.occurrences += stats2.occurrences

      stats1
    }

    val aggregate: RDD[(String, MutableTextStats)] = text.aggregateByKey(MutableTextStats.zero)(aggregateNewRecord, combineStats)
    aggregate.collectAsMap()
  }

  // version 3 using JVM arrays
  object UglyTextStats extends Serializable {
    val nLinesIndex = 0
    val nWordsIndex = 1
    val longestWordIndex = 2
    val occurrencesIndex = 3

    def aggregateNewRecord(textStats: Array[Int], record: String): Array[Int] = {
      val newWords = record.split(" ") // Array of strings

      var i = 0
      while (i < newWords.length) {
        val word = newWords(i)
        val wordLength = word.length

        textStats(longestWordIndex) = if (wordLength > textStats(longestWordIndex)) wordLength else textStats(longestWordIndex)
        textStats(occurrencesIndex) += (if (word == criticalWord) 1 else 0)

        i += 1
      }

      textStats(nLinesIndex) += 1
      textStats(nWordsIndex) += newWords.length

      textStats
    }

    def combineStats(stats1: Array[Int], stats2: Array[Int]): Array[Int] = {
      stats1(nLinesIndex) += stats2(nLinesIndex)
      stats1(nWordsIndex) += stats2(nWordsIndex)
      stats1(longestWordIndex) = if (stats1(longestWordIndex) > stats2(longestWordIndex)) stats1(longestWordIndex) else stats2(longestWordIndex)
      stats1(occurrencesIndex) += stats2(occurrencesIndex)

      stats1
    }
  }

  def collectStats3(): collection.Map[String, Array[Int]] = {
    val aggregate: RDD[(String, Array[Int])] = text.aggregateByKey(Array.fill(4)(0))(UglyTextStats.aggregateNewRecord, UglyTextStats.combineStats)
    aggregate.collectAsMap()
  }


  def main(args: Array[String]): Unit = {
    collectStats()   // 4 s
    collectStats2()  // 2 s
    collectStats3()  // 2 s
    Thread.sleep(10000000)
  }
}
