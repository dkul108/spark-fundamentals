package training.day1.rdd

import org.apache.spark.sql.SparkSession
import training.Utils.DATA_DIRECTORY_PATH

object RddOperationsScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("RDD operations scala")
      .getOrCreate()

    val text = spark.sparkContext.textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt")

    //Lets count number of non empty lines
    val numberOfNonEmptyLines = text.filter(line => !line.isEmpty).count
    println(s"There are $numberOfNonEmptyLines non empty lines")


    //TODO
    //Find what is the most frequent word length in text
    val mostFrequentWordLength: Integer = text.filter(line => !line.isEmpty)
      .flatMap {
        line => line.split(" ")
      }
      .map {
        x => x.toLowerCase.replaceAll("[^a-z]", "")
      }
      .map {
        word => ((word, word.length), 1)
      }
      .reduceByKey(_ + _)
      .map {
        case ((word, length), n) => (word, (length, n))
      }
      .sortBy(_._2._1, ascending = false)
      .first._2._1


    println(text.filter(line => !line.isEmpty)
      .flatMap {
        line => line.split(" ")
      }
      .map {
        x => x.toLowerCase.replaceAll("[^a-z]", "")
      }
      .map {
        word => ((word, word.length), 1)
      }
      .reduceByKey(_ + _)
      .map {
        case ((word, length), n) => (word, (length, n))
      }
      .sortBy(_._2._1, ascending = false)
      .take(10)
      .mkString(","))

    System.out.println(s"Most frequent word length in text is $mostFrequentWordLength")

    //TODO
    //Print all distinct words for the most frequent word length
    val words: Seq[String] = text.filter(line => !line.isEmpty)
      .flatMap {
        line => line.split(" ")
      }
      .map {
        x => x.toLowerCase.replaceAll("[^a-z]", "")
      }
      .map {
        word => ((word, word.length), 1)
      }
      .reduceByKey(_ + _)
      .map {
        case ((word, length), n) => (word, (length, n))
      }
      .filter(t => t._2._1 >= mostFrequentWordLength - 20)
      .sortBy(_._2._1, ascending = false)
      .map {
        _._1
      }
      .collect()

    System.out.println(s"Print all distinct words for the most frequent word length: ${words.mkString(", ")}")


  }
}
