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
    val mostFrequentWordLength: Integer = null

    System.out.println(s"Most frequent word length in text is $mostFrequentWordLength")

    //TODO
    //Print all distinct words for the most frequent word length
    val words: Seq[String] = null

    System.out.println(s"Print all distinct words for the most frequent word length: ${words.mkString(", ")}")
  }
}
