package training.day1.accumulators

import org.apache.spark.sql.SparkSession
import training.Utils.DATA_DIRECTORY_PATH

object AccumulatorAliceCountScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Accumulator Alice counter scala")
      .getOrCreate()

    //TODO
    //Create counter using spark accumulator
    //hint: Accumulator can be created using spark.sparkContext()

    //Create rdd for the text file
    val input = spark.sparkContext.textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt")

    //Transforming text to words and counting alice words on the way
    val words = input.flatMap(x => x.split(" ")).map(word => {
      val cleanWord = word.replaceAll("[^A-Za-z]", "")
      if (cleanWord == "Alice") {
        //TODO
        //increment
      }
      cleanWord
    })

    //Action that triggers computation
    val wordCounts = words.countByValue()

    println("Count of word 'Alice' using accumulator: " + null) //TODO
    println("Actual count of word 'Alice': " + wordCounts("Alice"))
  }
}
