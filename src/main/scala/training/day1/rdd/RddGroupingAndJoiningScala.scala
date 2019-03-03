package training.day1.rdd

import org.apache.spark.sql.SparkSession
import training.Utils.DATA_DIRECTORY_PATH

object RddGroupingAndJoiningScala {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("RDD grouping and joining java")
      .getOrCreate

    val personRdd = spark.sparkContext
      .textFile(DATA_DIRECTORY_PATH + "persons.csv")
      .map(line => {
        val columns = line.split(",")
        Person(columns(0), columns(1), columns(2))
      })

    val zipCodeRdd = spark.sparkContext
      .textFile(DATA_DIRECTORY_PATH + "zip.csv")
      .map(line => {
        val columns = line.split(",")
        (columns(1), columns(0))
      })

    //TODO
    //Group persons by city
    val groupedByCity: collection.Map[String, Iterable[Person]] = personRdd.groupBy(p => p.city).collectAsMap
    for ((city, persons) <- groupedByCity) {
      println(s"$city persons: $persons")
    }

    //TODO
    //Create pair rdd where key is a city and value is person
    val personPairRDD = personRdd.map(p => (p.city, p))

    //TODO
    //Join two RDDs and print zip code for each person
    val joined = personPairRDD.join(zipCodeRdd)
    //val personToZipCode: Map[Person, String] = joined.map(pair => (pair._2._1, pair._1)).collectAsMap
    val personToZipCode = {
      joined.map(pair => (pair._2._1, pair._1)).collectAsMap
    }
    for ((person, zipCode) <- personToZipCode) {
      println(s"$person zip code: $zipCode")
    }
  }

  case class Person(firstName: String, lastName: String, city: String)

}
