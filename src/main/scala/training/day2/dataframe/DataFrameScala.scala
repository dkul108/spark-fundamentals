package training.day2.dataframe

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import training.Utils.DATA_DIRECTORY_PATH

import scala.reflect.internal.util.TableDef.Column

object DataFrameScala {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("DataFrame example scala")
      .getOrCreate()

    val personsJsonTablePath = DATA_DIRECTORY_PATH + "persons.json"
    val personDF: Dataset[Row] = spark.read.json(personsJsonTablePath)

    //TODO
    //Print personDF schema
    personDF.printSchema()

    //TODO
    //Print 100 persons from personDF dataFrame
    personDF.show(100)

    //TODO
    //Create new dataFrame with only `firstName` and `lastName` columns of persons from Chicago
    val gangsters = personDF.select("firstName", "lastName").where(personDF.col("city") === "Chicago")

    //TODO
    //hint: there is a method in dataFrame to do that
    gangsters.show()
    gangsters.createOrReplaceTempView("gangsters")

    //TODO
    //Print all records from `gangsters` view ordered by lastName using select statement
    //hint: use `sql` method on `spark` object
    spark.sql("select * from gangsters order by lastName desc").show
  }
}
