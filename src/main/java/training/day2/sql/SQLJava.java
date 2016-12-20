package training.day2.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;
import scala.collection.TraversableOnce;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.spark.sql.functions.*;

import static training.Utils.DATA_DIRECTORY_PATH;

public class SQLJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SQL example java")
                .getOrCreate();

        //load emails data to DataFrame
        Dataset<Row> emails = spark.read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "item")
                .load(DATA_DIRECTORY_PATH + "emails.xml");

        //print emails schema
        emails.printSchema();

        //register DataFrame as table
        emails.createOrReplaceTempView("emails");

        //describe table
        spark.sql("describe emails").show();

        //TODO
        //Write query to get all unique location values from emails
        spark.sql("select distinct location from emails").show(10);

        //TODO
        //Write query to get all unique keyword values from top level `keyword` column
        //hint: use `explode` function to flatten array elements
        spark.sql("select  explode(keyword) from emails").show(100);
        //or
        emails.select(explode(col("keyword"))).show();

        //TODO
        //Create table `shopping` based on `emails` table data with only `name`, `payment`, `quantity` and `shipping` columns
        Dataset<Row> shoppingDs =  spark.sql("select name, payment, quantity, shipping from emails");
        spark.sqlContext().registerDataFrameAsTable(shoppingDs, "shopping");
        //or
        emails.select(col("name"),col("payment"),col("quantity"),col("shipping")).show();

        //TODO
        //Select records from `shopping` table where `quantity` is greater then 1
        spark.sql("select  * from shopping where quantity > 1").show();
        //or
        spark.sql("from shopping").where(col("quantity").gt(1)).show();

        //TODO
        //Create table 'shipping_dates' that contains all `date` values from the `mail` top level column
        //hint: create intermediate dataFrames or tables to handle nesting levels
        Dataset<Row> shipping_dates = spark.sql("select explode(mail.date) as date from emails");

        //Register custom user defined function to parse date string with format MM/DD/YYYY into java.sql.Date type
        UDF1<String, java.sql.Date> udf = (String string) -> {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/mm/yyyy");
            Date date = simpleDateFormat.parse(string);

            return new java.sql.Date(date.getTime());
        };

        spark.udf().register("parseDate", udf, DataTypes.DateType);

        //TODO
        //Select unique and sorted records from `shipping_dates` table
        //hint: use `parseDate` udf to get correct sorting
        //spark.sql("");
        shipping_dates.distinct().selectExpr("parseDate(date) as date").orderBy(col("date").desc()).show();

        //TODO
        //Save `emails`, `shopping` and 'shipping_dates' table to json, csv and text files accordingly

        emails.
                coalesce(1).
                write().
                mode(SaveMode.Overwrite).
                json(DATA_DIRECTORY_PATH+"/my-emails.json");

        shoppingDs.
                coalesce(1).
                write().
                mode(SaveMode.Overwrite).
                json(DATA_DIRECTORY_PATH+"/my-shopping.csv");

        shipping_dates.
                coalesce(1).
                write().
                mode(SaveMode.Overwrite).
                json(DATA_DIRECTORY_PATH+"/my-shopping.txt");
    }
}
