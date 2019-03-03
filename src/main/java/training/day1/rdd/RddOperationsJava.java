package training.day1.rdd;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static training.Utils.DATA_DIRECTORY_PATH;

public class RddOperationsJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("RDD operations java")
                .getOrCreate();

        JavaRDD<String> textRdd = spark.read().textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt").javaRDD();

        //Lets count number of non empty lines
        long numberOfNonEmptyLines = textRdd.filter(line -> !line.isEmpty()).count();
        System.out.println("There are " + numberOfNonEmptyLines + " non empty lines");

        //TODO
        //Find what is the most frequent word length in textRdd
        int mostFrequentWordLength = textRdd
                .filter(line -> !line.isEmpty())
                .flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .map(String::trim)
                .map(word -> word.toLowerCase().replaceAll("[^a-z]", ""))
                .map(String::length)
                .sortBy((Function<Integer, Integer>) v1 -> v1, false, 1)
                .first();

        System.out.println("Most frequent word length in textRdd is " + mostFrequentWordLength);

        //TODO
//        //Print all distinct words for the most frequent word length
        List<String> top10Words = textRdd
                .filter(line -> !line.isEmpty())
                .flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .map(String::trim)
                .map(word -> word.toLowerCase().replaceAll("[^a-zA-Z]", ""))
                .filter(x -> x.length() >= mostFrequentWordLength - 20)
                .distinct()
                .collect();

        System.out.println("Print all distinct top10Words for the most frequent word length: " + top10Words);
    }

}
