package training.day1.rdd;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static training.Utils.DATA_DIRECTORY_PATH;

public class RddOperationsJava {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("RDD operations java")
                .getOrCreate();

        JavaRDD<String> text = spark.read().textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt").javaRDD();

        //Lets count number of non empty lines
        long numberOfNonEmptyLines = text.filter(line -> !line.isEmpty()).count();
        System.out.println("There are " + numberOfNonEmptyLines + " non empty lines");

        //TODO
        //Find what is the most frequent word length in text
        Integer mostFrequentWordLength = null;

        System.out.println("Most frequent word length in text is " + mostFrequentWordLength);

        //TODO
        //Print all distinct words for the most frequent word length
        List<String> words = null;
        System.out.println("Print all distinct words for the most frequent word length: " + words);
    }
}
