package training.day1.rdd;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

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

        JavaRDD<String> text = spark.read().textFile(DATA_DIRECTORY_PATH + "alice-in-wonderland.txt").javaRDD();

        //Lets count number of non empty lines
        long numberOfNonEmptyLines = text.filter(line -> !line.isEmpty()).count();
        System.out.println("There are " + numberOfNonEmptyLines + " non empty lines");

        //TODO
        //Find what is the most frequent word length in text
        Integer mostFrequentWordLength =
                text.
                flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .map(word -> word.toLowerCase().replaceAll("[^a-z]", ""))
                .filter(str -> !str.isEmpty())
                .keyBy(word -> word.length())
                .aggregateByKey(0, (count, word) -> count + 1, (c1, c2) -> c1 + c2).
                max(new SerializableComparator()).
                 _1();

        System.out.println("Most frequent word length in text is " + mostFrequentWordLength);

        //TODO
        //Print all distinct words for the most frequent word length
        List<String> words = text.
                flatMap(x -> Arrays.asList(x.split(" ")).
                iterator()).
                filter(word->word.length() == mostFrequentWordLength).
                distinct().
                collect();
        System.out.println("Print all distinct words for the most frequent word length: " + words);
    }

    static class SerializableComparator implements Serializable, Comparator<Tuple2<Integer, Integer>> {

        @Override
        public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
            return o1._2() - o2._2();
        }
    }
}
