package training.day3.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.io.IOException;
import java.util.*;
import java.util.stream.BaseStream;

import static training.Utils.*;

public class TwitterStreamingJava {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Twitter streaming java")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext jstrc = new JavaStreamingContext(jsc, Durations.seconds(30));

        setupOAuth();
        JavaDStream<Status> tweets = TwitterUtils.createStream(jstrc);

//        Status status = null;
//        HashtagEntity[] hashtagEntities = status.getHashtagEntities();
//        hashtagEntities[0].getText();
//        status.getText();

        //Print tweets
        tweets.map(Status::getText).print();

        //TODO
        //DStream of hashtags from the tweets DStream
        //Write an implementation based on `tweets` dstream instead of `jstrc.queueStream(new ArrayDeque<>());`
//        JavaDStream<String> hashtags = jstrc.queueStream(new ArrayDeque<>());
        //JavaDStream<String> hashtags = ((JavaReceiverInputDStream<Status>) tweets).receiverInputDStream().map(x=>x.);
        final JavaDStream<String> hashtags = tweets.flatMap((FlatMapFunction<Status, String>) status -> Optional.ofNullable(status.getHashtagEntities())
                .filter(hashTag -> hashTag.length != 0)
                .map(hte -> Arrays.stream(hte).map(HashtagEntity::getText))
                .map(BaseStream::iterator)
                .orElseGet(Collections::emptyIterator));

        //TODO
        //Count hashtags over last window
        //Using `hashtags` write correct implementation for counting hashtags
        //JavaPairDStream<String, Long> countedHashtags = jstrc.queueStream(new ArrayDeque<>()).mapToPair(x -> new Tuple2<>("", 0L));
        JavaPairDStream<String, Long> countedHashtags = hashtags.countByValue();

        //TODO
        //Sort by popularity
        //Using `countedHashtags` get new dstream with hashtags
        //ordered by popularity where most popular hashtag are at the top
        //JavaDStream<String> trendingHashtags = jstrc.queueStream(new ArrayDeque<>());
        //JavaDStream<String> trendingHashtags = countedHashtags
        JavaDStream<String> trendingHashtags = countedHashtags
                .mapToPair((PairFunction<Tuple2<String, Long>, Long, String>) t2 -> new Tuple2<>(t2._2, t2._1))
                .transformToPair((Function<JavaPairRDD<Long, String>, JavaPairRDD<Long, String>>) t2 -> t2.sortByKey(false))
                .map(t2 -> t2._2);

        //Print top 10 hashtags
        trendingHashtags.print(10);

        //TODO
        //(OPTIONAL) Print tweets with hashtags from top 10 trending hashtags
        //Using `transformWith` transformation on `tweets` dstream create new dstream of tweets
        //new dstream should contain only those tweets that have hashtags from the top 10 hashtags of `trendingHashtags`
//        JavaDStream<Status> trendingTweets = jstrc.queueStream(new ArrayDeque<>());

        JavaDStream<Status> trendingTweets = tweets.filter(new Function<Status, Boolean>() {
            @Override
            public Boolean call(Status v1) throws Exception {
                //TODO: at streaming we can't convert trendingHashtags to rdd or collection of Strings
                //solution is just to repeat everything with Tuple3(hashtag, Status, hashTagCount) by changing tuple args places in positions in a touple per each step
                return Collections.disjoint(Arrays.asList(v1.getHashtagEntities()), trendingHashtags);
            }
        });


        trendingTweets.map(status -> formatTweet(status)).print();

        jstrc.start();
        jstrc.awaitTermination();
    }

    private static String formatTweet(Status tweet) {
        return tweet.getText() + "\n===========================================";
    }

    private static void setupOAuth() throws IOException {
        System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_KEY);
        System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_SECRET);
        System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN);
        System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET);
    }
}
