package com.youzan.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by xingjie.zhou on 2017/1/7.
 */
public class NetStream {

    private static JavaStreamingContext context;

    @BeforeClass
    public static void init() {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        context = new JavaStreamingContext(conf, Durations.seconds(30));
    }

    @Test
    public void test() {
        JavaReceiverInputDStream<String> lines = context.socketTextStream("localhost", 9999, StorageLevels.MEMORY_AND_DISK_SER);

        JavaDStream<String> words = lines.flatMap(line -> {
            return Arrays.asList(line.split(" ")).iterator();
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((x, y) -> x + y);
        wordCounts.print();

        context.start();
        try {
            context.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void after() {
        context.stop();
    }
}
