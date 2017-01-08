package com.youzan.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xingjie.zhou on 2017/1/8.
 */
public class KafkaStream {

    private static JavaStreamingContext context;

    @BeforeClass
    public static void init() {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        context = new JavaStreamingContext(conf, Durations.seconds(20));
    }

    @Test
    public void test() {
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("user-behavior-topic", 1);

        //0:JavaStreamingContext //1.zk server 2.consumer 3.topic
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(context, "127.0.0.1:2181", "user_behavior_gruop", topicMap);

        JavaDStream<String> lines = messages.map(t -> {
            System.out.println(t);
            return t._2();
        });
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        wordCounts.reduceByKey((x, y) -> x + y).print();

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
