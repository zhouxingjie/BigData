package com.youzan.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by xingjie.zhou on 2016/12/18.
 */
public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("demo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("hdfs://localhost:9000/tmp/temp.txt");
//        JavaRDD<String> lines = sc.textFile("src/main/resources/kv.txt");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        counts.collect().forEach((tuple) -> System.out.printf("%s:%d\n", tuple._1, tuple._2));
        sc.stop();
    }
}
