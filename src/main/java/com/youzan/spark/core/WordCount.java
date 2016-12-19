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
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        counts.collect().forEach(System.out::println);
        counts.collect().forEach((tuple) -> System.out.printf("%s:%d\n", tuple._1, tuple._2));
        sc.stop();
    }
}
