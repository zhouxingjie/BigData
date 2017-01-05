package com.youzan.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by xingjie.zhou on 2016/12/19.
 */
public class action {

    private static JavaSparkContext sc;
    private static JavaRDD<String> lines;

    @BeforeClass
    public static void init() {
        SparkConf conf = new SparkConf().setAppName("demo").setMaster("local[*]");
        sc = new JavaSparkContext(conf);
        lines = sc.textFile("hdfs://localhost:9000/tmp/temp.txt");
        lines.persist(StorageLevel.MEMORY_ONLY());
    }

    @Test
    public void coalesceTest() {
        lines.collect();
        System.out.println(lines.partitions().size());
        lines.coalesce(4, true);
        System.out.println(lines.partitions().size());

    }

    /**
     * rdd合并
     */
    @Test
    public void unionTest() {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello spark", "hello world"));
        JavaRDD<String> rdd1 = lines.union(rdd);
        rdd1.collect().forEach(System.out::println);
    }

    /**
     * 取交集
     */
    @Test
    public void intersectionTest() {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello spark", "hello world"));
        JavaRDD<String> rdd1 = lines.intersection(rdd);
        rdd1.collect().forEach(System.out::println);
    }

    /**
     * 去重
     */
    @Test
    public void subtractnTest() {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello spark", "hello world"));
        JavaRDD<String> rdd1 = lines.subtract(rdd);
        rdd1.collect().forEach(System.out::println);
    }

    /**
     * 每一个元素都变成了RDD分区中的迭代器
     */
    @Test
    public void mapPartitionsTest(){
        JavaRDD<String> words = lines.mapPartitions(it -> {
            List<String> list = new ArrayList<>();
            while(it.hasNext()){
                list.add(it.next().toUpperCase());
            }
            return list.iterator() ;
        });
        words.collect().forEach(System.out::println);
    }

    @Test
    public void mapValuesTest(){
        System.out.println( lines.count());
    }

}
