package com.youzan.spark.sql;

import org.apache.spark.sql.SparkSession;
import org.junit.Test;

/**
 * Created by xingjie.zhou on 2017/1/4.
 */
public class SqlExample {

    private static SparkSession session;

    @Test
    public void init(){
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java Spark SQL basic example")
                .getOrCreate();
        System.out.println(spark);
    }

}
