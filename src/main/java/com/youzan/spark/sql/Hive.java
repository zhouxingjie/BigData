package com.youzan.spark.sql;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by xingjie.zhou on 2017/1/6.
 */
public class Hive {

    private static SparkSession sparkSession;

    @BeforeClass
    public static void init() {
        String warehouseLocation = "/spark-warehouse";
        sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();
    }


    @Test
    public void test(){
        sparkSession.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        sparkSession.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv.txt' INTO TABLE src");
        sparkSession.sql("SELECT * FROM src").show();
    }

    @AfterClass
    public static void destory() {
        sparkSession.stop();
    }
}
