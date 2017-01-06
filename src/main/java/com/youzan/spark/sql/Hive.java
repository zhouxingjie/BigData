package com.youzan.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xingjie.zhou on 2017/1/6.
 */
public class Hive {

    private static SparkSession sparkSession;

    @BeforeClass
    public static void init() {
        sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", "/spark-warehouse")
                .enableHiveSupport()
                .getOrCreate();
    }


    @Test
    public void test(){
        sparkSession.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        sparkSession.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv.txt' INTO TABLE src");
        sparkSession.sql("SELECT * FROM src").show();
        sparkSession.sql("SELECT COUNT(*) FROM src").show();
        sparkSession.sql("SELECT key, value FROM src WHERE key < 10 AND key > 0 ORDER BY key").show();

        List<Record> records = new ArrayList<>();
        for (int key = 1; key < 100; key++) {
            Record record = new Record();
            record.setKey(key);
            record.setVal("val_" + key);
            records.add(record);
        }
        Dataset<Row> recordsDF = sparkSession.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");

        sparkSession.sql("SELECT r.key,r.val FROM records r JOIN src s ON r.key = s.key order by key").show();
    }

    @AfterClass
    public static void destory() {
        sparkSession.stop();
    }
}
