package com.youzan.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Created by xingjie.zhou on 2017/1/4.
 */
public class SqlSample {

    private static SparkSession sparkSession;

    @BeforeClass
    public static void init() {
        sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java Spark SQL basic example")
                .getOrCreate();
    }

    @Test
    public void dfJson() {
        Dataset<Row> df = sparkSession.read().json("src/main/resources/people.json");
        df.show();
        df.printSchema();
        df.select("name", "age").show();
        df.select(col("name"), col("age").plus(1)).show();
        df.filter(col("age").plus(1).gt(21)).show();
        df.groupBy("age").count().show();
        df.createOrReplaceTempView("people");
        sparkSession.sql("select * from people where age = 19").show();
    }

    @Test
    public void encoder() {
        Dataset<People> df = sparkSession.read().json("src/main/resources/people.json")
                .as(Encoders.bean(People.class));
        df.show();
        df.createOrReplaceTempView("people");
        sparkSession.sql("select * from people where age = 19").show();
    }

    @Test
    public void rdd() {
        JavaRDD<People> rdd = sparkSession.read().text("src/main/resources/people.txt")
                .javaRDD()
                .map(row -> {
                    String[] parts = row.getString(0).split(",");
                    People people = new People();
                    people.setName(parts[0]);
                    people.setAge(parts[1]);
                    return people;
                });

        Dataset<Row> df = sparkSession.createDataFrame(rdd, People.class);
        df.show();
        df.createOrReplaceTempView("people");
        Dataset<Row> df1 = sparkSession.sql("select name from people where age = 19");

        df1.map(row -> {
            return row.getAs("name") + ":" + row.getString(0);
        }, Encoders.STRING()).show();

    }

    @Test
    public void schema() {
        JavaRDD<String> rdd = sparkSession.read().textFile("src/main/resources/people.txt").toJavaRDD();
        String schemaString = "name age";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRdd = rdd.map(row -> {
            String[] arr = row.split(",");
            return RowFactory.create(arr[0], arr[1]);
        });
        Dataset<Row> df = sparkSession.createDataFrame(rowRdd, schema);
        df.createOrReplaceTempView("people");
        Dataset<Row> df1 = sparkSession.sql("select name,age from people where age = 19");

        JavaRDD<String> rdd1 = df1.map(row -> {
            return row.getString(0) + "," + row.getString(1);
        }, Encoders.STRING()).toJavaRDD();

        rdd1.saveAsTextFile("result");
    }

    @AfterClass
    public static void destory() {
        sparkSession.stop();
    }

}
