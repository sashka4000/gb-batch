package ru.gb.batch.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WordCountTest {

    private static JavaSparkContext sc;

    @BeforeClass
    public static void beforeClass() {
        SparkConf conf = new SparkConf().setAppName("Word Count Test").setMaster("local[*]");
        sc = new JavaSparkContext(conf);
        Logger.getRootLogger().setLevel(Level.ERROR);
    }

    @AfterClass
    public static void afterClass() {
        sc.stop();
    }

    @Test
    public void countWords() {
        // init data
        List<String> data = Arrays.asList(
                "row1, field, field",
                "row2, field, field, field"
        );
        Broadcast<String> delimiter = sc.broadcast(" ");
        JavaRDD<String> rdd = sc.parallelize(data);

        // transform data
        JavaPairRDD<String, Integer> result = WordCount.countWords(rdd, delimiter);
        Map<String, Integer> actual = result.collectAsMap();

        // validate
        Map<String, Integer> expected = new HashMap<>();
        expected.put("row1,", 1);
        expected.put("row2,", 1);
        expected.put("field", 2);
        expected.put("field,", 3);
        assertEquals(expected, actual);
    }
}