package cn.fufu.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class TopWords {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark Exp").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("G:/spark-data/spark-files/spark-files/file-*");

        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.toLowerCase().replace(",", " ").replace(".", " ").split("\\s+")).iterator();
            }
        });
        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        JavaPairRDD<Integer, String> rdd5 = rdd4.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });
        System.out.println(rdd5.sortByKey(false).values().take(10));

    }
}
