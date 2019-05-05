package cn.fufu.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CountLines {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark Exp").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("G:/spark-data/active_namenode/hadoop-hdfs-namenode-i186z19*");
        System.out.println("allLines: " + rdd1.count());

        JavaRDD<String> rdd2 = rdd1.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("INFO");
            }
        });
        System.out.println("INFOLines: " + rdd2.count());
        JavaRDD<String> rdd3 = rdd1.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return (s.contains("WARN") || s.contains("warn"));
            }
        });
        System.out.println("WARNLines: " + rdd3.count());


        JavaRDD<String> rdd4 = rdd1.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return (s.contains("error") || s.contains("ERROR"));
            }
        });
        System.out.println("errorLines: " + rdd4.count());
        JavaRDD<String> rdd5 = rdd4.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\\s+")).iterator();
            }
        });
        JavaPairRDD<String, Integer> rdd6 = rdd5.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> rdd7 = rdd6.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        JavaPairRDD<Integer, String> rdd8 = rdd7.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });
        List<Tuple2<Integer, String>> resList = rdd8.sortByKey(false).take(100);
        System.out.println(resList);
        String path = "G:/spark-result/ct1.txt";
        File f = new File(path);
        try {
            if(!f.isFile()) {
                f.createNewFile();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(path));
            for (Tuple2<Integer, String> item: resList) {
                writer.write(item.toString() + "\r\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
