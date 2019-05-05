package cn.fufu.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class InfoCounts {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark Exp").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("G:/spark-data/active_namenode/hadoop-hdfs-namenode-i186z19*");

        JavaRDD<String> rdd2 = rdd1.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("WARN");
            }
        });
        JavaRDD<String> rdd3 = rdd2.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                return s.substring(24);
            }
        });
        System.out.println("WARNLines: " + rdd3.count());

        JavaRDD<String> rdd4 = rdd3.distinct();
        System.out.println("distinctRes: " + rdd4.count());

    }
}
