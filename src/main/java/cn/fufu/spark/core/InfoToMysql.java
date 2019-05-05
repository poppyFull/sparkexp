package cn.fufu.spark.core;

import cn.fufu.spark.function.WarnFilterFunction;
import cn.fufu.spark.function.WriteToMySQLFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class InfoToMysql {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark Exp").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> sourceRdd = sc.textFile("G:/spark-data/active_namenode/hadoop-hdfs-namenode-i186z19*");
        JavaRDD<String> warnRdd = sourceRdd.filter(new WarnFilterFunction());

        warnRdd.foreachPartition(new WriteToMySQLFunction("jdbc:mysql://localhost:3306/testdb", "root", "123456"));

    }
}
