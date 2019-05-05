package cn.fufu.spark.core;

import cn.fufu.spark.function.WarnFilterFunction;
import cn.fufu.spark.function.WriteToKafkaFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Properties;


public class InfoToKafka {
    public static void main(String[] args) throws InterruptedException {
//        String bootstrapServers = "192.168.100.101:9092, 192.168.100.102:9092, 192.168.100.103:9092";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.100.101:9092, 192.168.100.102:9092, 192.168.100.103:9092");
        properties.put("acks", "-1");
        properties.put("retries", 1);
        properties.put("batch.size", 16384);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        SparkConf conf = new SparkConf().setAppName("Spark Exp").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> sourceRdd = sc.textFile("G:/spark-data/active_namenode/hadoop-hdfs-namenode-i186z19*");

        // rdd contains "warn"
        JavaRDD<String> warnRdd = sourceRdd.filter(new WarnFilterFunction());

        // warnRdd分区写入kafka
        warnRdd.foreachPartition(new WriteToKafkaFunction(properties));

        Thread.sleep(1000000);

    }
}
