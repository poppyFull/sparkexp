package cn.fufu.spark.core;

import cn.fufu.spark.function.WarnFilterFunction;
import cn.fufu.spark.function.WarnToHBaseFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public class InfoToHBase {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Spark Exp").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> sourceRdd = sc.textFile("file:///G:/spark-data/active_namenode/hadoop-hdfs-namenode-i186z19*");
        JavaRDD<String> warnRdd = sourceRdd.filter(new WarnFilterFunction());
        Configuration confiuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(confiuration);
        Admin admin = connection.getAdmin();

        String tableName = "warninfo";
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("no table_" +tableName + ", cant put data!");
            return;
        }
        warnRdd.foreachPartition(new WarnToHBaseFunction(tableName));
    }
}
