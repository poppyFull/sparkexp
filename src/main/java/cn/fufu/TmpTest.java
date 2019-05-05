package cn.fufu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.sql.*;
import java.util.Arrays;

public class TmpTest {

    public static void connMysql() {
        Connection conn;
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/";
        String user = "root";
        String password = "123456";
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed())
                System.out.println("connection to mysql");
            Statement stm = conn.createStatement();
            String sql = "select * from world.city limit 10";
            ResultSet rs = stm.executeQuery(sql);
            String cityName;
            while (rs.next()) {
                cityName = rs.getString("Name");
                System.out.println("cityName: " + cityName);
            }
            stm.close();
            conn.close();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String s = "2019-04-25 14:20:31,298 WARN org.apache.hadoop.ipc.Server: IPC Server handler 97 on 8020, call org.apache.hadoop.hdfs.protocol.ClientProtocol.getFileInfo from 190.168.177.13:36544 Call#77 Retry#0: output error";
        System.out.println(s.substring(24));
        System.out.println("time: " + s.substring(0, 23));

//        connMysql();

        System.out.println(Arrays.asList("a", "io", "9"));

        SparkConf conf = new SparkConf().setAppName("Spark Exp").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("file:///G:/spark-data/active_namenode/hadoop-hdfs-namenode-i186z19*");
        JavaRDD<String> rdd2 = rdd1.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return (s.contains("WARN"));
//                return (s.contains("WARN") || s.contains("warn"));
            }
        });
        JavaRDD<String> rddTime = rdd2.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                return s.substring(0, 23);
            }
        });
        JavaRDD<String> distinct = rddTime.distinct();
        System.out.println(distinct.count());
//        System.out.println("WARNLines: " + rdd2.count());



    }
}
