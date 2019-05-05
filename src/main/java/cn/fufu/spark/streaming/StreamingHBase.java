package cn.fufu.spark.streaming;

import cn.fufu.spark.function.LogToHBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class StreamingHBase {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Streaming Process");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));


        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "192.168.100.101:9092,192.168.100.102:9092,192.168.100.103:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-01");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> messageDstream = stream.map(new Function<ConsumerRecord<String, String>, String>() {
            public String call(ConsumerRecord<String, String> record) {
                return record.value();
            }
        });
        final String tableName = "log1";
        messageDstream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreachPartition(new LogToHBase(tableName));
            }
        });
        ssc.start();
        ssc.awaitTermination();

    }
}
