package cn.fufu.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.100.101:9092, 192.168.100.102:9092, 192.168.100.103:9092");
        properties.put("group.id", "group-4");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test"));
        int i = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record: records) {
                if (record.value().contains("qwerty")) {
                    System.out.println(record.value());
                }
//                i++;
//                System.out.println(i);
            }
        }

    }
}
