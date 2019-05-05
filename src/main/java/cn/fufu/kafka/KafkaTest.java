package cn.fufu.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaTest {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.100.101:9092, 192.168.100.102:9092, 192.168.100.103:9092");
        properties.put("acks", "-1");
        properties.put("retries", 1);
        properties.put("batch.size", 16384);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        producer = new KafkaProducer<String, String>(properties);
        String msg = "qwerty";
        while (true) {
            producer.send(new ProducerRecord<String, String>("test", msg));
            System.out.println(msg);
            Thread.sleep(100);
        }

//        producer.close();


    }
}
