package cn.fufu.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

public class KafkaProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.100.101:9092, 192.168.100.102:9092, 192.168.100.103:9092");
        properties.put("acks", "-1");
        properties.put("retries", 1);
        properties.put("batch.size", 16384);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

        try {
            FileReader fr = new FileReader(new File("G:/spark-data/active_namenode/hadoop-hdfs-namenode-i186z19.log.1"));
            BufferedReader br = new BufferedReader(fr);
            String msg = null;
            while ((msg = br.readLine())!=null) {
                producer.send(new ProducerRecord<String, String>("test", msg));
//                producer.send(new ProducerRecord<String, String>("test", "key-test", msg));
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                System.out.println(msg);
            }
            fr.close();
            br.close();
            producer.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
