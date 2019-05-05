package cn.fufu.spark.function;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;
import java.util.Properties;

public class WriteToKafkaFunction implements VoidFunction<Iterator<String>> {
    private Producer<String, String> producer = null;
    private Properties properties;

    public WriteToKafkaFunction(Properties properties) {
        this.properties = properties;
    }

    public void call(Iterator<String> stringIterator) throws Exception {
        if (producer == null) {
            producer = new KafkaProducer<String, String>(properties);
        }
        while (stringIterator.hasNext()) {
            String msg = stringIterator.next();
            producer.send(new ProducerRecord<String, String>("log", msg));
        }
        producer.close();
    }
}
