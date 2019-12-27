package org.udemy.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final String SERVER = "127.0.0.1:9092";

    public static void main(String[] args) {
        System.out.println("Hello Kafka!");

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("", "");
        properties.setProperty("", "");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "this is record !!!" + i);
            // asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception ex) {
                    // execute every time a record send or an exception throws
                    if (ex == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offsets: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        logger.error("Errors while producing", ex);
                    }
                }
            });
        }

        //  flush
        producer.flush();

        // flush and close
        producer.close();
    }

}
