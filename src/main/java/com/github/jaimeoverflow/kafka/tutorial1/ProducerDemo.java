package com.github.jaimeoverflow.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        // Create Producer properties
        Properties properties = new Properties();

        //properties.setProperty("bootstrap.servers", bootstrapServers)
        // ProducerConfig.BOOTSTRAP_SERVERS_CONFIG == "bootstrap.servers"
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);


        // We use key/value serializer to help the producer to know what type of value
        // you're sending to kafka and how this should be serialized to bytes.
        // Because kafka will convert whatever we send into bytes
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create producer record
        ProducerRecord<String, String> record
                = new ProducerRecord<String, String>("first_topic", "hello world");

        // send data
        // Asynchronous, it happens in the background so the program finish very quickly
        // and never produce the data.
        producer.send(record);

        // To solve this we use flush
        producer.flush();

        producer.close();
    }

}
