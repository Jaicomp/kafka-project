package com.github.jaimeoverflow.kafka.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args){

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // When the producer takes a string, serializes it to bytes and send it to Kafka
        // when Kafka sends these bytes right back to our consumer, our consumer has to take
        // these bytes and create a string from it. That's deserialization.
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // AUTO_OFFSET_RESET_CONFIG has three values:
        // - earliest: want to read from the very very beginning of your topic
        // - latest: only read new messages
        // - none: throw error if there's no offsets being saved.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer
                = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic(s)
        // With singleton we only subscribe to one topic
        // consumer.subscribe(Collections.singleton("first_topic"));

        // with arrays we can subscribe to more topics
        // consumer.subscribe(Arrays.asList("first_topic", "second_topic"))
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while(true) {
           ConsumerRecords<String, String> records
                   = consumer.poll(100); // new in kafka 2.0.0

            for (ConsumerRecord record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

            }


        }


    }
}
