package com.github.jaimeoverflow.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAsAssignSeek {

    public static void main(String[] args){

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAsAssignSeek.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
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


        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L; //long
        // assign
        // This consumer will read from partition
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        // We move to offset 15
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while(true) {
           ConsumerRecords<String, String> records
                   = consumer.poll(100); // new in kafka 2.0.0

            for (ConsumerRecord record : records) {
                numberOfMessagesReadSoFar += 1;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break; // to exit the for loop
                }

            }


        }

        logger.info("Exiting the application");


    }
}
