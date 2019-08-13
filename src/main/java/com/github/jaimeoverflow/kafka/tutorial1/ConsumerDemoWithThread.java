package com.github.jaimeoverflow.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args){

        new ConsumerDemoWithThread().run();
    }

    public ConsumerDemoWithThread() {

    }

    public void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        final Runnable myConsumerRunnable = new ConsumerRunnable(
                latch,
                bootstrapServers,
                groupId,
                topic
        );

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                logger.info("Caught shutdown hook");
                ((ConsumerRunnable) myConsumerRunnable).shutdown();
            }
        }));

        try {
            // wait until the application is over
            latch.await();
        } catch (InterruptedException e){
            logger.error("Application got interrupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        public ConsumerRunnable(CountDownLatch latch,
                              String bootstrapServers,
                              String groupId,
                              String topic) {
            this.latch = latch;

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


            //create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            //subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));

        }

        public void run() {
            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records
                            = consumer.poll(100); // new in kafka 2.0.0

                    for (ConsumerRecord record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                    }


                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // The wakeup() method is special method to interrupt consummer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }



    }
}
