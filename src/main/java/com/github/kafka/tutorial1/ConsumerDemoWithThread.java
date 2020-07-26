package com.github.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
  public static void main(String[] args) {
    new ConsumerDemoWithThread().run();
  }

  private ConsumerDemoWithThread() {

  }

  private void run() {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "my-sixth-application";
    String topic = "first_topic";

    CountDownLatch latch = new CountDownLatch(1);

    logger.info("Creating the consumer thread");

    Runnable myConsumerRunnable = new ConsumerRunnable(
      bootstrapServers,
      groupId,
      topic,
      latch
    );

    Thread myThread = new Thread(myConsumerRunnable);
    myThread.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Caught shutdown hook");
      ((ConsumerRunnable)myConsumerRunnable).shutDown();
      try {
        latch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      logger.info("Application has expired");
    }));

    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.error("Application interrupted", e);
    } finally {
      logger.info("Application is closing");
    }
  }
}

class ConsumerRunnable implements Runnable {

  private CountDownLatch latch;
  private KafkaConsumer<String, String> consumer;
  private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

  ConsumerRunnable(String bootstrapServers,
                   String groupId,
                   String topic,
                   CountDownLatch latch) {
      this.latch = latch;

    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    consumer = new KafkaConsumer<String, String>(properties);

    consumer.subscribe(Arrays.asList(topic));
  }

  @Override
  public void run() {
    try {
      while(true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
          logger.info("key: " + record.key() + "\n" +
                  "value: " + record.value() + "\n" +
                  "Partition: " + record.partition() + "\n" +
                  "Offset: " + record.offset());
        }
      }
    } catch (WakeupException e) {
      logger.info("Received shutdown signal!");
    } finally {
      consumer.close();
      latch.countDown();
    }
  }

  public void shutDown() {
    consumer.wakeup();
  }
}
