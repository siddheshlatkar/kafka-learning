package com.github.kafka.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

//Record with same keys will go to same partition. Can verify by rerunning the code
public class ProducerDemoWithKeys {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
    String bootstrapServers = "127.0.0.1:9092";
    //Create Producer Properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //Create Producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    for (int i = 0; i < 10; i++) {
      //Create producer record

      String topic = "first_topic";
      String value = "Hello World " + Integer.toString(i);
      String key = "id_" + Integer.toString(i);

      final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

      logger.info("Key: " + key);

      //send data - Async
      producer.send(record, new Callback() {
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          // executes every time a record is sent successfully or exception is thrown
          if (e == null) {
            logger.info("Recieved new metadata. \n" +
                    "Topic:" + recordMetadata.topic() + "\n" +
                    "Partition:" + recordMetadata.partition() + "\n" +
                    "Offset:" + recordMetadata.offset() + "\n" +
                    "Timestamp" + recordMetadata.timestamp());
          } else {
            logger.error("Error while producing", e);
          }
        }
      }).get();//Making .send() synchronous. Bad practice.
    }

    //Flush data and close producer
    producer.flush();
    producer.close();

  }
}
