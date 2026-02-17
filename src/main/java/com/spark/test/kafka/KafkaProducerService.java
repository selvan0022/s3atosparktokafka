package com.spark.test.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Service class for managing Kafka Producer and sending messages
 */
public class KafkaProducerService {
    private KafkaProducer<String, String> producer;
    private final String brokerAddress;
    private final String topicName;

    /**
     * Constructor
     * @param brokerAddress Kafka broker address (e.g., localhost:9092)
     * @param topicName Kafka topic name
     */
    public KafkaProducerService(String brokerAddress, String topicName) {
        this.brokerAddress = brokerAddress;
        this.topicName = topicName;
    }

    /**
     * Initialize Kafka Producer with proper configuration
     */
    public void initialize() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        this.producer = new KafkaProducer<>(props);
        System.out.println("✓ Kafka Producer initialized successfully!");
        System.out.println("  Broker: " + brokerAddress);
        System.out.println("  Topic: " + topicName);
    }

    /**
     * Send message to Kafka topic
     * @param key Message key
     * @param value Message value
     */
    public void sendMessage(String key, String value) {
        if (producer == null) {
            System.err.println("✗ Producer not initialized!");
            return;
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("✗ Failed to send message: " + exception.getMessage());
                exception.printStackTrace();
            } else {
                System.out.println("✓ Message sent successfully!");
                System.out.println("  Key: " + key);
                System.out.println("  Topic: " + metadata.topic());
                System.out.println("  Partition: " + metadata.partition());
                System.out.println("  Offset: " + metadata.offset());
            }
        });
    }

    /**
     * Close the producer
     */
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
            System.out.println("✓ Kafka Producer closed successfully!");
        }
    }

    /**
     * Check if producer is initialized
     */
    public boolean isInitialized() {
        return producer != null;
    }
}