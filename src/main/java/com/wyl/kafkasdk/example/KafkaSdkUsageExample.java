package com.wyl.kafkasdk.example;

import com.wyl.kafkasdk.KafkaConsumerClient;
import com.wyl.kafkasdk.KafkaProducerClient;
import com.wyl.kafkasdk.KafkaProperties;
import com.wyl.kafkasdk.KafkaTopicManager;
import com.wyl.kafkasdk.TopicDefinition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class KafkaSdkUsageExample {

    private KafkaSdkUsageExample() {
    }

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String topicName = "demo-topic";

        createTopic(bootstrapServers, topicName);
        sendMessage(bootstrapServers, topicName);
        consumeMessage(bootstrapServers, topicName);
    }

    public static void createTopic(String bootstrapServers, String topicName) {
        Properties adminProperties = KafkaProperties.adminProperties(bootstrapServers);
        TopicDefinition topicDefinition = TopicDefinition.builder(topicName)
                .partitions(3)
                .replicationFactor((short) 1)
                .config("cleanup.policy", "delete")
                .build();

        try (KafkaTopicManager topicManager = new KafkaTopicManager(adminProperties)) {
            topicManager.createTopic(topicDefinition);
        }
    }

    public static void sendMessage(String bootstrapServers, String topicName) {
        Properties producerProperties = KafkaProperties.producerProperties(
                bootstrapServers,
                StringSerializer.class,
                StringSerializer.class);

        try (KafkaProducerClient<String, String> producerClient = new KafkaProducerClient<>(producerProperties)) {
            RecordMetadata metadata = producerClient.send(
                    topicName,
                    0,
                    System.currentTimeMillis(),
                    "order-1001",
                    "{\"status\":\"created\"}",
                    Map.of("source", "sdk-demo".getBytes(StandardCharsets.UTF_8)));
            producerClient.flush();

            System.out.println("send ok, topic=" + metadata.topic()
                    + ", partition=" + metadata.partition()
                    + ", offset=" + metadata.offset());
        }
    }

    public static void consumeMessage(String bootstrapServers, String topicName) {
        Properties consumerProperties = KafkaProperties.consumerProperties(
                bootstrapServers,
                "demo-group",
                StringDeserializer.class,
                StringDeserializer.class);

        try (KafkaConsumerClient<String, String> consumerClient = new KafkaConsumerClient<>(consumerProperties)) {
            consumerClient.subscribe(Collections.singletonList(topicName));
            List<ConsumerRecord<String, String>> records = consumerClient.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("receive topic=" + record.topic()
                        + ", key=" + record.key()
                        + ", value=" + record.value());
            }
            consumerClient.commitSync();
        }
    }
}
