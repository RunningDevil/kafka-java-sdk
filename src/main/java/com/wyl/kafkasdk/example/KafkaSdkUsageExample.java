package com.wyl.kafkasdk.example;

import com.wyl.kafkasdk.KafkaAdmin;
import com.wyl.kafkasdk.KafkaConfig;
import com.wyl.kafkasdk.KafkaConsumer;
import com.wyl.kafkasdk.KafkaProducer;
import java.util.List;

public final class KafkaSdkUsageExample {

    private KafkaSdkUsageExample() {
    }

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        KafkaConfig config = KafkaConfig.builder(bootstrapServers)
                .topic("demo-topic")
                .build();

        createTopic(config);
        sendMessage(config);
        consumeMessage(config);
    }

    public static void createTopic(KafkaConfig config) {
        try (KafkaAdmin kafkaAdmin = new KafkaAdmin(config)) {
            kafkaAdmin.createTopic();
        }
    }

    public static void sendMessage(KafkaConfig config) {
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(config)) {
            kafkaProducer.send("order-1001", "{\"status\":\"created\"}");
            System.out.println("send ok");
        }
    }

    public static void consumeMessage(KafkaConfig config) {
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(config)) {
            List<KafkaConsumer.Message<String, String>> records = kafkaConsumer.poll();
            for (KafkaConsumer.Message<String, String> record : records) {
                System.out.println("receive topic=" + record.getTopic()
                        + ", key=" + record.getKey()
                        + ", value=" + record.getValue());
            }
            kafkaConsumer.commit();
        }
    }
}
