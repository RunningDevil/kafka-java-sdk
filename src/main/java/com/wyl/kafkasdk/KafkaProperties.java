package com.wyl.kafkasdk;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Objects;
import java.util.Properties;

public final class KafkaProperties {

    private KafkaProperties() {
    }

    public static Properties adminProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, requireText(bootstrapServers, "bootstrapServers"));
        return properties;
    }

    public static Properties producerProperties(
            String bootstrapServers,
            Class<? extends Serializer<?>> keySerializerClass,
            Class<? extends Serializer<?>> valueSerializerClass) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, requireText(bootstrapServers, "bootstrapServers"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, requireType(keySerializerClass, "keySerializerClass").getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, requireType(valueSerializerClass, "valueSerializerClass").getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    public static Properties consumerProperties(
            String bootstrapServers,
            String groupId,
            Class<? extends Deserializer<?>> keyDeserializerClass,
            Class<? extends Deserializer<?>> valueDeserializerClass) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, requireText(bootstrapServers, "bootstrapServers"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, requireText(groupId, "groupId"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, requireType(keyDeserializerClass, "keyDeserializerClass").getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, requireType(valueDeserializerClass, "valueDeserializerClass").getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static <T> Class<? extends T> requireType(Class<? extends T> type, String fieldName) {
        return Objects.requireNonNull(type, fieldName + " must not be null");
    }
}
