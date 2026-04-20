package com.wyl.kafkasdk;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaPropertiesTest {

    @Test
    void shouldBuildKafkaProperties() {
        Properties adminProperties = KafkaProperties.adminProperties("localhost:9092");
        Properties producerProperties = KafkaProperties.producerProperties(
                "localhost:9092",
                StringSerializer.class,
                StringSerializer.class);
        Properties consumerProperties = KafkaProperties.consumerProperties(
                "localhost:9092",
                "demo-group",
                StringDeserializer.class,
                StringDeserializer.class);

        assertEquals("localhost:9092", adminProperties.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("localhost:9092", producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class.getName(), producerProperties.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals("all", producerProperties.getProperty(ProducerConfig.ACKS_CONFIG));
        assertEquals("demo-group", consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("earliest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals("false", consumerProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }
}
