package com.wyl.kafkasdk;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaProducerTest {

    @Test
    void shouldSendMessageToConfiguredTopic() {
        MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        KafkaConfig config = KafkaConfig.builder("localhost:9092")
                .topic("orders")
                .build();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(config, mockProducer);

        kafkaProducer.send("order-1", "created");
        kafkaProducer.close();

        ProducerRecord<String, String> record = mockProducer.history().get(0);
        assertEquals("orders", record.topic());
        assertEquals("order-1", record.key());
        assertEquals("created", record.value());
    }
}
