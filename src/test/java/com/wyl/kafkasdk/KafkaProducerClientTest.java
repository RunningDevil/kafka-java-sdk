package com.wyl.kafkasdk;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaProducerClientTest {

    @Test
    void shouldSendMessage() {
        MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        KafkaProducerClient<String, String> producerClient = new KafkaProducerClient<>(mockProducer);

        producerClient.send(
                "orders",
                0,
                null,
                "order-1",
                "created",
                Map.of("source", "sdk".getBytes(StandardCharsets.UTF_8)));
        producerClient.flush();
        producerClient.close();

        ProducerRecord<String, String> record = mockProducer.history().get(0);
        assertEquals("orders", record.topic());
        assertEquals("order-1", record.key());
        assertEquals("created", record.value());
        assertArrayEquals("sdk".getBytes(StandardCharsets.UTF_8), record.headers().lastHeader("source").value());
    }
}
