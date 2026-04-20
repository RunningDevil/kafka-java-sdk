package com.wyl.kafkasdk;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaConsumerClientTest {

    @Test
    void shouldSubscribeAndPollMessages() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        KafkaConsumerClient<String, String> consumerClient = new KafkaConsumerClient<>(mockConsumer);
        TopicPartition topicPartition = new TopicPartition("orders", 0);

        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(Collections.singletonList(topicPartition));
            mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));

            ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 0, 0L, "order-1", "created");
            record.headers().add("source", "sdk".getBytes(StandardCharsets.UTF_8));
            mockConsumer.addRecord(record);
        });

        consumerClient.subscribe(Collections.singletonList("orders"));
        List<ConsumerRecord<String, String>> records = consumerClient.poll(Duration.ofMillis(50));
        consumerClient.commitSync();
        consumerClient.unsubscribe();
        consumerClient.close();

        assertEquals(1, records.size());
        assertEquals("orders", records.get(0).topic());
        assertEquals("order-1", records.get(0).key());
        assertEquals("created", records.get(0).value());
        assertArrayEquals("sdk".getBytes(StandardCharsets.UTF_8), records.get(0).headers().lastHeader("source").value());
    }
}
