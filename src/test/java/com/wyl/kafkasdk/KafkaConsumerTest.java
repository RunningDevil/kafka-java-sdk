package com.wyl.kafkasdk;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConsumerTest {

    @Test
    void shouldPollAndCommitMessagesFromConfiguredTopic() {
        TrackingMockConsumer<String, String> mockConsumer = new TrackingMockConsumer<>();
        KafkaConfig config = KafkaConfig.builder("localhost:9092")
                .topic("orders")
                .build();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(config, properties -> mockConsumer);
        TopicPartition topicPartition = new TopicPartition("orders", 0);

        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(Collections.singletonList(topicPartition));
            mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
            mockConsumer.addRecord(new ConsumerRecord<>("orders", 0, 0L, "order-1", "created"));
        });

        List<KafkaConsumer.Message<String, String>> messages = kafkaConsumer.poll();
        kafkaConsumer.commit();
        kafkaConsumer.close();

        assertEquals(1, messages.size());
        assertEquals("orders", messages.get(0).getTopic());
        assertEquals("order-1", messages.get(0).getKey());
        assertEquals("created", messages.get(0).getValue());
        assertEquals(1, mockConsumer.commitCount);
        assertTrue(mockConsumer.closed);
        assertEquals(Set.of("orders"), mockConsumer.subscription());
    }

    private static class TrackingMockConsumer<K, V> extends MockConsumer<K, V> {

        private int commitCount;
        private boolean closed;

        private TrackingMockConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public synchronized void commitSync() {
            commitCount++;
            super.commitSync();
        }

        @Override
        public synchronized void close() {
            closed = true;
            super.close();
        }
    }
}
