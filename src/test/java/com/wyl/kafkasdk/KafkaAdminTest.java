package com.wyl.kafkasdk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaAdminTest {

    @Test
    void shouldCreateConfiguredTopic() {
        KafkaConfig config = KafkaConfig.builder("localhost:9092")
                .topic("orders")
                .partitions(3)
                .replicationFactor((short) 2)
                .build();
        CapturingTopicOperator topicOperator = new CapturingTopicOperator();
        KafkaAdmin kafkaAdmin = new KafkaAdmin(config, topicOperator);

        kafkaAdmin.createTopic();

        assertEquals("orders", topicOperator.topic);
        assertEquals(3, topicOperator.partitions);
        assertEquals(2, topicOperator.replicationFactor);
        kafkaAdmin.close();
        assertTrue(topicOperator.closed);
    }

    private static final class CapturingTopicOperator implements KafkaAdmin.TopicOperator {

        private String topic;
        private int partitions;
        private short replicationFactor;
        private boolean closed;

        @Override
        public void create(String topic, int partitions, short replicationFactor) {
            this.topic = topic;
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }
}
