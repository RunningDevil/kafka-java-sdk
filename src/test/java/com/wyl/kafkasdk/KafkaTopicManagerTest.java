package com.wyl.kafkasdk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaTopicManagerTest {

    @Test
    void shouldCreateTopicWithSimpleDefinition() {
        CapturingTopicCreator topicCreator = new CapturingTopicCreator();
        KafkaTopicManager topicManager = new KafkaTopicManager(topicCreator);
        TopicDefinition topicDefinition = TopicDefinition.builder("orders")
                .partitions(3)
                .replicationFactor((short) 2)
                .config("cleanup.policy", "delete")
                .build();

        topicManager.createTopic(topicDefinition);
        topicManager.close();

        assertEquals("orders", topicCreator.capturedTopic.getName());
        assertEquals(3, topicCreator.capturedTopic.getPartitions());
        assertEquals(2, topicCreator.capturedTopic.getReplicationFactor());
        assertEquals("delete", topicCreator.capturedTopic.getConfigs().get("cleanup.policy"));
        assertTrue(topicCreator.closed);
    }

    private static final class CapturingTopicCreator implements KafkaTopicManager.TopicCreator {

        private TopicDefinition capturedTopic;
        private boolean closed;

        @Override
        public void create(TopicDefinition topicDefinition) {
            this.capturedTopic = topicDefinition;
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }
}
