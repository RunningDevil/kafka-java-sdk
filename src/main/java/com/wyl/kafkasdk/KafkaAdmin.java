package com.wyl.kafkasdk;

import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public final class KafkaAdmin implements AutoCloseable {

    private final String topic;
    private final int partitions;
    private final short replicationFactor;
    private final TopicOperator topicOperator;

    public KafkaAdmin(KafkaConfig config) {
        this(requireConfig(config), new AdminTopicOperator(config.buildAdminProperties()));
    }

    KafkaAdmin(KafkaConfig config, TopicOperator topicOperator) {
        KafkaConfig checkedConfig = requireConfig(config);
        this.topic = checkedConfig.getTopic();
        this.partitions = checkedConfig.getPartitions();
        this.replicationFactor = checkedConfig.getReplicationFactor();
        this.topicOperator = Objects.requireNonNull(topicOperator, "topicOperator must not be null");
    }

    public void createTopic() {
        topicOperator.create(topic, partitions, replicationFactor);
    }

    @Override
    public void close() {
        topicOperator.close();
    }

    interface TopicOperator extends AutoCloseable {

        void create(String topic, int partitions, short replicationFactor);

        @Override
        void close();
    }

    private static final class AdminTopicOperator implements TopicOperator {

        private final org.apache.kafka.clients.admin.Admin admin;

        private AdminTopicOperator(java.util.Properties properties) {
            this.admin = org.apache.kafka.clients.admin.Admin.create(properties);
        }

        @Override
        public void create(String topic, int partitions, short replicationFactor) {
            NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);
            try {
                admin.createTopics(Collections.singletonList(newTopic)).all().get();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while creating Kafka topic.", exception);
            } catch (ExecutionException exception) {
                Throwable cause = exception.getCause() == null ? exception : exception.getCause();
                throw new IllegalStateException("Failed to create Kafka topic.", cause);
            }
        }

        @Override
        public void close() {
            admin.close();
        }
    }

    private static KafkaConfig requireConfig(KafkaConfig config) {
        return Objects.requireNonNull(config, "config must not be null");
    }
}
