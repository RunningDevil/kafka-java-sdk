package com.wyl.kafkasdk;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public final class KafkaTopicManager implements AutoCloseable {

    private final TopicCreator topicCreator;

    public KafkaTopicManager(Properties properties) {
        this(new AdminTopicCreator(Admin.create(requireProperties(properties))));
    }

    KafkaTopicManager(TopicCreator topicCreator) {
        this.topicCreator = Objects.requireNonNull(topicCreator, "topicCreator must not be null");
    }

    public void createTopic(TopicDefinition topicDefinition) {
        topicCreator.create(Objects.requireNonNull(topicDefinition, "topicDefinition must not be null"));
    }

    @Override
    public void close() {
        topicCreator.close();
    }

    private static Properties requireProperties(Properties properties) {
        return Objects.requireNonNull(properties, "properties must not be null");
    }

    interface TopicCreator extends AutoCloseable {

        void create(TopicDefinition topicDefinition);

        @Override
        void close();
    }

    private static final class AdminTopicCreator implements TopicCreator {

        private final Admin admin;

        private AdminTopicCreator(Admin admin) {
            this.admin = Objects.requireNonNull(admin, "admin must not be null");
        }

        @Override
        public void create(TopicDefinition topicDefinition) {
            NewTopic topic = new NewTopic(
                    topicDefinition.getName(),
                    topicDefinition.getPartitions(),
                    topicDefinition.getReplicationFactor());
            if (!topicDefinition.getConfigs().isEmpty()) {
                topic.configs(topicDefinition.getConfigs());
            }
            try {
                admin.createTopics(Collections.singletonList(topic)).all().get();
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
}
