package com.wyl.kafkasdk;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

public final class KafkaProducer<K, V> implements AutoCloseable {

    private final String topic;
    private final org.apache.kafka.clients.producer.Producer<K, V> producer;

    public KafkaProducer(KafkaConfig config) {
        this(requireConfig(config), new org.apache.kafka.clients.producer.KafkaProducer<>(config.buildProducerProperties()));
    }

    KafkaProducer(KafkaConfig config, org.apache.kafka.clients.producer.Producer<K, V> producer) {
        KafkaConfig checkedConfig = requireConfig(config);
        this.topic = checkedConfig.getTopic();
        this.producer = Objects.requireNonNull(producer, "producer must not be null");
    }

    public void send(K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        try {
            producer.send(record).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending Kafka message.", exception);
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause() == null ? exception : exception.getCause();
            throw new IllegalStateException("Failed to send Kafka message.", cause);
        }
    }

    @Override
    public void close() {
        producer.close();
    }

    private static KafkaConfig requireConfig(KafkaConfig config) {
        return Objects.requireNonNull(config, "config must not be null");
    }
}
