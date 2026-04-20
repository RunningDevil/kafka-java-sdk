package com.wyl.kafkasdk;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public final class KafkaProducerClient<K, V> implements AutoCloseable {

    private final Producer<K, V> producer;

    public KafkaProducerClient(Properties properties) {
        this(new KafkaProducer<>(requireProperties(properties)));
    }

    KafkaProducerClient(Producer<K, V> producer) {
        this.producer = Objects.requireNonNull(producer, "producer must not be null");
    }

    public RecordMetadata send(String topic, K key, V value) {
        return send(topic, null, null, key, value, Collections.emptyMap());
    }

    public RecordMetadata send(
            String topic,
            Integer partition,
            Long timestamp,
            K key,
            V value,
            Map<String, byte[]> headers) {
        ProducerRecord<K, V> record = buildRecord(topic, partition, timestamp, key, value, headers);
        try {
            return producer.send(record).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending Kafka message.", exception);
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause() == null ? exception : exception.getCause();
            throw new IllegalStateException("Failed to send Kafka message.", cause);
        }
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }

    private ProducerRecord<K, V> buildRecord(
            String topic,
            Integer partition,
            Long timestamp,
            K key,
            V value,
            Map<String, byte[]> headers) {
        RecordHeaders recordHeaders = new RecordHeaders();
        if (headers != null) {
            for (Map.Entry<String, byte[]> entry : headers.entrySet()) {
                recordHeaders.add(requireText(entry.getKey(), "header key"), entry.getValue());
            }
        }
        return new ProducerRecord<>(
                requireText(topic, "topic"),
                partition,
                timestamp,
                key,
                value,
                recordHeaders);
    }

    private static Properties requireProperties(Properties properties) {
        return Objects.requireNonNull(properties, "properties must not be null");
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
