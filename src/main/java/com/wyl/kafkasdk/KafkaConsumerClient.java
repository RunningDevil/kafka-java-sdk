package com.wyl.kafkasdk;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public final class KafkaConsumerClient<K, V> implements AutoCloseable {

    private final Consumer<K, V> consumer;

    public KafkaConsumerClient(Properties properties) {
        this(new KafkaConsumer<>(requireProperties(properties)));
    }

    KafkaConsumerClient(Consumer<K, V> consumer) {
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
    }

    public void subscribe(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("topics must not be empty");
        }
        consumer.subscribe(List.copyOf(topics));
    }

    public List<ConsumerRecord<K, V>> poll(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        ConsumerRecords<K, V> records = consumer.poll(timeout);
        List<ConsumerRecord<K, V>> result = new ArrayList<>(records.count());
        for (ConsumerRecord<K, V> record : records) {
            result.add(record);
        }
        return result;
    }

    public void commitSync() {
        consumer.commitSync();
    }

    public void unsubscribe() {
        consumer.unsubscribe();
    }

    @Override
    public void close() {
        consumer.close();
    }

    private static Properties requireProperties(Properties properties) {
        return Objects.requireNonNull(properties, "properties must not be null");
    }
}
