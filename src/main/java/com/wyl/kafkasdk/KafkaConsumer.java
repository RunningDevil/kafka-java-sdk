package com.wyl.kafkasdk;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class KafkaConsumer<K, V> implements AutoCloseable {

    private final KafkaConfig config;
    private final String topic;
    private final ConsumerFactory<K, V> consumerFactory;

    private org.apache.kafka.clients.consumer.Consumer<K, V> syncConsumer;
    private boolean subscribed;

    public KafkaConsumer(KafkaConfig config) {
        this(requireConfig(config), properties -> new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties));
    }

    KafkaConsumer(KafkaConfig config, ConsumerFactory<K, V> consumerFactory) {
        this.config = requireConfig(config);
        this.topic = this.config.getTopic();
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory must not be null");
    }

    public List<Message<K, V>> poll() {
        subscribeIfNeeded();
        ConsumerRecords<K, V> records = syncConsumer().poll(config.getPollTimeout());
        return toMessages(records);
    }

    public void commit() {
        subscribeIfNeeded();
        syncConsumer().commitSync();
    }

    @Override
    public synchronized void close() {
        if (syncConsumer != null) {
            syncConsumer.close();
            syncConsumer = null;
            subscribed = false;
        }
    }

    private synchronized org.apache.kafka.clients.consumer.Consumer<K, V> syncConsumer() {
        if (syncConsumer == null) {
            syncConsumer = consumerFactory.create(config.buildConsumerProperties());
        }
        return syncConsumer;
    }

    private void subscribeIfNeeded() {
        if (!subscribed) {
            syncConsumer().subscribe(List.of(topic));
            subscribed = true;
        }
    }

    private static KafkaConfig requireConfig(KafkaConfig config) {
        return Objects.requireNonNull(config, "config must not be null");
    }

    private static <K, V> List<Message<K, V>> toMessages(ConsumerRecords<K, V> records) {
        List<Message<K, V>> messages = new ArrayList<>(records.count());
        for (ConsumerRecord<K, V> record : records) {
            messages.add(Message.fromRecord(record));
        }
        return messages;
    }

    interface ConsumerFactory<K, V> {

        org.apache.kafka.clients.consumer.Consumer<K, V> create(java.util.Properties properties);
    }

    public static final class Message<K, V> {

        private final String topic;
        private final int partition;
        private final long offset;
        private final K key;
        private final V value;

        private Message(
                String topic,
                int partition,
                long offset,
                K key,
                V value) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.key = key;
            this.value = value;
        }

        private static <K, V> Message<K, V> fromRecord(ConsumerRecord<K, V> record) {
            return new Message<>(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value());
        }

        public String getTopic() {
            return topic;
        }

        public int getPartition() {
            return partition;
        }

        public long getOffset() {
            return offset;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}
