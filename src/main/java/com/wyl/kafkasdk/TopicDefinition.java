package com.wyl.kafkasdk;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class TopicDefinition {

    private final String name;
    private final int partitions;
    private final short replicationFactor;
    private final Map<String, String> configs;

    private TopicDefinition(Builder builder) {
        this.name = requireText(builder.name, "name");
        this.partitions = requirePositive(builder.partitions, "partitions");
        this.replicationFactor = (short) requirePositive(builder.replicationFactor, "replicationFactor");
        this.configs = Collections.unmodifiableMap(new LinkedHashMap<>(builder.configs));
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public String getName() {
        return name;
    }

    public int getPartitions() {
        return partitions;
    }

    public short getReplicationFactor() {
        return replicationFactor;
    }

    public Map<String, String> getConfigs() {
        return configs;
    }

    public static final class Builder {

        private final String name;
        private int partitions = 1;
        private int replicationFactor = 1;
        private final Map<String, String> configs = new LinkedHashMap<>();

        private Builder(String name) {
            this.name = name;
        }

        public Builder partitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder replicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public Builder config(String key, String value) {
            this.configs.put(requireText(key, "config key"), value);
            return this;
        }

        public Builder configs(Map<String, String> configs) {
            if (configs != null) {
                this.configs.putAll(configs);
            }
            return this;
        }

        public TopicDefinition build() {
            return new TopicDefinition(this);
        }
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static int requirePositive(int value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than zero");
        }
        return value;
    }
}
