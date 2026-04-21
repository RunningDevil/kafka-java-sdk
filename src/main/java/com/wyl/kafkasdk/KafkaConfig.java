package com.wyl.kafkasdk;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public final class KafkaConfig {

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final String clientId;
    private final String keySerializerClassName;
    private final String valueSerializerClassName;
    private final String keyDeserializerClassName;
    private final String valueDeserializerClassName;
    private final String autoOffsetReset;
    private final boolean enableAutoCommit;
    private final Duration pollTimeout;
    private final int partitions;
    private final short replicationFactor;
    private final int consumerThreads;
    private final int sessionTimeoutMs;
    private final int maxPollIntervalMs;
    private final int receiveBufferBytes;
    private final int fetchMinBytes;
    private final int heartbeatIntervalMs;
    private final int maxPollRecords;
    private final Duration statsLogInterval;
    private final Duration pollErrorBackoff;
    private final int queueThrottleThreshold;
    private final String securityProtocol;
    private final boolean sslEnabled;
    private final String sslDirectory;
    private final String sslKeyStoreLocation;
    private final String sslTrustStoreLocation;
    private final String sslEnabledProtocols;
    private final String sslCipherSuites;
    private final String sslKeyStorePassword;
    private final String sslTrustStorePassword;
    private final String sslKeyPassword;
    private final String sslEndpointIdentificationAlgorithm;
    private final Map<String, Object> commonProperties;
    private final Map<String, Object> producerProperties;
    private final Map<String, Object> consumerProperties;

    private KafkaConfig(Builder builder) {
        this.bootstrapServers = requireText(builder.bootstrapServers, "bootstrapServers");
        this.topic = requireText(builder.topic, "topic");
        this.groupId = requireText(builder.groupId, "groupId");
        this.clientId = requireText(builder.clientId, "clientId");
        this.keySerializerClassName = requireText(builder.keySerializerClassName, "keySerializer");
        this.valueSerializerClassName = requireText(builder.valueSerializerClassName, "valueSerializer");
        this.keyDeserializerClassName = requireText(builder.keyDeserializerClassName, "keyDeserializer");
        this.valueDeserializerClassName = requireText(builder.valueDeserializerClassName, "valueDeserializer");
        this.autoOffsetReset = requireText(builder.autoOffsetReset, "autoOffsetReset");
        this.enableAutoCommit = builder.enableAutoCommit;
        this.pollTimeout = requirePositiveDuration(builder.pollTimeout, "pollTimeout");
        this.partitions = requirePositiveInt(builder.partitions, "partitions");
        this.replicationFactor = requirePositiveShort(builder.replicationFactor, "replicationFactor");
        this.consumerThreads = requirePositiveInt(builder.consumerThreads, "consumerThreads");
        this.sessionTimeoutMs = requirePositiveInt(builder.sessionTimeoutMs, "sessionTimeoutMs");
        this.maxPollIntervalMs = requirePositiveInt(builder.maxPollIntervalMs, "maxPollIntervalMs");
        this.receiveBufferBytes = requirePositiveInt(builder.receiveBufferBytes, "receiveBufferBytes");
        this.fetchMinBytes = requirePositiveInt(builder.fetchMinBytes, "fetchMinBytes");
        this.heartbeatIntervalMs = requirePositiveInt(builder.heartbeatIntervalMs, "heartbeatIntervalMs");
        this.maxPollRecords = requirePositiveInt(builder.maxPollRecords, "maxPollRecords");
        this.statsLogInterval = requirePositiveDuration(builder.statsLogInterval, "statsLogInterval");
        this.pollErrorBackoff = requirePositiveDuration(builder.pollErrorBackoff, "pollErrorBackoff");
        this.queueThrottleThreshold = requirePositiveInt(builder.queueThrottleThreshold, "queueThrottleThreshold");
        this.securityProtocol = requireText(builder.securityProtocol, "securityProtocol");
        this.sslEnabled = builder.sslEnabled;
        this.sslDirectory = normalizeNullableText(builder.sslDirectory);
        this.sslKeyStoreLocation = normalizeNullableText(builder.sslKeyStoreLocation);
        this.sslTrustStoreLocation = normalizeNullableText(builder.sslTrustStoreLocation);
        this.sslEnabledProtocols = normalizeNullableText(builder.sslEnabledProtocols);
        this.sslCipherSuites = normalizeNullableText(builder.sslCipherSuites);
        this.sslKeyStorePassword = normalizeNullableText(builder.sslKeyStorePassword);
        this.sslTrustStorePassword = normalizeNullableText(builder.sslTrustStorePassword);
        this.sslKeyPassword = normalizeNullableText(builder.sslKeyPassword);
        this.sslEndpointIdentificationAlgorithm = builder.sslEndpointIdentificationAlgorithm;
        this.commonProperties = Map.copyOf(builder.commonProperties);
        this.producerProperties = Map.copyOf(builder.producerProperties);
        this.consumerProperties = Map.copyOf(builder.consumerProperties);
    }

    public static Builder builder(String bootstrapServers) {
        return new Builder(bootstrapServers);
    }

    Properties buildAdminProperties() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        applyClientId(properties, clientId);
        applySecurityProperties(properties);
        applyProperties(properties, commonProperties);
        return properties;
    }

    Properties buildProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        applyClientId(properties, clientId);
        applySecurityProperties(properties);
        applyProperties(properties, commonProperties);
        applyProperties(properties, producerProperties);
        return properties;
    }

    Properties buildConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(enableAutoCommit));
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(sessionTimeoutMs));
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(maxPollIntervalMs));
        properties.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, Integer.toString(receiveBufferBytes));
        properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, Integer.toString(fetchMinBytes));
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, Integer.toString(heartbeatIntervalMs));
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(maxPollRecords));
        applyClientId(properties, clientId);
        applySecurityProperties(properties);
        applyProperties(properties, commonProperties);
        applyProperties(properties, consumerProperties);
        return properties;
    }

    String getTopic() {
        return topic;
    }

    Duration getPollTimeout() {
        return pollTimeout;
    }

    int getPartitions() {
        return partitions;
    }

    short getReplicationFactor() {
        return replicationFactor;
    }

    int getConsumerThreads() {
        return consumerThreads;
    }

    int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    int getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    int getReceiveBufferBytes() {
        return receiveBufferBytes;
    }

    int getFetchMinBytes() {
        return fetchMinBytes;
    }

    int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    int getMaxPollRecords() {
        return maxPollRecords;
    }

    Duration getStatsLogInterval() {
        return statsLogInterval;
    }

    Duration getPollErrorBackoff() {
        return pollErrorBackoff;
    }

    int getQueueThrottleThreshold() {
        return queueThrottleThreshold;
    }

    String getSecurityProtocol() {
        return securityProtocol;
    }

    boolean isSslEnabled() {
        return sslEnabled;
    }

    String getSslDirectory() {
        return sslDirectory;
    }

    private void applySecurityProperties(Properties properties) {
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        if (!usesSsl()) {
            return;
        }
        setOptionalProperty(properties, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, resolveSslKeyStoreLocation());
        setOptionalProperty(properties, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, resolveSslTrustStoreLocation());
        setOptionalProperty(properties, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, sslEnabledProtocols);
        setOptionalProperty(properties, SslConfigs.SSL_CIPHER_SUITES_CONFIG, sslCipherSuites);
        setOptionalProperty(properties, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeyStorePassword);
        setOptionalProperty(properties, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTrustStorePassword);
        setOptionalProperty(properties, SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
        setOptionalNullableProperty(
                properties,
                SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                sslEndpointIdentificationAlgorithm);
    }

    private boolean usesSsl() {
        return sslEnabled || securityProtocol.toUpperCase(Locale.ROOT).contains("SSL");
    }

    private String resolveSslKeyStoreLocation() {
        if (sslKeyStoreLocation != null) {
            return sslKeyStoreLocation;
        }
        if (sslDirectory == null) {
            return null;
        }
        return sslDirectory + File.separator + "p12.cert";
    }

    private String resolveSslTrustStoreLocation() {
        if (sslTrustStoreLocation != null) {
            return sslTrustStoreLocation;
        }
        if (sslDirectory == null) {
            return null;
        }
        return sslDirectory + File.separator + "trust.jks";
    }

    private static void applyClientId(Properties properties, String clientId) {
        if (clientId != null && !clientId.trim().isEmpty()) {
            properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        }
    }

    private static void applyProperties(Properties properties, Map<String, Object> values) {
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
    }

    private static void setOptionalProperty(Properties properties, String key, String value) {
        if (value != null && !value.trim().isEmpty()) {
            properties.setProperty(key, value);
        }
    }

    private static void setOptionalNullableProperty(Properties properties, String key, String value) {
        if (value != null) {
            properties.setProperty(key, value);
        }
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static String normalizeNullableText(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static Duration requirePositiveDuration(Duration duration, String fieldName) {
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(fieldName + " must be greater than zero");
        }
        return duration;
    }

    private static int requirePositiveInt(int value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than zero");
        }
        return value;
    }

    private static short requirePositiveShort(short value, String fieldName) {
        if (value <= 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than zero");
        }
        return value;
    }

    public static final class Builder {

        private final String bootstrapServers;
        private String topic = "demo-topic";
        private String groupId = "demo-group";
        private String clientId = "demo-client";
        private String keySerializerClassName = StringSerializer.class.getName();
        private String valueSerializerClassName = StringSerializer.class.getName();
        private String keyDeserializerClassName = StringDeserializer.class.getName();
        private String valueDeserializerClassName = StringDeserializer.class.getName();
        private String autoOffsetReset = "earliest";
        private boolean enableAutoCommit;
        private Duration pollTimeout = Duration.ofMillis(100);
        private int partitions = 1;
        private short replicationFactor = 1;
        private int consumerThreads = 1;
        private int sessionTimeoutMs = 300000;
        private int maxPollIntervalMs = 300000;
        private int receiveBufferBytes = 8_192_000;
        private int fetchMinBytes = 1;
        private int heartbeatIntervalMs = 3_000;
        private int maxPollRecords = 1000;
        private Duration statsLogInterval = Duration.ofSeconds(30);
        private Duration pollErrorBackoff = Duration.ofSeconds(10);
        private int queueThrottleThreshold = 200_000;
        private String securityProtocol = "PLAINTEXT";
        private boolean sslEnabled;
        private String sslDirectory;
        private String sslKeyStoreLocation;
        private String sslTrustStoreLocation;
        private String sslEnabledProtocols = "TLSv1.2";
        private String sslCipherSuites = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
        private String sslKeyStorePassword;
        private String sslTrustStorePassword;
        private String sslKeyPassword;
        private String sslEndpointIdentificationAlgorithm = "";
        private final Map<String, Object> commonProperties = new LinkedHashMap<>();
        private final Map<String, Object> producerProperties = new LinkedHashMap<>();
        private final Map<String, Object> consumerProperties = new LinkedHashMap<>();

        private Builder(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public Builder topic(String topic) {
            this.topic = requireText(topic, "topic");
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = requireText(groupId, "groupId");
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = requireText(clientId, "clientId");
            return this;
        }

        public Builder keySerializer(Class<? extends Serializer<?>> serializerClass) {
            this.keySerializerClassName = requireClass(serializerClass, "keySerializer").getName();
            return this;
        }

        public Builder valueSerializer(Class<? extends Serializer<?>> serializerClass) {
            this.valueSerializerClassName = requireClass(serializerClass, "valueSerializer").getName();
            return this;
        }

        public Builder keyDeserializer(Class<? extends Deserializer<?>> deserializerClass) {
            this.keyDeserializerClassName = requireClass(deserializerClass, "keyDeserializer").getName();
            return this;
        }

        public Builder valueDeserializer(Class<? extends Deserializer<?>> deserializerClass) {
            this.valueDeserializerClassName = requireClass(deserializerClass, "valueDeserializer").getName();
            return this;
        }

        public Builder autoOffsetReset(String autoOffsetReset) {
            this.autoOffsetReset = requireText(autoOffsetReset, "autoOffsetReset");
            return this;
        }

        public Builder enableAutoCommit(boolean enableAutoCommit) {
            this.enableAutoCommit = enableAutoCommit;
            return this;
        }

        public Builder pollTimeout(Duration pollTimeout) {
            this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout must not be null");
            return this;
        }

        public Builder partitions(int partitions) {
            this.partitions = requirePositiveInt(partitions, "partitions");
            return this;
        }

        public Builder replicationFactor(short replicationFactor) {
            this.replicationFactor = requirePositiveShort(replicationFactor, "replicationFactor");
            return this;
        }

        public Builder consumerThreads(int consumerThreads) {
            this.consumerThreads = requirePositiveInt(consumerThreads, "consumerThreads");
            return this;
        }

        public Builder sessionTimeoutMs(int sessionTimeoutMs) {
            this.sessionTimeoutMs = requirePositiveInt(sessionTimeoutMs, "sessionTimeoutMs");
            return this;
        }

        public Builder maxPollIntervalMs(int maxPollIntervalMs) {
            this.maxPollIntervalMs = requirePositiveInt(maxPollIntervalMs, "maxPollIntervalMs");
            return this;
        }

        public Builder receiveBufferBytes(int receiveBufferBytes) {
            this.receiveBufferBytes = requirePositiveInt(receiveBufferBytes, "receiveBufferBytes");
            return this;
        }

        public Builder fetchMinBytes(int fetchMinBytes) {
            this.fetchMinBytes = requirePositiveInt(fetchMinBytes, "fetchMinBytes");
            return this;
        }

        public Builder heartbeatIntervalMs(int heartbeatIntervalMs) {
            this.heartbeatIntervalMs = requirePositiveInt(heartbeatIntervalMs, "heartbeatIntervalMs");
            return this;
        }

        public Builder maxPollRecords(int maxPollRecords) {
            this.maxPollRecords = requirePositiveInt(maxPollRecords, "maxPollRecords");
            return this;
        }

        public Builder statsLogInterval(Duration statsLogInterval) {
            this.statsLogInterval = Objects.requireNonNull(statsLogInterval, "statsLogInterval must not be null");
            return this;
        }

        public Builder pollErrorBackoff(Duration pollErrorBackoff) {
            this.pollErrorBackoff = Objects.requireNonNull(pollErrorBackoff, "pollErrorBackoff must not be null");
            return this;
        }

        public Builder queueThrottleThreshold(int queueThrottleThreshold) {
            this.queueThrottleThreshold = requirePositiveInt(queueThrottleThreshold, "queueThrottleThreshold");
            return this;
        }

        public Builder securityProtocol(String securityProtocol) {
            this.securityProtocol = requireText(securityProtocol, "securityProtocol");
            return this;
        }

        public Builder sslEnabled(boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
            if (sslEnabled && "PLAINTEXT".equalsIgnoreCase(this.securityProtocol)) {
                this.securityProtocol = "SSL";
            }
            return this;
        }

        public Builder sslDirectory(String sslDirectory) {
            this.sslDirectory = requireText(sslDirectory, "sslDirectory");
            return this;
        }

        public Builder sslPassword(String sslPassword) {
            String password = requireText(sslPassword, "sslPassword");
            this.sslKeyStorePassword = password;
            this.sslTrustStorePassword = password;
            this.sslKeyPassword = password;
            return this;
        }

        public Builder sslKeyStoreLocation(String sslKeyStoreLocation) {
            this.sslKeyStoreLocation = requireText(sslKeyStoreLocation, "sslKeyStoreLocation");
            return this;
        }

        public Builder sslTrustStoreLocation(String sslTrustStoreLocation) {
            this.sslTrustStoreLocation = requireText(sslTrustStoreLocation, "sslTrustStoreLocation");
            return this;
        }

        public Builder sslEnabledProtocols(String sslEnabledProtocols) {
            this.sslEnabledProtocols = requireText(sslEnabledProtocols, "sslEnabledProtocols");
            return this;
        }

        public Builder sslCipherSuites(String sslCipherSuites) {
            this.sslCipherSuites = requireText(sslCipherSuites, "sslCipherSuites");
            return this;
        }

        public Builder sslKeyStorePassword(String sslKeyStorePassword) {
            this.sslKeyStorePassword = requireText(sslKeyStorePassword, "sslKeyStorePassword");
            return this;
        }

        public Builder sslTrustStorePassword(String sslTrustStorePassword) {
            this.sslTrustStorePassword = requireText(sslTrustStorePassword, "sslTrustStorePassword");
            return this;
        }

        public Builder sslKeyPassword(String sslKeyPassword) {
            this.sslKeyPassword = requireText(sslKeyPassword, "sslKeyPassword");
            return this;
        }

        public Builder sslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm) {
            this.sslEndpointIdentificationAlgorithm = Objects.requireNonNull(
                    sslEndpointIdentificationAlgorithm,
                    "sslEndpointIdentificationAlgorithm must not be null");
            return this;
        }

        public Builder property(String key, Object value) {
            commonProperties.put(requireText(key, "property key"), requireValue(value, "property value"));
            return this;
        }

        public Builder producerProperty(String key, Object value) {
            producerProperties.put(requireText(key, "producer property key"), requireValue(value, "producer property value"));
            return this;
        }

        public Builder consumerProperty(String key, Object value) {
            consumerProperties.put(requireText(key, "consumer property key"), requireValue(value, "consumer property value"));
            return this;
        }

        public KafkaConfig build() {
            return new KafkaConfig(this);
        }
    }

    private static <T> Class<? extends T> requireClass(Class<? extends T> value, String fieldName) {
        return Objects.requireNonNull(value, fieldName + " must not be null");
    }

    private static Object requireValue(Object value, String fieldName) {
        return Objects.requireNonNull(value, fieldName + " must not be null");
    }
}
