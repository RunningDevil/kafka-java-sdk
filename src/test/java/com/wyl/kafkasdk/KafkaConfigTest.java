package com.wyl.kafkasdk;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConfigTest {

    @Test
    void shouldProvideWslFriendlyDefaults() {
        KafkaConfig config = KafkaConfig.builder("localhost:9092").build();

        Properties adminProperties = config.buildAdminProperties();
        Properties producerProperties = config.buildProducerProperties();
        Properties consumerProperties = config.buildConsumerProperties();

        assertEquals("localhost:9092", adminProperties.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("PLAINTEXT", adminProperties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));

        assertEquals("demo-topic", config.getTopic());
        assertEquals("demo-group", consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("demo-client", consumerProperties.getProperty(CommonClientConfigs.CLIENT_ID_CONFIG));
        assertEquals("earliest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals("false", consumerProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals("300000", consumerProperties.getProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG));
        assertEquals("300000", consumerProperties.getProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals("8192000", consumerProperties.getProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG));
        assertEquals("1", consumerProperties.getProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        assertEquals("3000", consumerProperties.getProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG));
        assertEquals("1000", consumerProperties.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals("all", producerProperties.getProperty(ProducerConfig.ACKS_CONFIG));
        assertEquals(Duration.ofMillis(100), config.getPollTimeout());
        assertEquals(1, config.getPartitions());
        assertEquals(1, config.getReplicationFactor());
        assertEquals(1, config.getConsumerThreads());
        assertEquals(Duration.ofSeconds(30), config.getStatsLogInterval());
        assertEquals(Duration.ofSeconds(10), config.getPollErrorBackoff());
        assertEquals(200000, config.getQueueThrottleThreshold());
        assertEquals("PLAINTEXT", config.getSecurityProtocol());
        assertFalse(config.isSslEnabled());
    }

    @Test
    void shouldApplyLegacyLikeConsumerAndSslOverrides() {
        KafkaConfig config = KafkaConfig.builder("localhost:9092")
                .topic("north-topic")
                .groupId("north-service")
                .clientId("north-service-0")
                .partitions(3)
                .replicationFactor((short) 2)
                .consumerThreads(4)
                .sessionTimeoutMs(300000)
                .maxPollIntervalMs(300000)
                .receiveBufferBytes(8_192_000)
                .fetchMinBytes(100000)
                .heartbeatIntervalMs(100000)
                .maxPollRecords(1000)
                .pollTimeout(Duration.ofMillis(100))
                .statsLogInterval(Duration.ofSeconds(30))
                .pollErrorBackoff(Duration.ofSeconds(10))
                .queueThrottleThreshold(200000)
                .sslEnabled(true)
                .sslDirectory("/tmp/kafka-certs")
                .sslPassword("changeit")
                .sslEndpointIdentificationAlgorithm("")
                .build();

        Properties adminProperties = config.buildAdminProperties();
        Properties consumerProperties = config.buildConsumerProperties();

        assertEquals("north-topic", config.getTopic());
        assertEquals(4, config.getConsumerThreads());
        assertTrue(config.isSslEnabled());
        assertEquals("SSL", config.getSecurityProtocol());
        assertEquals("SSL", adminProperties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("/tmp/kafka-certs" + File.separator + "p12.cert",
                consumerProperties.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals("/tmp/kafka-certs" + File.separator + "trust.jks",
                consumerProperties.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals("changeit", consumerProperties.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        assertEquals("changeit", consumerProperties.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        assertEquals("changeit", consumerProperties.getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
        assertEquals("", consumerProperties.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
        assertEquals("100000", consumerProperties.getProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        assertEquals("100000", consumerProperties.getProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG));
    }
}
