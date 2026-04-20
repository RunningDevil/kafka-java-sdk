# kafka-java-sdk

`kafka-java-sdk` is a lightweight Java wrapper around Apache Kafka clients for three common tasks:

- create topics
- produce messages
- consume messages

The project keeps the API intentionally small so it is easy to embed into other Java applications or package as a reusable JAR.

## Project Structure

- `src/main/java/com/wyl/kafkasdk/KafkaProperties.java`: builds Kafka admin, producer, and consumer properties.
- `src/main/java/com/wyl/kafkasdk/TopicDefinition.java`: immutable topic definition model.
- `src/main/java/com/wyl/kafkasdk/KafkaTopicManager.java`: creates Kafka topics.
- `src/main/java/com/wyl/kafkasdk/KafkaProducerClient.java`: sends Kafka messages.
- `src/main/java/com/wyl/kafkasdk/KafkaConsumerClient.java`: subscribes and polls Kafka messages.
- `src/main/java/com/wyl/kafkasdk/example/KafkaSdkUsageExample.java`: example usage for creating a topic, producing, and consuming.

## Build

Requirements:

- JDK 11+
- Maven 3.9+

Run tests:

```bash
mvn test
```

Build artifacts:

```bash
mvn package
```

Generated outputs:

- `target/kafka-java-sdk-1.0.0-SNAPSHOT.jar`
- `target/kafka-java-sdk-1.0.0-SNAPSHOT-all.jar`

## Run the Example

Before running the example, make sure Kafka is already running and accessible from the configured bootstrap server.

```bash
java -cp target/kafka-java-sdk-1.0.0-SNAPSHOT-all.jar com.wyl.kafkasdk.example.KafkaSdkUsageExample
```

The example uses:

- bootstrap server: `127.0.0.1:9092`
- topic: `demo-topic`
- consumer group: `demo-group`

## GitHub Upload Checklist

1. Initialize or verify the Git repository.
2. Review changes with `git status`.
3. Stage files with `git add -A`.
4. Commit with a clear message.
5. Create a GitHub repository named `kafka-java-sdk`.
6. Add the remote and push the branch.

## License

Add a license file before publishing publicly if you want others to reuse the project.
