package com.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MongoDBToKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env
            .getCheckpointConfig()
            .setCheckpointStorage("file:///checkpoint-dir");

        MongoDBSource<String> mongoSource = MongoDBSource.<String>builder()
            .hosts("mongodb_primary:27017")
            .username("admin")
            .password("admin")
            .databaseList("demo")
            .collectionList("demo.outbox_event")
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setRecordSerializer(new DynamicTopicKafkaSerializer())
            .build();

        DataStream<String> stream = env.fromSource(
            mongoSource,
            WatermarkStrategy.forMonotonousTimestamps(),
            "MongoDB Source"
        );
        stream.sinkTo(sink);

        env.execute("MongoDB CDC to Kafka with Dynamic Topic");
    }

    private static class DynamicTopicKafkaSerializer
        implements KafkaRecordSerializationSchema<String> {

        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
            String element,
            KafkaSinkContext context,
            Long timestamp
        ) {
            try {
                // Log 原始收到的 element
                System.out.println("[DEBUG] Raw element: " + element);

                JsonNode jsonNode = objectMapper.readTree(element);
                String fullDocumentText = jsonNode
                    .path("fullDocument")
                    .asText();
                System.out.println(
                    "[DEBUG] fullDocument text: " + fullDocumentText
                );

                // 這裡第二次 parse 成物件
                JsonNode fullDocumentNode = objectMapper.readTree(
                    fullDocumentText
                );

                String topic = fullDocumentNode
                    .path("topic")
                    .asText("default-topic");
                System.out.println("[DEBUG] topic: " + topic);

                JsonNode payloadNode = fullDocumentNode.path("payload");
                System.out.println("[DEBUG] payloadNode: " + payloadNode);

                byte[] value;
                if (payloadNode.has("$binary")) {
                    String base64String = payloadNode.path("$binary").asText();
                    System.out.println(
                        "[DEBUG] Detected binary payload, base64String: " +
                        base64String
                    );
                    value = Base64.getDecoder().decode(base64String);
                } else {
                    String payloadText = payloadNode.asText("");
                    System.out.println(
                        "[DEBUG] Plain payload text: " + payloadText
                    );
                    value = payloadText.getBytes(StandardCharsets.UTF_8);
                }

                return new ProducerRecord<>(topic, value);
            } catch (Exception e) {
                System.err.println(
                    "[ERROR] Failed to serialize record: " +
                    element +
                    ", error: " +
                    e.getMessage()
                );
                return new ProducerRecord<>(
                    "error-topic",
                    element.getBytes(StandardCharsets.UTF_8)
                );
            }
        }
    }
}
