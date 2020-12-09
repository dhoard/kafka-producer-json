package com.github.dhoard.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JSONProducer.class);

    private static final String BOOTSTRAP_SERVERS = "cp-5-5-x.address.cx:9092";

    private static final String TOPIC = "test-topic-json";

    private static final Random RANDOM = new Random();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        new JSONProducer().run(args);
    }

    public void run(String[] args) throws Exception {
        KafkaProducer<String, String> kafkaProducer = null;

        try {
            Properties properties = new Properties();

            properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

            properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

            properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                MonitoringProducerInterceptor.class.getName());

            /*
            // Default Operator deployment requires SASL_PLAINTEXT
            // Build the sasl.jaas.config String
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.append("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"");
            stringBuilder.append("test"); // If username has double quotes, they will need to be escaped
            stringBuilder.append("\" password=\"");
            stringBuilder.append("test123"); // If password has double quotes, they will need to be escaped
            stringBuilder.append("\";");

            // Set the security properties
            properties.setProperty(
                "sasl.jaas.config", stringBuilder.toString());

            properties.setProperty("sasl.login.callback.handler.class", KafkaClientAuthenticationCallbackHandler.class.getName());

            properties.setProperty(
                "security.protocol", "SASL_PLAINTEXT"); // Or correct protocol

            properties.setProperty("sasl.mechanism", "PLAIN");
            */

            kafkaProducer = new KafkaProducer<String, String>(properties);

            for (int i = 0; i <  100; i++) {
                /*
                try {
                    Thread.sleep(randomLong(5000, 10000));
                } catch (Throwable t) {
                    // DO NOTHING
                }
                 */

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("id", String.valueOf(UUID.randomUUID()));
                jsonObject.put("data", randomString(10));
                jsonObject.put("timestamp", System.currentTimeMillis());

                String id = jsonObject.getString("id");
                String value = jsonObject.toString();

                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
                    TOPIC, jsonObject.getString("id"), value);

                ExtendedCallback extendedCallback = new ExtendedCallback(producerRecord);
                Future<RecordMetadata> future = kafkaProducer
                    .send(producerRecord, extendedCallback);

                future.get();

                LOGGER.info("message(" + id + ", " + value + ")");

                if (extendedCallback.getException() != null) {
                    LOGGER.error("Exception", extendedCallback.getException());
                }

                if (extendedCallback.isError()) {
                    LOGGER.error("isError = [" + extendedCallback.isError() + "]");
                }
            }
        } finally {
            if (null != kafkaProducer) {
                kafkaProducer.flush();
                kafkaProducer.close();
            }
        }
    }

    public class ExtendedCallback implements Callback {

        private ProducerRecord producerRecord;

        private RecordMetadata recordMetadata;

        private Exception exception;

        public ExtendedCallback(ProducerRecord<String, String> producerRecord) {
            this.producerRecord = producerRecord;
        }

        public boolean isError() {
            return (null != this.exception);
        }

        private RecordMetadata getRecordMetadata() {
            return this.recordMetadata;
        }

        public Exception getException() {
            return this.exception;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
            this.recordMetadata = recordMetadata;

            if (null == exception) {
                //logger.info("Received, key = [" + producerRecord.key() + "] value = [" + producerRecord.value() + "] topic = [" + recordMetadata.topic() + "] partition = [" + recordMetadata.partition() + "] offset = [" + recordMetadata.offset() + "] timestamp = [" + toISOTimestamp(recordMetadata.timestamp(), "America/New_York") + "]");
            }

            this.exception = exception;
        }
    }

    private static String getISOTimestamp() {
        return toISOTimestamp(System.currentTimeMillis(), "America/New_York");
    }

    private static String toISOTimestamp(long milliseconds, String timeZoneId) {
        return Instant.ofEpochMilli(milliseconds).atZone(ZoneId.of(timeZoneId)).toString().replace("[" + timeZoneId + "]", "");
    }

    private static long randomLong(int min, int max) {
        if (max == min) {
            return min;
        }

        if (min > max) {
            throw new IllegalArgumentException("min must be <= max, min = [" + min + "] max = [" + max + "]");
        }

        return + (long) (Math.random() * (max - min));
    }

    private String randomString(int length) {
        return RANDOM.ints(48, 122 + 1)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
            .limit(length)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }
}
