package br.com.moraesit.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String group, String topic, ConsumerFunction parse, Class<T> type) {
        this(group, parse, type);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String group, Pattern topic, ConsumerFunction parse, Class<T> type) {
        this(group, parse, type);
        consumer.subscribe(topic);
    }

    private KafkaService(String groupId, ConsumerFunction parse, Class<T> type) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(type, groupId));
    }

    public void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("I've found " + records.count() + " records..");
                records.forEach(parse::consume);
            }
        }
    }

    private Properties properties(Class<T> type, final String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
