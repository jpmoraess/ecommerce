package br.com.moraesit.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        var service = new KafkaService(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudService::parse);
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("-----------------------------------------");
        System.out.println("Processing new order, checking for fraud.");
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());
        System.out.println("-----------------------------------------");

        try {
            TimeUnit.SECONDS.sleep(4);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        System.out.println("Order processed!!!");
    }
}
