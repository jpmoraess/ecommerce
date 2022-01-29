package br.com.moraesit.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) {
        try (var dispatcher = new KafkaDispatcher()) {
            for (var i = 0; i < 20; i++) {
                var key = UUID.randomUUID().toString();
                var order = key + ",4343, 123232";

                var email = "Thank you!! We are processing your order..!";
                try {
                    dispatcher.send("ECOMMERCE_NEW_ORDER", key, order);
                    dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
                } catch (InterruptedException | ExecutionException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
