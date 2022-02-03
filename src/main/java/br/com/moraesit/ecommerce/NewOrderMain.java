package br.com.moraesit.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<String>()) {
                for (var i = 0; i < 20; i++) {

                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = Math.random() * 5000 + 1;
                    var order = new Order(userId, orderId, new BigDecimal(amount));

                    var email = "Thank you!! We are processing your order..!";
                    //var email = new Email("New Order", "Thank you!! We are processing your order..!");

                    try {
                        orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);
                        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
                    } catch (InterruptedException | ExecutionException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }
}
