package com.demo.paymenthub.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventConsumer {

    @KafkaListener(
            topics = "payment.completed",
            groupId = "paymenthub-notification-service"
    )
    public void consumeCompletedPayment(PaymentEvent event) {
        System.out.println("Received COMPLETED payment event: " + event);

        // Simulate downstream notification service
        System.out.println(
                "Notification sent: Payment " + event.getPaymentId()
                        + " completed for " + event.getAmount() + " " + event.getCurrency()
        );
    }

    @KafkaListener(
            topics = "payment.failed",
            groupId = "paymenthub-notification-service"
    )
    public void consumeFailedPayment(PaymentEvent event) {
        System.out.println("Received FAILED payment event: " + event);

        // Simulate downstream notification service
        System.out.println(
                "Notification sent: Payment " + event.getPaymentId()
                        + " failed for " + event.getAmount() + " " + event.getCurrency()
        );
    }
}