package com.demo.paymenthub.kafka;

import com.demo.paymenthub.entity.Payment;
import com.demo.paymenthub.enums.PaymentStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class PaymentEventProducer {

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    public PaymentEventProducer(KafkaTemplate<String, PaymentEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentEvent(Payment payment) {
        PaymentEvent event = new PaymentEvent(
                payment.getPaymentId(),
                payment.getFromAccount(),
                payment.getToAccount(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getStatus(),
                LocalDateTime.now()
        );

        String topic = resolveTopic(payment.getStatus());

        kafkaTemplate.send(topic, payment.getPaymentId(), event);

        System.out.println("Published Kafka event to topic: " + topic + ", event: " + event);
    }

    private String resolveTopic(PaymentStatus status) {
        if (status == PaymentStatus.COMPLETED) {
            return PAYMENT_COMPLETED_TOPIC;
        }

        if (status == PaymentStatus.FAILED) {
            return PAYMENT_FAILED_TOPIC;
        }

        throw new IllegalStateException("Unsupported payment status for Kafka event: " + status);
    }
}