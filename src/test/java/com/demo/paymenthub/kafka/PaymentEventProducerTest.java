package com.demo.paymenthub.kafka;

import com.demo.paymenthub.entity.Payment;
import com.demo.paymenthub.enums.PaymentStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PaymentEventProducerTest {

    @Mock
    private KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    private PaymentEventProducer paymentEventProducer;

    @BeforeEach
    void setUp() {
        paymentEventProducer = new PaymentEventProducer(kafkaTemplate);
    }

    @Test
    void publishPaymentEvent_completedPayment_sendsToCompletedTopic() {
        Payment payment = paymentWithStatus(PaymentStatus.COMPLETED);

        paymentEventProducer.publishPaymentEvent(payment);

        ArgumentCaptor<PaymentEvent> eventCaptor = ArgumentCaptor.forClass(PaymentEvent.class);
        verify(kafkaTemplate).send(eq("payment.completed"), eq("PAY-12345678"), eventCaptor.capture());

        PaymentEvent event = eventCaptor.getValue();
        assertThat(event.getPaymentId()).isEqualTo("PAY-12345678");
        assertThat(event.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(event.getAmount()).isEqualByComparingTo(new BigDecimal("250.00"));
    }

    @Test
    void publishPaymentEvent_failedPayment_sendsToFailedTopic() {
        Payment payment = paymentWithStatus(PaymentStatus.FAILED);

        paymentEventProducer.publishPaymentEvent(payment);

        verify(kafkaTemplate).send(eq("payment.failed"), eq("PAY-12345678"), org.mockito.ArgumentMatchers.any(PaymentEvent.class));
    }

    @Test
    void publishPaymentEvent_unsupportedStatus_throwsException() {
        Payment payment = paymentWithStatus(PaymentStatus.INITIATED);

        assertThatThrownBy(() -> paymentEventProducer.publishPaymentEvent(payment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unsupported payment status");
    }

    private Payment paymentWithStatus(PaymentStatus status) {
        Payment payment = new Payment();
        payment.setPaymentId("PAY-12345678");
        payment.setFromAccount("A1001");
        payment.setToAccount("B2001");
        payment.setAmount(new BigDecimal("250.00"));
        payment.setCurrency("MYR");
        payment.setStatus(status);
        return payment;
    }
}
