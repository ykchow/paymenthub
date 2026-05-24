package com.demo.paymenthub.service;

import com.demo.paymenthub.entity.Payment;
import com.demo.paymenthub.enums.PaymentStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentProcessorServiceTest {

    private PaymentProcessorService paymentProcessorService;

    @BeforeEach
    void setUp() {
        paymentProcessorService = new PaymentProcessorService();
    }

    @Test
    void process_validPayment_returnsCompleted() {
        Payment payment = payment("A1001", "B2001", new BigDecimal("250.00"));

        PaymentStatus status = paymentProcessorService.process(payment);

        assertThat(status).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.PROCESSING);
    }

    @Test
    void process_nullAmount_returnsFailed() {
        Payment payment = payment("A1001", "B2001", null);

        assertThat(paymentProcessorService.process(payment)).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void process_zeroAmount_returnsFailed() {
        Payment payment = payment("A1001", "B2001", BigDecimal.ZERO);

        assertThat(paymentProcessorService.process(payment)).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void process_negativeAmount_returnsFailed() {
        Payment payment = payment("A1001", "B2001", new BigDecimal("-1.00"));

        assertThat(paymentProcessorService.process(payment)).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void process_amountAboveLimit_returnsFailed() {
        Payment payment = payment("A1001", "B2001", new BigDecimal("10000.01"));

        assertThat(paymentProcessorService.process(payment)).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void process_amountAtLimit_returnsCompleted() {
        Payment payment = payment("A1001", "B2001", new BigDecimal("10000.00"));

        assertThat(paymentProcessorService.process(payment)).isEqualTo(PaymentStatus.COMPLETED);
    }

    @Test
    void process_sameAccount_returnsFailed() {
        Payment payment = payment("A1001", "A1001", new BigDecimal("100.00"));

        assertThat(paymentProcessorService.process(payment)).isEqualTo(PaymentStatus.FAILED);
    }

    private Payment payment(String fromAccount, String toAccount, BigDecimal amount) {
        Payment payment = new Payment();
        payment.setFromAccount(fromAccount);
        payment.setToAccount(toAccount);
        payment.setAmount(amount);
        return payment;
    }
}
