package com.demo.paymenthub.service;

import com.demo.paymenthub.entity.Payment;
import com.demo.paymenthub.enums.PaymentStatus;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class PaymentProcessorService {

    public PaymentStatus process(Payment payment) {

        payment.setStatus(PaymentStatus.PROCESSING);

        if (payment.getAmount() == null) {
            return PaymentStatus.FAILED;
        }

        if (payment.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            return PaymentStatus.FAILED;
        }

        if (payment.getAmount().compareTo(new BigDecimal("10000")) > 0) {
            return PaymentStatus.FAILED;
        }

        if (payment.getFromAccount().equals(payment.getToAccount())) {
            return PaymentStatus.FAILED;
        }

        return PaymentStatus.COMPLETED;
    }
}