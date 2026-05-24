package com.demo.paymenthub.repository;

import com.demo.paymenthub.entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment, Long> {

    Optional<Payment> findByPaymentId(String paymentId);

    Optional<Payment> findByIdempotencyKey(String idempotencyKey);
}