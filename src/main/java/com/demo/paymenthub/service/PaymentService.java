package com.demo.paymenthub.service;

import com.demo.paymenthub.dto.CreatePaymentRequest;
import com.demo.paymenthub.dto.PaymentResponse;
import com.demo.paymenthub.entity.Payment;
import com.demo.paymenthub.enums.PaymentStatus;
import com.demo.paymenthub.repository.PaymentRepository;
import com.demo.paymenthub.kafka.PaymentEventProducer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentProcessorService paymentProcessorService;
    private final PaymentEventProducer paymentEventProducer;

    public PaymentService(
            PaymentRepository paymentRepository,
            PaymentProcessorService paymentProcessorService,
            PaymentEventProducer paymentEventProducer
    ) {
        this.paymentRepository = paymentRepository;
        this.paymentProcessorService = paymentProcessorService;
        this.paymentEventProducer = paymentEventProducer;
    }

    @Transactional
    public PaymentResponse createPayment(CreatePaymentRequest request) {

        paymentRepository.findByIdempotencyKey(request.getIdempotencyKey())
                .ifPresent(existingPayment -> {
                    throw new IllegalStateException(
                            "Duplicate payment request. Existing paymentId: " + existingPayment.getPaymentId()
                    );
                });

        Payment payment = new Payment();
        payment.setPaymentId(generatePaymentId());
        payment.setFromAccount(request.getFromAccount());
        payment.setToAccount(request.getToAccount());
        payment.setAmount(request.getAmount());
        payment.setCurrency(request.getCurrency());
        payment.setIdempotencyKey(request.getIdempotencyKey());
        payment.setStatus(PaymentStatus.INITIATED);
        payment.setCreatedAt(LocalDateTime.now());
        payment.setUpdatedAt(LocalDateTime.now());

        payment = paymentRepository.save(payment);

        PaymentStatus finalStatus = paymentProcessorService.process(payment);

        payment.setStatus(finalStatus);
        payment.setUpdatedAt(LocalDateTime.now());

        payment = paymentRepository.save(payment);

        paymentEventProducer.publishPaymentEvent(payment);

        return toResponse(payment);
    }

    public PaymentResponse getPayment(String paymentId) {
        Payment payment = paymentRepository.findByPaymentId(paymentId)
                .orElseThrow(() -> new IllegalArgumentException("Payment not found: " + paymentId));

        return toResponse(payment);
    }

    private String generatePaymentId() {
        return "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    private PaymentResponse toResponse(Payment payment) {
        return new PaymentResponse(
                payment.getPaymentId(),
                payment.getFromAccount(),
                payment.getToAccount(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getStatus(),
                payment.getCreatedAt(),
                payment.getUpdatedAt()
        );
    }
}