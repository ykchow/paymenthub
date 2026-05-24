package com.demo.paymenthub.controller;

import com.demo.paymenthub.dto.CreatePaymentRequest;
import com.demo.paymenthub.dto.PaymentResponse;
import com.demo.paymenthub.enums.PaymentStatus;
import com.demo.paymenthub.exception.GlobalExceptionHandler;
import com.demo.paymenthub.service.PaymentService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(PaymentController.class)
@Import(GlobalExceptionHandler.class)
class PaymentControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private PaymentService paymentService;

    @Test
    void health_returnsOk() throws Exception {
        mockMvc.perform(get("/payments/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").value("Payment Hub is running"));
    }

    @Test
    void createPayment_validRequest_returns201() throws Exception {
        CreatePaymentRequest request = validRequest();
        PaymentResponse response = new PaymentResponse(
                "PAY-ABCDEF12",
                "A1001",
                "B2001",
                new BigDecimal("250.00"),
                "MYR",
                PaymentStatus.COMPLETED,
                LocalDateTime.now(),
                LocalDateTime.now()
        );

        when(paymentService.createPayment(any(CreatePaymentRequest.class))).thenReturn(response);

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.paymentId").value("PAY-ABCDEF12"))
                .andExpect(jsonPath("$.status").value("COMPLETED"));
    }

    @Test
    void createPayment_missingFields_returns400() throws Exception {
        CreatePaymentRequest request = new CreatePaymentRequest();

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation failed"))
                .andExpect(jsonPath("$.details.fromAccount").exists());
    }

    @Test
    void getPayment_notFound_returns404() throws Exception {
        when(paymentService.getPayment("PAY-MISSING"))
                .thenThrow(new IllegalArgumentException("Payment not found: PAY-MISSING"));

        mockMvc.perform(get("/payments/PAY-MISSING"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value("Payment not found: PAY-MISSING"));
    }

    @Test
    void createPayment_duplicateIdempotencyKey_returns409() throws Exception {
        when(paymentService.createPayment(any(CreatePaymentRequest.class)))
                .thenThrow(new IllegalStateException("Duplicate payment request. Existing paymentId: PAY-EXISTING"));

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validRequest())))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value("Duplicate payment request. Existing paymentId: PAY-EXISTING"));
    }

    private CreatePaymentRequest validRequest() {
        CreatePaymentRequest request = new CreatePaymentRequest();
        request.setFromAccount("A1001");
        request.setToAccount("B2001");
        request.setAmount(new BigDecimal("250.00"));
        request.setCurrency("MYR");
        request.setIdempotencyKey("demo-success-001");
        return request;
    }
}
