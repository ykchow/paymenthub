package com.demo.paymenthub.service;

import com.demo.paymenthub.dto.CreatePaymentRequest;
import com.demo.paymenthub.dto.PaymentResponse;
import com.demo.paymenthub.enums.PaymentStatus;
import com.demo.paymenthub.kafka.PaymentEvent;
import com.demo.paymenthub.repository.PaymentRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {"payment.completed", "payment.failed"}
)
class PaymentServiceIntegrationTest {

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<String, PaymentEvent> consumer;

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAll();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "paymenthub-integration-test",
                "true",
                embeddedKafkaBroker
        );
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.demo.paymenthub.kafka");

        consumer = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                new JsonDeserializer<>(PaymentEvent.class, false)
        ).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void createPayment_validRequest_persistsAndPublishesCompletedEvent() {
        CreatePaymentRequest request = createRequest("integration-success-001", "A1001", "B2001", "250.00");

        PaymentResponse response = paymentService.createPayment(request);

        assertThat(response.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(response.getPaymentId()).startsWith("PAY-");
        assertThat(paymentRepository.findByIdempotencyKey("integration-success-001")).isPresent();

        var record = waitForEvent("payment.completed", response.getPaymentId());
        assertThat(record.key()).isEqualTo(response.getPaymentId());
        assertThat(record.value().getStatus()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(record.value().getAmount()).isEqualByComparingTo(new BigDecimal("250.00"));
    }

    @Test
    void createPayment_sameAccount_persistsFailedAndPublishesFailedEvent() {
        CreatePaymentRequest request = createRequest("integration-failed-001", "A1001", "A1001", "100.00");

        PaymentResponse response = paymentService.createPayment(request);

        assertThat(response.getStatus()).isEqualTo(PaymentStatus.FAILED);

        var record = waitForEvent("payment.failed", response.getPaymentId());
        assertThat(record.key()).isEqualTo(response.getPaymentId());
        assertThat(record.value().getStatus()).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void createPayment_duplicateIdempotencyKey_throwsConflict() {
        CreatePaymentRequest request = createRequest("integration-duplicate-001", "A1001", "B2001", "50.00");

        paymentService.createPayment(request);

        assertThatThrownBy(() -> paymentService.createPayment(request))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate payment request");
    }

    @Test
    void getPayment_existingPayment_returnsDetails() {
        CreatePaymentRequest request = createRequest("integration-get-001", "A1001", "B2001", "75.00");
        PaymentResponse created = paymentService.createPayment(request);

        PaymentResponse fetched = paymentService.getPayment(created.getPaymentId());

        assertThat(fetched.getPaymentId()).isEqualTo(created.getPaymentId());
        assertThat(fetched.getFromAccount()).isEqualTo("A1001");
        assertThat(fetched.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
    }

    @Test
    void getPayment_unknownPayment_throwsNotFound() {
        assertThatThrownBy(() -> paymentService.getPayment("PAY-NOTFOUND"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Payment not found");
    }

    private CreatePaymentRequest createRequest(
            String idempotencyKey,
            String fromAccount,
            String toAccount,
            String amount
    ) {
        CreatePaymentRequest request = new CreatePaymentRequest();
        request.setFromAccount(fromAccount);
        request.setToAccount(toAccount);
        request.setAmount(new BigDecimal(amount));
        request.setCurrency("MYR");
        request.setIdempotencyKey(idempotencyKey);
        return request;
    }

    private ConsumerRecord<String, PaymentEvent> waitForEvent(String topic, String paymentId) {
        long deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, PaymentEvent> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, PaymentEvent> record : records.records(topic)) {
                if (paymentId.equals(record.key())) {
                    return record;
                }
            }
        }

        throw new AssertionError("No Kafka event found for payment " + paymentId + " on topic " + topic);
    }
}
