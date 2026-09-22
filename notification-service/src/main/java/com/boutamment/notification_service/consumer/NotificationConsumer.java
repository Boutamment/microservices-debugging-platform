package com.boutamment.notification_service.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
class NotificationConsumer {

    @KafkaListener(topics = "order-events", groupId = "notification-group")
    public void handleOrderEvent(
            @Payload Map<String, Object> event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        String eventType = (String) event.get("event");
        log.info("[Notification] Received order event: {} for orderId: {}", eventType, key);

        switch (eventType) {
            case "ORDER_COMPLETED" -> sendEmail(
                event.get("userId").toString(),
                "Order Confirmed",
                "Your order " + event.get("orderId") + " has been confirmed! Amount: " + event.get("amount")
            );
            case "ORDER_CANCELLED" -> sendEmail(
                event.get("userId") != null ? event.get("userId").toString() : "unknown",
                "Order Cancelled",
                "Your order " + event.get("orderId") + " was cancelled. Reason: " + event.get("reason")
            );
            case "ORDER_PAYMENT_FAILED" -> sendEmail(
                "unknown",
                "Payment Failed",
                "Payment failed for order " + event.get("orderId") + ". Reason: " + event.get("reason")
            );
            default -> log.debug("[Notification] Unhandled event type: {}", eventType);
        }
    }

    @KafkaListener(topics = "payment-events", groupId = "notification-group")
    public void handlePaymentEvent(@Payload Map<String, Object> event) {
        String eventType = (String) event.get("event");
        log.info("[Notification] Received payment event: {}", eventType);

        if ("PAYMENT_SUCCESS".equals(eventType)) {
            log.info("[Notification] Payment confirmed — orderId: {}, amount: {}",
                event.get("orderId"), event.get("amount"));
            // In production: send receipt email via SendGrid/SES
        }
    }

    private void sendEmail(String userId, String subject, String body) {
        // In production: integrate SendGrid / AWS SES / Twilio
        // For now: structured log that ELK will index and display
        log.info("[EMAIL] To=user:{} | Subject='{}' | Body='{}'", userId, subject, body);
    }
}
