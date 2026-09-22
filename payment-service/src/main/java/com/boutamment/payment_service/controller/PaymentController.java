package com.boutamment.payment_service.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RestController
@RequestMapping("/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping("/charge")
    public ResponseEntity<Map<String, Object>> charge(@RequestBody Map<String, Object> req) {
        String orderId = String.valueOf(req.getOrDefault("orderId", UUID.randomUUID()));
        String userId = String.valueOf(req.getOrDefault("userId", "anonymous"));
        BigDecimal amount = new BigDecimal(req.get("amount").toString());
        return ResponseEntity.ok(paymentService.charge(orderId, userId, amount));
    }

    @PostMapping("/simulate/failure-rate")
    public ResponseEntity<Map<String, Object>> setFailureRate(@RequestParam double rate) {
        paymentService.setFailureRate(rate);
        return ResponseEntity.ok(Map.of(
                "message", "Payment failure rate set to: " + (int) (rate * 100) + "%",
                "failureRate", paymentService.getFailureRate()
        ));
    }

    @GetMapping("/simulate/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of("failureRate", paymentService.getFailureRate()));
    }
}

@Service
@Slf4j
class PaymentService {
    private final AtomicReference<Double> failureRate = new AtomicReference<>(0.0);
    private final ConcurrentMap<String, PaymentRecord> payments = new ConcurrentHashMap<>();

    public Map<String, Object> charge(String orderId, String userId, BigDecimal amount) {
        boolean shouldFail = Math.random() < failureRate.get();
        String paymentId = UUID.randomUUID().toString();
        PaymentRecord payment = new PaymentRecord(paymentId, orderId, userId, amount, shouldFail ? "FAILED" : "SUCCESS", Instant.now());
        payments.put(paymentId, payment);

        if (shouldFail) {
            log.warn("[Payment] SIMULATED FAILURE for order {}", orderId);
            return Map.of(
                    "paymentId", paymentId,
                    "success", false,
                    "message", "Payment gateway timeout (simulated)"
            );
        }

        log.info("[Payment] SUCCESS for order {} — amount: {}", orderId, amount);
        return Map.of(
                "paymentId", paymentId,
                "success", true,
                "message", "OK",
                "amount", amount
        );
    }

    public void setFailureRate(double rate) {
        double safeRate = Math.min(1.0, Math.max(0.0, rate));
        failureRate.set(safeRate);
        log.warn("SIMULATION: Payment failure rate set to {}%", (int) (safeRate * 100));
    }

    public double getFailureRate() {
        return failureRate.get();
    }

    private record PaymentRecord(String paymentId, String orderId, String userId, BigDecimal amount,
                                String status, Instant processedAt) {
    }
}
