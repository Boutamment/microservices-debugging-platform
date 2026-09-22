package com.boutamment.order_service.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

// ─── Inventory Client ───────────────────────────────────────────────────────

@FeignClient(name = "inventory-service", path = "/inventory")
interface InventoryFeignClient {
    @GetMapping("/check/{productId}")
    InventoryResponse checkStock(@PathVariable String productId, @RequestParam int quantity);

    @PostMapping("/reserve")
    ReservationResponse reserveStock(@RequestBody ReservationRequest request);

    @DeleteMapping("/reserve/{reservationId}")
    void cancelReservation(@PathVariable String reservationId);
}

// ─── Payment Client ──────────────────────────────────────────────────────────

@FeignClient(name = "payment-service", path = "/payments")
interface PaymentFeignClient {
    @PostMapping("/charge")
    PaymentResponse charge(@RequestBody ChargeRequest request);
}

// ─── DTOs ────────────────────────────────────────────────────────────────────

record InventoryResponse(String productId, boolean available, int stock) {}
record ReservationRequest(String productId, int quantity, String orderId) {}
record ReservationResponse(String reservationId, boolean success) {}
record ChargeRequest(String orderId, String userId, BigDecimal amount) {}
record PaymentResponse(String paymentId, boolean success, String message) {}
