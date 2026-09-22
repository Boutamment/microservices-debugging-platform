package com.boutamment.order_service.controller;

import com.boutamment.order_service.client.OrderService;
import com.boutamment.order_service.model.Order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;


@RestController
@RequestMapping("/orders")
@CrossOrigin(origins="*")
@RequiredArgsConstructor
public class OrderController {
    
    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<Order> createOrder(
            @RequestHeader("X-User-Id") UUID userId,
            @Valid @RequestBody CreateOrderRequest req) {
        Order order = orderService.createOrder(userId, req.productId(), req.quantity(), req.pricePerUnit());
        return ResponseEntity.ok(order);
    }

    @GetMapping
    public ResponseEntity<List<Order>> getMyOrders(@RequestHeader("X-User-Id") UUID userId) {
        List<Order> orders = orderService.getOrdersByUser(userId);
        return ResponseEntity.ok(orders);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<Order> getOrder(@PathVariable UUID orderId){
        Order order =orderService.getOrder(orderId);
        return ResponseEntity.ok(order);
    }


    // ── Failure simulation endpoints ─────────────────────────────────────────
    @PostMapping("/simulate/latency")
    public ResponseEntity<String> simulateLatency(@RequestParam(defaultValue = "3000") int ms)
            throws InterruptedException {
        orderService.simulateLatencySpike(ms);
        return ResponseEntity.ok("Latency spike of " + ms + "ms completed");
    }
    
    @PostMapping("/simulate/error")
    public ResponseEntity<String> simulateError(@RequestParam(defaultValue = "1") int count) {
        orderService.simulateErrorBurst(count);
        return ResponseEntity.ok("Never reached");
    }

    
    public record CreateOrderRequest(
        @NotBlank String productId,
        @Min(1) @Max(1000) Integer quantity,
        @NotNull @DecimalMin("0.01") BigDecimal pricePerUnit
    ) {}
    
}
