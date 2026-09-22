package com.boutamment.order_service.client;

import com.boutamment.order_service.model.Order;
import com.boutamment.order_service.model.OrderStatus;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final InventoryFeignClient inventoryClient;
    private final PaymentFeignClient paymentClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    // ── Create Order (full orchestration) ───────────────────────────────────

    @CircuitBreaker(name = "order-orchestration", fallbackMethod = "createOrderFallback")
    public Order createOrder(UUID userId, String productId, int quantity, BigDecimal price) {
        Timer.Sample sample = Timer.start(meterRegistry);

        Order order = Order.builder()
            .userId(userId)
            .productId(productId)
            .quantity(quantity)
            .totalAmount(price.multiply(BigDecimal.valueOf(quantity)))
            .status(OrderStatus.PENDING)
            .build();
        orderRepository.save(order);

        try {
            // Step 1: Check inventory
            log.info("[Order {}] Checking inventory for product {}", order.getId(), productId);
            InventoryResponse inventory = inventoryClient.checkStock(productId, quantity);
            if (!inventory.available()) {
                order.setStatus(OrderStatus.CANCELLED);
                orderRepository.save(order);
                publishEvent("order-events", order.getId().toString(),
                    Map.of("event", "ORDER_CANCELLED", "reason", "INSUFFICIENT_STOCK", "orderId", order.getId()));
                throw new IllegalStateException("Insufficient stock for product: " + productId);
            }

            // Step 2: Reserve inventory
            log.info("[Order {}] Reserving inventory", order.getId());
            ReservationResponse reservation = inventoryClient.reserveStock(
                new ReservationRequest(productId, quantity, order.getId().toString())
            );
            if (!reservation.success()) {
                order.setStatus(OrderStatus.CANCELLED);
                orderRepository.save(order);
                throw new IllegalStateException("Failed to reserve inventory");
            }
            order.setStatus(OrderStatus.INVENTORY_RESERVED);
            orderRepository.save(order);

            // Step 3: Process payment
            log.info("[Order {}] Processing payment", order.getId());
            order.setStatus(OrderStatus.PAYMENT_PROCESSING);
            orderRepository.save(order);

            PaymentResponse payment = paymentClient.charge(
                new ChargeRequest(order.getId().toString(), userId.toString(), order.getTotalAmount())
            );

            if (!payment.success()) {
                // Rollback inventory reservation
                inventoryClient.cancelReservation(reservation.reservationId());
                order.setStatus(OrderStatus.PAYMENT_FAILED);
                orderRepository.save(order);
                publishEvent("order-events", order.getId().toString(),
                    Map.of("event", "ORDER_PAYMENT_FAILED", "orderId", order.getId(), "reason", payment.message()));
                throw new IllegalStateException("Payment failed: " + payment.message());
            }

            // Step 4: Confirm
            order.setStatus(OrderStatus.COMPLETED);
            orderRepository.save(order);

            publishEvent("order-events", order.getId().toString(),
                Map.of("event", "ORDER_COMPLETED", "orderId", order.getId(),
                       "userId", userId, "amount", order.getTotalAmount()));

            meterRegistry.counter("orders.completed").increment();
            log.info("[Order {}] Successfully completed", order.getId());

        } catch (Exception e) {
            meterRegistry.counter("orders.failed").increment();
            log.error("[Order {}] Failed during processing: {}", order.getId(), e.getMessage());
            throw e;
        } finally {
            sample.stop(meterRegistry.timer("order.processing.time"));
        }

        return order;
    }

    // Fallback when circuit is open
    public Order createOrderFallback(UUID userId, String productId, int quantity,
                                     BigDecimal price, Throwable t) {
        log.warn("Circuit breaker open for order creation. Fallback triggered: {}", t.getMessage());
        meterRegistry.counter("orders.circuit_breaker_fallback").increment();
        throw new RuntimeException("Order service temporarily unavailable. Please retry in a moment.");
    }

    public List<Order> getOrdersByUser(UUID userId) {
        return orderRepository.findByUserId(userId);
    }

    public Order getOrder(UUID orderId) {
        return orderRepository.findById(orderId)
            .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
    }

    // ── Failure simulation endpoints (for demo & testing intelligence layer) ─

    public void simulateLatencySpike(int durationMs) throws InterruptedException {
        log.warn("SIMULATION: Injecting latency spike of {}ms", durationMs);
        meterRegistry.counter("simulation.latency_spike").increment();
        Thread.sleep(durationMs);
    }

    public void simulateErrorBurst(int errorCount) {
        log.warn("SIMULATION: Injecting {} error burst", errorCount);
        meterRegistry.counter("simulation.error_burst", "count", String.valueOf(errorCount)).increment();
        throw new RuntimeException("SIMULATED_ERROR: Artificial failure for RCA testing");
    }

    private void publishEvent(String topic, String key, Object payload) {
        try {
            kafkaTemplate.send(topic, key, payload);
        } catch (Exception e) {
            log.error("Failed to publish event to Kafka topic {}: {}", topic, e.getMessage());
        }
    }
}

// ─── Repository ──────────────────────────────────────────────────────────────

@Repository
interface OrderRepository extends JpaRepository<Order, UUID> {
    List<Order> findByUserId(UUID userId);
}
