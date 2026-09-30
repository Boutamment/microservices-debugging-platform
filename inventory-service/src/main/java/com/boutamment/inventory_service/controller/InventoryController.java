package com.boutamment.inventory_service.controller;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;
import jakarta.annotation.PostConstruct;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

// ─── Entity ──────────────────────────────────────────────────────────────────
@Entity
@Table(name = "inventory")
@Getter @Setter @Builder
@NoArgsConstructor @AllArgsConstructor
class InventoryItem {
    @Id
    private String productId;
    private String name;
    private int stock;
    private int reserved;

    public int getAvailableStock() {
        return stock - reserved;
    }
}

@Entity
@Table(name = "reservations")
@Getter @Setter @Builder
@NoArgsConstructor @AllArgsConstructor
class Reservation {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    private String productId;
    private String orderId;
    private int quantity;
    private Instant createdAt;

    @PrePersist
    void onCreate() { createdAt = Instant.now(); }
}

// ─── Repositories ─────────────────────────────────────────────────────────────

interface InventoryRepository extends JpaRepository<InventoryItem, String> {}


interface ReservationRepository extends JpaRepository<Reservation, UUID> {}

// ─── Service ─────────────────────────────────────────────────────────────────
@Slf4j
@Service
@RequiredArgsConstructor
class InventoryService {

    private final InventoryRepository inventoryRepo;
    private final ReservationRepository reservationRepo;
    private final MeterRegistry meterRegistry;

    // Simulate slow DB (toggled via /simulate/slow endpoint)
    private final AtomicBoolean slowModeEnabled = new AtomicBoolean(false);
    private int slowDelayMs = 2000;

    @PostConstruct
    void seedDemoInventory() {
        if (!inventoryRepo.existsById("PROD-1")) {
            inventoryRepo.save(InventoryItem.builder()
                .productId("PROD-1")
                .name("Demo Product")
                .stock(100)
                .reserved(0)
                .build());
            log.info("Seeded demo inventory product PROD-1 with 100 units");
        }
    }

    public Map<String, Object> checkStock(String productId, int quantity) throws InterruptedException {
        // KEY failure simulation — this is the bottleneck the intelligence layer will detect
        if (slowModeEnabled.get()) {
            log.warn("SIMULATION: Slow DB mode active — adding {}ms delay", slowDelayMs);
            meterRegistry.counter("inventory.slow_query").increment();
            Thread.sleep(slowDelayMs);
        }

        InventoryItem item = inventoryRepo.findById(productId)
            .orElseThrow(() -> new NoSuchElementException("Product not found: " + productId));

        boolean available = item.getAvailableStock() >= quantity;
        meterRegistry.gauge("inventory.available_stock",
            List.of(io.micrometer.core.instrument.Tag.of("product", productId)),
            item.getAvailableStock());

        return Map.of(
            "productId", productId,
            "available", available,
            "stock", item.getAvailableStock()
        );
    }

    public Map<String, Object> reserveStock(String productId, int quantity, String orderId) {
        InventoryItem item = inventoryRepo.findById(productId)
            .orElseThrow(() -> new NoSuchElementException("Product not found: " + productId));

        if (item.getAvailableStock() < quantity) {
            return Map.of("reservationId", "", "success", false);
        }

        item.setReserved(item.getReserved() + quantity);
        inventoryRepo.save(item);

        Reservation reservation = Reservation.builder()
            .productId(productId)
            .orderId(orderId)
            .quantity(quantity)
            .build();
        reservation = reservationRepo.save(reservation);

        log.info("Reserved {} units of {} for order {}", quantity, productId, orderId);
        return Map.of("reservationId", reservation.getId().toString(), "success", true);
    }

    public void cancelReservation(UUID reservationId) {
        reservationRepo.findById(reservationId).ifPresent(r -> {
            inventoryRepo.findById(r.getProductId()).ifPresent(item -> {
                item.setReserved(Math.max(0, item.getReserved() - r.getQuantity()));
                inventoryRepo.save(item);
            });
            reservationRepo.delete(r);
            log.info("Cancelled reservation {}", reservationId);
        });
    }

    // ── Failure simulation ────────────────────────────────────────────────────
    public void enableSlowMode(int delayMs) {
        this.slowDelayMs = delayMs;
        this.slowModeEnabled.set(true);
        log.warn("SIMULATION: Slow DB mode ENABLED with {}ms delay", delayMs);
    }

    public void disableSlowMode() {
        this.slowModeEnabled.set(false);
        log.info("SIMULATION: Slow DB mode DISABLED");
    }

    public boolean isSlowModeEnabled() { return slowModeEnabled.get(); }
}

// ─── Controller ──────────────────────────────────────────────────────────────
@Slf4j
@RestController
@RequestMapping("/inventory")
@RequiredArgsConstructor
class InventoryController {

    private final InventoryService inventoryService;

    @GetMapping("/check/{productId}")
    public ResponseEntity<Map<String, Object>> checkStock(
            @PathVariable String productId,
            @RequestParam int quantity) throws InterruptedException {
        return ResponseEntity.ok(inventoryService.checkStock(productId, quantity));
    }

    @PostMapping("/reserve")
    public ResponseEntity<Map<String, Object>> reserve(@RequestBody Map<String, Object> req) {
        String productId = (String) req.get("productId");
        int quantity = (Integer) req.get("quantity");
        String orderId = (String) req.get("orderId");
        return ResponseEntity.ok(inventoryService.reserveStock(productId, quantity, orderId));
    }

    @DeleteMapping("/reserve/{reservationId}")
    public ResponseEntity<Void> cancelReservation(@PathVariable UUID reservationId) {
        inventoryService.cancelReservation(reservationId);
        return ResponseEntity.noContent().build();
    }

    // ── Simulation endpoints ──────────────────────────────────────────────────

    @PostMapping("/simulate/slow")
    public ResponseEntity<String> enableSlow(@RequestParam(defaultValue = "2000") int delayMs) {
        inventoryService.enableSlowMode(delayMs);
        return ResponseEntity.ok("Slow DB mode enabled: " + delayMs + "ms");
    }

    @DeleteMapping("/simulate/slow")
    public ResponseEntity<String> disableSlow() {
        inventoryService.disableSlowMode();
        return ResponseEntity.ok("Slow DB mode disabled");
    }

    @GetMapping("/simulate/status")
    public ResponseEntity<Map<String, Object>> simulationStatus() {
        return ResponseEntity.ok(Map.of("slowMode", inventoryService.isSlowModeEnabled()));
    }
}
