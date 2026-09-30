package com.boutamment.intelligence_service.stream;

import com.boutamment.intelligence_service.anomaly.AnomalyDetectionEngine.AnomalyEvent;
import com.boutamment.intelligence_service.anomaly.AnomalyDetectionEngine.Severity;
import com.boutamment.intelligence_service.explanation.ExplanationEngine.Explanation;
import com.boutamment.intelligence_service.rca.RootCauseAnalysisEngine.RcaResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

@SpringBootApplication
@EnableDiscoveryClient
@EnableScheduling
class IntelligenceApplication {
    public static void main(String[] args) {
        SpringApplication.run(IntelligenceApplication.class, args);
    }
}

/**
 * Intelligence REST API
 *
 * - GET /intelligence/anomalies           — recent detected anomalies
 * - GET /intelligence/rca/{service}       — latest RCA result for a service
 * - GET /intelligence/insights            — all final LLM explanations
 * - GET /intelligence/stream (SSE)        — real-time push to React dashboard
 * - GET /intelligence/health-map          — overall system health overview
 */
@Slf4j
@RestController
@RequestMapping("/intelligence")
@RequiredArgsConstructor
class IntelligenceController {

    // In-memory stores (in production: persist to PostgreSQL / Redis)
    private final CopyOnWriteArrayList<AnomalyEvent>  recentAnomalies = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<RcaResult>     rcaResults      = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Explanation>   insights        = new CopyOnWriteArrayList<>();
    private final KafkaTemplate<String, Object> kafkaTemplate;

    // SSE sink for real-time dashboard streaming
    private final Sinks.Many<Object> eventSink = Sinks.many().multicast().onBackpressureBuffer();

    // ── Kafka consumers to feed local stores ──────────────────────────────────

    @KafkaListener(topics = "anomaly-events", groupId = "intelligence-api")
    public void onAnomaly(AnomalyEvent event) {
        recentAnomalies.add(0, event);
        if (recentAnomalies.size() > 200) recentAnomalies.remove(recentAnomalies.size() - 1);
        eventSink.tryEmitNext(Map.of("type", "ANOMALY", "data", event));
    }

    @KafkaListener(topics = "rca-events", groupId = "intelligence-api")
    public void onRca(RcaResult result) {
        rcaResults.add(0, result);
        if (rcaResults.size() > 100) rcaResults.remove(rcaResults.size() - 1);
        eventSink.tryEmitNext(Map.of("type", "RCA", "data", result));
    }

    @KafkaListener(topics = "insight-events", groupId = "intelligence-api")
    public void onInsight(Explanation explanation) {
        insights.add(0, explanation);
        if (insights.size() > 100) insights.remove(insights.size() - 1);
        eventSink.tryEmitNext(Map.of("type", "INSIGHT", "data", explanation));
        log.info("📊 New insight pushed to dashboard: {}", explanation.summary());
    }

    // ── REST endpoints ─────────────────────────────────────────────────────────

    @PostMapping("/simulate/anomaly")
    public ResponseEntity<AnomalyEvent> simulateAnomaly(
            @RequestParam(defaultValue = "order-service") String service) {
        AnomalyEvent event = new AnomalyEvent(
            UUID.randomUUID().toString(),
            service,
            "error_rate",
            0.25,
            0.01,
            4.0,
            Instant.now(),
            Severity.HIGH
        );
        kafkaTemplate.send("anomaly-events", service, event);
        return ResponseEntity.accepted().body(event);
    }

    @GetMapping("/anomalies")
    public ResponseEntity<List<AnomalyEvent>> getAnomalies(
            @RequestParam(defaultValue = "20") int limit,
            @RequestParam(required = false) String service) {
        List<AnomalyEvent> result = recentAnomalies.stream()
            .filter(a -> service == null || a.service().equals(service))
            .limit(limit)
            .toList();
        return ResponseEntity.ok(result);
    }

    @GetMapping("/rca")
    public ResponseEntity<List<RcaResult>> getRcaResults(
            @RequestParam(defaultValue = "10") int limit) {
        return ResponseEntity.ok(new ArrayList<>(rcaResults.subList(0, Math.min(limit, rcaResults.size()))));
    }

    @GetMapping("/rca/{service}")
    public ResponseEntity<RcaResult> getRcaForService(@PathVariable String service) {
        return rcaResults.stream()
            .filter(r -> r.rootService().equals(service))
            .findFirst()
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/insights")
    public ResponseEntity<List<Explanation>> getInsights(
            @RequestParam(defaultValue = "10") int limit) {
        return ResponseEntity.ok(new ArrayList<>(insights.subList(0, Math.min(limit, insights.size()))));
    }

    @GetMapping("/health-map")
    public ResponseEntity<Map<String, Object>> getHealthMap() {
        // Derive service health from recent anomalies
        Map<String, String> serviceHealth = new java.util.HashMap<>();
        List.of("order-service", "payment-service", "inventory-service", "auth-service", "api-gateway")
            .forEach(svc -> {
                boolean hasRecentAnomaly = recentAnomalies.stream()
                    .limit(50)
                    .anyMatch(a -> a.service().equals(svc));
                serviceHealth.put(svc, hasRecentAnomaly ? "DEGRADED" : "HEALTHY");
            });

        return ResponseEntity.ok(Map.of(
            "services", serviceHealth,
            "totalAnomalies", recentAnomalies.size(),
            "totalInsights", insights.size(),
            "latestInsight", insights.isEmpty() ? null : insights.get(0)
        ));
    }

    // ── SSE stream for React dashboard ────────────────────────────────────────

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Object> streamEvents() {
        return eventSink.asFlux();
    }
}
