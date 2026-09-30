package com.boutamment.intelligence_service.anomaly;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Anomaly Detection Engine
 *
 * Strategy:
 *  1. Every 15s, query Prometheus for key metrics per service
 *  2. Maintain a sliding window of recent values (baseline)
 *  3. Compute Z-Score for the latest value
 *  4. If |Z| > threshold → anomaly detected → publish to Kafka
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AnomalyDetectionEngine {

    private final WebClient.Builder webClientBuilder;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    // Sliding window: serviceName -> metricName -> list of recent values
    private final Map<String, Map<String, Deque<Double>>> metricWindows = new ConcurrentHashMap<>();
    private static final int WINDOW_SIZE = 60;     // last 60 data points
    private static final double Z_THRESHOLD = 2.5; // standard deviations to flag

    private static final List<String> SERVICES = List.of(
        "order-service", "payment-service", "inventory-service", "auth-service"
    );

    @Scheduled(fixedDelay = 15_000)
    public void detectAnomalies() {
        for (String service : SERVICES) {
            checkLatency(service);
            checkErrorRate(service);
        }
    }

    private void checkLatency(String service) {
        queryPrometheus(
            "histogram_quantile(0.95, rate(http_server_requests_seconds_bucket{application=\"" + service + "\"}[1m]))",
            service, "latency_p95"
        );
    }

    private void checkErrorRate(String service) {
        queryPrometheus(
            "rate(http_server_requests_seconds_count{application=\"" + service + "\",status=~\"5..\"}[1m])",
            service, "error_rate"
        );
    }

    private void queryPrometheus(String promQL, String service, String metricName) {
    String prometheusUrl = System.getenv()
        .getOrDefault("PROMETHEUS_URL", "http://localhost:9090");

    webClientBuilder.build()
        .get()
        .uri(UriComponentsBuilder.fromUriString(prometheusUrl)
            .path("/api/v1/query")
            .queryParam("query", promQL)
            .build(false)
            .encode()
            .toUri())
        .retrieve()
        .bodyToMono(PrometheusResponse.class)
        .subscribe(
            response -> {
                if (response == null
                        || response.data() == null
                        || response.data().result() == null
                        || response.data().result().isEmpty()) {
                    return;
                }

                Map<String, Object> resultItem = response.data().result().get(0);
                Object valueEntry = resultItem.get("value");

                if (valueEntry instanceof List<?> valueList && valueList.size() >= 2) {
                    Object rawValue = valueList.get(1);

                    if (rawValue instanceof Number number) {
                        evaluateMetric(
                            service,
                            metricName,
                            number.doubleValue()
                        );
                    } else if (rawValue instanceof String str) {
                        evaluateMetric(
                            service,
                            metricName,
                            Double.parseDouble(str)
                        );
                    }
                }
            },
            error -> log.debug(
                "Prometheus query failed for {}/{}: {}",
                service,
                metricName,
                error.getMessage()
            )
        );
}

    private void evaluateMetric(String service, String metricName, double currentValue) {
        metricWindows.computeIfAbsent(service, k -> new ConcurrentHashMap<>())
                     .computeIfAbsent(metricName, k -> new ArrayDeque<>());

        Deque<Double> window = metricWindows.get(service).get(metricName);
        window.addLast(currentValue);
        if (window.size() > WINDOW_SIZE) window.pollFirst();

        if (window.size() < 10) return; // need enough data for baseline

        double mean = window.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double variance = window.stream().mapToDouble(v -> Math.pow(v - mean, 2)).average().orElse(0);
        double stdDev = Math.sqrt(variance);

        if (stdDev == 0) return;

        double zScore = Math.abs((currentValue - mean) / stdDev);

        if (zScore > Z_THRESHOLD) {
            AnomalyEvent anomaly = new AnomalyEvent(
                UUID.randomUUID().toString(),
                service,
                metricName,
                currentValue,
                mean,
                zScore,
                Instant.now(),
                zScore > 4.0 ? Severity.CRITICAL : zScore > 3.0 ? Severity.HIGH : Severity.MEDIUM
            );

            log.warn("🚨 ANOMALY DETECTED — service={} metric={} value={:.3f} mean={:.3f} z={:.2f} severity={}",
                service, metricName, currentValue, mean, zScore, anomaly.severity());

            kafkaTemplate.send("anomaly-events", service, anomaly);
            meterRegistry.counter("intelligence.anomalies_detected",
                "service", service, "metric", metricName, "severity", anomaly.severity().name())
                .increment();
        }
    }

    // ── DTOs ──────────────────────────────────────────────────────────────────
    public record AnomalyEvent(
        String id,
        String service,
        String metricName,
        double currentValue,
        double baseline,
        double zScore,
        Instant detectedAt,
        Severity severity
    ) {}

    public enum Severity { MEDIUM, HIGH, CRITICAL }

    record PrometheusResponse(String status, PrometheusData data) {}
    record PrometheusData(String resultType, List<Map<String, Object>> result) {}
}
