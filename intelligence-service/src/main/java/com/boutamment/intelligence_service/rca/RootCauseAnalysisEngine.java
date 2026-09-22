package com.boutamment.intelligence_service.rca;

import com.boutamment.intelligence_service.anomaly.AnomalyDetectionEngine.AnomalyEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Instant;
import java.util.*;

/**
 * Root Cause Analysis Engine
 *
 * When an anomaly fires:
 *  1. Query Jaeger for recent traces involving the affected service
 *  2. Build a call graph from span parent-child relationships
 *  3. Walk upstream to find the ORIGIN of the slow/failing call
 *  4. Produce an RCA event with the call chain and root service identified
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RootCauseAnalysisEngine {

    private final WebClient.Builder webClientBuilder;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "anomaly-events", groupId = "rca-group")
    public void analyzeAnomaly(AnomalyEvent anomaly) {
        log.info("🔍 Starting RCA for anomaly in service={} metric={}", anomaly.service(), anomaly.metricName());

        try {
            // Step 1: Fetch recent failing traces from Jaeger
            List<TraceSpan> spans = queryJaeger(anomaly.service(), anomaly.metricName());

            if (spans.isEmpty()) {
                log.warn("No traces found for service {} — skipping RCA", anomaly.service());
                return;
            }

            // Step 2: Build call graph
            CallGraph graph = buildCallGraph(spans);

            // Step 3: Find root cause using upstream traversal
            RcaResult result = findRootCause(graph, anomaly);

            log.info("✅ RCA complete — rootService={} callChain={} confidence={}%",
                result.rootService(), result.callChain(), result.confidencePercent());

            // Step 4: Publish RCA result to Kafka → triggers Explanation Engine
            kafkaTemplate.send("rca-events", anomaly.service(), result);

        } catch (Exception e) {
            log.error("RCA failed for service {}: {}", anomaly.service(), e.getMessage());
        }
    }

    private List<TraceSpan> queryJaeger(String service, String metricName) {
        String jaegerUrl = System.getenv().getOrDefault("JAEGER_URL", "http://localhost:16686");
        String operation = metricName.contains("latency") ? "GET" : "POST";

        try {
            return webClientBuilder.build()
                .get()
                .uri(jaegerUrl + "/api/traces?service=" + service
                    + "&operation=" + operation
                    + "&limit=20&lookback=5m")
                .retrieve()
                .bodyToMono(JaegerResponse.class)
                .map(r -> r != null && r.data() != null ? r.data() : Collections.<TraceSpan>emptyList())
                .block();
        } catch (Exception e) {
            log.debug("Jaeger query failed: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private CallGraph buildCallGraph(List<TraceSpan> spans) {
        Map<String, TraceSpan> spanById = new HashMap<>();
        Map<String, List<String>> children = new HashMap<>();

        for (TraceSpan span : spans) {
            spanById.put(span.spanId(), span);
            children.computeIfAbsent(span.parentSpanId(), k -> new ArrayList<>()).add(span.spanId());
        }

        return new CallGraph(spanById, children);
    }

    private RcaResult findRootCause(CallGraph graph, AnomalyEvent anomaly) {
        // Find the span(s) with highest duration — that's our starting point
        Optional<TraceSpan> slowestSpan = graph.spans().values().stream()
            .max(Comparator.comparingLong(TraceSpan::durationMicros));

        if (slowestSpan.isEmpty()) {
            return new RcaResult(anomaly.service(), List.of(anomaly.service()),
                "No spans available", 30, anomaly.detectedAt());
        }

        // Walk UP the call chain to find the originating service
        List<String> callChain = new ArrayList<>();
        String currentSpanId = slowestSpan.get().spanId();
        String rootService = anomaly.service();

        while (currentSpanId != null) {
            TraceSpan span = graph.spans().get(currentSpanId);
            if (span == null) break;
            callChain.add(0, span.serviceName() + "(" + span.operationName() + ")");
            rootService = span.serviceName();
            currentSpanId = span.parentSpanId();
            if (callChain.size() > 20) break; // guard against cycles
        }

        // Confidence: higher z-score → more confidence
        int confidence = Math.min(95, 50 + (int)(anomaly.zScore() * 8));

        return new RcaResult(rootService, callChain,
            buildSummary(rootService, anomaly, slowestSpan.get()),
            confidence, Instant.now());
    }

    private String buildSummary(String rootService, AnomalyEvent anomaly, TraceSpan slowestSpan) {
        return String.format(
            "%s showed abnormal %s (value=%.3f, baseline=%.3f, z-score=%.1f). " +
            "Slowest span: %s.%s (%dms). Likely caused by: %s",
            anomaly.service(), anomaly.metricName(),
            anomaly.currentValue(), anomaly.baseline(), anomaly.zScore(),
            slowestSpan.serviceName(), slowestSpan.operationName(),
            slowestSpan.durationMicros() / 1000,
            rootService.equals(anomaly.service()) ? "internal issue" : "upstream dependency " + rootService
        );
    }

    // ── Records / DTOs ────────────────────────────────────────────────────────

    public record RcaResult(
        String rootService,
        List<String> callChain,
        String technicalSummary,
        int confidencePercent,
        Instant analyzedAt
    ) {}

    record CallGraph(Map<String, TraceSpan> spans, Map<String, List<String>> children) {}

    record TraceSpan(
        String traceId,
        String spanId,
        String parentSpanId,
        String serviceName,
        String operationName,
        long durationMicros,
        boolean hasError
    ) {}

    record JaegerResponse(List<TraceSpan> data) {}
}
