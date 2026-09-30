package com.boutamment.intelligence_service.rca;

import com.boutamment.intelligence_service.anomaly.AnomalyDetectionEngine.AnomalyEvent;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

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
                RcaResult result = new RcaResult(
                    anomaly.service(),
                    List.of(anomaly.service()),
                    buildTraceUnavailableSummary(anomaly),
                    Math.min(70, 40 + (int) (anomaly.zScore() * 5)),
                    Instant.now()
                );
                log.warn("No traces found for service {} — publishing metrics-only RCA", anomaly.service());
                kafkaTemplate.send("rca-events", anomaly.service(), result);
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

        try {
            JsonNode response = webClientBuilder.build()
                .get()
                .uri(UriComponentsBuilder.fromUriString(jaegerUrl)
                    .path("/api/traces")
                    .queryParam("service", service)
                    .queryParam("limit", 100)
                    .queryParam("lookback", "5m")
                    .build()
                    .encode()
                    .toUri())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();
            return parseTraceSpans(response, service);
        } catch (Exception e) {
            log.warn("Jaeger query failed for service {}: {}", service, e.getMessage());
            return Collections.emptyList();
        }
    }

    private List<TraceSpan> parseTraceSpans(JsonNode response, String defaultService) {
        List<TraceSpan> spans = new ArrayList<>();
        JsonNode traces = response == null ? null : response.path("data");
        if (traces == null || !traces.isArray()) {
            return spans;
        }

        for (JsonNode trace : traces) {
            JsonNode processes = trace.path("processes");
            for (JsonNode span : trace.path("spans")) {
                String parentSpanId = null;
                for (JsonNode reference : span.path("references")) {
                    if ("CHILD_OF".equals(reference.path("refType").asText())) {
                        parentSpanId = reference.path("spanID").asText(null);
                        break;
                    }
                }

                String processId = span.path("processID").asText();
                String serviceName = processes.path(processId).path("serviceName").asText(defaultService);
                boolean hasError = false;
                for (JsonNode tag : span.path("tags")) {
                    String key = tag.path("key").asText();
                    String value = tag.path("value").asText();
                    if (("error".equals(key) && Boolean.parseBoolean(value))
                            || ("otel.status_code".equals(key) && "ERROR".equalsIgnoreCase(value))
                            || (("http.status_code".equals(key) || "http.response.status_code".equals(key))
                                && parseStatusCode(value) >= 500)) {
                        hasError = true;
                        break;
                    }
                }

                spans.add(new TraceSpan(
                    span.path("traceID").asText(),
                    span.path("spanID").asText(),
                    parentSpanId,
                    serviceName,
                    span.path("operationName").asText("unknown"),
                    span.path("duration").asLong(),
                    hasError
                ));
            }
        }
        return spans;
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
        List<TraceSpan> applicationSpans = graph.spans().values().stream()
            .filter(span -> !span.operationName().toLowerCase(Locale.ROOT).contains("actuator"))
            .toList();
        List<TraceSpan> candidates = applicationSpans.isEmpty()
            ? new ArrayList<>(graph.spans().values())
            : applicationSpans;
        Comparator<TraceSpan> duration = Comparator.comparingLong(TraceSpan::durationMicros);
        Optional<TraceSpan> failedLeaf = candidates.stream()
            .filter(TraceSpan::hasError)
            .filter(span -> graph.children().getOrDefault(span.spanId(), List.of()).isEmpty())
            .max(duration);
        Optional<TraceSpan> failedSpan = candidates.stream()
            .filter(TraceSpan::hasError)
            .max(duration);
        Optional<TraceSpan> slowestLeaf = candidates.stream()
            .filter(span -> graph.children().getOrDefault(span.spanId(), List.of()).isEmpty())
            .max(duration);
        Optional<TraceSpan> slowestSpan = failedLeaf.or(() -> failedSpan).or(() -> slowestLeaf)
            .or(() -> candidates.stream().max(duration));

        if (slowestSpan.isEmpty()) {
            return new RcaResult(anomaly.service(), List.of(anomaly.service()),
                "No spans available", 30, anomaly.detectedAt());
        }

        // Walk from the selected failed/slow leaf back through its callers.
        List<String> callChain = new ArrayList<>();
        String currentSpanId = slowestSpan.get().spanId();
        String rootService = slowestSpan.get().serviceName();

        while (currentSpanId != null) {
            TraceSpan span = graph.spans().get(currentSpanId);
            if (span == null) break;
            callChain.add(0, span.serviceName() + "(" + span.operationName() + ")");
            currentSpanId = span.parentSpanId();
            if (callChain.size() > 20) break; // guard against cycles
        }

        // Confidence: higher z-score → more confidence
        int confidence = Math.min(95, 50 + (int)(anomaly.zScore() * 8));

        return new RcaResult(rootService, callChain,
            buildSummary(rootService, anomaly, slowestSpan.get(), callChain),
            confidence, Instant.now());
    }

        private String buildSummary(
            String rootService,
            AnomalyEvent anomaly,
            TraceSpan slowestSpan,
            List<String> callChain) {
        return String.format(
            "%s showed abnormal %s (value=%.3f, baseline=%.3f, z-score=%.1f). " +
            "%s: %s.%s (%dms). Trace path: %s. Suspected service: %s.",
            anomaly.service(), anomaly.metricName(),
            anomaly.currentValue(), anomaly.baseline(), anomaly.zScore(),
            slowestSpan.hasError() ? "Failed span" : "Slowest downstream span",
            slowestSpan.serviceName(), slowestSpan.operationName(),
            slowestSpan.durationMicros() / 1000,
            String.join(" -> ", callChain), rootService
        );
    }

    private int parseStatusCode(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private String buildTraceUnavailableSummary(AnomalyEvent anomaly) {
        return String.format(
            "%s showed abnormal %s (value=%.3f, baseline=%.3f, z-score=%.1f). " +
            "No Jaeger traces were available, so the root cause could not be narrowed beyond the affected service.",
            anomaly.service(), anomaly.metricName(), anomaly.currentValue(),
            anomaly.baseline(), anomaly.zScore()
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

}
