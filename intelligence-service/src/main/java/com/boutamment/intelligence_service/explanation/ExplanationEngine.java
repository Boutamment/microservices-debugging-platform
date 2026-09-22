package com.boutamment.intelligence_service.explanation;

import com.boutamment.intelligence_service.rca.RootCauseAnalysisEngine.RcaResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Explanation Engine
 *
 * Consumes RCA results → feeds structured context to an LLM →
 * produces plain-English explanations that engineers can act on immediately.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ExplanationEngine {

    private final WebClient.Builder webClientBuilder;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String SYSTEM_PROMPT = """
        You are an expert Site Reliability Engineer analyzing distributed system failures.
        You receive structured Root Cause Analysis data and must produce:
        1. A plain-English explanation (2-3 sentences) that a non-expert engineer can understand.
        2. The probable root cause in one sentence.
        3. Three concrete, prioritized remediation steps.

        Always be specific — use service names, metric values, and timestamps from the data.
        Never be vague. Format your response as JSON with keys:
        "explanation", "rootCause", "remediationSteps" (array of 3 strings).
        """;

    @KafkaListener(topics = "rca-events", groupId = "explanation-group")
    public void generateExplanation(RcaResult rcaResult) {
        log.info("🧠 Generating LLM explanation for rootService={}", rcaResult.rootService());

        try {
            String userPrompt = buildPrompt(rcaResult);
            String llmResponse = callOpenAi(userPrompt);

            if (llmResponse == null || llmResponse.isBlank()) {
                throw new IllegalStateException("OpenAI response was empty");
            }

            log.debug("LLM raw response: {}", llmResponse);

            Explanation explanation = parseExplanation(llmResponse, rcaResult);

            log.info("💡 EXPLANATION READY:\n  Summary: {}\n  Root cause: {}\n  Steps: {}",
                explanation.summary(), explanation.rootCause(), explanation.remediationSteps());

            kafkaTemplate.send("insight-events", rcaResult.rootService(), explanation);

        } catch (Exception e) {
            log.error("Explanation generation failed: {}", e.getMessage());
            Explanation fallback = new Explanation(
                rcaResult.technicalSummary(),
                "Root service: " + rcaResult.rootService(),
                List.of("Investigate " + rcaResult.rootService(),
                        "Check recent deployments",
                        "Review Prometheus metrics for service dependencies"),
                rcaResult.confidencePercent(),
                Instant.now()
            );
            kafkaTemplate.send("insight-events", rcaResult.rootService(), fallback);
        }
    }

    private String callOpenAi(String userPrompt) {
        String apiKey = System.getenv("OPENAI_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            log.warn("OPENAI_API_KEY is not configured; skipping LLM explanation generation");
            return null;
        }

        String openAiUrl = System.getenv().getOrDefault("OPENAI_API_URL", "https://api.openai.com/v1/chat/completions");
        String model = System.getenv().getOrDefault("OPENAI_MODEL", "gpt-4o-mini");

        Map<String, Object> request = Map.of(
            "model", model,
            "messages", List.of(
                Map.of("role", "system", "content", SYSTEM_PROMPT),
                Map.of("role", "user", "content", userPrompt)
            ),
            "temperature", 0.2
        );

        return webClientBuilder.build()
            .post()
            .uri(openAiUrl)
            .header("Authorization", "Bearer " + apiKey)
            .header("Content-Type", "application/json")
            .bodyValue(request)
            .retrieve()
            .bodyToMono(Map.class)
            .map(body -> {
                if (body == null) {
                    return null;
                }

                Object choices = body.get("choices");
                if (!(choices instanceof List<?> list) || list.isEmpty()) {
                    return null;
                }

                Object firstChoice = list.get(0);
                if (!(firstChoice instanceof Map<?, ?> choice)) {
                    return null;
                }

                Object message = choice.get("message");
                if (!(message instanceof Map<?, ?> msg)) {
                    return null;
                }

                Object content = msg.get("content");
                if (content instanceof String str) {
                    return str;
                }

                return null;
            })
            .block();
    }

    private String buildPrompt(RcaResult result) {
        return String.format("""
            Analyze this distributed system failure:

            - Affected Service: %s
            - Root Service (origin of failure): %s
            - Call Chain: %s
            - Technical Summary: %s
            - Detection Confidence: %d%%
            - Detected At: %s

            Generate a clear explanation and remediation plan.
            """,
            result.callChain().isEmpty() ? "unknown" : result.callChain().get(result.callChain().size() - 1),
            result.rootService(),
            String.join(" → ", result.callChain()),
            result.technicalSummary(),
            result.confidencePercent(),
            result.analyzedAt()
        );
    }

    private Explanation parseExplanation(String json, RcaResult fallbackResult) {
        try {
            JsonNode root = objectMapper.readTree(json);
            JsonNode explanationNode = root.path("explanation");
            JsonNode rootCauseNode = root.path("rootCause");
            JsonNode remediationNode = root.path("remediationSteps");

            String summary = explanationNode.isTextual() ? explanationNode.asText() : "";
            String rootCause = rootCauseNode.isTextual() ? rootCauseNode.asText() : "";
            List<String> steps = remediationNode.isArray()
                ? objectMapper.convertValue(remediationNode, List.class)
                : List.of();

            return new Explanation(summary, rootCause, steps, fallbackResult.confidencePercent(), Instant.now());
        } catch (Exception e) {
            return new Explanation(json, "See full response above",
                List.of("Review LLM output"), fallbackResult.confidencePercent(), Instant.now());
        }
    }

    public record Explanation(
        String summary,
        String rootCause,
        List<String> remediationSteps,
        int confidencePercent,
        Instant generatedAt
    ) {}
}
