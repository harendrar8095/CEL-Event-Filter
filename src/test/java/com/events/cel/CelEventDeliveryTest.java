package com.events.cel;

import com.events.cel.consumer.EventConsumer;
import com.events.cel.engine.CelFilterRegistry;
import com.events.cel.model.CloudEvent;
import com.events.cel.model.DeliveryResult;
import com.events.cel.publisher.EventPublisher;
import com.events.cel.publisher.EventPublisher.PublishReport;
import com.events.cel.util.EventFactory;
import com.events.cel.util.EventFlattener;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests covering:
 *   - CelFilterRegistry: compilation, evaluation, lifecycle
 *   - EventPublisher: subscribe, publish, unsubscribe, hot update
 *   - EventFlattener: correct dot-notation flattening
 *   - CEL expressions: all operators + functions against the Artifactory event
 *   - Compile-time error detection
 *   - Edge cases: null fields, empty values, array properties
 */
class CelEventDeliveryTest {

    // =========================================================================
    // CelFilterRegistry — unit tests
    // =========================================================================

    @Nested
    @DisplayName("CelFilterRegistry")
    class RegistryTests {

        private CelFilterRegistry registry;
        private CloudEvent         event;

        @BeforeEach
        void setUp() {
            registry = new CelFilterRegistry();
            event    = EventFactory.canonical();
        }

        // ── Basic equality ────────────────────────────────────────────────────

        @Test @DisplayName("Match: exact type equality")
        void matchExactType() {
            assertDelivered(registry, event, "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\"");
        }

        @Test @DisplayName("Match: repoType equality")
        void matchRepoType() {
            assertDelivered(registry, event, "event[\"data.artifact.repoType\"] == \"REMOTE\"");
        }

        @Test @DisplayName("Match: repoKey equality")
        void matchRepoKey() {
            assertDelivered(registry, event, "event[\"data.artifact.repoKey\"] == \"helm-remote\"");
        }

        @Test @DisplayName("No Match: wrong type")
        void noMatchWrongType() {
            assertFiltered(registry, event, "event[\"type\"] == \"AFTER_LOCAL_DOWNLOAD\"");
        }

        @Test @DisplayName("No Match: wrong repoType")
        void noMatchWrongRepoType() {
            assertFiltered(registry, event, "event[\"data.artifact.repoType\"] == \"LOCAL\"");
        }

        // ── Logical operators ─────────────────────────────────────────────────

        @Test @DisplayName("Match: AND operator - both conditions true")
        void matchAnd() {
            assertDelivered(registry, event,
                "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\" && event[\"data.artifact.repoType\"] == \"REMOTE\"");
        }

        @Test @DisplayName("No Match: AND operator - one condition false")
        void noMatchAndFails() {
            assertFiltered(registry, event,
                "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\" && event[\"data.artifact.repoType\"] == \"LOCAL\"");
        }

        @Test @DisplayName("Match: OR operator - first condition true")
        void matchOrFirstTrue() {
            assertDelivered(registry, event,
                "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\" || event[\"type\"] == \"AFTER_LOCAL_DOWNLOAD\"");
        }

        @Test @DisplayName("Match: OR operator - second condition true")
        void matchOrSecondTrue() {
            assertDelivered(registry, event,
                "event[\"type\"] == \"WRONG\" || event[\"data.artifact.repoType\"] == \"REMOTE\"");
        }

        @Test @DisplayName("No Match: NOT operator")
        void matchNot() {
            assertFiltered(registry, event,
                "!(event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\")");
        }

        @Test @DisplayName("Match: complex grouping with OR inside AND")
        void matchComplexGrouping() {
            assertDelivered(registry, event,
                "(event[\"data.artifact.repoType\"] == \"REMOTE\" || event[\"data.artifact.repoType\"] == \"LOCAL\") " +
                "&& event[\"type\"].startsWith(\"AFTER_\")");
        }

        // ── String functions ─────────────────────────────────────────────────

        @Test @DisplayName("Match: contains() — source contains 'jfrt'")
        void matchContains() {
            assertDelivered(registry, event, "event[\"source\"].contains(\"jfrt\")");
        }

        @Test @DisplayName("Match: startsWith() — type starts with AFTER_")
        void matchStartsWith() {
            assertDelivered(registry, event, "event[\"type\"].startsWith(\"AFTER_\")");
        }

        @Test @DisplayName("Match: endsWith() — path ends with helm")
        void matchEndsWith() {
            assertDelivered(registry, event, "event[\"data.artifact.path\"].endsWith(\"helm\")");
        }

        @Test @DisplayName("Match: actor contains admin")
        void matchActorContains() {
            assertDelivered(registry, event, "event[\"data.context.actor\"].contains(\"admin\")");
        }

        @Test @DisplayName("Match: migrated-from property contains bintray")
        void matchPropertyArray() {
            assertDelivered(registry, event,
                "event[\"data.artifact.properties.migrated-from\"].contains(\"bintray\")");
        }

        @Test @DisplayName("Match: source-bintray-organization contains jfrog")
        void matchBintrayOrg() {
            assertDelivered(registry, event,
                "event[\"data.artifact.properties.source-bintray-organization\"].contains(\"jfrog\")");
        }

        // ── 'in' operator ────────────────────────────────────────────────────

        @Test @DisplayName("Match: repoType in list")
        void matchInList() {
            assertDelivered(registry, event,
                "event[\"data.artifact.repoType\"] in [\"REMOTE\", \"LOCAL\", \"VIRTUAL\"]");
        }

        @Test @DisplayName("No Match: repoType NOT in list")
        void noMatchNotInList() {
            assertFiltered(registry, event,
                "event[\"data.artifact.repoType\"] in [\"LOCAL\", \"VIRTUAL\"]");
        }

        @Test @DisplayName("Match: type in list of two event types")
        void matchTypeInList() {
            assertDelivered(registry, event,
                "event[\"type\"] in [\"AFTER_REMOTE_DOWNLOAD\", \"BEFORE_REMOTE_DOWNLOAD\"]");
        }

        // ── Null/empty field handling ─────────────────────────────────────────

        @Test @DisplayName("Match: sha256 is not empty")
        void matchSha256NotEmpty() {
            assertDelivered(registry, event, "event[\"data.artifact.sha256\"] != \"\"");
        }

        @Test @DisplayName("Match: name field is empty (null serialized to empty string)")
        void matchNullFieldIsEmpty() {
            assertDelivered(registry, event, "event[\"data.artifact.name\"] == \"\"");
        }

        // ── CEL literal expressions ───────────────────────────────────────────

        @Test @DisplayName("Match: CEL literal true — always matches")
        void matchLiteralTrue() {
            assertDelivered(registry, event, "true");
        }

        @Test @DisplayName("No Match: CEL literal false — never matches")
        void noMatchLiteralFalse() {
            assertFiltered(registry, event, "false");
        }

        // ── Consumer lifecycle ────────────────────────────────────────────────

        @Test @DisplayName("Deregistered consumer is no longer evaluated")
        void deregisteredConsumerExcluded() {
            registry.registerConsumer("temp", "true");
            registry.deregisterConsumer("temp");
            // evaluateAll should not include the deregistered consumer
            Map<String, DeliveryResult> results = registry.evaluateAll(event);
            assertFalse(results.containsKey("temp"), "Deregistered consumer should not appear");
        }

        @Test @DisplayName("getMatchingConsumers returns only matching consumer IDs")
        void getMatchingConsumersReturnsCorrectSet() {
            registry.registerConsumer("consumer-match", "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\"");
            registry.registerConsumer("consumer-no-match", "event[\"type\"] == \"AFTER_LOCAL_DOWNLOAD\"");

            Set<String> matched = registry.getMatchingConsumers(event);
            assertTrue(matched.contains("consumer-match"), "Matching consumer should be present");
            assertFalse(matched.contains("consumer-no-match"), "Non-matching consumer should be absent");
        }

        @Test @DisplayName("Hot filter update changes routing without losing consumer")
        void hotFilterUpdateWorks() {
            registry.registerConsumer("updatable", "event[\"type\"] == \"AFTER_LOCAL_DOWNLOAD\"");
            // Currently doesn't match (wrong type)
            DeliveryResult before = registry.evaluateOne("updatable", EventFlattener.flatten(event));
            assertEquals(DeliveryResult.Status.FILTERED, before.getStatus());

            // Update to correct filter
            registry.updateFilter("updatable", "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\"");
            DeliveryResult after = registry.evaluateOne("updatable", EventFlattener.flatten(event));
            assertEquals(DeliveryResult.Status.DELIVERED, after.getStatus());
        }
    }

    // =========================================================================
    // Compile-time error detection
    // =========================================================================

    @Nested
    @DisplayName("CEL Compile-time Validation")
    class CompileTimeTests {

        private CelFilterRegistry registry;

        @BeforeEach void setUp() { registry = new CelFilterRegistry(); }

        @Test @DisplayName("Type mismatch: string + int is caught at compile time")
        void typeMismatchCaughtAtCompile() {
            assertThrows(CelFilterRegistry.CelFilterCompilationException.class, () ->
                registry.registerConsumer("t", "event[\"type\"] + 42")
            );
        }

        @Test @DisplayName("Undefined variable is caught at compile time")
        void undefinedVariableCaughtAtCompile() {
            assertThrows(CelFilterRegistry.CelFilterCompilationException.class, () ->
                registry.registerConsumer("t", "undefined_var == \"foo\"")
            );
        }

        @Test @DisplayName("Invalid function call is caught at compile time")
        void invalidFunctionCaughtAtCompile() {
            assertThrows(CelFilterRegistry.CelFilterCompilationException.class, () ->
                registry.registerConsumer("t", "event[\"type\"].nonExistentFunction()")
            );
        }

        @Test @DisplayName("Syntax error is caught at compile time")
        void syntaxErrorCaughtAtCompile() {
            assertThrows(CelFilterRegistry.CelFilterCompilationException.class, () ->
                registry.registerConsumer("t", "event[\"type\"] == (")  // unclosed paren
            );
        }

        @Test @DisplayName("Valid expression does NOT throw")
        void validExpressionDoesNotThrow() {
            assertDoesNotThrow(() ->
                registry.registerConsumer("t",
                    "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\" && event[\"source\"].contains(\"jfrt\")")
            );
        }
    }

    // =========================================================================
    // EventPublisher — integration tests
    // =========================================================================

    @Nested
    @DisplayName("EventPublisher Integration")
    class PublisherTests {

        private EventPublisher publisher;

        @BeforeEach void setUp() { publisher = new EventPublisher(); }

        @Test @DisplayName("Matching consumer receives event via onEvent()")
        void matchingConsumerReceivesEvent() {
            EventConsumer consumer = subscribe(publisher, "c1",
                "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\"");

            publisher.publish(EventFactory.canonical());

            assertEquals(1, consumer.getDeliveredCount());
            assertEquals(0, consumer.getFilteredCount());
            assertEquals(1, consumer.getReceivedEvents().size());
        }

        @Test @DisplayName("Non-matching consumer does NOT receive event")
        void nonMatchingConsumerExcluded() {
            EventConsumer consumer = subscribe(publisher, "c1",
                "event[\"type\"] == \"AFTER_LOCAL_DOWNLOAD\"");

            publisher.publish(EventFactory.canonical());

            assertEquals(0, consumer.getDeliveredCount());
            assertEquals(1, consumer.getFilteredCount());
            assertTrue(consumer.getReceivedEvents().isEmpty());
        }

        @Test @DisplayName("Multiple consumers: only matching ones receive event")
        void onlyMatchingConsumersReceiveEvent() {
            EventConsumer helm   = subscribe(publisher, "helm-team",
                "event[\"data.artifact.repoKey\"] == \"helm-remote\"");
            EventConsumer docker = subscribe(publisher, "docker-team",
                "event[\"data.artifact.repoKey\"] == \"docker-remote\"");
            EventConsumer all    = subscribe(publisher, "catch-all", "true");

            publisher.publish(EventFactory.canonical()); // helm event

            assertEquals(1, helm.getDeliveredCount(),   "helm-team should match");
            assertEquals(0, docker.getDeliveredCount(), "docker-team should not match");
            assertEquals(1, all.getDeliveredCount(),    "catch-all should always match");
        }

        @Test @DisplayName("Unsubscribed consumer stops receiving events")
        void unsubscribedConsumerStopped() {
            EventConsumer consumer = subscribe(publisher, "c1", "true");
            publisher.publish(EventFactory.canonical());
            assertEquals(1, consumer.getDeliveredCount());

            publisher.unsubscribe("c1");
            publisher.publish(EventFactory.canonical());
            // Count should still be 1 — consumer received nothing after unsubscribe
            assertEquals(1, consumer.getDeliveredCount());
        }

        @Test @DisplayName("PublishReport contains correct counts")
        void publishReportCounts() {
            subscribe(publisher, "match1", "true");
            subscribe(publisher, "match2", "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\"");
            subscribe(publisher, "nomatch", "event[\"type\"] == \"AFTER_LOCAL_DOWNLOAD\"");

            PublishReport report = publisher.publish(EventFactory.canonical());

            assertEquals(2, report.getDeliveredCount(), "Two consumers should match");
            assertEquals(1, report.getFilteredCount(),  "One consumer should be filtered");
            assertEquals(0, report.getErrorCount());
        }

        @Test @DisplayName("Hot filter update is reflected in next publish")
        void hotFilterUpdateReflectedImmediately() {
            EventConsumer consumer = subscribe(publisher, "c1",
                "event[\"type\"] == \"AFTER_LOCAL_DOWNLOAD\""); // won't match

            publisher.publish(EventFactory.canonical());
            assertEquals(0, consumer.getDeliveredCount());

            publisher.updateFilter("c1", "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\""); // now will match
            publisher.publish(EventFactory.canonical());
            assertEquals(1, consumer.getDeliveredCount());
        }

        @Test @DisplayName("Variant events: docker-team only gets docker events")
        void dockerTeamVariantRouting() {
            EventConsumer dockerTeam = subscribe(publisher, "docker-team",
                "event[\"data.artifact.repoKey\"] == \"docker-remote\"");

            publisher.publish(EventFactory.canonical());    // helm event
            publisher.publish(EventFactory.dockerPull());   // docker event
            publisher.publish(EventFactory.localUpload());  // local event

            assertEquals(1, dockerTeam.getDeliveredCount(), "Only docker pull should match");
        }

        @Test @DisplayName("Before-event consumer only gets BEFORE_ events")
        void preFlightConsumerOnlyGetsBeforeEvents() {
            EventConsumer preFlightConsumer = subscribe(publisher, "pre-flight",
                "event[\"type\"].startsWith(\"BEFORE_\")");

            publisher.publish(EventFactory.canonical());       // AFTER_REMOTE_DOWNLOAD
            publisher.publish(EventFactory.beforeDownload());  // BEFORE_REMOTE_DOWNLOAD
            publisher.publish(EventFactory.localUpload());     // AFTER_CREATE

            assertEquals(1, preFlightConsumer.getDeliveredCount(), "Only BEFORE_ event should match");
        }
    }

    // =========================================================================
    // EventFlattener — unit tests
    // =========================================================================

    @Nested
    @DisplayName("EventFlattener")
    class FlattenerTests {

        @Test @DisplayName("Top-level fields are present with correct keys")
        void topLevelFieldsFlattened() {
            Map<String, String> flat = EventFlattener.flatten(EventFactory.canonical());
            assertEquals("AFTER_REMOTE_DOWNLOAD", flat.get("type"));
            assertEquals("1.0",                   flat.get("specVersion"));
            assertEquals("4f3dcdd6-202b-425e-968f-0f52e3b11822", flat.get("id"));
        }

        @Test @DisplayName("Nested artifact fields use dot notation")
        void nestedFieldsUseDotNotation() {
            Map<String, String> flat = EventFlattener.flatten(EventFactory.canonical());
            assertEquals("helm-remote", flat.get("data.artifact.repoKey"));
            assertEquals("REMOTE",      flat.get("data.artifact.repoType"));
            assertEquals("2.7.2-j/linux-amd64/helm", flat.get("data.artifact.path"));
        }

        @Test @DisplayName("Array properties are joined with comma")
        void arrayPropertiesJoined() {
            Map<String, String> flat = EventFlattener.flatten(EventFactory.canonical());
            assertEquals("bintray", flat.get("data.artifact.properties.migrated-from"));
        }

        @Test @DisplayName("Array elements also accessible by index")
        void arrayElementsByIndex() {
            Map<String, String> flat = EventFlattener.flatten(EventFactory.canonical());
            assertEquals("bintray", flat.get("data.artifact.properties.migrated-from[0]"));
        }

        @Test @DisplayName("Null fields map to empty string")
        void nullFieldMapsToEmpty() {
            Map<String, String> flat = EventFlattener.flatten(EventFactory.canonical());
            assertEquals("", flat.get("data.artifact.name"));
        }

        @Test @DisplayName("Context actor field correctly flattened")
        void contextActorFlattened() {
            Map<String, String> flat = EventFlattener.flatten(EventFactory.canonical());
            assertTrue(flat.get("data.context.actor").contains("admin"));
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private void assertDelivered(CelFilterRegistry reg, CloudEvent event, String expr) {
        reg.registerConsumer("__t__", expr);
        DeliveryResult result = reg.evaluateOne("__t__", EventFlattener.flatten(event));
        reg.deregisterConsumer("__t__");
        assertEquals(DeliveryResult.Status.DELIVERED, result.getStatus(),
            "Expected DELIVERED for: " + expr + " → " + result);
    }

    private void assertFiltered(CelFilterRegistry reg, CloudEvent event, String expr) {
        reg.registerConsumer("__t__", expr);
        DeliveryResult result = reg.evaluateOne("__t__", EventFlattener.flatten(event));
        reg.deregisterConsumer("__t__");
        assertEquals(DeliveryResult.Status.FILTERED, result.getStatus(),
            "Expected FILTERED for: " + expr + " → " + result);
    }

    private EventConsumer subscribe(EventPublisher pub, String id, String filter) {
        EventConsumer c = new EventConsumer(id, id, filter);
        pub.subscribe(c);
        return c;
    }
}
