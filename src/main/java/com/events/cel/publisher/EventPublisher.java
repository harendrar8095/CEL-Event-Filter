package com.events.cel.publisher;

import com.events.cel.consumer.EventConsumer;
import com.events.cel.engine.CelFilterRegistry;
import com.events.cel.model.CloudEvent;
import com.events.cel.model.DeliveryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Event publisher that routes CloudEvents to consumers using CEL filter evaluation.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * Workflow:
 * ─────────────────────────────────────────────────────────────────────────────
 *
 *  Subscription time (once per consumer):
 *    1. Consumer provides a CEL expression as their filter.
 *    2. Publisher calls {@link CelFilterRegistry#registerConsumer} which parses
 *       and type-checks the CEL expression. Any errors surface immediately.
 *
 *  Publish time (once per event, evaluated against N consumers):
 *    1. Publisher receives a CloudEvent.
 *    2. EventFlattener converts the nested JSON to Map<String, String>.
 *    3. For each consumer: CelFilterRegistry evaluates the pre-compiled CEL program.
 *    4. Matching consumers receive the event via {@link EventConsumer#onEvent}.
 *    5. Non-matching consumers are notified via {@link EventConsumer#onFiltered}.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * Key properties:
 * ─────────────────────────────────────────────────────────────────────────────
 *   - Compile-once per consumer: CEL AST is compiled at subscription time
 *   - Evaluate-many per event: O(consumers) with O(expression_size) per eval
 *   - Thread-safe: CelFilterRegistry uses ConcurrentHashMap internally
 *   - Fail-open: if CEL evaluation throws, the event IS delivered (over-deliver
 *     rather than silently drop — change to fail-closed if required)
 */
public class EventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(EventPublisher.class);

    private final CelFilterRegistry celRegistry = new CelFilterRegistry();
    private final Map<String, EventConsumer> consumers = new LinkedHashMap<>();

    // ─────────────────────────────────────────────────────────────────────────
    // Consumer lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Subscribe a consumer. Their CEL filter is compiled immediately.
     *
     * @param consumer  The consumer to subscribe
     * @throws CelFilterRegistry.CelFilterCompilationException if the CEL expression is invalid
     */
    public void subscribe(EventConsumer consumer) {
        celRegistry.registerConsumer(consumer.getId(), consumer.getCelFilter());
        consumers.put(consumer.getId(), consumer);
        LOG.info("Subscribed: {} | filter: {}", consumer.getId(), consumer.getCelFilter());
    }

    /**
     * Unsubscribe a consumer — they will no longer receive events.
     *
     * @param consumerId  The consumer to remove
     */
    public void unsubscribe(String consumerId) {
        celRegistry.deregisterConsumer(consumerId);
        consumers.remove(consumerId);
        LOG.info("Unsubscribed: {}", consumerId);
    }

    /**
     * Update a consumer's CEL filter expression at runtime without losing
     * their subscription. The old filter stays active until the new one
     * compiles successfully.
     *
     * @param consumerId  Consumer to update
     * @param newCelExpr  The new CEL filter expression
     */
    public void updateFilter(String consumerId, String newCelExpr) {
        celRegistry.updateFilter(consumerId, newCelExpr);
        LOG.info("Filter updated for: {}", consumerId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Event publishing
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Publish an event — evaluate it against all consumer CEL filters and
     * deliver to matching consumers.
     *
     * @param event  The CloudEvent to publish
     * @return       PublishReport containing per-consumer delivery decisions
     */
    public PublishReport publish(CloudEvent event) {
//        LOG.debug("Publishing event: {}", event);

        // Evaluate all consumers' CEL filters in one pass
        Map<String, DeliveryResult> results = celRegistry.evaluateAll(event);

        // Dispatch to matching consumers; notify filtered-out consumers
        results.forEach((consumerId, result) -> {
            EventConsumer consumer = consumers.get(consumerId);
            if (consumer == null) return; // consumer was removed mid-flight

            if (result.isDelivered()) {
                consumer.onEvent(event);
            } else {
                consumer.onFiltered();
            }
        });

        return new PublishReport(event, results);
    }

    /** Returns all currently subscribed consumers (unmodifiable). */
    public Collection<EventConsumer> getConsumers() {
        return Collections.unmodifiableCollection(consumers.values());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PublishReport — result of a single publish() call
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Summary of a single event publish — which consumers received the event
     * and which were filtered out, with timing per consumer.
     */
    public static class PublishReport {

        private final CloudEvent event;
        private final Map<String, DeliveryResult> results;

        PublishReport(CloudEvent event, Map<String, DeliveryResult> results) {
            this.event   = event;
            this.results = Collections.unmodifiableMap(results);
        }

        public CloudEvent                        getEvent()   { return event; }
        public Map<String, DeliveryResult>       getResults() { return results; }

        public long getDeliveredCount() {
            return results.values().stream().filter(DeliveryResult::isDelivered).count();
        }

        public long getFilteredCount() {
            return results.values().stream()
                .filter(r -> r.getStatus() == DeliveryResult.Status.FILTERED).count();
        }

        public long getErrorCount() {
            return results.values().stream()
                .filter(r -> r.getStatus() == DeliveryResult.Status.ERROR).count();
        }

        /** Print a full routing decision table to stdout. */
        public void print() {
            System.out.printf("%n  Event: %s | id=%s%n", event.type, event.id);
            results.values().forEach(r -> System.out.println(r.toString()));
            System.out.printf("  Summary: %d delivered, %d filtered, %d errors%n",
                getDeliveredCount(), getFilteredCount(), getErrorCount());
        }
    }
}
