package com.events.cel.consumer;

import com.events.cel.model.CloudEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a downstream event consumer.
 *
 * Each consumer has:
 *   - a unique ID
 *   - a human-readable description
 *   - a CEL filter expression (registered separately in {@link com.events.cel.engine.CelFilterRegistry})
 *   - an inbox of received events (in-memory for POC purposes)
 *   - delivery counters for observability
 *
 * In a real system, the {@link #onEvent} method would publish to a message queue,
 * call a webhook, write to a Kafka topic, etc.
 */
public class EventConsumer {

    private final String id;
    private final String description;
    private final String celFilter;

    // In-memory inbox — in production this would be a real delivery mechanism
    private final List<CloudEvent> receivedEvents = Collections.synchronizedList(new ArrayList<>());

    private final AtomicInteger deliveredCount = new AtomicInteger(0);
    private final AtomicInteger filteredCount  = new AtomicInteger(0);

    public EventConsumer(String id, String description, String celFilter) {
        this.id          = id;
        this.description = description;
        this.celFilter   = celFilter;
    }

    /**
     * Called by the publisher when an event passes this consumer's CEL filter.
     * In production: publish to Kafka / call webhook / write to DB / etc.
     *
     * @param event  The CloudEvent that was matched and should be delivered
     */
    public void onEvent(CloudEvent event) {
        receivedEvents.add(event);
        deliveredCount.incrementAndGet();
    }

    /** Called by the publisher when an event was filtered out. */
    public void onFiltered() {
        filteredCount.incrementAndGet();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Getters
    // ─────────────────────────────────────────────────────────────────────────

    public String           getId()             { return id; }
    public String           getDescription()    { return description; }
    public String           getCelFilter()      { return celFilter; }
    public List<CloudEvent> getReceivedEvents() { return Collections.unmodifiableList(receivedEvents); }
    public int              getDeliveredCount() { return deliveredCount.get(); }
    public int              getFilteredCount()  { return filteredCount.get(); }

    @Override
    public String toString() {
        return String.format("Consumer[%s] delivered=%d filtered=%d | filter: %s",
            id, deliveredCount.get(), filteredCount.get(), celFilter);
    }
}
