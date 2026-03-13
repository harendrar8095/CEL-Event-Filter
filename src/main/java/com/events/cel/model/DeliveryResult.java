package com.events.cel.model;

/**
 * Outcome of evaluating one consumer's CEL filter against one event.
 *
 * Encapsulates:
 *   - whether the filter matched (→ event should be delivered)
 *   - the consumer that was evaluated
 *   - the CEL expression that was evaluated
 *   - evaluation time in nanoseconds
 *   - any error that occurred during evaluation
 */
public class DeliveryResult {

    public enum Status { DELIVERED, FILTERED, ERROR }

    private final String      consumerId;
    private final String      celExpression;
    private final Status      status;
    private final long        evaluationNanos;
    private final String      errorMessage;

    private DeliveryResult(String consumerId, String celExpression,
                           Status status, long evaluationNanos, String errorMessage) {
        this.consumerId      = consumerId;
        this.celExpression   = celExpression;
        this.status          = status;
        this.evaluationNanos = evaluationNanos;
        this.errorMessage    = errorMessage;
    }

    public static DeliveryResult delivered(String consumerId, String expr, long nanos) {
        return new DeliveryResult(consumerId, expr, Status.DELIVERED, nanos, null);
    }

    public static DeliveryResult filtered(String consumerId, String expr, long nanos) {
        return new DeliveryResult(consumerId, expr, Status.FILTERED, nanos, null);
    }

    public static DeliveryResult error(String consumerId, String expr, String errorMsg) {
        return new DeliveryResult(consumerId, expr, Status.ERROR, -1, errorMsg);
    }

    public String  getConsumerId()      { return consumerId; }
    public String  getCelExpression()   { return celExpression; }
    public Status  getStatus()          { return status; }
    public long    getEvaluationNanos() { return evaluationNanos; }
    public String  getErrorMessage()    { return errorMessage; }
    public boolean isDelivered()        { return status == Status.DELIVERED; }

    @Override
    public String toString() {
        return switch (status) {
            case DELIVERED -> String.format("  ✅ DELIVERED  → %s  [%.4f ms]  | filter: %s",
                consumerId, evaluationNanos / 1_000_000.0, celExpression);
            case FILTERED  -> String.format("  ⛔ FILTERED   → %s  [%.4f ms]  | filter: %s",
                consumerId, evaluationNanos / 1_000_000.0, celExpression);
            case ERROR     -> String.format("  ❌ ERROR      → %s  | %s | filter: %s",
                consumerId, errorMessage, celExpression);
        };
    }
}
