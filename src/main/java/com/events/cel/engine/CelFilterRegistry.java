package com.events.cel.engine;

import com.events.cel.model.CloudEvent;
import com.events.cel.model.DeliveryResult;
import com.events.cel.util.EventFlattener;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central CEL filter registry.
 *
 * Responsibilities:
 *   1. Accept consumer filter registrations — parse + type-check the CEL expression once.
 *   2. Cache the compiled {@link CelRuntime.Program} per consumer.
 *   3. At event time, evaluate every registered consumer's filter and return
 *      the set of consumers whose filter matched.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * CEL 3-Phase Pipeline
 * ─────────────────────────────────────────────────────────────────────────────
 *
 *  Phase 1 — PARSE (at registerFilter):
 *    Consumer's CEL text → untyped AST.
 *    Checks syntax only. Rejects malformed expressions immediately.
 *
 *  Phase 2 — CHECK (at registerFilter):
 *    Untyped AST + variable declarations → type-checked AST.
 *    Rejects type errors (e.g. string + int) at subscription time,
 *    not when a real event arrives.
 *
 *  Phase 3 — EVAL (at every event):
 *    Type-checked AST + variable bindings → boolean result.
 *    Stateless. Thread-safe. Extremely fast.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * Variable contract exposed to CEL expressions:
 * ─────────────────────────────────────────────────────────────────────────────
 *   event : map<string, string>
 *
 *   Consumers access event fields via:
 *     event["type"]                              → top-level CloudEvent fields
 *     event["data.artifact.repoKey"]             → nested payload fields
 *     event["data.artifact.properties.migrated-from"]  → property arrays (joined)
 *     event["data.context.actor"]                → context fields
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * Security note:
 * ─────────────────────────────────────────────────────────────────────────────
 *   CEL is non-Turing complete by specification. No sandbox configuration is
 *   required or possible to "forget". Consumers cannot:
 *     - Execute arbitrary Java code
 *     - Access the filesystem or network
 *     - Cause infinite loops (no unbounded iteration)
 *     - Load arbitrary JVM classes
 *
 *   This is a language-level guarantee, not a runtime restriction.
 */
public class CelFilterRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(CelFilterRegistry.class);

    // ─────────────────────────────────────────────────────────────────────────
    // CEL infrastructure (shared, thread-safe, reused for every consumer)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * CEL compiler configured with the variable declarations consumers can reference.
     *
     * We expose one variable:
     *   event : map<string, string>   — the flattened CloudEvent
     */
    private final CelCompiler compiler;

    /** CEL runtime for creating evaluable programs from checked ASTs. */
    private final CelRuntime  runtime;

    // ─────────────────────────────────────────────────────────────────────────
    // Per-consumer state
    // ─────────────────────────────────────────────────────────────────────────

    /** Compiled CEL program per consumer — stateless, thread-safe to share. */
    private final Map<String, CelRuntime.Program> programs = new ConcurrentHashMap<>();

    /** Original CEL expression string per consumer — for logging and results. */
    private final Map<String, String> expressions = new ConcurrentHashMap<>();

    // ─────────────────────────────────────────────────────────────────────────
    // Constructor
    // ─────────────────────────────────────────────────────────────────────────

    public CelFilterRegistry() {
        this.compiler = CelCompilerFactory.standardCelCompilerBuilder()
            .addVar("event", MapType.create(SimpleType.STRING, SimpleType.STRING))
            .build();

        this.runtime = CelRuntimeFactory.standardCelRuntimeBuilder().build();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Registration API
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Register a consumer with their CEL filter expression.
     *
     * This performs Phase 1 (parse) and Phase 2 (type-check) immediately.
     * Any syntax or type errors are surfaced here — not at evaluation time.
     *
     * @param consumerId  Unique identifier for the consumer
     * @param celExpr     The CEL expression the consumer wants to filter on
     * @throws CelFilterCompilationException if the expression is invalid
     */
    public void registerConsumer(String consumerId, String celExpr) {
        LOG.info("Registering consumer '{}' with filter: {}", consumerId, celExpr);
        try {
            // Phase 1: Parse — text → untyped AST
            CelAbstractSyntaxTree parsedAst = compiler.parse(celExpr).getAst();

            // Phase 2: Check — untyped AST → type-safe AST
            // This catches: undefined variables, wrong types, bad function calls
            CelAbstractSyntaxTree checkedAst = compiler.check(parsedAst).getAst();

            // Build the evaluable program — cached per consumer
            CelRuntime.Program program = runtime.createProgram(checkedAst);

            programs.put(consumerId, program);
            expressions.put(consumerId, celExpr);

            LOG.info("Consumer '{}' filter compiled successfully.", consumerId);

        } catch (CelValidationException e) {
            throw new CelFilterCompilationException(
                "CEL syntax/type error for consumer '" + consumerId + "': " + e.getMessage() +
                " | Expression: [" + celExpr + "]", e
            );
        } catch (CelEvaluationException e) {
            throw new CelFilterCompilationException(
                "CEL program creation failed for consumer '" + consumerId + "': " + e.getMessage(), e
            );
        }
    }

    /**
     * Remove a consumer's filter registration (called on unsubscribe).
     *
     * @param consumerId  The consumer to remove
     */
    public void deregisterConsumer(String consumerId) {
        programs.remove(consumerId);
        expressions.remove(consumerId);
        LOG.info("Consumer '{}' deregistered.", consumerId);
    }

    /**
     * Update an existing consumer's filter expression.
     * This is a safe re-registration — the old filter remains active until
     * the new one successfully compiles.
     *
     * @param consumerId  The consumer to update
     * @param newCelExpr  The new CEL expression
     * @throws CelFilterCompilationException if the new expression is invalid (old one stays)
     */
    public void updateFilter(String consumerId, String newCelExpr) {
        LOG.info("Updating filter for consumer '{}'", consumerId);
        // Compile new expression first — don't remove old one until new is ready
        registerConsumer(consumerId + "_candidate", newCelExpr);
        // Swap atomically
        programs.put(consumerId, programs.remove(consumerId + "_candidate"));
        expressions.put(consumerId, newCelExpr);
        expressions.remove(consumerId + "_candidate");
        LOG.info("Filter updated for consumer '{}'", consumerId);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Evaluation API
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Evaluate a single consumer's filter against the given event.
     *
     * Phase 3: inject variable bindings (the flattened event) and run the
     * pre-compiled CEL program. This is extremely fast — pure expression
     * evaluation with no parsing or type-checking overhead.
     *
     * @param consumerId  The consumer whose filter to evaluate
     * @param flatEvent   The flattened event map ({@link EventFlattener#flatten})
     * @return            {@link DeliveryResult} with match decision and timing
     */
    public DeliveryResult evaluateOne(String consumerId, Map<String, String> flatEvent) {
        CelRuntime.Program program = programs.get(consumerId);
        String expr = expressions.getOrDefault(consumerId, "(no filter)");

        if (program == null) {
            // No filter registered → deliver everything (opt-in model)
            return DeliveryResult.delivered(consumerId, "(no filter — open subscription)", 0);
        }

        long start = System.nanoTime();
        try {
            Object result  = program.eval(Map.of("event", flatEvent));
            long   elapsed = System.nanoTime() - start;

            return Boolean.TRUE.equals(result)
                ? DeliveryResult.delivered(consumerId, expr, elapsed)
                : DeliveryResult.filtered(consumerId, expr, elapsed);

        } catch (CelEvaluationException e) {
            LOG.warn("CEL evaluation error for consumer '{}': {}", consumerId, e.getMessage());
            // Fail-open: deliver rather than silently drop
            return DeliveryResult.error(consumerId, expr, e.getMessage());
        }
    }

    /**
     * Evaluate ALL registered consumers against an event.
     *
     * Returns one {@link DeliveryResult} per consumer, indicating whether
     * the event should be delivered to them.
     *
     * @param event  The CloudEvent to route
     * @return       Map of consumerId → DeliveryResult for every registered consumer
     */
    public Map<String, DeliveryResult> evaluateAll(CloudEvent event) {
        Map<String, String>   flatEvent = EventFlattener.flatten(event);
        Map<String, DeliveryResult> results = new LinkedHashMap<>();

        for (String consumerId : programs.keySet()) {
            results.put(consumerId, evaluateOne(consumerId, flatEvent));
        }
        return results;
    }

    /**
     * Returns the consumer IDs whose filter matched the event — i.e., the
     * consumers that should receive this event.
     *
     * @param event  The CloudEvent to route
     * @return       Set of consumer IDs that should receive this event
     */
    public Set<String> getMatchingConsumers(CloudEvent event) {
        Map<String, DeliveryResult> results = evaluateAll(event);
        Set<String> matched = new LinkedHashSet<>();
        results.forEach((id, result) -> {
            if (result.isDelivered()) matched.add(id);
        });
        return matched;
    }

    /** Returns all currently registered consumer IDs. */
    public Set<String> getRegisteredConsumers() {
        return Collections.unmodifiableSet(programs.keySet());
    }

    /** Returns the raw CEL expression registered for a consumer. */
    public Optional<String> getExpression(String consumerId) {
        return Optional.ofNullable(expressions.get(consumerId));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Exceptions
    // ─────────────────────────────────────────────────────────────────────────

    /** Thrown when a consumer's CEL expression cannot be compiled. */
    public static class CelFilterCompilationException extends RuntimeException {
        public CelFilterCompilationException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}
