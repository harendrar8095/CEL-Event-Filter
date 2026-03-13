# CEL Event Delivery POC

POC for CEL-based event delivery filtering. Consumers register CEL expressions as filters; the publisher evaluates each event against each consumer's filter at publish time — only matching consumers receive the event.

- **Safety:** CEL is non-Turing complete by design — no sandbox config needed.
- **Performance:** Compile-once (at subscription), evaluate-many (per event).

---

## Run

```bash
mvn package
java -jar target/cel-event-delivery-poc-1.0.0.jar
```

## Tests

```bash
mvn test
```

---

## How the delivery pipeline works

```
Consumer subscribes
        │
        ▼
EventPublisher.subscribe(consumer)
        │
        ▼
CelFilterRegistry.registerConsumer(id, celExpr)
        ├── Phase 1: PARSE  →  text → untyped AST       (syntax check)
        └── Phase 2: CHECK  →  AST + type declarations → typed AST   (type-safety check)
                                      ↑
                              Any error surfaces HERE at subscription time
                              (NOT at event time, not at 3am)

Event arrives
        │
        ▼
EventPublisher.publish(event)
        │
        ▼
EventFlattener.flatten(event)   →   Map<String, String>
    {"type" → "AFTER_REMOTE_DOWNLOAD",
     "data.artifact.repoKey" → "helm-remote", ...}
        │
        ▼
CelFilterRegistry.evaluateAll(event)
        │  for each consumer:
        ├── Phase 3: EVAL  →  program.eval({"event": flatMap}) → boolean
        │                       (pre-compiled, stateless, thread-safe)
        │
        ├── matched  → consumer.onEvent(event)     ← real delivery
        └── filtered → consumer.onFiltered()       ← counter only
```

---

## Project structure

```
cel-event-poc/
├── pom.xml
└── src/
    ├── main/java/com/events/cel/
    │   ├── CelEventDeliveryRunner.java     ← main() — 6-step demo
    │   ├── engine/
    │   │   └── CelFilterRegistry.java      ← THE core: compile + evaluate
    │   ├── publisher/
    │   │   └── EventPublisher.java         ← subscribe, publish, unsubscribe
    │   ├── consumer/
    │   │   └── EventConsumer.java          ← downstream consumer with inbox
    │   ├── model/
    │   │   ├── CloudEvent.java             ← Artifactory CloudEvent schema
    │   │   └── DeliveryResult.java         ← DELIVERED | FILTERED | ERROR
    │   └── util/
    │       ├── EventFlattener.java         ← nested JSON → dot-notation map
    │       └── EventFactory.java           ← canonical + 5 variant events
    └── test/java/com/events/cel/
        └── CelEventDeliveryTest.java       ← 40+ unit + integration tests
```
