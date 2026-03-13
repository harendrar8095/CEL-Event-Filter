package com.events.cel;

import com.events.cel.consumer.EventConsumer;
import com.events.cel.engine.CelFilterRegistry;
import com.events.cel.model.CloudEvent;
import com.events.cel.publisher.EventPublisher;
import com.events.cel.publisher.EventPublisher.PublishReport;
import com.events.cel.util.EventFactory;

import java.util.List;

/**
 * End-to-end POC runner demonstrating CEL-based event delivery.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * Run with:
 *   mvn package
 *   java -jar target/cel-event-delivery-poc-1.0.0.jar
 *
 * What it demonstrates:
 *   1. Consumer subscription with diverse CEL filter expressions
 *   2. Publishing the canonical Artifactory event + variants
 *   3. Routing decisions — which consumers receive each event
 *   4. Hot filter update — consumer changes their filter at runtime
 *   5. Compile-time error detection — invalid CEL rejected at registration
 *   6. Throughput benchmark — compile-once, evaluate-many pattern
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * CEL EXPRESSION SYNTAX REFERENCE (for the Artifactory CloudEvent):
 *
 *   Field access via map key:
 *     event["type"]                                 → CloudEvent type
 *     event["source"]                               → Source URI
 *     event["data.artifact.repoKey"]                → Repository key
 *     event["data.artifact.repoType"]               → REMOTE | LOCAL | VIRTUAL
 *     event["data.artifact.path"]                   → Artifact path
 *     event["data.artifact.sha256"]                 → SHA-256 hash
 *     event["data.artifact.created"]                → Unix timestamp (ms) as string
 *     event["data.artifact.properties.migrated-from"] → joined array value
 *     event["data.context.actor"]                   → Actor who triggered event
 *
 *   Operators:
 *     ==  !=  >  <  >=  <=  &&  ||  !
 *     in  (list membership)
 *
 *   Built-in string functions:
 *     .contains("x")   .startsWith("x")   .endsWith("x")
 *     .matches("regex") .size()  .upper()  .lower()
 *
 *   Ternary:
 *     condition ? "a" : "b"
 *
 *   has() macro (field presence check):
 *     has(event.someField)
 */
public class CelEventDeliveryRunner {

    public static void main(String[] args) {
        printBanner();

        // ── Step 1: Create publisher ──────────────────────────────────────────
        EventPublisher publisher = new EventPublisher();

        // ── Step 2: Define and subscribe consumers with CEL filters ───────────
        List<EventConsumer> consumers = List.of(

            // ----------------------------------------------------------------
            // Consumer A: Security team — wants ALL remote downloads regardless
            //             of repo or type. Simplest possible filter.
            // ----------------------------------------------------------------
            new EventConsumer(
                "security-team",
                "Receives every AFTER_REMOTE_DOWNLOAD regardless of source",
                "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\""
            ),

            // ----------------------------------------------------------------
            // Consumer B: Helm chart team — only helm-remote repo downloads
            // ----------------------------------------------------------------
            new EventConsumer(
                "helm-team",
                "Helm chart downloads from helm-remote only",
                "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\" " +
                "&& event[\"data.artifact.repoKey\"] == \"helm-remote\""
            ),

            // ----------------------------------------------------------------
            // Consumer C: Compliance auditor — any REMOTE or VIRTUAL resolution,
            //             AND actor must be an admin user
            // ----------------------------------------------------------------
            new EventConsumer(
                "compliance-auditor",
                "Remote/Virtual events triggered by admin users",
                "event[\"data.artifact.repoType\"] in [\"REMOTE\", \"VIRTUAL\"] " +
                "&& event[\"data.context.actor\"].contains(\"admin\")"
            ),

            // ----------------------------------------------------------------
            // Consumer D: Bintray migration monitor — only artifacts that were
            //             migrated from bintray (property-based filter)
            // ----------------------------------------------------------------
            new EventConsumer(
                "bintray-migration-monitor",
                "Artifacts with migrated-from=bintray property",
                "event[\"data.artifact.properties.migrated-from\"].contains(\"bintray\")"
            ),

            // ----------------------------------------------------------------
            // Consumer E: Docker team — only docker-remote repository
            // ----------------------------------------------------------------
            new EventConsumer(
                "docker-team",
                "Docker image pulls only",
                "event[\"data.artifact.repoKey\"] == \"docker-remote\""
            ),

            // ----------------------------------------------------------------
            // Consumer F: CI pipeline — any event from non-admin actors
            //             for LOCAL repos (i.e. builds, not admin manual ops)
            // ----------------------------------------------------------------
            new EventConsumer(
                "ci-pipeline-monitor",
                "LOCAL repo events NOT triggered by admin",
                "event[\"data.artifact.repoType\"] == \"LOCAL\" " +
                "&& !event[\"data.context.actor\"].contains(\"admin\")"
            ),

            // ----------------------------------------------------------------
            // Consumer G: Path-based filter — anything under a linux-amd64 path
            // ----------------------------------------------------------------
            new EventConsumer(
                "linux-amd64-watcher",
                "Any artifact path containing linux-amd64",
                "event[\"data.artifact.path\"].contains(\"linux-amd64\")"
            ),

            // ----------------------------------------------------------------
            // Consumer H: Pre-flight events — BEFORE_ type prefix
            // ----------------------------------------------------------------
            new EventConsumer(
                "pre-flight-interceptor",
                "Before-events only (BEFORE_ prefix)",
                "event[\"type\"].startsWith(\"BEFORE_\")"
            ),

            // ----------------------------------------------------------------
            // Consumer I: Composite — OR across multiple event types with path match
            // ----------------------------------------------------------------
            new EventConsumer(
                "after-events-helm-or-docker",
                "AFTER events for helm OR docker repos",
                "event[\"type\"].startsWith(\"AFTER_\") " +
                "&& (event[\"data.artifact.repoKey\"].contains(\"helm\") " +
                "    || event[\"data.artifact.repoKey\"].contains(\"docker\"))"
            ),

            // ----------------------------------------------------------------
            // Consumer J: Open subscription — no filter → receives everything
            //             (simulates a catch-all audit consumer)
            // ----------------------------------------------------------------
            new EventConsumer(
                "catch-all-audit",
                "Catch-all: receives every event (no filter)",
                "true"   // CEL literal true = always match
            )
        );

        // Subscribe all consumers
        System.out.println("╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║  STEP 1 — Subscribing consumers (CEL compile phase)              ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════╝");
        consumers.forEach(c -> {
            publisher.subscribe(c);
            System.out.printf("  ✓ %-28s  filter: %s%n", c.getId(), c.getCelFilter());
        });

        // ── Step 3: Publish events and observe routing ────────────────────────
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║  STEP 2 — Publishing events (CEL evaluate phase)                 ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════╝");

        List<CloudEvent> events = List.of(
            EventFactory.canonical(),          // The reference event from spec
            EventFactory.dockerPull(),         // Docker image pull
            EventFactory.localUpload(),        // Local repo upload
            EventFactory.virtualRepoDownload(),// Virtual repo resolution
            EventFactory.adminDeploy(),        // Admin-deployed artifact
            EventFactory.beforeDownload()      // BEFORE_ pre-flight event
        );

        List<String> eventLabels = List.of(
            "Canonical Helm remote download",
            "Docker image pull (docker-remote)",
            "Local JAR upload (AFTER_CREATE)",
            "Virtual repo resolution (helm-virtual)",
            "Admin manual deploy (AFTER_CREATE/LOCAL)",
            "Before download preflight (BEFORE_REMOTE_DOWNLOAD)"
        );

        for (int i = 0; i < events.size(); i++) {
            System.out.printf("%n  ── Event %d: %s ──%n", i + 1, eventLabels.get(i));
            PublishReport report = publisher.publish(events.get(i));
            report.print();
        }

        // ── Step 4: Hot filter update ─────────────────────────────────────────
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║  STEP 3 — Hot filter update (runtime CEL recompilation)          ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════╝");

        System.out.println("  Updating helm-team filter to also include virtual repos...");
        publisher.updateFilter("helm-team",
            "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\" " +
            "&& (event[\"data.artifact.repoKey\"] == \"helm-remote\" " +
            "    || event[\"data.artifact.repoKey\"] == \"virtual-helm-all\")"
        );
        System.out.println("  Publishing virtual repo event again after filter update:");
        publisher.publish(EventFactory.virtualRepoDownload()).print();

        // ── Step 5: Compile-time error demonstration ──────────────────────────
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║  STEP 4 — CEL compile-time error detection                       ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════╝");
        demonstrateCompileErrors(publisher);

        // ── Step 6: Delivery summary per consumer ────────────────────────────
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║  STEP 5 — Consumer delivery summary                              ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════╝");
        System.out.printf("  %-30s  %9s  %9s%n", "Consumer", "Delivered", "Filtered");
        System.out.println("  " + "─".repeat(52));
        publisher.getConsumers().forEach(c ->
            System.out.printf("  %-30s  %9d  %9d%n",
                c.getId(), c.getDeliveredCount(), c.getFilteredCount())
        );

        // ── Step 7: Throughput benchmark ─────────────────────────────────────
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("║  STEP 6 — Throughput benchmark (compile-once / evaluate-many)    ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════╝");
        runBenchmark();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Compile-time error demonstration
    // ─────────────────────────────────────────────────────────────────────────

    private static void demonstrateCompileErrors(EventPublisher publisher) {
        record ErrorCase(String description, String expression) {}

        List<ErrorCase> cases = List.of(
            new ErrorCase("Type mismatch: string + int",
                "event[\"type\"] + 42"),
            new ErrorCase("Undefined variable (not in declarations)",
                "undefined_var == \"foo\""),
            new ErrorCase("Invalid function call",
                "event[\"type\"].nonExistentFunction()"),
            new ErrorCase("Syntax error: unclosed parenthesis",
                "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\" && (event[\"data.artifact.repoType\"] == \"REMOTE\"")
        );

        for (ErrorCase c : cases) {
            System.out.printf("  Testing: %s%n  Expression: %s%n", c.description(), c.expression());
            try {
                publisher.subscribe(new EventConsumer("error-test", c.description(), c.expression()));
                System.out.println("  ⚠️  Unexpectedly compiled (should have failed)");
                publisher.unsubscribe("error-test");
            } catch (CelFilterRegistry.CelFilterCompilationException e) {
                System.out.println("  ✅ Caught at compile time: " + extractRootCause(e));
            }
            System.out.println();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Throughput benchmark
    // ─────────────────────────────────────────────────────────────────────────

    private static void runBenchmark() {
        EventPublisher bench = new EventPublisher();

        // Register a realistic 3-condition AND filter
        bench.subscribe(new EventConsumer(
            "bench-consumer",
            "Benchmark consumer",
            "event[\"type\"] == \"AFTER_REMOTE_DOWNLOAD\" " +
            "&& event[\"data.artifact.repoType\"] == \"REMOTE\" " +
            "&& event[\"source\"].contains(\"jfrt\")"
        ));

        CloudEvent event = EventFactory.canonical();

        // Warm-up
        for (int i = 0; i < 5_000; i++) bench.publish(event);

        int iterations = 500_000;
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            bench.publish(event);
        }
        long elapsed = System.nanoTime() - start;

        System.out.printf(
            "  %,d events published in %.1f ms%n" +
            "  Average per event:  %.3f µs%n" +
            "  Throughput:         %.0f K events/sec%n",
            iterations,
            elapsed / 1_000_000.0,
            elapsed / 1000.0 / iterations,
            iterations / (elapsed / 1e9) / 1000.0
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static String extractRootCause(Exception e) {
        String msg = e.getMessage();
        // Trim to first line for readability
        int nl = msg.indexOf('\n');
        return nl > 0 ? msg.substring(0, nl) : msg;
    }

    private static void printBanner() {
        System.out.println("""
            ╔══════════════════════════════════════════════════════════════════════════╗
            ║         CEL Event Delivery POC — Artifactory CloudEvent Routing         ║
            ║                                                                          ║
            ║  Engine: Google Common Expression Language (CEL-Java 0.12.0)            ║
            ║  Safety: Non-Turing complete · No sandbox config needed                 ║
            ║  Model:  Compile-once at subscription · Evaluate-many at publish time   ║
            ╚══════════════════════════════════════════════════════════════════════════╝
            """);
    }
}
