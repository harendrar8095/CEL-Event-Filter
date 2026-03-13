package com.events.cel.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Flattens a nested CloudEvent to a single-level {@code Map<String, String>}
 * using dot-notation keys — the format CEL expressions consume.
 *
 * Example input (nested):
 * <pre>
 * {
 *   "type": "AFTER_REMOTE_DOWNLOAD",
 *   "data": {
 *     "artifact": { "repoKey": "helm-remote", "repoType": "REMOTE" },
 *     "context":  { "actor": ".../users/admin" }
 *   }
 * }
 * </pre>
 *
 * Flattened output (CEL accesses via event["key"]):
 * <pre>
 *   event["type"]                    → "AFTER_REMOTE_DOWNLOAD"
 *   event["data.artifact.repoKey"]   → "helm-remote"
 *   event["data.artifact.repoType"]  → "REMOTE"
 *   event["data.context.actor"]      → ".../users/admin"
 * </pre>
 *
 * Array handling:
 *   event["data.artifact.properties.migrated-from"]      → "bintray"       (joined)
 *   event["data.artifact.properties.migrated-from[0]"]   → "bintray"       (by index)
 */
public class EventFlattener {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Flatten a CloudEvent POJO into a {@code Map<String, String>} for CEL evaluation.
     *
     * @param event Any POJO (typically a {@link com.events.cel.model.CloudEvent})
     * @return      Flat map with dot-notation keys and string values
     */
    public static Map<String, String> flatten(Object event) {
        try {
            JsonNode root   = MAPPER.valueToTree(event);
            Map<String, String> result = new HashMap<>();
            flattenNode(root, "", result);
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to flatten event to CEL map: " + e.getMessage(), e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Recursive walker
    // ─────────────────────────────────────────────────────────────────────────

    private static void flattenNode(JsonNode node, String prefix, Map<String, String> out) {
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            Iterator<Map.Entry<String, JsonNode>> fields = obj.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                flattenNode(entry.getValue(), key, out);
            }
        } else if (node.isArray()) {
            ArrayNode arr = (ArrayNode) node;
            // Joined value for CONTAINS checks  →  "bintray,jfrog"
            StringBuilder joined = new StringBuilder();
            for (int i = 0; i < arr.size(); i++) {
                if (i > 0) joined.append(",");
                String val = arr.get(i).asText();
                joined.append(val);
                out.put(prefix + "[" + i + "]", val);    // indexed access
            }
            out.put(prefix, joined.toString());
        } else if (!node.isNull()) {
            out.put(prefix, node.asText());
        } else {
            out.put(prefix, "");   // null → empty string (CEL handles "" safely)
        }
    }
}
