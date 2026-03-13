package com.events.cel.util;

import com.events.cel.model.CloudEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Builds CloudEvent instances for the POC.
 *
 * Provides the canonical Artifactory event from the spec,
 * plus a set of variant events for exercising different filter paths.
 */
public class EventFactory {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** ── The canonical Artifactory event as provided in the requirements ── */
    public static final String CANONICAL_JSON = """
        {
          "specVersion" : "1.0",
          "id" : "4f3dcdd6-202b-425e-968f-0f52e3b11822",
          "type" : "AFTER_REMOTE_DOWNLOAD",
          "source" : "artifactory://jfrt@01j3jdcfdb1trn0xttpnwy1gm0@287e7d62-4d98-43a2-842f-b99c8eab984a",
          "time" : "2026-03-10T09:20:30.126875Z",
          "subject" : "helm-remote/2.7.2-j/linux-amd64/helm",
          "dataSchema" : null,
          "dataContentType" : "application/json",
          "eventHeaders" : null,
          "data" : {
            "artifact" : {
              "name" : null,
              "repoKey" : "helm-remote",
              "repoType" : "REMOTE",
              "path" : "2.7.2-j/linux-amd64/helm",
              "created" : 1773134322979,
              "lastModified" : 1514469920000,
              "deployedBy" : null,
              "md5" : "02fcb0c62508a6c0708191076e3291f8",
              "sha1" : "43add4d8aae3457c076d066702891ff69aee7b2d",
              "sha256" : "5b9d19e8ce1840f5fe81765a633d52918ccc3ed8de5ac691b797c5b3cd663575",
              "properties" : {
                "artifactory.internal.etag" : [
                  "5b9d19e8ce1840f5fe81765a633d52918ccc3ed8de5ac691b797c5b3cd663575",
                  "43add4d8aae3457c076d066702891ff69aee7b2d"
                ],
                "migrated-from" : [ "bintray" ],
                "source-bintray-organization" : [ "jfrog" ],
                "source-bintray-repository" : [ "helm" ],
                "source-bintray-url" : [ "https://jfrog.bintray.com/helm" ],
                "jf.origin.remote.path" : [
                  "local:https://releases.jfrog.io/artifactory/helm/2.7.2-j/linux-amd64/helm"
                ]
              }
            },
            "context" : {
              "actor" : "jffe@01j3jdcjhhg8zv06s9mf5s16ns/users/admin"
            }
          }
        }
        """;

    /** Parse the canonical event JSON into a CloudEvent POJO. */
    public static CloudEvent canonical() {
        try {
            return MAPPER.readValue(CANONICAL_JSON, CloudEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse canonical event", e);
        }
    }

    /** Variant: LOCAL upload event (different type + repoType). */
    public static CloudEvent localUpload() {
        CloudEvent e = canonical();
        e.id                  = UUID.randomUUID().toString();
        e.type                = "AFTER_CREATE";
        e.data.artifact.repoType = "LOCAL";
        e.data.artifact.repoKey  = "libs-release-local";
        e.data.artifact.path     = "com/example/myapp/1.0.0/myapp-1.0.0.jar";
        return e;
    }

    /** Variant: Different package type (Docker image). */
    public static CloudEvent dockerPull() {
        CloudEvent e = canonical();
        e.id                  = UUID.randomUUID().toString();
        e.type                = "AFTER_REMOTE_DOWNLOAD";
        e.data.artifact.repoKey  = "docker-remote";
        e.data.artifact.repoType = "REMOTE";
        e.data.artifact.path     = "library/nginx/manifests/latest";
        e.source              = "artifactory://jfrt@prod@docker-registry";
        e.data.context.actor  = "ci-bot@service-accounts/users/ci-pipeline";
        return e;
    }

    /** Variant: Virtual repo resolution. */
    public static CloudEvent virtualRepoDownload() {
        CloudEvent e = canonical();
        e.id                  = UUID.randomUUID().toString();
        e.type                = "AFTER_REMOTE_DOWNLOAD";
        e.data.artifact.repoKey  = "virtual-helm-all";
        e.data.artifact.repoType = "VIRTUAL";
        e.data.artifact.path     = "charts/prometheus/1.0.0/prometheus-1.0.0.tgz";
        return e;
    }

    /** Variant: Admin-deployed artifact. */
    public static CloudEvent adminDeploy() {
        CloudEvent e = canonical();
        e.id                  = UUID.randomUUID().toString();
        e.type                = "AFTER_CREATE";
        e.data.artifact.repoType = "LOCAL";
        e.data.artifact.deployedBy = "admin";
        e.data.context.actor  = "admin@system/users/admin";
        return e;
    }

    /** Variant: Large artifact (sha256 heavy file). */
    public static CloudEvent largeArtifact() {
        CloudEvent e = canonical();
        e.id = UUID.randomUUID().toString();
        e.type = "AFTER_REMOTE_DOWNLOAD";
        e.subject = "helm-remote/5.0.0/linux-amd64/helm-large";
        e.data.artifact.path = "5.0.0/linux-amd64/helm-large";
        e.data.artifact.sha256 = "aaaa1234567890abcdef1234567890abcdef1234567890abcdef1234567890aa";
        return e;
    }

    /** Variant: Before (pre-flight) event type. */
    public static CloudEvent beforeDownload() {
        CloudEvent e = canonical();
        e.id   = UUID.randomUUID().toString();
        e.type = "BEFORE_REMOTE_DOWNLOAD";
        return e;
    }
}
