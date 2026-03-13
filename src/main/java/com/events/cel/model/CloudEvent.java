package com.events.cel.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * CloudEvent envelope matching the Artifactory event payload.
 *
 * Top-level fields follow the CloudEvents 1.0 spec:
 *   specVersion, id, type, source, time, subject, dataContentType, data
 *
 * The {@code data} field contains Artifactory-specific artifact metadata.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CloudEvent {

    @JsonProperty("specVersion")     public String specVersion;
    @JsonProperty("id")              public String id;
    @JsonProperty("type")            public String type;
    @JsonProperty("source")         public String source;
    @JsonProperty("time")            public String time;
    @JsonProperty("subject")         public String subject;
    @JsonProperty("dataSchema")      public String dataSchema;
    @JsonProperty("dataContentType") public String dataContentType;
    @JsonProperty("eventHeaders")    public Map<String, Object> eventHeaders;
    @JsonProperty("data")            public EventData data;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class EventData {
        @JsonProperty("artifact") public Artifact artifact;
        @JsonProperty("context")  public Context  context;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Artifact {
        @JsonProperty("name")         public String name;
        @JsonProperty("repoKey")      public String repoKey;
        @JsonProperty("repoType")     public String repoType;
        @JsonProperty("path")         public String path;
        @JsonProperty("created")      public Long   created;
        @JsonProperty("lastModified") public Long   lastModified;
        @JsonProperty("deployedBy")   public String deployedBy;
        @JsonProperty("md5")          public String md5;
        @JsonProperty("sha1")         public String sha1;
        @JsonProperty("sha256")       public String sha256;
        @JsonProperty("properties")   public Map<String, List<String>> properties;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Context {
        @JsonProperty("actor") public String actor;
    }

    @Override
    public String toString() {
        return String.format("CloudEvent{id='%s', type='%s', source='%s'}", id, type, source);
    }
}
