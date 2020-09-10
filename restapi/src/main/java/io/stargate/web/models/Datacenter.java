package io.stargate.web.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Datacenter {
    String name;
    int replicas;

    public String getName() {
        return name;
    }

    public Datacenter setName(String name) {
        this.name = name;
        return this;
    }

    public int getReplicas() {
        return replicas;
    }

    public Datacenter setReplicas(int replicas) {
        this.replicas = replicas;
        return this;
    }

    @JsonCreator
    public Datacenter(@JsonProperty("name")String name, @JsonProperty("replicas") int replicas) {
        this.name = name;
        this.replicas = replicas;
    }
}
