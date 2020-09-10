package io.stargate.web.models;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Keyspace {
    @JsonProperty("name")
    String name;
    @JsonProperty("datacenters")
    List<Datacenter> datacenters;

    public List<Datacenter> getDatacenters() {
        return datacenters;
    }

    public void setDatacenters(List<Datacenter> datacenters) {
        this.datacenters = datacenters;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    public Keyspace setName(String name) {
        this.name = name;
        return this;
    }

    @JsonCreator
    public Keyspace(@JsonProperty("name") String name, @JsonProperty("datacenters") List<Datacenter> datacenters) {
        this.name = name;
        this.datacenters = datacenters;
    }
}
