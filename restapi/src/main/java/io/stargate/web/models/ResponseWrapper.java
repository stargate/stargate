package io.stargate.web.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResponseWrapper<T> {
    @JsonProperty("data")
    T data;

    public T getData() {
        return data;
    }

    public ResponseWrapper setData(T data) {
        this.data = data;
        return this;
    }

    @JsonCreator
    public ResponseWrapper(@JsonProperty("data") final T data) {
        this.data = data;
    }
}
