package io.stargate.web.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetResponseWrapper<T> {
    @JsonProperty("count")
    int count;
    @JsonProperty("pageState")
    String pageState;
    @JsonProperty("data")
    T data;

    public int getCount() {
        return count;
    }

    public GetResponseWrapper setCount(int count) {
        this.count = count;
        return this;
    }

    public String getPageState() {
        return pageState;
    }

    public GetResponseWrapper setPageState(String pageState) {
        this.pageState = pageState;
        return this;
    }

    public T getData() {
        return data;
    }

    public GetResponseWrapper setData(T data) {
        this.data = data;
        return this;
    }

    @JsonCreator
    public GetResponseWrapper(@JsonProperty("count") final int count, @JsonProperty("pageState") final String pageState,
                              @JsonProperty("data") final T data) {
        this.count = count;
        this.pageState = pageState;
        this.data = data;
    }
}
