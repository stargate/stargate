package io.stargate.web.models;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Rows {
    int count;
    String pageState;
    List<Map<String, Object>> rows;

    public int getCount() {
        return count;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public String getPageState() {
        return pageState;
    }

    @JsonCreator
    public Rows(@JsonProperty("count") int count, @JsonProperty("pageState") String pageState, @JsonProperty("rows") List<Map<String, Object>> rows) {
        this.count = count;
        this.pageState = pageState;
        this.rows = rows;
    }
}
